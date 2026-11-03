package cleanup

// apiclient.go adapts the generated lakeFS client (pkg/api/apigen) to the API interface
// of api.go. It is the only file in this package that knows about HTTP: status codes
// become sentinel errors (ErrNotFound, ErrGCRulesNotFound, ErrForbidden), pagination is
// turned into explicit cursors, and the generated response structs are narrowed to the
// repository/entry/commit shapes the phases use.
//
// Directives that hold for every method here:
//
//   - a 404 on an entry read is ErrNotFound, never a generic failure. The phases treat a
//     missing catalog file entry as "this table is not managed by the catalog" and skip
//     it (expire) or protect all of its candidates (sweep);
//   - every other non-2xx is an error that names the operation and the object, because
//     the report quotes it and an operator must be able to find the entry;
//   - listing passes the cursor the caller gave and returns the server's has-more flag,
//     so a caller controls the page size and the memory it uses (repositories can hold
//     hundreds of thousands of file entries);
//   - no call ever passes force=true, so a protected branch or a read-only repository
//     rejects the write exactly like a catalog write does
//     (pkg/icebergcatalog/store.go, ICEBERG_CATALOG_PLAN.md §6.2 "why stage instead
//     of auto-commit").

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

// Client adapts an apigen generated client to API.
type Client struct {
	client *apigen.ClientWithResponses

	// pageSize is the amount requested from every paginated call. Zero means
	// DefaultPageSize.
	pageSize int
}

// DefaultPageSize is the page size of every listing this client performs: 1000 is the
// server's maximum for ListObjects (catalog.ListEntriesLimitMax) and a comfortable page
// for branch, tag and commit listings, so a large repository needs one round trip per
// thousand entries rather than per entry.
const DefaultPageSize = 1000

// NewClient wraps a generated lakeFS client. A nil pageSize means DefaultPageSize.
func NewClient(client *apigen.ClientWithResponses, pageSize int) *Client {
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}
	return &Client{client: client, pageSize: pageSize}
}

// compile-time assertion that the adapter keeps satisfying the interface.
var _ API = (*Client)(nil)

// GetRepository implements API.
func (c *Client) GetRepository(ctx context.Context, repository string) (Repository, error) {
	resp, err := c.client.GetRepositoryWithResponse(ctx, repository)
	if err != nil {
		return Repository{}, fmt.Errorf("get repository %q: %w", repository, err)
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "get repository", repository); err != nil {
		return Repository{}, err
	}
	return Repository{
		Name:             resp.JSON200.Id,
		StorageNamespace: strings.TrimRight(resp.JSON200.StorageNamespace, "/"),
		DefaultBranch:    resp.JSON200.DefaultBranch,
	}, nil
}

// ListBranches implements API.
func (c *Client) ListBranches(ctx context.Context, repository string) ([]string, error) {
	var branches []string
	after := ""
	for {
		params := &apigen.ListBranchesParams{
			Amount: apiutil.Ptr(apigen.PaginationAmount(c.pageSize)),
		}
		if after != "" {
			params.After = apiutil.Ptr(apigen.PaginationAfter(after))
		}
		resp, err := c.client.ListBranchesWithResponse(ctx, repository, params)
		if err != nil {
			return nil, fmt.Errorf("list branches of %q: %w", repository, err)
		}
		if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "list branches", repository); err != nil {
			return nil, err
		}
		for _, branch := range resp.JSON200.Results {
			branches = append(branches, branch.Id)
		}
		if !resp.JSON200.Pagination.HasMore {
			return branches, nil
		}
		after = resp.JSON200.Pagination.NextOffset
		if after == "" {
			// A has-more response without a cursor would loop forever; refuse it rather
			// than spin.
			return nil, fmt.Errorf("%w: list branches of %q: more results without a next cursor", ErrBadPagination, repository)
		}
	}
}

// ListTags implements API.
func (c *Client) ListTags(ctx context.Context, repository string) ([]Tag, error) {
	var tags []Tag
	after := ""
	for {
		params := &apigen.ListTagsParams{
			Amount: apiutil.Ptr(apigen.PaginationAmount(c.pageSize)),
		}
		if after != "" {
			params.After = apiutil.Ptr(apigen.PaginationAfter(after))
		}
		resp, err := c.client.ListTagsWithResponse(ctx, repository, params)
		if err != nil {
			return nil, fmt.Errorf("list tags of %q: %w", repository, err)
		}
		if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "list tags", repository); err != nil {
			return nil, err
		}
		for _, tag := range resp.JSON200.Results {
			tags = append(tags, Tag{Name: tag.Id, CommitID: tag.CommitId})
		}
		if !resp.JSON200.Pagination.HasMore {
			return tags, nil
		}
		after = resp.JSON200.Pagination.NextOffset
		if after == "" {
			return nil, fmt.Errorf("%w: list tags of %q: more results without a next cursor", ErrBadPagination, repository)
		}
	}
}

// GetGCRules implements API.
func (c *Client) GetGCRules(ctx context.Context, repository string) (GCRules, error) {
	resp, err := c.client.GetGCRulesWithResponse(ctx, repository)
	if err != nil {
		return GCRules{}, fmt.Errorf("get GC rules of %q: %w", repository, err)
	}
	if resp.StatusCode() == http.StatusNotFound {
		return GCRules{}, ErrGCRulesNotFound
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "get GC rules", repository); err != nil {
		return GCRules{}, err
	}
	rules := GCRules{
		DefaultRetentionDays: resp.JSON200.DefaultRetentionDays,
		Branches:             make(map[string]int, len(resp.JSON200.Branches)),
	}
	for _, branch := range resp.JSON200.Branches {
		rules.Branches[branch.BranchId] = branch.RetentionDays
	}
	return rules, nil
}

// ListCommits implements API. It always asks for the first-parent chain: that is the
// edge pkg/graveler/retention follows when it decides which commits are active, and
// using a different edge here would protect a different set than the one lakeFS GC
// actually retains.
func (c *Client) ListCommits(ctx context.Context, repository, ref, after string) ([]CommitInfo, bool, error) {
	params := &apigen.LogCommitsParams{
		Amount:      apiutil.Ptr(apigen.PaginationAmount(c.pageSize)),
		FirstParent: apiutil.Ptr(true),
	}
	if after != "" {
		params.After = apiutil.Ptr(apigen.PaginationAfter(after))
	}
	resp, err := c.client.LogCommitsWithResponse(ctx, repository, ref, params)
	if err != nil {
		return nil, false, fmt.Errorf("list commits of %q at %q: %w", repository, ref, err)
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "list commits", ref); err != nil {
		return nil, false, err
	}
	commits := make([]CommitInfo, 0, len(resp.JSON200.Results))
	for _, commit := range resp.JSON200.Results {
		commits = append(commits, CommitInfo{
			ID:           commit.Id,
			CreationDate: time.Unix(commit.CreationDate, 0).UTC(),
			Parents:      commit.Parents,
		})
	}
	return commits, resp.JSON200.Pagination.HasMore, nil
}

// ListObjects implements API.
func (c *Client) ListObjects(ctx context.Context, repository, ref, prefix, after string) ([]ObjectInfo, bool, error) {
	params := &apigen.ListObjectsParams{
		Amount:       apiutil.Ptr(apigen.PaginationAmount(c.pageSize)),
		UserMetadata: apiutil.Ptr(true),
	}
	if prefix != "" {
		params.Prefix = apiutil.Ptr(apigen.PaginationPrefix(prefix))
	}
	if after != "" {
		params.After = apiutil.Ptr(apigen.PaginationAfter(after))
	}
	resp, err := c.client.ListObjectsWithResponse(ctx, repository, ref, params)
	if err != nil {
		return nil, false, fmt.Errorf("list objects of %q at %q under %q: %w", repository, ref, prefix, err)
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "list objects", ref); err != nil {
		return nil, false, err
	}
	objects := make([]ObjectInfo, 0, len(resp.JSON200.Results))
	for _, object := range resp.JSON200.Results {
		objects = append(objects, objectInfo(object))
	}
	return objects, resp.JSON200.Pagination.HasMore, nil
}

// StatObject implements API.
func (c *Client) StatObject(ctx context.Context, repository, ref, path string) (ObjectInfo, error) {
	resp, err := c.client.StatObjectWithResponse(ctx, repository, ref, &apigen.StatObjectParams{
		Path:         path,
		UserMetadata: apiutil.Ptr(true),
	})
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("stat %q at %q of %q: %w", path, ref, repository, err)
	}
	if resp.StatusCode() == http.StatusNotFound {
		return ObjectInfo{}, fmt.Errorf("%q at %q: %w", path, ref, ErrNotFound)
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "stat", path); err != nil {
		return ObjectInfo{}, err
	}
	return objectInfo(*resp.JSON200), nil
}

// GetObject implements API.
func (c *Client) GetObject(ctx context.Context, repository, ref, path string) ([]byte, error) {
	resp, err := c.client.GetObjectWithResponse(ctx, repository, ref, &apigen.GetObjectParams{Path: path})
	if err != nil {
		return nil, fmt.Errorf("get %q at %q of %q: %w", path, ref, repository, err)
	}
	if resp.StatusCode() == http.StatusNotFound {
		return nil, fmt.Errorf("%q at %q: %w", path, ref, ErrNotFound)
	}
	if err := checkStatus(resp.StatusCode(), len(resp.Body) > 0, resp.Body, "get object", path); err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// UploadObject implements API.
func (c *Client) UploadObject(ctx context.Context, repository, branch, path string, data []byte) (ObjectInfo, error) {
	resp, err := c.client.UploadObjectWithBodyWithResponse(ctx, repository, branch,
		&apigen.UploadObjectParams{Path: path}, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("upload %q to %q of %q: %w", path, branch, repository, err)
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON201 != nil, resp.Body, "upload object", path); err != nil {
		return ObjectInfo{}, err
	}
	return objectInfo(*resp.JSON201), nil
}

// StageObject implements API.
func (c *Client) StageObject(ctx context.Context, repository, branch, path string, object StagedObject) error {
	creation := apigen.ObjectStageCreation{
		PhysicalAddress: object.PhysicalAddress,
		Checksum:        object.Checksum,
		SizeBytes:       object.SizeBytes,
	}
	if !object.Mtime.IsZero() {
		creation.Mtime = apiutil.Ptr(object.Mtime.Unix())
	}
	if object.Metadata != nil {
		creation.Metadata = &apigen.ObjectUserMetadata{AdditionalProperties: object.Metadata}
	}
	resp, err := c.client.StageObjectWithResponse(ctx, repository, branch, &apigen.StageObjectParams{Path: path}, apigen.StageObjectJSONRequestBody(creation))
	if err != nil {
		return fmt.Errorf("stage %q on %q of %q: %w", path, branch, repository, err)
	}
	return checkStatus(resp.StatusCode(), resp.JSON201 != nil, resp.Body, "stage object", path)
}

// DeleteObject implements API.
func (c *Client) DeleteObject(ctx context.Context, repository, branch, path string) error {
	resp, err := c.client.DeleteObjectWithResponse(ctx, repository, branch, &apigen.DeleteObjectParams{Path: path})
	if err != nil {
		return fmt.Errorf("delete %q on %q of %q: %w", path, branch, repository, err)
	}
	return checkStatus(resp.StatusCode(), true, resp.Body, "delete object", path)
}

// DiffBranch implements API.
func (c *Client) DiffBranch(ctx context.Context, repository, branch, prefix, after string) ([]DiffInfo, bool, error) {
	params := &apigen.DiffBranchParams{
		Amount: apiutil.Ptr(apigen.PaginationAmount(c.pageSize)),
	}
	if prefix != "" {
		params.Prefix = apiutil.Ptr(apigen.PaginationPrefix(prefix))
	}
	if after != "" {
		params.After = apiutil.Ptr(apigen.PaginationAfter(after))
	}
	resp, err := c.client.DiffBranchWithResponse(ctx, repository, branch, params)
	if err != nil {
		return nil, false, fmt.Errorf("diff branch %q of %q: %w", branch, repository, err)
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "diff branch", branch); err != nil {
		return nil, false, err
	}
	changes := make([]DiffInfo, 0, len(resp.JSON200.Results))
	for _, change := range resp.JSON200.Results {
		changes = append(changes, DiffInfo{Path: change.Path, Type: change.Type})
	}
	return changes, resp.JSON200.Pagination.HasMore, nil
}

// objectInfo narrows one generated ObjectStats.
func objectInfo(object apigen.ObjectStats) ObjectInfo {
	info := ObjectInfo{
		Path:            object.Path,
		PhysicalAddress: object.PhysicalAddress,
		Checksum:        object.Checksum,
		Mtime:           time.Unix(object.Mtime, 0).UTC(),
	}
	if object.SizeBytes != nil {
		info.SizeBytes = *object.SizeBytes
	}
	if object.Metadata != nil && len(object.Metadata.AdditionalProperties) > 0 {
		info.Metadata = make(map[string]string, len(object.Metadata.AdditionalProperties))
		for key, value := range object.Metadata.AdditionalProperties {
			info.Metadata[key] = value
		}
	}
	return info
}

// checkStatus turns one response into an error. ok says whether the payload the caller
// needs is present; body is the response body, used only to quote a short server message
// in the error.
func checkStatus(code int, ok bool, body []byte, operation, subject string) error {
	switch {
	case code == http.StatusNotFound:
		return fmt.Errorf("%s %q: %w", operation, subject, ErrNotFound)
	case code == http.StatusForbidden, code == http.StatusUnauthorized:
		return fmt.Errorf("%s %q: %w: %s", operation, subject, ErrForbidden, errorMessage(body))
	case code >= 200 && code < 300 && ok:
		return nil
	default:
		return fmt.Errorf("%w: %s %q: status %d: %s", ErrUnexpectedResponse, operation, subject, code, errorMessage(body))
	}
}

// errorMessage returns a short, single-line message from a lakeFS error body. The body
// of a lakeFS error is a JSON object {"message": "..."}; anything unexpected is quoted
// verbatim but truncated, because an error string is a report line, not a document.
func errorMessage(body []byte) string {
	const maxLength = 512
	message := strings.TrimSpace(string(body))
	if len(message) > maxLength {
		message = message[:maxLength] + "..."
	}
	if message == "" {
		return "no response body"
	}
	return message
}

// isNotFound reports whether err is the API's absent-entry sentinel, so the phases do not
// have to import errors just for that check.
func isNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}
