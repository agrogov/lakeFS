// Package client is the lakeFS half of the Metadata Search indexer
// (METADATA_SEARCH_DESIGN.md §4.2–§4.9). The indexer is a REST client of lakeFS: it does not
// import pkg/catalog or pkg/graveler, so every operation it performs on a data repository or
// on the companion metadata repository goes through this package.
//
// It wraps the generated client (pkg/api/apigen) the way
// pkg/icebergcatalog/cleanup/apiclient.go does:
//
//   - generated response structs are narrowed to the handful of fields the indexer uses, so
//     callers never see an apigen type;
//   - a 404 is a *NotFoundError and errors.Is(err, ErrNotFound) is true, never a generic
//     failure: an absent branch, tag or commit is a normal answer of the recovery and seeding
//     paths (§4.4, §4.5), not an outage;
//   - listing is cursor-based. A call takes the server's "after" cursor and returns the items
//     and whether another page exists; the cursor of the next page is the name, commit ID or
//     path of the last returned item. The caller decides how many pages it walks, which is
//     what bounds max_commits (§4.2) and rebuild_threshold (§4.3).
//
// The interfaces below are the complete list of lakeFS calls the indexer makes, which makes
// the indexer principal's permissions (§8) checkable by reading one file. They are split by
// resource so tests can fake only the part a case needs.
package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

const (
	// DefaultPageSize is the page size of every listing when New is given a non-positive
	// page size: 1000 is the server's maximum for ListObjects
	// (catalog.ListEntriesLimitMax) and a comfortable page for branch and commit listings.
	DefaultPageSize = 1000

	// maxErrorMessage bounds the server message quoted in an error: an error string is a log
	// line, not a document.
	maxErrorMessage = 512

	// resetAll is the ResetCreation type that discards every uncommitted change of a branch
	// (cmd/lakectl/cmd/branch_reset.go). §4.4 uses it to throw away a partial Iceberg commit
	// before indexing normally.
	resetAll = "reset"

	// diffTypeTwoDot compares the two refs directly instead of finding a merge base. §4.3
	// applies the diff between a commit and its first parent, so three-dot would be wrong
	// after a merge.
	diffTypeTwoDot = "two_dot"
)

// Errors the client reports. A 404 is always a *NotFoundError that unwraps to ErrNotFound;
// the others are sentinels so a caller can react without parsing the server's message.
var (
	// ErrNotFound is unwrapped by every *NotFoundError.
	ErrNotFound = errors.New("lakeFS resource not found")

	// ErrForbidden is reported for HTTP 401 and 403: the indexer principal is missing a
	// permission of §8, and retrying will not help.
	ErrForbidden = errors.New("lakeFS operation forbidden")

	// ErrConflict is reported for HTTP 409: a concurrent writer, an existing branch or tag,
	// or a commit whose table moved (the indexer reloads and retries, §4.4).
	ErrConflict = errors.New("lakeFS conflict")

	// ErrBadRequest is reported for HTTP 400. For the indexer the interesting case is a
	// commit without changes when allow_empty is false (§4.3 step 4).
	ErrBadRequest = errors.New("lakeFS request rejected")

	// ErrBadPagination is reported when the server says another page exists but gives no
	// cursor to reach it. Refusing that response is what keeps a listing from looping forever.
	ErrBadPagination = errors.New("invalid pagination response")

	// ErrNoMergeBase is reported when FindMergeBase finds no common ancestor (§4.7). A
	// force-moved branch can have an unrelated history, and pkg/api answers that case with a
	// 500. The indexer needs the typed error to take the rewrite fallback instead of failing
	// the branch forever.
	ErrNoMergeBase = errors.New("no merge base")

	// ErrUnexpectedResponse is wrapped for a status or payload the client does not expect.
	ErrUnexpectedResponse = errors.New("unexpected lakeFS response")
)

// NotFoundError is the typed form of a 404. errors.Is(err, ErrNotFound) matches it, and
// errors.As(err, &notFound) gives the operation and the object that was missing.
type NotFoundError struct {
	// Operation is the call that answered 404: "get commit", "get tag", ...
	Operation string

	// Subject names what was missing, in the vocabulary of the operation: a repository, a
	// branch, a commit ID, a tag, an object path, or a composite like "repo@main..feature".
	Subject string
}

// Error implements error.
func (e *NotFoundError) Error() string {
	return fmt.Sprintf("%s %q: %v", e.Operation, e.Subject, ErrNotFound)
}

// Unwrap makes errors.Is(err, ErrNotFound) true for every *NotFoundError.
func (e *NotFoundError) Unwrap() error { return ErrNotFound }

// Repository is the part of a lakeFS repository the indexer needs (§3.3): where the object
// data lives, which branch is the default, and the storage ID a companion repository inherits.
type Repository struct {
	Name string

	// StorageNamespace is the repository's namespace with the trailing "/" removed, so that
	// "-metadata" can be appended to it (§3.3) without producing a double slash.
	StorageNamespace string

	DefaultBranch string
	StorageID     string
	ReadOnly      bool
}

// RepositoryOptions is the input of CreateRepository. The zero values leave the server
// defaults: no explicit storage ID, the server's default branch name, not read-only.
type RepositoryOptions struct {
	Name             string
	StorageNamespace string
	DefaultBranch    string
	StorageID        string
	ReadOnly         bool
}

// Branch is one branch of a repository. CommitID is the branch head, which §4.2 compares
// with the indexed position to decide whether there is work to do.
type Branch struct {
	Name     string
	CommitID string
}

// Tag is one tag of a repository: a name and the commit it points at. §4.4 uses
// "mds-<commit>" tags of the metadata repository as its crash-recovery marker.
type Tag struct {
	Name     string
	CommitID string
}

// Commit is one commit of a repository (§4.2, §4.3).
type Commit struct {
	ID      string
	Message string
	Parents []string

	Committer string

	// CreationDate is the commit time. It becomes the creation_date column of every row the
	// commit writes (§3.1).
	CreationDate time.Time

	// Metadata is the commit metadata. In the metadata repository it carries
	// lakefs.mds.source-commit, which is the indexing position (§3.3).
	Metadata map[string]string
}

// CommitOptions is the input of Commit.
type CommitOptions struct {
	Message string

	// Metadata is attached to the commit. §3.3 writes lakefs.mds.source-commit and
	// lakefs.mds.source-repository here.
	Metadata map[string]string

	// AllowEmpty lets a commit contain no changes. §4.3 step 4 passes false because an
	// indexed commit always stages a table property, so an empty commit is a bug.
	AllowEmpty bool
}

// LogCommitsOptions is the input of LogCommits. The cursor of a next page is the ID of the
// last returned commit.
type LogCommitsOptions struct {
	// FirstParent follows only the first parent at a merge commit. §4.2 walks the
	// first-parent chain, so the indexer passes true.
	FirstParent bool

	// StopAt stops the walk at this reference (a commit ID in §4.2). Empty means no stop.
	StopAt string

	// After is the pagination cursor. Empty asks for the first page.
	After string
}

// ObjectStat is one object of a ref (§4.3): the fields of a row of the table, minus the
// commit-derived ones.
type ObjectStat struct {
	Path      string
	SizeBytes int64

	// Mtime is the object's modification time. It is the zero time when the server reports
	// none, which is converted to NULL by the table writer.
	Mtime time.Time

	// ETag is lakeFS's checksum of the object content.
	ETag string

	// ContentType is the object's media type, empty when it has none.
	ContentType string

	// UserMetadata is the object's user metadata map, nil when it has none. Objects written
	// through the S3 gateway use X-Amz-Meta-<lowercased name> keys (§3.1).
	UserMetadata map[string]string
}

// ChangeType is the type of one entry of a two-dot diff. The values are the strings the API
// returns (pkg/api/transform.go); any other value is passed through unchanged so a caller
// can reject it rather than silently lose a change.
type ChangeType string

// Change types reported by DiffRefs.
const (
	ChangeAdded   ChangeType = "added"
	ChangeChanged ChangeType = "changed"
	ChangeRemoved ChangeType = "removed"
)

// Change is one entry of the diff between a commit and its first parent (§4.3 step 1).
type Change struct {
	Path string
	Type ChangeType

	// SizeBytes is the size the API reports for the entry. It is present for every object
	// entry, including a removed one.
	SizeBytes int64

	// Right holds the right-side object stats when DiffRefs was asked for them. It is nil
	// for a removed entry and for entries the server did not describe. Fields the server
	// omits stay zero, which is §4.3 step 1's signal to call StatObject instead.
	Right *ObjectStat
}

// RepositoryReader reads one repository.
type RepositoryReader interface {
	GetRepository(ctx context.Context, repository string) (Repository, error)
}

// RepositoryCreator creates a repository.
type RepositoryCreator interface {
	CreateRepository(ctx context.Context, opts RepositoryOptions) (Repository, error)
}

// Repositories reads and creates repositories.
type Repositories interface {
	RepositoryReader
	RepositoryCreator
}

// BranchLister lists the branches of a repository, one page at a time.
type BranchLister interface {
	ListBranches(ctx context.Context, repository, after string) (branches []Branch, hasMore bool, err error)
}

// BranchWriter changes branches of the metadata repository (§4.4, §4.7, §4.8).
type BranchWriter interface {
	CreateBranch(ctx context.Context, repository, branch, source string) error
	DeleteBranch(ctx context.Context, repository, branch string) error
	ResetBranch(ctx context.Context, repository, branch string) error
	HardResetBranch(ctx context.Context, repository, branch, ref string) error
}

// MergeBaseFinder reports the merge base of two references (§4.2 step 4, §4.7).
type MergeBaseFinder interface {
	FindMergeBase(ctx context.Context, repository, sourceRef, destinationRef string) (string, error)
}

// Branches lists and changes branches and finds merge bases.
type Branches interface {
	BranchLister
	BranchWriter
	MergeBaseFinder
}

// CommitReader reads commits and commit history.
type CommitReader interface {
	LogCommits(ctx context.Context, repository, ref string, opts LogCommitsOptions) (commits []Commit, hasMore bool, err error)
	GetCommit(ctx context.Context, repository, commitID string) (Commit, error)
}

// CommitWriter writes commits.
type CommitWriter interface {
	Commit(ctx context.Context, repository, branch string, opts CommitOptions) (Commit, error)
}

// Commits reads and writes commits.
type Commits interface {
	CommitReader
	CommitWriter
}

// Tags reads and writes tags. ListTags and DeleteTag are what §4.9's tag retention needs to
// retire "mds-*" tags; keeping them here keeps the indexer's lakeFS surface in one file.
type Tags interface {
	ListTags(ctx context.Context, repository, after string) (tags []Tag, hasMore bool, err error)
	CreateTag(ctx context.Context, repository, tag, ref string, force bool) (Tag, error)
	GetTag(ctx context.Context, repository, tag string) (Tag, error)
	DeleteTag(ctx context.Context, repository, tag string) error
}

// ObjectLister pages through objects and diffs.
type ObjectLister interface {
	ListObjects(ctx context.Context, repository, ref, after string) (objects []ObjectStat, hasMore bool, err error)
	DiffRefs(ctx context.Context, repository, leftRef, rightRef, after string) (changes []Change, hasMore bool, err error)

	// DiffBranch pages through the uncommitted changes of a branch. §4.4 recovery uses it to
	// tell an empty staging from a foreign staged write whose property happens to equal the
	// recorded position.
	DiffBranch(ctx context.Context, repository, branch, after string) (changes []Change, hasMore bool, err error)
}

// ObjectStatter reads one object.
type ObjectStatter interface {
	StatObject(ctx context.Context, repository, ref, path string) (ObjectStat, error)
}

// Objects lists objects and diffs and stats one object.
type Objects interface {
	ObjectLister
	ObjectStatter
}

// LakeFS is the complete lakeFS surface the indexer uses. M4's indexer takes this interface;
// a test can fake it, or fake only one of the small interfaces above.
type LakeFS interface {
	Repositories
	Branches
	Commits
	Tags
	Objects
}

// Client adapts an apigen generated client to LakeFS.
type Client struct {
	client *apigen.ClientWithResponses

	// pageSize is the amount requested from every paginated call. It is never zero.
	pageSize int
}

// New wraps a generated lakeFS client. A non-positive pageSize means DefaultPageSize.
func New(client *apigen.ClientWithResponses, pageSize int) *Client {
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}
	return &Client{client: client, pageSize: pageSize}
}

var _ LakeFS = (*Client)(nil)

// GetRepository implements RepositoryReader.
func (c *Client) GetRepository(ctx context.Context, repository string) (Repository, error) {
	resp, err := c.client.GetRepositoryWithResponse(ctx, repository)
	if err != nil {
		return Repository{}, fmt.Errorf("get repository %q: %w", repository, err)
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "get repository", repository); err != nil {
		return Repository{}, err
	}
	return repositoryFromAPI(*resp.JSON200), nil
}

// CreateRepository implements RepositoryCreator. §4.8 creates the metadata repository with
// the data repository's default branch, storage ID and, if configured, an explicit
// storage namespace.
func (c *Client) CreateRepository(ctx context.Context, opts RepositoryOptions) (Repository, error) {
	body := apigen.RepositoryCreation{
		Name:             opts.Name,
		StorageNamespace: opts.StorageNamespace,
	}
	if opts.DefaultBranch != "" {
		body.DefaultBranch = apiutil.Ptr(opts.DefaultBranch)
	}
	if opts.StorageID != "" {
		body.StorageId = apiutil.Ptr(opts.StorageID)
	}
	if opts.ReadOnly {
		body.ReadOnly = apiutil.Ptr(true)
	}
	resp, err := c.client.CreateRepositoryWithResponse(ctx, &apigen.CreateRepositoryParams{}, apigen.CreateRepositoryJSONRequestBody(body))
	if err != nil {
		return Repository{}, fmt.Errorf("create repository %q: %w", opts.Name, err)
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON201 != nil, resp.Body, "create repository", opts.Name); err != nil {
		return Repository{}, err
	}
	return repositoryFromAPI(*resp.JSON201), nil
}

// ListBranches implements BranchLister. The cursor of the next page is the Name of the last
// returned branch.
func (c *Client) ListBranches(ctx context.Context, repository, after string) ([]Branch, bool, error) {
	params := &apigen.ListBranchesParams{
		Amount: apiutil.Ptr(apigen.PaginationAmount(c.pageSize)),
	}
	if after != "" {
		params.After = apiutil.Ptr(apigen.PaginationAfter(after))
	}
	resp, err := c.client.ListBranchesWithResponse(ctx, repository, params)
	if err != nil {
		return nil, false, fmt.Errorf("list branches of %q: %w", repository, err)
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "list branches", repository); err != nil {
		return nil, false, err
	}
	branches := make([]Branch, 0, len(resp.JSON200.Results))
	for _, branch := range resp.JSON200.Results {
		branches = append(branches, Branch{Name: branch.Id, CommitID: branch.CommitId})
	}
	hasMore, err := morePage(resp.JSON200.Pagination, "list branches", repository)
	if err != nil {
		return nil, false, err
	}
	return branches, hasMore, nil
}

// CreateBranch implements BranchWriter. §4.5 creates a metadata branch from a tag.
func (c *Client) CreateBranch(ctx context.Context, repository, branch, source string) error {
	body := apigen.BranchCreation{Name: branch, Source: source}
	resp, err := c.client.CreateBranchWithResponse(ctx, repository, apigen.CreateBranchJSONRequestBody(body))
	if err != nil {
		return fmt.Errorf("create branch %q of %q: %w", branch, repository, err)
	}
	return checkStatus(resp.StatusCode(), true, resp.Body, "create branch", branch)
}

// DeleteBranch implements BranchWriter. §4.8 deletes the metadata branch of a deleted data
// branch; its tags stay, so commit and tag reads of the old history still work.
func (c *Client) DeleteBranch(ctx context.Context, repository, branch string) error {
	resp, err := c.client.DeleteBranchWithResponse(ctx, repository, branch, &apigen.DeleteBranchParams{})
	if err != nil {
		return fmt.Errorf("delete branch %q of %q: %w", branch, repository, err)
	}
	return checkStatus(resp.StatusCode(), true, resp.Body, "delete branch", branch)
}

// ResetBranch implements BranchWriter: it discards every uncommitted change of a branch and
// is a no-op on a clean branch. §4.4 recovery uses it when a crashed Iceberg commit left
// staging behind that does not describe the next commit.
func (c *Client) ResetBranch(ctx context.Context, repository, branch string) error {
	body := apigen.ResetCreation{Type: resetAll}
	resp, err := c.client.ResetBranchWithResponse(ctx, repository, branch, apigen.ResetBranchJSONRequestBody(body))
	if err != nil {
		return fmt.Errorf("reset branch %q of %q: %w", branch, repository, err)
	}
	return checkStatus(resp.StatusCode(), true, resp.Body, "reset branch", branch)
}

// HardResetBranch implements BranchWriter: it moves a branch to ref, dropping the commits
// after it. §4.7 uses it to move a metadata branch to the tag of a merge base.
func (c *Client) HardResetBranch(ctx context.Context, repository, branch, ref string) error {
	params := &apigen.HardResetBranchParams{Ref: ref}
	resp, err := c.client.HardResetBranchWithResponse(ctx, repository, branch, params)
	if err != nil {
		return fmt.Errorf("hard reset branch %q of %q to %q: %w", branch, repository, ref, err)
	}
	return checkStatus(resp.StatusCode(), true, resp.Body, "hard reset branch", branch)
}

// FindMergeBase implements MergeBaseFinder and returns the base commit ID. §4.2 step 4 tests
// whether the indexed position is still an ancestor of the head: findMergeBase(last, head)
// answers last exactly when it is.
//
// A force-moved branch can have no common ancestor with its old position. graveler reports that
// as ErrNoMergeBase, but pkg/api does not map it and answers 500 with "no merge base" in the
// body. §4.7 still has a fallback for that state, so the body is recognised here and reported as
// the typed ErrNoMergeBase rather than a generic server failure.
func (c *Client) FindMergeBase(ctx context.Context, repository, sourceRef, destinationRef string) (string, error) {
	resp, err := c.client.FindMergeBaseWithResponse(ctx, repository, sourceRef, destinationRef)
	if err != nil {
		return "", fmt.Errorf("find merge base of %q and %q in %q: %w", sourceRef, destinationRef, repository, err)
	}
	subject := repository + "@" + sourceRef + ".." + destinationRef
	if resp.StatusCode() >= http.StatusInternalServerError && isNoMergeBase(resp.Body) {
		return "", fmt.Errorf("find merge base %q: %w: %s", subject, ErrNoMergeBase, errorMessage(resp.Body))
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "find merge base", subject); err != nil {
		return "", err
	}
	return resp.JSON200.BaseCommitId, nil
}

// LogCommits implements CommitReader. §4.2 step 5 asks for at most max_commits first-parent
// commits after the indexed position, then reverses the pages. The cursor of the next page
// is the ID of the last returned commit.
func (c *Client) LogCommits(ctx context.Context, repository, ref string, opts LogCommitsOptions) ([]Commit, bool, error) {
	params := &apigen.LogCommitsParams{
		Amount:      apiutil.Ptr(apigen.PaginationAmount(c.pageSize)),
		FirstParent: apiutil.Ptr(opts.FirstParent),
	}
	if opts.StopAt != "" {
		params.StopAt = apiutil.Ptr(opts.StopAt)
	}
	if opts.After != "" {
		params.After = apiutil.Ptr(apigen.PaginationAfter(opts.After))
	}
	resp, err := c.client.LogCommitsWithResponse(ctx, repository, ref, params)
	if err != nil {
		return nil, false, fmt.Errorf("log commits of %q at %q: %w", repository, ref, err)
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "log commits", ref); err != nil {
		return nil, false, err
	}
	commits := make([]Commit, 0, len(resp.JSON200.Results))
	for _, commit := range resp.JSON200.Results {
		commits = append(commits, commitFromAPI(commit))
	}
	hasMore, err := morePage(resp.JSON200.Pagination, "log commits", ref)
	if err != nil {
		return nil, false, err
	}
	return commits, hasMore, nil
}

// GetCommit implements CommitReader. The reference may be a commit ID, a branch, a tag or a
// ref expression: §4.4 reads the head commit of a metadata branch this way.
func (c *Client) GetCommit(ctx context.Context, repository, commitID string) (Commit, error) {
	resp, err := c.client.GetCommitWithResponse(ctx, repository, commitID)
	if err != nil {
		return Commit{}, fmt.Errorf("get commit %q of %q: %w", commitID, repository, err)
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "get commit", commitID); err != nil {
		return Commit{}, err
	}
	return commitFromAPI(*resp.JSON200), nil
}

// Commit implements CommitWriter. §4.3 step 4 commits the staged metadata table and the
// index position together; §4.8 writes the initialisation commit the same way.
func (c *Client) Commit(ctx context.Context, repository, branch string, opts CommitOptions) (Commit, error) {
	body := apigen.CommitCreation{
		Message:    opts.Message,
		AllowEmpty: apiutil.Ptr(opts.AllowEmpty),
	}
	if opts.Metadata != nil {
		body.Metadata = &apigen.CommitCreation_Metadata{AdditionalProperties: opts.Metadata}
	}
	resp, err := c.client.CommitWithResponse(ctx, repository, branch, &apigen.CommitParams{}, apigen.CommitJSONRequestBody(body))
	if err != nil {
		return Commit{}, fmt.Errorf("commit %q of %q: %w", branch, repository, err)
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON201 != nil, resp.Body, "commit", branch); err != nil {
		return Commit{}, err
	}
	return commitFromAPI(*resp.JSON201), nil
}

// ListTags implements Tags. §4.9 pages the "mds-*" tags to apply tag_retention. The cursor
// of the next page is the Name of the last returned tag.
func (c *Client) ListTags(ctx context.Context, repository, after string) ([]Tag, bool, error) {
	params := &apigen.ListTagsParams{
		Amount: apiutil.Ptr(apigen.PaginationAmount(c.pageSize)),
	}
	if after != "" {
		params.After = apiutil.Ptr(apigen.PaginationAfter(after))
	}
	resp, err := c.client.ListTagsWithResponse(ctx, repository, params)
	if err != nil {
		return nil, false, fmt.Errorf("list tags of %q: %w", repository, err)
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "list tags", repository); err != nil {
		return nil, false, err
	}
	tags := make([]Tag, 0, len(resp.JSON200.Results))
	for _, tag := range resp.JSON200.Results {
		tags = append(tags, Tag{Name: tag.Id, CommitID: tag.CommitId})
	}
	hasMore, err := morePage(resp.JSON200.Pagination, "list tags", repository)
	if err != nil {
		return nil, false, err
	}
	return tags, hasMore, nil
}

// CreateTag implements Tags. force only lets the tag be created in a read-only repository: lakeFS
// never overwrites an existing tag (pkg/graveler/graveler.go CreateTag returns
// ErrTagAlreadyExists, a 409), so a caller that moves a tag deletes it first (§4.4 step 5).
func (c *Client) CreateTag(ctx context.Context, repository, tag, ref string, force bool) (Tag, error) {
	body := apigen.TagCreation{
		Id:    tag,
		Ref:   ref,
		Force: apiutil.Ptr(force),
	}
	resp, err := c.client.CreateTagWithResponse(ctx, repository, apigen.CreateTagJSONRequestBody(body))
	if err != nil {
		return Tag{}, fmt.Errorf("create tag %q of %q: %w", tag, repository, err)
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON201 != nil, resp.Body, "create tag", tag); err != nil {
		return Tag{}, err
	}
	return Tag{Name: resp.JSON201.Id, CommitID: resp.JSON201.CommitId}, nil
}

// GetTag implements Tags. §4.5 uses a 404 (errors.Is(err, ErrNotFound)) as "this commit is
// not indexed", which is a normal answer, not a failure.
func (c *Client) GetTag(ctx context.Context, repository, tag string) (Tag, error) {
	resp, err := c.client.GetTagWithResponse(ctx, repository, tag)
	if err != nil {
		return Tag{}, fmt.Errorf("get tag %q of %q: %w", tag, repository, err)
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "get tag", tag); err != nil {
		return Tag{}, err
	}
	return Tag{Name: resp.JSON200.Id, CommitID: resp.JSON200.CommitId}, nil
}

// DeleteTag implements Tags. §4.9 retires an "mds-*" tag whose source commit is older than
// tag_retention, which is what lets lakeFS GC reclaim its data.
func (c *Client) DeleteTag(ctx context.Context, repository, tag string) error {
	resp, err := c.client.DeleteTagWithResponse(ctx, repository, tag, &apigen.DeleteTagParams{})
	if err != nil {
		return fmt.Errorf("delete tag %q of %q: %w", tag, repository, err)
	}
	return checkStatus(resp.StatusCode(), true, resp.Body, "delete tag", tag)
}

// DiffRefs implements ObjectLister. It always asks for a two-dot diff with right-side stats
// and no delimiter (§4.3 step 1). The cursor of the next page is the Path of the last
// returned change.
func (c *Client) DiffRefs(ctx context.Context, repository, leftRef, rightRef, after string) ([]Change, bool, error) {
	params := &apigen.DiffRefsParams{
		Amount:            apiutil.Ptr(apigen.PaginationAmount(c.pageSize)),
		Type:              apiutil.Ptr(diffTypeTwoDot),
		IncludeRightStats: apiutil.Ptr(true),
	}
	if after != "" {
		params.After = apiutil.Ptr(apigen.PaginationAfter(after))
	}
	resp, err := c.client.DiffRefsWithResponse(ctx, repository, leftRef, rightRef, params)
	if err != nil {
		return nil, false, fmt.Errorf("diff %q..%q of %q: %w", leftRef, rightRef, repository, err)
	}
	subject := repository + "@" + leftRef + ".." + rightRef
	if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "diff refs", subject); err != nil {
		return nil, false, err
	}
	changes := make([]Change, 0, len(resp.JSON200.Results))
	for _, diff := range resp.JSON200.Results {
		changes = append(changes, changeFromAPI(diff))
	}
	hasMore, err := morePage(resp.JSON200.Pagination, "diff refs", subject)
	if err != nil {
		return nil, false, err
	}
	return changes, hasMore, nil
}

// DiffBranch implements ObjectLister: it pages through the uncommitted changes of a branch
// (GET /repositories/{repository}/branches/{branch}/diff). §4.4 uses the first page as the
// authoritative answer to "is anything staged on R-metadata@b?", which the table's
// lakefs.mds.source-commit property alone cannot answer. The cursor of the next page is the
// Path of the last returned change.
func (c *Client) DiffBranch(ctx context.Context, repository, branch, after string) ([]Change, bool, error) {
	params := &apigen.DiffBranchParams{
		Amount: apiutil.Ptr(apigen.PaginationAmount(c.pageSize)),
	}
	if after != "" {
		params.After = apiutil.Ptr(apigen.PaginationAfter(after))
	}
	resp, err := c.client.DiffBranchWithResponse(ctx, repository, branch, params)
	if err != nil {
		return nil, false, fmt.Errorf("diff branch %q of %q: %w", branch, repository, err)
	}
	subject := repository + "@" + branch
	if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "diff branch", subject); err != nil {
		return nil, false, err
	}
	changes := make([]Change, 0, len(resp.JSON200.Results))
	for _, diff := range resp.JSON200.Results {
		changes = append(changes, changeFromAPI(diff))
	}
	hasMore, err := morePage(resp.JSON200.Pagination, "diff branch", subject)
	if err != nil {
		return nil, false, err
	}
	return changes, hasMore, nil
}

// ListObjects implements ObjectLister. It asks for user metadata because every row of §3.1
// carries it. The cursor of the next page is the Path of the last returned object.
func (c *Client) ListObjects(ctx context.Context, repository, ref, after string) ([]ObjectStat, bool, error) {
	params := &apigen.ListObjectsParams{
		Amount:       apiutil.Ptr(apigen.PaginationAmount(c.pageSize)),
		UserMetadata: apiutil.Ptr(true),
	}
	if after != "" {
		params.After = apiutil.Ptr(apigen.PaginationAfter(after))
	}
	resp, err := c.client.ListObjectsWithResponse(ctx, repository, ref, params)
	if err != nil {
		return nil, false, fmt.Errorf("list objects of %q at %q: %w", repository, ref, err)
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "list objects", ref); err != nil {
		return nil, false, err
	}
	objects := make([]ObjectStat, 0, len(resp.JSON200.Results))
	for _, object := range resp.JSON200.Results {
		objects = append(objects, objectStatFromAPI(object))
	}
	hasMore, err := morePage(resp.JSON200.Pagination, "list objects", ref)
	if err != nil {
		return nil, false, err
	}
	return objects, hasMore, nil
}

// StatObject implements ObjectStatter. §4.3 step 1 falls back to it when a right-side diff
// stat is missing a field.
func (c *Client) StatObject(ctx context.Context, repository, ref, path string) (ObjectStat, error) {
	params := &apigen.StatObjectParams{
		Path:         path,
		UserMetadata: apiutil.Ptr(true),
	}
	resp, err := c.client.StatObjectWithResponse(ctx, repository, ref, params)
	if err != nil {
		return ObjectStat{}, fmt.Errorf("stat %q at %q of %q: %w", path, ref, repository, err)
	}
	if err := checkStatus(resp.StatusCode(), resp.JSON200 != nil, resp.Body, "stat object", path); err != nil {
		return ObjectStat{}, err
	}
	return objectStatFromAPI(*resp.JSON200), nil
}

// repositoryFromAPI narrows one generated repository. The trailing "/" is removed so that
// §3.3 can append "-metadata" to the namespace.
func repositoryFromAPI(repository apigen.Repository) Repository {
	return Repository{
		Name:             repository.Id,
		StorageNamespace: strings.TrimRight(repository.StorageNamespace, "/"),
		DefaultBranch:    repository.DefaultBranch,
		StorageID:        apiutil.Value(repository.StorageId),
		ReadOnly:         apiutil.Value(repository.ReadOnly),
	}
}

// commitFromAPI narrows one generated commit.
func commitFromAPI(commit apigen.Commit) Commit {
	return Commit{
		ID:           commit.Id,
		Message:      commit.Message,
		Parents:      append([]string(nil), commit.Parents...),
		Committer:    commit.Committer,
		CreationDate: timeFromUnix(commit.CreationDate),
		Metadata:     commitMetadata(commit.Metadata),
	}
}

// commitMetadata copies a generated commit metadata map, returning nil when there is none.
func commitMetadata(metadata *apigen.Commit_Metadata) map[string]string {
	if metadata == nil || len(metadata.AdditionalProperties) == 0 {
		return nil
	}
	out := make(map[string]string, len(metadata.AdditionalProperties))
	for key, value := range metadata.AdditionalProperties {
		out[key] = value
	}
	return out
}

// objectStatFromAPI narrows one generated object stat.
func objectStatFromAPI(object apigen.ObjectStats) ObjectStat {
	return ObjectStat{
		Path:         object.Path,
		SizeBytes:    apiutil.Value(object.SizeBytes),
		Mtime:        timeFromUnix(object.Mtime),
		ETag:         object.Checksum,
		ContentType:  apiutil.Value(object.ContentType),
		UserMetadata: userMetadata(object.Metadata),
	}
}

// changeFromAPI narrows one generated diff entry. The right-side stat is optional: it is
// absent for a removed entry, and §4.3 step 1 treats a missing field as "call StatObject".
func changeFromAPI(diff apigen.Diff) Change {
	change := Change{
		Path:      diff.Path,
		Type:      ChangeType(diff.Type),
		SizeBytes: apiutil.Value(diff.SizeBytes),
	}
	if diff.Right != nil {
		change.Right = &ObjectStat{
			Path:         diff.Path,
			SizeBytes:    change.SizeBytes,
			Mtime:        timeFromUnix(diff.Right.Mtime),
			ETag:         diff.Right.Checksum,
			ContentType:  diff.Right.ContentType,
			UserMetadata: userMetadata(diff.Right.Metadata),
		}
	}
	return change
}

// userMetadata copies a generated user metadata map, returning nil when there is none.
func userMetadata(metadata *apigen.ObjectUserMetadata) map[string]string {
	if metadata == nil || len(metadata.AdditionalProperties) == 0 {
		return nil
	}
	out := make(map[string]string, len(metadata.AdditionalProperties))
	for key, value := range metadata.AdditionalProperties {
		out[key] = value
	}
	return out
}

// timeFromUnix converts seconds since the epoch, treating a zero or negative value as "no
// time": lakeFS reports a missing modification time as 0, and time.Unix would otherwise turn
// a zero Go time into a large negative instant.
func timeFromUnix(seconds int64) time.Time {
	if seconds <= 0 {
		return time.Time{}
	}
	return time.Unix(seconds, 0).UTC()
}

// morePage validates the has-more flag of a listing. A server that says "more" without a
// cursor would make a caller loop forever, so that response is an error rather than a page.
func morePage(pagination apigen.Pagination, operation, subject string) (bool, error) {
	if !pagination.HasMore {
		return false, nil
	}
	if pagination.NextOffset == "" {
		return false, fmt.Errorf("%w: %s %q: more results without a next cursor", ErrBadPagination, operation, subject)
	}
	return true, nil
}

// isNoMergeBase reports whether an error body is graveler's ErrNoMergeBase. pkg/api leaves that
// error unmapped, so it answers 500 with the message instead of a typed status (§4.7).
func isNoMergeBase(body []byte) bool {
	return strings.Contains(strings.ToLower(string(body)), "no merge base")
}

// checkStatus turns one generated response into an error. ok says whether the payload the
// caller needs is present; body is the response body, used only to quote a short server
// message in the error.
func checkStatus(code int, ok bool, body []byte, operation, subject string) error {
	switch {
	case code == http.StatusNotFound:
		return &NotFoundError{Operation: operation, Subject: subject}
	case code == http.StatusForbidden, code == http.StatusUnauthorized:
		return fmt.Errorf("%s %q: %w: %s", operation, subject, ErrForbidden, errorMessage(body))
	case code == http.StatusConflict:
		return fmt.Errorf("%s %q: %w: %s", operation, subject, ErrConflict, errorMessage(body))
	case code == http.StatusBadRequest:
		return fmt.Errorf("%s %q: %w: %s", operation, subject, ErrBadRequest, errorMessage(body))
	case code >= 200 && code < 300 && ok:
		return nil
	default:
		return fmt.Errorf("%w: %s %q: status %d: %s", ErrUnexpectedResponse, operation, subject, code, errorMessage(body))
	}
}

// errorMessage returns a short, single-line message from a lakeFS error body, truncated
// because an error string is a log line, not a document.
func errorMessage(body []byte) string {
	message := strings.TrimSpace(string(body))
	if len(message) > maxErrorMessage {
		message = message[:maxErrorMessage] + "..."
	}
	if message == "" {
		return "no response body"
	}
	return message
}
