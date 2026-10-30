package datasets

// proposals_http_test.go covers the HTTP contract of the dataset proposals of §5.6 end to
// end: the generated request validator, api.GenericAuthMiddleware, the generated server and
// the handlers, mounted exactly as RoutesBuilder mounts them. It pins the status codes of
// §10.4 on this path - 201/200, 400 for a malformed request, 403 naming the item, 404 for an
// unknown dataset or proposal, and the 409 that carries latest_version for a stale base.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
	"github.com/treeverse/lakefs/pkg/graveler"
)

// proposalCreateBody renders a POST /{dataset}/proposals body.
func proposalCreateBody(t *testing.T, title string, def Definition, description string) string {
	t.Helper()
	body := apigen.ProposalCreateRequest{Title: title, Definition: definitionToAPI(def)}
	if description != "" {
		body.Description = &description
	}
	return marshalBody(t, body)
}

// proposalUpdateBody renders a PATCH /{dataset}/proposals/{id} body.
func proposalUpdateBody(t *testing.T, body apigen.ProposalUpdateRequest) string {
	t.Helper()
	return marshalBody(t, body)
}

// createHTTPProposal opens a proposal over HTTP and returns the record.
func createHTTPProposal(t *testing.T, handler http.Handler, accessKey, secret, dataset, title string, def Definition) apigen.Proposal {
	t.Helper()
	rec := doRequest(t, handler, http.MethodPost, "/"+dataset+"/proposals",
		proposalCreateBody(t, title, def, ""), accessKey, secret)
	require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())
	var proposal apigen.Proposal
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &proposal))
	return proposal
}

// readDatasetObjectHTTP reads a dataset entry the way the S3 gateway does, for the HTTP tests
// that assert a reviewer can browse the preview branch (the fixture helpers of helpers_test.go
// belong to the service fixtures, not to the control-plane deps).
func readDatasetObjectHTTP(t *testing.T, deps cmdpkg.ServerDeps, dataset, ref, path string) []byte {
	t.Helper()
	ctx := t.Context()
	entry, err := deps.Catalog.GetEntry(ctx, dataset, ref, path, catalog.GetEntryParams{})
	require.NoError(t, err, "get entry %s/%s/%s", dataset, ref, path)
	repo, err := deps.Catalog.GetRepository(ctx, dataset)
	require.NoError(t, err)
	reader, err := deps.Catalog.BlockAdapter.Get(ctx, block.ObjectPointer{
		StorageID:        repo.StorageID,
		StorageNamespace: repo.StorageNamespace,
		Identifier:       entry.PhysicalAddress,
		IdentifierType:   entry.AddressType.ToIdentifierType(),
	})
	require.NoError(t, err, "read %s/%s", dataset, path)
	defer func() { require.NoError(t, reader.Close()) }()
	data, err := io.ReadAll(reader)
	require.NoError(t, err, "read %s/%s", dataset, path)
	return data
}

// seedHTTPObjects writes real object bytes into a source repository and commits them, so a
// materialised preview entry points at data that can actually be read back through the block
// adapter (seedHandlerSource only stages entries with synthetic addresses).
func seedHTTPObjects(t *testing.T, deps cmdpkg.ServerDeps, repo, branch string, objects ...testObject) graveler.CommitID {
	t.Helper()
	ctx := t.Context()
	rec, err := deps.Catalog.GetRepository(ctx, repo)
	require.NoError(t, err, "get repository %s", repo)
	for i, obj := range objects {
		address := fmt.Sprintf("objects/http/%s/%d/%s", repo, i, obj.key)
		_, err := deps.Catalog.BlockAdapter.Put(ctx, block.ObjectPointer{
			StorageID:        rec.StorageID,
			StorageNamespace: rec.StorageNamespace,
			Identifier:       address,
			IdentifierType:   block.IdentifierTypeRelative,
		}, int64(len(obj.content)), bytes.NewReader(obj.content), block.PutOpts{})
		require.NoError(t, err, "put %s/%s", repo, obj.key)
		require.NoError(t, deps.Catalog.CreateEntry(ctx, repo, branch, catalog.DBEntry{
			Path:            obj.key,
			PhysicalAddress: address,
			AddressType:     catalog.AddressTypeRelative,
			CreationDate:    time.Now(),
			Size:            int64(len(obj.content)),
			Checksum:        etagOf(obj.content),
			ContentType:     "application/octet-stream",
		}), "create entry %s/%s", repo, obj.key)
	}
	log, err := deps.Catalog.Commit(ctx, repo, branch, "seed "+repo, "tester", nil, nil, nil, false)
	require.NoError(t, err, "commit %s/%s", repo, branch)
	return graveler.CommitID(log.Reference)
}

// TestDatasetsHTTPProposalLifecycle is the create -> list -> get -> patch -> merge -> close
// flow of §10.1 over the real router.
func TestDatasetsHTTPProposalLifecycle(t *testing.T) {
	handler, deps, accessKey, secret := newControlPlaneWithDeps(t)
	commit := seedHandlerSource(t, deps.Catalog)
	def := objectDefinition("handler-src", commit.String(), "reference/countries.csv", "geo/countries.csv")

	created := doRequest(t, handler, http.MethodPost, "", createDatasetBody(t, "http-proposals", def, true), accessKey, secret)
	require.Equal(t, http.StatusCreated, created.Code, "body: %s", created.Body.String())

	// A second source commit with real bytes, so the proposal's definition differs from v1
	// and its preview can be read back.
	second := seedHTTPObjects(t, deps, "handler-src", "main",
		testObject{key: "geo/cities.csv", content: []byte("cities-9\n")})
	proposalDef := objectDefinition("handler-src", second.String(), "reference/cities.csv", "geo/cities.csv")

	proposal := createHTTPProposal(t, handler, accessKey, secret, "http-proposals", "add cities", proposalDef)
	require.NotEmpty(t, proposal.Id)
	require.Equal(t, "http-proposals", proposal.Dataset)
	require.Equal(t, 1, proposal.BaseVersion)
	require.Equal(t, ProposalStatusOpen, proposal.Status)
	require.Equal(t, testUsername, proposal.Author)
	require.Equal(t, "add cities", proposal.Title)
	require.Equal(t, ProposalPreviewBranch(proposal.Id), proposal.PreviewBranch)
	require.NotNil(t, proposal.Definition)
	require.Equal(t, second.String(), proposal.Definition.Data[0].Ref.Id, "the response carries the resolved definition")
	require.Equal(t, "commit", proposal.Definition.Data[0].Ref.Type)
	require.NotNil(t, proposal.Diff, "§10.1: the response carries the definition diff")
	require.Len(t, proposal.Diff.Items, 2, "the proposal adds cities and drops countries")
	require.Equal(t, []string{"reference/cities.csv", "reference/countries.csv"},
		[]string{proposal.Diff.Items[0].Target, proposal.Diff.Items[1].Target})
	require.Equal(t, string(ChangeAdded), proposal.Diff.Items[0].Change)
	require.Equal(t, string(ChangeRemoved), proposal.Diff.Items[1].Change)
	require.NotNil(t, proposal.DataDiff, "§10.1: the response carries the data diff")
	require.Contains(t, dataPaths(proposal.DataDiff), "reference/cities.csv")

	// A reviewer can browse the materialised preview through the data plane (§5.6, §7.1).
	require.Equal(t, []byte("cities-9\n"), readDatasetObjectHTTP(t, deps, "http-proposals", proposal.PreviewBranch, "reference/cities.csv"))

	t.Run("list and filters", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/http-proposals/proposals", "", accessKey, secret)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var list apigen.ProposalList
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
		require.Len(t, list.Results, 1)
		require.Equal(t, proposal.Id, list.Results[0].Id)
		require.Equal(t, 1, list.Pagination.Results)

		rec = doRequest(t, handler, http.MethodGet, "/http-proposals/proposals?status=merged", "", accessKey, secret)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
		require.Empty(t, list.Results)

		// The status enum is validated before the handler runs (§10.4 400).
		rec = doRequest(t, handler, http.MethodGet, "/http-proposals/proposals?status=bogus", "", accessKey, secret)
		require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("get", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/http-proposals/proposals/"+proposal.Id, "", accessKey, secret)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var got apigen.Proposal
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		require.Equal(t, proposal.Id, got.Id)
		require.NotNil(t, got.Diff)
		require.NotNil(t, got.DataDiff)
	})

	t.Run("patch title and description", func(t *testing.T) {
		title, description := "add cities (v2)", "reviewed by the platform team"
		rec := doRequest(t, handler, http.MethodPatch, "/http-proposals/proposals/"+proposal.Id,
			proposalUpdateBody(t, apigen.ProposalUpdateRequest{Title: &title, Description: &description}), accessKey, secret)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var updated apigen.Proposal
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
		require.Equal(t, title, updated.Title)
		require.NotNil(t, updated.Description)
		require.Equal(t, description, *updated.Description)
		require.Equal(t, 1, updated.BaseVersion, "no version was published, so the base is unchanged")
	})

	t.Run("merge", func(t *testing.T) {
		message := "merge the cities proposal"
		rec := doRequest(t, handler, http.MethodPost, "/http-proposals/proposals/"+proposal.Id+"/merge",
			marshalBody(t, apigen.ProposalMergeRequest{Message: &message}), accessKey, secret)
		require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())
		var version apigen.Version
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &version))
		require.Equal(t, 2, version.Version)
		require.Equal(t, testUsername, version.Publisher)
		require.NotNil(t, version.ProposalId)
		require.Equal(t, proposal.Id, *version.ProposalId)
		require.NotNil(t, version.Message)
		require.Equal(t, message, *version.Message)

		// The dataset moved to v2 and the data plane serves it (§7.1).
		datasetRec := doRequest(t, handler, http.MethodGet, "/http-proposals", "", accessKey, secret)
		require.Equal(t, http.StatusOK, datasetRec.Code)
		var dataset apigen.Dataset
		require.NoError(t, json.Unmarshal(datasetRec.Body.Bytes(), &dataset))
		require.Equal(t, 2, dataset.LatestVersion)
		require.Equal(t, []byte("cities-9\n"), readDatasetObjectHTTP(t, deps, "http-proposals", "v2", "reference/cities.csv"))

		// §5.6: the proposal is merged, carries the version, and its preview branch is gone.
		proposalRec := doRequest(t, handler, http.MethodGet, "/http-proposals/proposals/"+proposal.Id, "", accessKey, secret)
		require.Equal(t, http.StatusOK, proposalRec.Code, "body: %s", proposalRec.Body.String())
		var merged apigen.Proposal
		require.NoError(t, json.Unmarshal(proposalRec.Body.Bytes(), &merged))
		require.Equal(t, ProposalStatusMerged, merged.Status)
		require.NotNil(t, merged.MergedVersion)
		require.Equal(t, 2, *merged.MergedVersion)
		require.NotNil(t, merged.ClosedAt)

		_, err := deps.Catalog.GetBranchReference(t.Context(), "http-proposals", proposal.PreviewBranch)
		require.Error(t, err, "the preview branch is deleted by the merge")
		require.True(t, isNotFound(err))
	})

	t.Run("a merged proposal cannot be merged or updated again", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodPost, "/http-proposals/proposals/"+proposal.Id+"/merge", "", accessKey, secret)
		require.Equal(t, http.StatusConflict, rec.Code, "body: %s", rec.Body.String())

		title := "too late"
		rec = doRequest(t, handler, http.MethodPatch, "/http-proposals/proposals/"+proposal.Id,
			proposalUpdateBody(t, apigen.ProposalUpdateRequest{Title: &title}), accessKey, secret)
		require.Equal(t, http.StatusConflict, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("unknown proposal is 404", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/http-proposals/proposals/01J8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8", "", accessKey, secret)
		require.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())
		require.Contains(t, decodeError(t, rec).Message, "proposal")

		rec = doRequest(t, handler, http.MethodPost, "/http-proposals/proposals/01J8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8/merge", "", accessKey, secret)
		require.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())
	})

	t.Run("unknown dataset is 404", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodGet, "/no-such-dataset/proposals", "", accessKey, secret)
		require.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())
	})
}

// TestDatasetsHTTPProposalRebaseConflict is the stale-base answer of §5.6 at the API boundary:
// 409, the message says "rebase required", and the body carries latest_version (§10.4).
func TestDatasetsHTTPProposalRebaseConflict(t *testing.T) {
	handler, deps, accessKey, secret := newControlPlaneWithDeps(t)
	commit := seedHandlerSource(t, deps.Catalog)
	def := objectDefinition("handler-src", commit.String(), "countries.csv", "geo/countries.csv")

	rec := doRequest(t, handler, http.MethodPost, "", createDatasetBody(t, "http-rebase", def, true), accessKey, secret)
	require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

	second := seedHTTPObjects(t, deps, "handler-src", "main",
		testObject{key: "geo/cities.csv", content: []byte("cities-9\n")})
	proposal := createHTTPProposal(t, handler, accessKey, secret, "http-rebase", "cities",
		objectDefinition("handler-src", second.String(), "cities.csv", "geo/cities.csv"))
	require.Equal(t, 1, proposal.BaseVersion)

	// A direct publish moves latest, so the proposal's base is stale.
	rec = doRequest(t, handler, http.MethodPost, "/http-rebase/versions",
		publishBody(objectDefinition("handler-src", second.String(), "other.csv", "geo/cities.csv"), "direct v2", nil),
		accessKey, secret)
	require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

	rec = doRequest(t, handler, http.MethodPost, "/http-rebase/proposals/"+proposal.Id+"/merge", "", accessKey, secret)
	require.Equal(t, http.StatusConflict, rec.Code, "body: %s", rec.Body.String())
	var conflict versionConflictBody
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &conflict))
	require.Equal(t, 2, conflict.LatestVersion, "the 409 body carries latest_version (§5.6)")
	require.Contains(t, conflict.Message, "rebase required")

	// The proposal is untouched: still open, base still 1, preview branch still there.
	got := doRequest(t, handler, http.MethodGet, "/http-rebase/proposals/"+proposal.Id, "", accessKey, secret)
	require.Equal(t, http.StatusOK, got.Code, "body: %s", got.Body.String())
	var proposalAgain apigen.Proposal
	require.NoError(t, json.Unmarshal(got.Body.Bytes(), &proposalAgain))
	require.Equal(t, ProposalStatusOpen, proposalAgain.Status)
	require.Equal(t, 1, proposalAgain.BaseVersion)
	_, err := deps.Catalog.GetBranchReference(t.Context(), "http-rebase", proposal.PreviewBranch)
	require.NoError(t, err, "the preview branch survives a refused merge")
}

// TestDatasetsHTTPProposalSourceDenied pins the §9.3 403 on both halves of the proposal path:
// the author at create (which attaches the data) and the merger at merge (who publishes it
// under their own authority when they are not the author). The message names the item.
func TestDatasetsHTTPProposalSourceDenied(t *testing.T) {
	handler, deps, accessKey, secret := newControlPlaneWithDeps(t)
	commit := seedHandlerSource(t, deps.Catalog)
	def := objectDefinition("handler-src", commit.String(), "countries.csv", "geo/countries.csv")

	rec := doRequest(t, handler, http.MethodPost, "", createDatasetBody(t, "http-authz", def, true), accessKey, secret)
	require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())

	// DatasetsFullAccess only: every datasets action, none of the ACL base grants, so the
	// principal cannot read the source repository.
	const weak = "weak-proposal-user"
	_, err := deps.AuthService.CreateUser(t.Context(), &model.User{Username: weak})
	require.NoError(t, err)
	weakCred, err := deps.AuthService.AddCredentials(t.Context(), weak, "AKIAWEAKPROPOSAL0001", "weak-secret")
	require.NoError(t, err)
	require.NoError(t, deps.AuthService.AddUserToGroup(t.Context(), weak, DatasetAdminsGroup))

	rec = doRequest(t, handler, http.MethodPost, "/http-authz/proposals",
		proposalCreateBody(t, "denied", def, ""), weakCred.AccessKeyID, weakCred.SecretAccessKey)
	require.Equal(t, http.StatusForbidden, rec.Code, "body: %s", rec.Body.String())
	message := decodeError(t, rec).Message
	require.Contains(t, message, "data[0]", "the 403 names the item (§9.3)")
	require.Contains(t, message, "handler-src")

	// The admin opens a proposal with a different definition; the weak principal is not the
	// author, so merging it would publish the data under their authority and the same check
	// refuses them.
	proposal := createHTTPProposal(t, handler, accessKey, secret, "http-authz", "allowed",
		objectDefinition("handler-src", commit.String(), "reference/countries-2.csv", "geo/countries.csv"))
	rec = doRequest(t, handler, http.MethodPost, "/http-authz/proposals/"+proposal.Id+"/merge", "",
		weakCred.AccessKeyID, weakCred.SecretAccessKey)
	require.Equal(t, http.StatusForbidden, rec.Code, "body: %s", rec.Body.String())
	require.Contains(t, decodeError(t, rec).Message, "handler-src")

	// The proposal is still open, so it was not merged by the refusal.
	got := doRequest(t, handler, http.MethodGet, "/http-authz/proposals/"+proposal.Id, "", accessKey, secret)
	require.Equal(t, http.StatusOK, got.Code)
	var proposalAgain apigen.Proposal
	require.NoError(t, json.Unmarshal(got.Body.Bytes(), &proposalAgain))
	require.Equal(t, ProposalStatusOpen, proposalAgain.Status)

	// With source read (the ACL base groups) the same principal may merge it (§9.3).
	require.NoError(t, deps.AuthService.AddUserToGroup(t.Context(), weak, acl.AdminsGroup))
	rec = doRequest(t, handler, http.MethodPost, "/http-authz/proposals/"+proposal.Id+"/merge", "",
		weakCred.AccessKeyID, weakCred.SecretAccessKey)
	require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())
	var version apigen.Version
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &version))
	require.Equal(t, 2, version.Version)
	require.Equal(t, weak, version.Publisher, "the merger publishes under their own authority")
}

// TestDatasetsHTTPProposalBadRequest pins the 400s of §10.4 on the proposal path, including the
// request-validation answers that come before any handler runs.
func TestDatasetsHTTPProposalBadRequest(t *testing.T) {
	handler, deps, accessKey, secret := newControlPlaneWithDeps(t)
	commit := seedHandlerSource(t, deps.Catalog)
	def := objectDefinition("handler-src", commit.String(), "countries.csv", "geo/countries.csv")

	rec := doRequest(t, handler, http.MethodPost, "", createDatasetBody(t, "http-bad-proposal", def, true), accessKey, secret)
	require.Equal(t, http.StatusCreated, rec.Code, "body: %s", rec.Body.String())
	proposal := createHTTPProposal(t, handler, accessKey, secret, "http-bad-proposal", "ok", def)

	cases := []struct {
		name string
		path string
		body string
	}{
		{
			name: "missing title",
			path: "/http-bad-proposal/proposals",
			body: `{"definition": ` + definitionJSON(t, def) + `}`,
		},
		{
			name: "missing definition",
			path: "/http-bad-proposal/proposals",
			body: `{"title": "no definition"}`,
		},
		{
			name: "patch status outside the enum",
			path: "/http-bad-proposal/proposals/" + proposal.Id,
			body: `{"status": "merged"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			method := http.MethodPost
			if tc.name == "patch status outside the enum" {
				method = http.MethodPatch
			}
			rec := doRequest(t, handler, method, tc.path, tc.body, accessKey, secret)
			require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
			require.NotEmpty(t, decodeError(t, rec).Message)
		})
	}

	t.Run("close", func(t *testing.T) {
		rec := doRequest(t, handler, http.MethodPatch, "/http-bad-proposal/proposals/"+proposal.Id,
			proposalUpdateBody(t, apigen.ProposalUpdateRequest{Status: ptr(ProposalStatusClosed)}), accessKey, secret)
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		var closed apigen.Proposal
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &closed))
		require.Equal(t, ProposalStatusClosed, closed.Status)
		require.NotNil(t, closed.ClosedAt)

		_, err := deps.Catalog.GetBranchReference(t.Context(), "http-bad-proposal", proposal.PreviewBranch)
		require.Error(t, err, "closing deletes the preview branch")
		require.True(t, isNotFound(err))
	})
}

// dataPaths returns the paths of a data diff page, for assertions.
func dataPaths(diff *apigen.DataDiffList) []string {
	paths := make([]string, 0, len(diff.Results))
	for _, entry := range diff.Results {
		paths = append(paths, entry.Path)
	}
	return paths
}

// definitionJSON renders a definition as the JSON object a request body embeds.
func definitionJSON(t *testing.T, def Definition) string {
	t.Helper()
	encoded, err := json.Marshal(definitionToAPI(def))
	require.NoError(t, err)
	return string(encoded)
}
