package datasets

// proposals_test.go covers the dataset proposals of DATASETS_DESIGN.md §5.6 through the
// control plane: the preview branch a reviewer browses, the diffs against latest, the
// merge path (§5.4 CAS + the publish primitive), close, update with re-resolved refs, and
// the two source-read checks of §9.3 (author at creation, merger at merge).
//
// The fixture is the §15.1 one - a real catalog on kv/mem + block/mem and the ACL auth
// service - so every assertion is made against both planes: the ProposalRecord in KV and
// the branches and commits of the dataset repository.

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

// createProposal opens a proposal through the service and fails the test on error.
func (f *serviceFixture) createProposal(t *testing.T, dataset string, req CreateProposalRequest) *ProposalView {
	t.Helper()
	view, err := f.svc.CreateProposal(f.ctx, f.caller(), dataset, req)
	require.NoError(t, err, "create proposal on %s", dataset)
	return view
}

// createPublishedDataset creates a dataset with version v1 of def and returns the record.
func (f *serviceFixture) createPublishedDataset(t *testing.T, name string, def Definition) *DatasetView {
	t.Helper()
	view, err := f.svc.Create(f.ctx, f.caller(), CreateRequest{Name: name, Definition: def, Publish: true, Message: "v1"})
	require.NoError(t, err, "create dataset %s", name)
	require.Equal(t, 1, view.Record.LatestVersion)
	return view
}

// branchCommit returns the commit a branch of the dataset repository points at, or
// graveler.ErrBranchNotFound.
func (f *serviceFixture) branchCommit(t *testing.T, dataset, branch string) (string, error) {
	t.Helper()
	return f.cat.GetBranchReference(f.ctx, dataset, branch)
}

// requireBranchGone asserts that a branch of the dataset repository no longer exists.
func (f *serviceFixture) requireBranchGone(t *testing.T, dataset, branch string) {
	t.Helper()
	_, err := f.branchCommit(t, dataset, branch)
	require.Error(t, err, "branch %s should be gone", branch)
	require.True(t, isNotFound(err), "branch %s: %v", branch, err)
}

// hiddenBranchNames lists the branches of a repository including the hidden ones (§5.6: the
// preview branch must be hidden from the default listing).
func (f *serviceFixture) hiddenBranchNames(t *testing.T, repo string) []string {
	t.Helper()
	branches, _, err := f.cat.ListBranches(f.ctx, repo, "", -1, "", graveler.WithShowHidden(true))
	require.NoError(t, err, "list branches of %s", repo)
	names := make([]string, len(branches))
	for i, branch := range branches {
		names[i] = branch.Name
	}
	return names
}

// proposalRecord reads the stored proposal record.
func (f *serviceFixture) proposalRecord(t *testing.T, dataset, id string) *ProposalRecord {
	t.Helper()
	rec, err := f.svc.store.GetProposal(f.ctx, dataset, id)
	require.NoError(t, err, "get proposal %s/%s", dataset, id)
	return rec
}

// addPolicyUser creates a user and attaches one inline policy to it, for the narrowly
// privileged principals of the §9.3 tests.
func (f *serviceFixture) addPolicyUser(t *testing.T, username, policyName string, statements model.Statements) {
	t.Helper()
	f.addUser(t, username)
	f.grant(t, username, policyName, statements)
}

// TestProposalCreateMaterialisesPreview is the create path of §5.6: the definition is
// materialised on the hidden `_proposal-<id>` branch, the record is stored with
// BaseVersion = latest_version, and the detail view carries the definition and data diffs
// against latest.
func TestProposalCreateMaterialisesPreview(t *testing.T) {
	const (
		dataset = "proposal-preview"
		source  = "src-proposal-preview"
	)
	f := newServiceFixture(t)
	first := f.sourceRepo(t, source, map[string]string{
		"data/a.parquet": "a",
		"data/b.parquet": "b",
	})
	f.createPublishedDataset(t, dataset, prefixDefinition(source, first.String(), "data", "data/"))

	second := f.commitTo(t, source, "main", map[string]string{
		"data/a.parquet": "a",
		"data/b.parquet": "b",
		"data/c.parquet": "c",
	})
	view := f.createProposal(t, dataset, CreateProposalRequest{
		Definition:  prefixDefinition(source, second.String(), "data", "data/"),
		Title:       "add c",
		Description: "the customer asked for it",
	})

	rec := view.Record
	require.Len(t, rec.ID, 26, "the id is a ULID (§5.6)")
	require.Equal(t, dataset, rec.Dataset)
	require.Equal(t, 1, rec.BaseVersion)
	require.Equal(t, ProposalStatusOpen, rec.Status)
	require.Equal(t, testServiceUser, rec.Author)
	require.Equal(t, "add c", rec.Title)
	require.Equal(t, "the customer asked for it", rec.Description)
	require.Nil(t, rec.ClosedAt)
	require.Zero(t, rec.MergedVersion)
	require.Equal(t, ProposalPreviewBranch(rec.ID), rec.PreviewBranch)
	require.Equal(t, second.String(), rec.Definition.Data[0].Ref.ID, "the stored definition is resolved")
	require.Equal(t, RefTypeCommit, rec.Definition.Data[0].Ref.Type)

	// The record is in KV under proposals/<dataset>/<id>, not a graveler pull request (§5.6).
	stored := f.proposalRecord(t, dataset, rec.ID)
	require.Equal(t, rec.Title, stored.Title)
	require.Equal(t, rec.PreviewBranch, stored.PreviewBranch)

	// The preview branch exists, is hidden, and serves the proposal's data, so a reviewer can
	// browse lakefs://<dataset>/_proposal-<id>/data/c.parquet (§5.6, §7.1).
	head, err := f.branchCommit(t, dataset, rec.PreviewBranch)
	require.NoError(t, err, "preview branch")
	require.NotEmpty(t, head)
	require.NotContains(t, f.branchNames(t, dataset), rec.PreviewBranch, "the preview branch is hidden")
	require.Contains(t, f.hiddenBranchNames(t, dataset), rec.PreviewBranch)
	require.Equal(t, []byte("c"), f.readDatasetObject(t, dataset, rec.PreviewBranch, "data/c.parquet"))

	// latest is untouched: a proposal is not a version.
	require.Equal(t, 1, f.datasetRecord(t, dataset).LatestVersion)
	_, err = f.tagCommit(t, dataset, VersionTag(2))
	require.Error(t, err, "no v2 tag before the merge")
	require.Equal(t, []string{DefinitionPath, "data/a.parquet", "data/b.parquet"},
		f.listPaths(t, dataset, DefaultBranch))

	// §10.1: the detail view carries the diff against latest, both definition and data.
	require.NotNil(t, view.Diff)
	require.Len(t, view.Diff.Items, 1)
	require.Equal(t, "data", view.Diff.Items[0].Target)
	require.Equal(t, ChangeChanged, view.Diff.Items[0].Change)
	require.NotNil(t, view.Diff.Items[0].To)
	require.Equal(t, second.String(), view.Diff.Items[0].To.Ref.ID)
	require.NotNil(t, view.DataDiff)
	require.Equal(t, []DataDiffEntry{{Path: "data/c.parquet", Type: ChangeAdded, SizeBytes: int64(len("c"))}},
		view.DataDiff.Entries)

	// The preview commit is not tagged: a proposal is not a version (§5.6).
	_, err = f.tagCommit(t, dataset, VersionTag(2))
	require.Error(t, err)
}

// TestProposalMergePublishesVersion covers the merge of §5.6: the proposal's definition goes
// through the publish path (a new version, tag, `latest` head), the proposal is marked
// merged with the version it produced, and the preview branch is deleted with Force.
func TestProposalMergePublishesVersion(t *testing.T) {
	const (
		dataset = "proposal-merge"
		source  = "src-proposal-merge"
	)
	f := newServiceFixture(t)
	first := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "a"})
	f.createPublishedDataset(t, dataset, prefixDefinition(source, first.String(), "data", "data/"))

	second := f.commitTo(t, source, "main", map[string]string{
		"data/a.parquet": "a2",
		"data/b.parquet": "b2",
	})
	proposal := f.createProposal(t, dataset, CreateProposalRequest{
		Definition: prefixDefinition(source, second.String(), "data", "data/"),
		Title:      "cut two",
	})

	version, err := f.svc.MergeProposal(f.ctx, f.caller(), dataset, proposal.Record.ID, MergeProposalRequest{})
	require.NoError(t, err, "merge proposal")
	require.Equal(t, 2, version.Version)
	require.Equal(t, testServiceUser, version.Publisher)
	require.Equal(t, proposal.Record.ID, version.ProposalID, "the version records the proposal it came from")
	require.Equal(t, "merge proposal "+proposal.Record.ID+": cut two", version.Message)

	// The version is a real version: tag v2, `latest` head, dataset record (§6.2 steps 9-11).
	tagCommit, err := f.tagCommit(t, dataset, VersionTag(2))
	require.NoError(t, err, "tag v2")
	require.Equal(t, version.CommitID, tagCommit)
	require.Equal(t, version.CommitID, f.latestHead(t, dataset))
	require.Equal(t, 2, f.datasetRecord(t, dataset).LatestVersion)

	// §6.2 step 9: the commit metadata names the proposal (§12.4).
	log, err := f.cat.GetCommit(f.ctx, dataset, version.CommitID)
	require.NoError(t, err)
	require.Equal(t, proposal.Record.ID, log.Metadata[CommitMetadataProposalKey])

	// The data plane serves the merged definition at v2.
	require.Equal(t, []byte("a2"), f.readDatasetObject(t, dataset, VersionTag(2), "data/a.parquet"))
	require.Equal(t, []byte("b2"), f.readDatasetObject(t, dataset, VersionTag(2), "data/b.parquet"))

	// §5.6: the merge closes the proposal and deletes the preview branch.
	merged := f.proposalRecord(t, dataset, proposal.Record.ID)
	require.Equal(t, ProposalStatusMerged, merged.Status)
	require.Equal(t, 2, merged.MergedVersion)
	require.NotNil(t, merged.ClosedAt)
	require.False(t, merged.ClosedAt.IsZero())
	f.requireBranchGone(t, dataset, proposal.Record.PreviewBranch)

	// The merged proposal is read-only: a second merge is a conflict, not a second version.
	_, err = f.svc.MergeProposal(f.ctx, f.caller(), dataset, proposal.Record.ID, MergeProposalRequest{})
	require.ErrorIs(t, err, ErrProposalNotOpen)
}

// TestProposalMergeRequiresRebase is the stale-base case of §5.6: a proposal made against v1
// cannot be merged once v2 exists, the error says "rebase required" and carries the current
// latest version, and updating the proposal re-bases it so a later merge succeeds.
func TestProposalMergeRequiresRebase(t *testing.T) {
	const (
		dataset = "proposal-rebase"
		source  = "src-proposal-rebase"
	)
	f := newServiceFixture(t)
	first := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "a"})
	f.createPublishedDataset(t, dataset, prefixDefinition(source, first.String(), "data", "data/"))

	second := f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "b"})
	proposal := f.createProposal(t, dataset, CreateProposalRequest{
		Definition: prefixDefinition(source, second.String(), "data", "data/"),
		Title:      "b",
	})
	require.Equal(t, 1, proposal.Record.BaseVersion)

	// Somebody publishes v2 directly - with different data - so the proposal's base is stale.
	third := f.commitTo(t, source, "main", map[string]string{"data/a.parquet": "c"})
	f.publishVersion(t, dataset, PublishVersionRequest{
		Definition: prefixDefinition(source, third.String(), "data", "data/"),
		Message:    "direct v2",
	})

	_, err := f.svc.MergeProposal(f.ctx, f.caller(), dataset, proposal.Record.ID, MergeProposalRequest{})
	require.Error(t, err, "merging a stale proposal must fail")
	var rebase *RebaseRequiredError
	require.ErrorAs(t, err, &rebase)
	require.Equal(t, 1, rebase.BaseVersion)
	require.Equal(t, 2, rebase.LatestVersion)
	require.Equal(t, 2, rebase.latestVersion(), "the 409 body carries latest_version (§5.6)")
	require.Contains(t, rebase.Error(), "rebase required")
	require.ErrorIs(t, err, ErrDatasetConflict, "the HTTP mapping is the 409 of §10.4")

	// The rebase is explicit: updating the proposal moves its base to latest.
	updated, err := f.svc.UpdateProposal(f.ctx, f.caller(), dataset, proposal.Record.ID, UpdateProposalRequest{
		Title: ptr("b, rebased"),
	})
	require.NoError(t, err, "update proposal")
	require.Equal(t, 2, updated.Record.BaseVersion)
	require.Equal(t, "b, rebased", updated.Record.Title)

	version, err := f.svc.MergeProposal(f.ctx, f.caller(), dataset, proposal.Record.ID, MergeProposalRequest{})
	require.NoError(t, err, "merge after rebase")
	require.Equal(t, 3, version.Version)
	require.Equal(t, 3, f.datasetRecord(t, dataset).LatestVersion)
	require.Equal(t, []byte("b"), f.readDatasetObject(t, dataset, VersionTag(3), "data/a.parquet"),
		"the merge publishes the proposal's definition, not the version it was rebased onto")
}

// TestProposalTwoAgainstTheSameBase pins the race of §5.6: two proposals made against the
// same version can both be reviewed, but the first merge advances latest and the second
// merge is a 409 - never a second version of the same base.
func TestProposalTwoAgainstTheSameBase(t *testing.T) {
	const (
		dataset = "proposal-race"
		source  = "src-proposal-race"
	)
	f := newServiceFixture(t)
	first := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "a"})
	f.createPublishedDataset(t, dataset, prefixDefinition(source, first.String(), "data", "data/"))

	left := f.createProposal(t, dataset, CreateProposalRequest{
		Definition: prefixDefinition(source, first.String(), "left", "data/"),
		Title:      "left",
	})
	right := f.createProposal(t, dataset, CreateProposalRequest{
		Definition: prefixDefinition(source, first.String(), "right", "data/"),
		Title:      "right",
	})
	require.Equal(t, left.Record.BaseVersion, right.Record.BaseVersion)
	require.NotEqual(t, left.Record.PreviewBranch, right.Record.PreviewBranch)

	version, err := f.svc.MergeProposal(f.ctx, f.caller(), dataset, left.Record.ID, MergeProposalRequest{})
	require.NoError(t, err, "first merge wins")
	require.Equal(t, 2, version.Version)

	_, err = f.svc.MergeProposal(f.ctx, f.caller(), dataset, right.Record.ID, MergeProposalRequest{})
	var rebase *RebaseRequiredError
	require.ErrorAs(t, err, &rebase, "second merge of the same base")
	require.Equal(t, 2, rebase.LatestVersion)

	// The losing proposal is still open and still has its preview branch: nothing about it
	// was silently dropped, the author has to rebase and resubmit (§5.6).
	losing := f.proposalRecord(t, dataset, right.Record.ID)
	require.Equal(t, ProposalStatusOpen, losing.Status)
	_, err = f.branchCommit(t, dataset, losing.PreviewBranch)
	require.NoError(t, err, "the losing preview branch is untouched")
	require.Equal(t, 2, f.datasetRecord(t, dataset).LatestVersion)
}

// TestProposalClose covers closing a proposal: the record is closed, the preview branch is
// deleted, latest does not move, and the closed proposal can no longer be merged or updated.
func TestProposalClose(t *testing.T) {
	const (
		dataset = "proposal-close"
		source  = "src-proposal-close"
	)
	f := newServiceFixture(t)
	first := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "a"})
	f.createPublishedDataset(t, dataset, prefixDefinition(source, first.String(), "data", "data/"))

	proposal := f.createProposal(t, dataset, CreateProposalRequest{
		Definition: prefixDefinition(source, first.String(), "closed", "data/"),
		Title:      "never mind",
	})

	closed, err := f.svc.UpdateProposal(f.ctx, f.caller(), dataset, proposal.Record.ID, UpdateProposalRequest{
		Status: ProposalStatusClosed,
	})
	require.NoError(t, err, "close proposal")
	require.Equal(t, ProposalStatusClosed, closed.Record.Status)
	require.NotNil(t, closed.Record.ClosedAt)
	require.Zero(t, closed.Record.MergedVersion)

	stored := f.proposalRecord(t, dataset, proposal.Record.ID)
	require.Equal(t, ProposalStatusClosed, stored.Status)
	require.NotNil(t, stored.ClosedAt)

	f.requireBranchGone(t, dataset, proposal.Record.PreviewBranch)
	require.Equal(t, 1, f.datasetRecord(t, dataset).LatestVersion)
	require.Equal(t, proposal.Record.PreviewBranch, stored.PreviewBranch,
		"the record keeps the branch name it had, for the audit trail")

	t.Run("merge of a closed proposal is a conflict", func(t *testing.T) {
		_, err := f.svc.MergeProposal(f.ctx, f.caller(), dataset, proposal.Record.ID, MergeProposalRequest{})
		require.ErrorIs(t, err, ErrProposalNotOpen)
	})

	t.Run("closing twice is a conflict", func(t *testing.T) {
		_, err := f.svc.UpdateProposal(f.ctx, f.caller(), dataset, proposal.Record.ID, UpdateProposalRequest{
			Status: ProposalStatusClosed,
		})
		require.ErrorIs(t, err, ErrProposalNotOpen)
	})

	t.Run("unknown status is a validation problem", func(t *testing.T) {
		open := f.createProposal(t, dataset, CreateProposalRequest{
			Definition: prefixDefinition(source, first.String(), "again", "data/"),
			Title:      "again",
		})
		_, err := f.svc.UpdateProposal(f.ctx, f.caller(), dataset, open.Record.ID, UpdateProposalRequest{
			Status: ProposalStatusMerged,
		})
		var validation *ValidationError
		require.ErrorAs(t, err, &validation)
		require.Equal(t, "status", validation.Problems[0].Path)
	})
}

// TestProposalUpdateReResolvesRefs covers the update path of §10.1: a definition that carries
// a branch ref is re-resolved to the branch's current commit, re-validated, re-checked for
// source reads and re-materialised on the preview branch.
func TestProposalUpdateReResolvesRefs(t *testing.T) {
	const (
		dataset = "proposal-update"
		source  = "src-proposal-update"
	)
	f := newServiceFixture(t)
	first := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "a"})
	f.createPublishedDataset(t, dataset, prefixDefinition(source, first.String(), "data", "data/"))

	branchDefinition := prefixDefinition(source, "ignored", "data", "data/")
	branchDefinition.Data[0].Ref = SourceRef{Type: RefTypeBranch, ID: "main"}

	proposal := f.createProposal(t, dataset, CreateProposalRequest{
		Definition: branchDefinition,
		Title:      "from main",
	})
	require.Equal(t, first.String(), proposal.Record.Definition.Data[0].Ref.ID,
		"a branch ref is resolved when the proposal is created (§5.2)")
	require.Equal(t, []string{DefinitionPath, "data/a.parquet"},
		f.listPaths(t, dataset, proposal.Record.PreviewBranch))

	// The branch moves on, then the author updates the proposal with the same branch ref.
	second := f.commitTo(t, source, "main", map[string]string{
		"data/a.parquet":   "a",
		"data/new.parquet": "new",
	})
	updated, err := f.svc.UpdateProposal(f.ctx, f.caller(), dataset, proposal.Record.ID, UpdateProposalRequest{
		Definition:  &branchDefinition,
		Description: ptr("picked up main"),
	})
	require.NoError(t, err, "update proposal")
	require.Equal(t, second.String(), updated.Record.Definition.Data[0].Ref.ID,
		"the update re-resolves the branch ref to the new commit")
	require.Equal(t, RefTypeCommit, updated.Record.Definition.Data[0].Ref.Type)
	require.Equal(t, "picked up main", updated.Record.Description)
	require.Equal(t, "from main", updated.Record.Title, "a nil member is left alone")
	require.Equal(t, 1, updated.Record.BaseVersion, "no version was published, so the base is unchanged")

	// The preview branch now serves the re-resolved definition's data.
	require.Equal(t, []byte("new"),
		f.readDatasetObject(t, dataset, proposal.Record.PreviewBranch, "data/new.parquet"))
	require.NotNil(t, updated.DataDiff)
	require.Contains(t, updated.DataDiff.Entries, DataDiffEntry{Path: "data/new.parquet", Type: ChangeAdded, SizeBytes: int64(len("new"))})

	// The stored record is the updated one, and the read path agrees.
	stored := f.proposalRecord(t, dataset, proposal.Record.ID)
	require.Equal(t, second.String(), stored.Definition.Data[0].Ref.ID)
	read, err := f.svc.GetProposal(f.ctx, f.caller(), dataset, proposal.Record.ID)
	require.NoError(t, err, "get proposal")
	require.Equal(t, stored.Definition, read.Record.Definition)

	// A later merge publishes the updated definition, not the one the proposal opened with.
	version, err := f.svc.MergeProposal(f.ctx, f.caller(), dataset, proposal.Record.ID, MergeProposalRequest{Message: "merged main"})
	require.NoError(t, err)
	require.Equal(t, "merged main", version.Message)
	require.Equal(t, second.String(), version.Definition.Data[0].Ref.ID)
}

// TestProposalCreateRequiresSourceRead is the author half of §9.3: opening a proposal attaches
// data, so the author must be able to read every source, and the 403 names the item.
func TestProposalCreateRequiresSourceRead(t *testing.T) {
	const (
		dataset = "proposal-create-authz"
		source  = "src-proposal-create-authz"
	)
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "a"})
	f.createPublishedDataset(t, dataset, prefixDefinition(source, commit.String(), "data", "data/"))

	// DatasetsFullAccess (DatasetAdmins) but none of the ACL base grants: the principal may
	// open a proposal on the dataset, but must not read the source it attaches.
	f.addUser(t, "weak-author", DatasetAdminsGroup)

	def := objectDefinition(source, commit.String(), "a.parquet", "data/a.parquet")
	_, err := f.svc.CreateProposal(f.ctx, f.callerFor("weak-author"), dataset, CreateProposalRequest{
		Definition: def,
		Title:      "denied",
	})
	require.ErrorIs(t, err, ErrPermissionDenied)
	var denied *PermissionError
	require.ErrorAs(t, err, &denied)
	require.Contains(t, denied.Item, "data[0]")
	require.Contains(t, denied.Item, source)

	// Nothing was written: no record, no preview branch, no version.
	proposals, err := f.svc.store.ListProposals(f.ctx, dataset)
	require.NoError(t, err)
	require.Empty(t, proposals)
	for _, branch := range f.hiddenBranchNames(t, dataset) {
		require.False(t, strings.HasPrefix(branch, proposalPreviewBranchPrefix),
			"no preview branch must be left behind: %s", branch)
	}

	// A principal that can read the source (the admin) can open the same proposal.
	f.createProposal(t, dataset, CreateProposalRequest{Definition: def, Title: "allowed"})
}

// TestProposalMergeRequiresSourceReadForMerger is the merger half of §9.3: the merger
// publishes the data under their own authority, so the source check runs again for them even
// though the author passed it when the proposal was opened.
func TestProposalMergeRequiresSourceReadForMerger(t *testing.T) {
	const (
		dataset = "proposal-merge-authz"
		source  = "src-proposal-merge-authz"
	)
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "a"})
	f.createPublishedDataset(t, dataset, prefixDefinition(source, commit.String(), "data", "data/"))
	proposal := f.createProposal(t, dataset, CreateProposalRequest{
		Definition: objectDefinition(source, commit.String(), "a.parquet", "data/a.parquet"),
		Title:      "a",
	})

	// PublishVersion on this dataset, but no read access to the source repository.
	f.addPolicyUser(t, "weak-merger", "weak-merger-publish", model.Statements{{
		Effect:   model.StatementEffectAllow,
		Action:   []string{ActionPublishVersion},
		Resource: DatasetArn(dataset),
	}})

	_, err := f.svc.MergeProposal(f.ctx, f.callerFor("weak-merger"), dataset, proposal.Record.ID, MergeProposalRequest{})
	require.ErrorIs(t, err, ErrPermissionDenied)
	var denied *PermissionError
	require.ErrorAs(t, err, &denied)
	require.Contains(t, denied.Item, "data[0]")
	require.Contains(t, denied.Item, source)

	// The proposal is still open and latest did not move: a refused merge changes nothing.
	require.Equal(t, ProposalStatusOpen, f.proposalRecord(t, dataset, proposal.Record.ID).Status)
	require.Equal(t, 1, f.datasetRecord(t, dataset).LatestVersion)
	_, err = f.branchCommit(t, dataset, proposal.Record.PreviewBranch)
	require.NoError(t, err, "the preview branch survives a refused merge")

	// With the source read permission the same principal can merge it.
	require.NoError(t, f.authService.AddUserToGroup(f.ctx, "weak-merger", acl.AdminsGroup))
	version, err := f.svc.MergeProposal(f.ctx, f.callerFor("weak-merger"), dataset, proposal.Record.ID,
		MergeProposalRequest{})
	require.NoError(t, err, "merge as the reviewer")
	require.Equal(t, 2, version.Version)
	require.Equal(t, "weak-merger", version.Publisher)
	require.Equal(t, ProposalStatusMerged, f.proposalRecord(t, dataset, proposal.Record.ID).Status)
}

// TestProposalMergeRequiresPublishPermission pins the merge action of §9.1: merging is
// datasets:PublishVersion on the dataset, and a reader without it is refused.
func TestProposalMergeRequiresPublishPermission(t *testing.T) {
	const (
		dataset = "proposal-merge-perm"
		source  = "src-proposal-merge-perm"
	)
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "a"})
	f.createPublishedDataset(t, dataset, prefixDefinition(source, commit.String(), "data", "data/"))
	proposal := f.createProposal(t, dataset, CreateProposalRequest{
		Definition: prefixDefinition(source, commit.String(), "data", "data/"),
		Title:      "a",
	})

	f.addPolicyUser(t, "proposal-reader", "proposal-reader", model.Statements{{
		Effect:   model.StatementEffectAllow,
		Action:   []string{ActionReadDataset},
		Resource: DatasetArn(dataset),
	}})

	_, err := f.svc.MergeProposal(f.ctx, f.callerFor("proposal-reader"), dataset, proposal.Record.ID, MergeProposalRequest{})
	require.ErrorIs(t, err, ErrPermissionDenied)

	// ... and the reader can read the proposal, which needs only ReadDataset.
	_, err = f.svc.GetProposal(f.ctx, f.callerFor("proposal-reader"), dataset, proposal.Record.ID)
	require.NoError(t, err)
	list, err := f.svc.ListProposals(f.ctx, f.callerFor("proposal-reader"), dataset, "", "", 0)
	require.NoError(t, err)
	require.Len(t, list.Proposals, 1)
}

// TestProposalListAndNotFound covers the list contract of §10.1 (newest first, status filter,
// id cursor) and the 404s of §10.4.
func TestProposalListAndNotFound(t *testing.T) {
	const (
		dataset = "proposal-list"
		source  = "src-proposal-list"
	)
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "a"})
	f.createPublishedDataset(t, dataset, prefixDefinition(source, commit.String(), "data", "data/"))

	opened := make([]*ProposalView, 0, 3)
	for i := 0; i < 3; i++ {
		opened = append(opened, f.createProposal(t, dataset, CreateProposalRequest{
			Definition:  prefixDefinition(source, commit.String(), fmt.Sprintf("target%d", i), "data/"),
			Title:       fmt.Sprintf("proposal %d", i),
			Description: "list test",
		}))
	}

	t.Run("newest first with an id cursor", func(t *testing.T) {
		first, err := f.svc.ListProposals(f.ctx, f.caller(), dataset, "", "", 2)
		require.NoError(t, err)
		require.Len(t, first.Proposals, 2)
		require.True(t, first.HasMore)
		require.Equal(t, opened[2].Record.ID, first.Proposals[0].ID)
		require.Equal(t, opened[1].Record.ID, first.Proposals[1].ID)
		require.Equal(t, opened[1].Record.ID, first.NextOffset)

		second, err := f.svc.ListProposals(f.ctx, f.caller(), dataset, "", first.NextOffset, 2)
		require.NoError(t, err)
		require.Len(t, second.Proposals, 1)
		require.False(t, second.HasMore)
		require.Equal(t, opened[0].Record.ID, second.Proposals[0].ID)
	})

	t.Run("status filter", func(t *testing.T) {
		_, err := f.svc.UpdateProposal(f.ctx, f.caller(), dataset, opened[0].Record.ID, UpdateProposalRequest{
			Status: ProposalStatusClosed,
		})
		require.NoError(t, err)

		open, err := f.svc.ListProposals(f.ctx, f.caller(), dataset, ProposalStatusOpen, "", 0)
		require.NoError(t, err)
		require.Len(t, open.Proposals, 2)
		closed, err := f.svc.ListProposals(f.ctx, f.caller(), dataset, ProposalStatusClosed, "", 0)
		require.NoError(t, err)
		require.Len(t, closed.Proposals, 1)
		require.Equal(t, opened[0].Record.ID, closed.Proposals[0].ID)
		merged, err := f.svc.ListProposals(f.ctx, f.caller(), dataset, ProposalStatusMerged, "", 0)
		require.NoError(t, err)
		require.Empty(t, merged.Proposals)
	})

	t.Run("bad filters are validation problems", func(t *testing.T) {
		_, err := f.svc.ListProposals(f.ctx, f.caller(), dataset, "bogus", "", 0)
		var validation *ValidationError
		require.ErrorAs(t, err, &validation)
		require.Equal(t, "status", validation.Problems[0].Path)

		_, err = f.svc.ListProposals(f.ctx, f.caller(), dataset, "", "not-a-proposal", 0)
		require.ErrorAs(t, err, &validation)
		require.Equal(t, "after", validation.Problems[0].Path)
	})

	t.Run("unknown proposal is 404", func(t *testing.T) {
		_, err := f.svc.GetProposal(f.ctx, f.caller(), dataset, "01J8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8")
		require.ErrorIs(t, err, ErrProposalNotFound)

		_, err = f.svc.UpdateProposal(f.ctx, f.caller(), dataset, "01J8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8", UpdateProposalRequest{})
		require.ErrorIs(t, err, ErrProposalNotFound)

		_, err = f.svc.MergeProposal(f.ctx, f.caller(), dataset, "01J8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8Z8", MergeProposalRequest{})
		require.ErrorIs(t, err, ErrProposalNotFound)
	})

	t.Run("unknown dataset is 404", func(t *testing.T) {
		_, err := f.svc.ListProposals(f.ctx, f.caller(), "not-a-dataset", "", "", 0)
		require.ErrorIs(t, err, ErrDatasetNotFound)

		_, err = f.svc.CreateProposal(f.ctx, f.caller(), "not-a-dataset", CreateProposalRequest{
			Definition: prefixDefinition(source, commit.String(), "data", "data/"),
			Title:      "nope",
		})
		require.ErrorIs(t, err, ErrDatasetNotFound)
	})
}

// TestProposalUpdateRequiresProposalAction checks the authorization split of §10.1: the
// author writes with datasets:CreateProposal, another principal needs datasets:PublishVersion.
func TestProposalUpdateRequiresProposalAction(t *testing.T) {
	const (
		dataset = "proposal-update-authz"
		source  = "src-proposal-update-authz"
	)
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "a"})
	f.createPublishedDataset(t, dataset, prefixDefinition(source, commit.String(), "data", "data/"))
	proposal := f.createProposal(t, dataset, CreateProposalRequest{
		Definition: prefixDefinition(source, commit.String(), "data", "data/"),
		Title:      "mine",
	})

	// Read-only on the dataset: neither the author action nor the publish action.
	f.addPolicyUser(t, "proposal-viewer", "proposal-viewer", model.Statements{{
		Effect:   model.StatementEffectAllow,
		Action:   []string{ActionReadDataset},
		Resource: DatasetArn(dataset),
	}})
	_, err := f.svc.UpdateProposal(f.ctx, f.callerFor("proposal-viewer"), dataset, proposal.Record.ID,
		UpdateProposalRequest{Title: ptr("hijacked")})
	require.ErrorIs(t, err, ErrPermissionDenied)

	// A principal with PublishVersion may edit somebody else's proposal (§10.1), and the
	// source check of §9.3 then runs for them.
	f.addPolicyUser(t, "proposal-maintainer", "proposal-maintainer", model.Statements{{
		Effect:   model.StatementEffectAllow,
		Action:   []string{ActionPublishVersion},
		Resource: DatasetArn(dataset),
	}})
	updated, err := f.svc.UpdateProposal(f.ctx, f.callerFor("proposal-maintainer"), dataset, proposal.Record.ID,
		UpdateProposalRequest{Title: ptr("maintained")})
	require.ErrorIs(t, err, ErrPermissionDenied, "the maintainer cannot read the source either")
	require.Nil(t, updated)

	require.NoError(t, f.authService.AddUserToGroup(f.ctx, "proposal-maintainer", acl.AdminsGroup))
	updated, err = f.svc.UpdateProposal(f.ctx, f.callerFor("proposal-maintainer"), dataset, proposal.Record.ID,
		UpdateProposalRequest{Title: ptr("maintained")})
	require.NoError(t, err)
	require.Equal(t, "maintained", updated.Record.Title)
	require.Equal(t, testServiceUser, updated.Record.Author, "the author never changes")
}

// TestProposalPreviewBranchIsNotAVersion pins the boundary between a preview and a version:
// materialising a preview writes no version record, no tag and no dataset record change, and
// its definition object is scoped away from the version's (§6.4), so a preview and the version
// it becomes never overwrite each other's bookkeeping.
func TestProposalPreviewBranchIsNotAVersion(t *testing.T) {
	const (
		dataset = "proposal-not-a-version"
		source  = "src-proposal-not-a-version"
	)
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "a"})
	f.createPublishedDataset(t, dataset, prefixDefinition(source, commit.String(), "data", "data/"))
	proposal := f.createProposal(t, dataset, CreateProposalRequest{
		Definition: prefixDefinition(source, commit.String(), "preview", "data/"),
		Title:      "preview only",
	})

	versions, err := f.svc.store.ListVersions(f.ctx, dataset)
	require.NoError(t, err)
	require.Len(t, versions, 1, "the proposal created no version record")
	require.Equal(t, 1, f.datasetRecord(t, dataset).LatestVersion)

	// The preview's definition document carries the version the merge would produce, and the
	// preview keeps the dataset's data in the targets of the proposal.
	doc := f.readDefinitionDocument(t, dataset, proposal.Record.PreviewBranch)
	require.Equal(t, 2, doc.Version, "the preview carries the version the merge would produce")
	require.Equal(t, proposal.Record.Definition, doc.Definition)
	require.Equal(t, []string{DefinitionPath, "preview/a.parquet"},
		f.listPaths(t, dataset, proposal.Record.PreviewBranch))

	// Merging publishes v2 from the same definition; the version's own definition document is
	// the version's, and the preview is gone (§5.6).
	version, err := f.svc.MergeProposal(f.ctx, f.caller(), dataset, proposal.Record.ID, MergeProposalRequest{})
	require.NoError(t, err)
	require.Equal(t, 2, version.Version)
	require.Equal(t, proposal.Record.Definition, f.readDefinitionDocument(t, dataset, VersionTag(2)).Definition)
	f.requireBranchGone(t, dataset, proposal.Record.PreviewBranch)
}

// TestProposalPaginationAfterCloseKeepsCursor pins the cursor against a status filter that
// changes under the client: the cursor is applied before the filter, so a proposal that leaves
// the open set cannot invalidate the next page.
func TestProposalPaginationAfterCloseKeepsCursor(t *testing.T) {
	const (
		dataset = "proposal-cursor"
		source  = "src-proposal-cursor"
	)
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "a"})
	f.createPublishedDataset(t, dataset, prefixDefinition(source, commit.String(), "data", "data/"))

	var opened []*ProposalView
	for i := 0; i < 3; i++ {
		opened = append(opened, f.createProposal(t, dataset, CreateProposalRequest{
			Definition: prefixDefinition(source, commit.String(), fmt.Sprintf("target%d", i), "data/"),
			Title:      fmt.Sprintf("proposal %d", i),
		}))
	}
	first, err := f.svc.ListProposals(f.ctx, f.caller(), dataset, "", "", 2)
	require.NoError(t, err)
	require.True(t, first.HasMore)
	require.Equal(t, opened[1].Record.ID, first.NextOffset, "newest first: page one is [2, 1]")

	// Close the proposal the cursor points at, then ask for the next page of open ones.
	_, err = f.svc.UpdateProposal(f.ctx, f.caller(), dataset, first.NextOffset, UpdateProposalRequest{
		Status: ProposalStatusClosed,
	})
	require.NoError(t, err)

	next, err := f.svc.ListProposals(f.ctx, f.caller(), dataset, ProposalStatusOpen, first.NextOffset, 10)
	require.NoError(t, err, "the cursor still resolves")
	require.Len(t, next.Proposals, 1)
	require.Equal(t, opened[0].Record.ID, next.Proposals[0].ID, "the oldest proposal is still the next page")
}

// TestProposalMergeKeepsStoreConsistentOnUnchangedDefinition pins the no-op rule of §6.2 step
// 8 for a merge: a proposal whose definition equals latest cannot become a version, and the
// proposal stays open for the author to fix.
func TestProposalMergeUnchangedDefinition(t *testing.T) {
	const (
		dataset = "proposal-noop"
		source  = "src-proposal-noop"
	)
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "a"})
	f.createPublishedDataset(t, dataset, prefixDefinition(source, commit.String(), "data", "data/"))
	proposal := f.createProposal(t, dataset, CreateProposalRequest{
		Definition: prefixDefinition(source, commit.String(), "data", "data/"),
		Title:      "nothing changes",
	})

	_, err := f.svc.MergeProposal(f.ctx, f.caller(), dataset, proposal.Record.ID, MergeProposalRequest{})
	require.Error(t, err, "a merge that changes nothing is not a version")
	require.True(t, errors.Is(err, ErrDatasetConflict), "the no-op is a 409 (§6.2 step 8)")

	require.Equal(t, 1, f.datasetRecord(t, dataset).LatestVersion)
	require.Equal(t, ProposalStatusOpen, f.proposalRecord(t, dataset, proposal.Record.ID).Status)
	_, err = f.branchCommit(t, dataset, proposal.Record.PreviewBranch)
	require.NoError(t, err, "the preview branch is kept so the review can continue")
}

// TestProposalCreateValidationProblems pins the 400s of §10.4 on the proposal path.
func TestProposalCreateValidationProblems(t *testing.T) {
	const (
		dataset = "proposal-validation"
		source  = "src-proposal-validation"
	)
	f := newServiceFixture(t)
	commit := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "a"})
	f.createPublishedDataset(t, dataset, prefixDefinition(source, commit.String(), "data", "data/"))

	t.Run("overlapping targets", func(t *testing.T) {
		def := Definition{
			FormatVersion: FormatVersion,
			Data: []DataItem{
				prefixDefinition(source, commit.String(), "a", "data/").Data[0],
				prefixDefinition(source, commit.String(), "a/b", "data/").Data[0],
			},
		}
		_, err := f.svc.CreateProposal(f.ctx, f.caller(), dataset, CreateProposalRequest{Definition: def, Title: "bad"})
		var validation *ValidationError
		require.ErrorAs(t, err, &validation)
		require.NotEmpty(t, validation.Problems)
	})

	t.Run("missing source object", func(t *testing.T) {
		def := objectDefinition(source, commit.String(), "missing", "data/missing.parquet")
		_, err := f.svc.CreateProposal(f.ctx, f.caller(), dataset, CreateProposalRequest{Definition: def, Title: "bad"})
		var validation *ValidationError
		require.ErrorAs(t, err, &validation)
		require.Equal(t, "data[0].object.path", validation.Problems[0].Path)
	})

	t.Run("reserved item type is 501", func(t *testing.T) {
		def := Definition{
			FormatVersion: FormatVersion,
			Data: []DataItem{{
				Target:     "tbl",
				Repository: source,
				Ref:        SourceRef{Type: RefTypeCommit, ID: commit.String()},
				Type:       ItemType("table"),
			}},
		}
		_, err := f.svc.CreateProposal(f.ctx, f.caller(), dataset, CreateProposalRequest{Definition: def, Title: "bad"})
		require.ErrorIs(t, err, ErrUnsupported)
	})
}

// TestProposalPreviewBranchReuseOnUpdate pins that an update materialises onto the existing
// preview branch and keeps the proposal reachable through the diff ref grammar
// (`proposal-<id>`), which reads the preview branch of §10.3.
func TestProposalPreviewBranchReuseOnUpdate(t *testing.T) {
	const (
		dataset = "proposal-ref"
		source  = "src-proposal-ref"
	)
	f := newServiceFixture(t)
	first := f.sourceRepo(t, source, map[string]string{"data/a.parquet": "a"})
	f.createPublishedDataset(t, dataset, prefixDefinition(source, first.String(), "data", "data/"))
	proposal := f.createProposal(t, dataset, CreateProposalRequest{
		Definition: prefixDefinition(source, first.String(), "data", "data/"),
		Title:      "ref",
	})
	branchHead, err := f.branchCommit(t, dataset, proposal.Record.PreviewBranch)
	require.NoError(t, err)

	second := f.commitTo(t, source, "main", map[string]string{
		"data/a.parquet": "a",
		"data/b.parquet": "b",
	})
	updated, err := f.svc.UpdateProposal(f.ctx, f.caller(), dataset, proposal.Record.ID, UpdateProposalRequest{
		Definition: ptr(prefixDefinition(source, second.String(), "data", "data/")),
	})
	require.NoError(t, err)
	newHead, err := f.branchCommit(t, dataset, proposal.Record.PreviewBranch)
	require.NoError(t, err, "the update commits onto the same preview branch")
	require.NotEqual(t, branchHead, newHead)
	require.Equal(t, proposal.Record.PreviewBranch, updated.Record.PreviewBranch)

	// §10.3: the proposal ref resolves to the preview branch and the stored definition.
	result, err := f.svc.Diff(f.ctx, f.caller(), DiffRequest{
		Dataset:  dataset,
		LeftRef:  RefNameLatest,
		RightRef: ProposalRefPrefix + proposal.Record.ID,
		Type:     DiffTypeData,
	})
	require.NoError(t, err)
	require.Equal(t, DiffTypeData, result.Type)
	require.Contains(t, result.Data.Entries, DataDiffEntry{Path: "data/b.parquet", Type: ChangeAdded, SizeBytes: int64(len("b"))})

	// The proposal id is a valid store key and a valid preview branch name.
	key, err := proposalKey(dataset, proposal.Record.ID)
	require.NoError(t, err)
	require.Contains(t, string(key), proposal.Record.ID)
	require.NoError(t, graveler.ValidateBranchID(graveler.BranchID(updated.Record.PreviewBranch)))

	// The record is a plain KV record: deleting it is the only cleanup a proposal needs.
	require.NoError(t, f.svc.store.DeleteProposal(f.ctx, dataset, proposal.Record.ID))
	_, err = f.svc.store.GetProposal(f.ctx, dataset, proposal.Record.ID)
	require.True(t, errors.Is(err, kv.ErrNotFound))
}
