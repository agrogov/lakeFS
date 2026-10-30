package datasets

// Spike S2 (DATASETS_DESIGN.md section 18) — GC pin branches and their protection from
// deletion by policy. Two questions, both answered by executable evidence below:
//
//	S2a: Can a hidden pin branch be created in a repository whose branch protection rules match "*"?
//	     ANSWER: YES. Branch protection gates *staging writes* and *commits* only; branch creation is
//	     not checked against the rules, so Catalog.CreateBranch(ctx, repo, "_lakefs_ds_pin_<commit>",
//	     commitID, graveler.WithHidden(true), graveler.WithForce(true)) succeeds (and also succeeds on a
//	     read-only repository because Force bypasses ReadOnly). The pin branch is hidden from the default
//	     listing and IS returned by the GC branch iterator (ShowHidden: true), which is what makes pins
//	     protect data from GC.
//
//	S2b: Does auth.ArnMatch match a wildcard in the middle of an ARN resource?
//	     ANSWER: YES. ArnMatch wildcard-matches the resource part only, and "*" spans "/", so
//	     arn:lakefs:fs:::repository/*/branch/_lakefs_ds_pin_* matches the pin branch of every repository
//	     (and it does not match other branches). The design's datasets-pins-protect deny policy works.

import (
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// TestSpikeS2a_HiddenPinBranchVsBranchProtection pins the answer to S2a: a hidden, forced pin
// branch can be created in a repository whose protection rules match "*", it stays hidden from
// default branch listings, and it is still write-protected afterwards.
func TestSpikeS2a_HiddenPinBranchVsBranchProtection(t *testing.T) {
	const (
		repo    = "src-protected"
		pinPref = "_lakefs_ds_pin_"
	)
	f := newSpikeFixture(t)
	f.createRepository(t, repo, "main", false)

	pinned := f.commitEntry(t, repo, "main", "data/customers/c1.parquet", "data/customers/c1.parquet", catalog.AddressTypeRelative)

	// Protect every branch: this is the worst case for a pin branch (pattern "*" matches all).
	require.NoError(t, f.cat.SetBranchProtectionRules(f.ctx, repo, &graveler.BranchProtectionRules{
		BranchPatternToBlockedActions: map[string]*graveler.BranchProtectionBlockedActions{
			"*": {Value: []graveler.BranchProtectionBlockedAction{
				graveler.BranchProtectionBlockedAction_STAGING_WRITE,
				graveler.BranchProtectionBlockedAction_COMMIT,
			}},
		},
	}, swag.String("")), "set branch protection rules")

	pin := graveler.BranchID(pinPref + pinned)

	// Wait until protection is known to be active, so the positive result below is not an artefact of
	// the settings manager's cache (repository settings are eventually consistent, ~3s by default).
	require.Eventually(t, func() bool {
		err := f.cat.CreateEntry(f.ctx, repo, "main", catalog.DBEntry{
			Path: "probe/never-committed", PhysicalAddress: "probe/never-committed",
			AddressType: catalog.AddressTypeRelative, Size: 1, Checksum: "probe",
		})
		return errors.Is(err, graveler.ErrWriteToProtectedBranch)
	}, 10*time.Second, 20*time.Millisecond, "protection rules matching \"*\" must become enforced on main")

	_, err := f.cat.CreateBranch(f.ctx, repo, pin.String(), pinned.String(),
		graveler.WithHidden(true), graveler.WithForce(true))
	require.NoError(t, err, "S2a ANSWER: creating a hidden pin branch is NOT blocked by protection rules matching \"*\"")

	// The pin is hidden and points at the pinned commit.
	rec := f.repositoryRecord(t, repo)
	branch, err := f.refMgr.GetBranch(f.ctx, rec, pin)
	require.NoError(t, err)
	require.True(t, branch.Hidden, "pin branch must be hidden")
	require.Equal(t, pinned, branch.CommitID, "pin branch must point at the pinned commit")

	// Hidden from the default listing (what lakectl / the UI show)...
	require.NotContains(t, f.branchNames(t, repo), pin.String(), "pin branch must not appear in the default listing")
	// ...but visible when hidden branches are requested explicitly.
	hiddenListing, err := f.refMgr.ListBranches(f.ctx, rec, graveler.ListOptions{ShowHidden: true})
	require.NoError(t, err)
	var found bool
	for hiddenListing.Next() {
		if hiddenListing.Value().BranchID == pin {
			found = true
		}
	}
	require.NoError(t, hiddenListing.Err())
	hiddenListing.Close()
	require.True(t, found, "pin branch must be visible with ShowHidden")

	// The catalog listing honors the same option (this is what the REST API exposes as show_hidden).
	withHidden, _, err := f.cat.ListBranches(f.ctx, repo, "", -1, "", graveler.WithShowHidden(true))
	require.NoError(t, err)
	var apiFound bool
	for _, b := range withHidden {
		if b.Name == pin.String() {
			apiFound = true
		}
	}
	require.True(t, apiFound, "catalog listing with show_hidden must include the pin branch")

	// Protection still applies to the pin branch itself: the service never writes to it, but a
	// user (or a bug) that tries to commit there is blocked.
	err = f.cat.CreateEntry(f.ctx, repo, pin.String(), catalog.DBEntry{
		Path: "data/customers/c2.parquet", PhysicalAddress: "data/customers/c2.parquet",
		AddressType: catalog.AddressTypeRelative, Size: 1, Checksum: "x",
	})
	require.ErrorIs(t, err, graveler.ErrWriteToProtectedBranch, "staging writes to a protected pin branch are blocked")

	_, err = f.cat.Commit(f.ctx, repo, pin.String(), "nope", "spike", nil, nil, nil, false)
	require.ErrorIs(t, err, graveler.ErrCommitToProtectedBranch, "commits to a protected pin branch are blocked")

	// Sanity: protection does not block branch creation at all — even a non-hidden branch.
	_, err = f.cat.CreateBranch(f.ctx, repo, "plain-feature-branch", pinned.String())
	require.NoError(t, err, "branch protection does not gate branch creation")
}

// TestSpikeS2a_PinBranchOnReadOnlyRepository pins the related design claim (section 8.2): pin
// branches can be created in a read-only source repository, because Force bypasses ReadOnly.
func TestSpikeS2a_PinBranchOnReadOnlyRepository(t *testing.T) {
	const repo = "src-readonly"
	f := newSpikeFixture(t)
	f.createRepository(t, repo, "main", true)

	pinned := f.commitEntry(t, repo, "main", "data/a.parquet", "data/a.parquet",
		catalog.AddressTypeRelative, graveler.WithForce(true))

	pin := "_lakefs_ds_pin_" + pinned.String()

	// Without Force a read-only repository rejects the write.
	_, err := f.cat.CreateBranch(f.ctx, repo, pin, pinned.String(), graveler.WithHidden(true))
	require.ErrorIs(t, err, graveler.ErrReadOnlyRepository, "read-only repositories reject writes without Force")

	_, err = f.cat.CreateBranch(f.ctx, repo, pin, pinned.String(), graveler.WithHidden(true), graveler.WithForce(true))
	require.NoError(t, err, "S2a ANSWER: Force bypasses ReadOnly, so pins work on read-only source repos")
}

// TestSpikeS2a_GCBranchIteratorIncludesHiddenPins pins the second half of S2a: the GC branch
// iterator (ref.Manager.GCBranchIterator, i.e. ListBranches with ShowHidden: true) returns hidden
// pin branches, so a pinned commit stays a GC starting point.
// retention.GarbageCollectionManager.SaveGarbageCollectionCommits builds its starting points from
// exactly this iterator, and branch heads are always retained regardless of the retention rules.
func TestSpikeS2a_GCBranchIteratorIncludesHiddenPins(t *testing.T) {
	const repo = "src-gc"
	f := newSpikeFixture(t)
	f.createRepository(t, repo, "main", false)

	// A commit that is only reachable from a branch we are about to delete: without the pin it has no
	// branch head (and therefore no GC starting point) at all.
	f.commitEntry(t, repo, "main", "data/a.parquet", "data/a.parquet", catalog.AddressTypeRelative)
	base := f.commitEntry(t, repo, "main", "data/b.parquet", "data/b.parquet", catalog.AddressTypeRelative)
	_, err := f.cat.CreateBranch(f.ctx, repo, "feature", base.String())
	require.NoError(t, err)
	pinned := f.commitEntry(t, repo, "feature", "data/c.parquet", "data/c.parquet", catalog.AddressTypeRelative)
	require.NoError(t, f.cat.DeleteBranch(f.ctx, repo, "feature"))

	rec := f.repositoryRecord(t, repo)

	// Without the pin, the pinned commit is nobody's branch head.
	assertNoBranchHeadAt(t, f, rec, pinned)

	pin := graveler.BranchID("_lakefs_ds_pin_" + pinned.String())
	_, err = f.cat.CreateBranch(f.ctx, repo, pin.String(), pinned.String(),
		graveler.WithHidden(true), graveler.WithForce(true))
	require.NoError(t, err)

	// With the hidden pin the commit is a GC starting point again.
	require.Equal(t, pin, branchHeadAt(t, f, rec, pinned),
		"S2a ANSWER: GCBranchIterator includes the hidden pin, so the pinned commit is a GC starting point")

	// Deleting the pin removes that protection — this is why the reconciler exists (design section 12.3).
	require.NoError(t, f.cat.DeleteBranch(f.ctx, repo, pin.String(), graveler.WithForce(true)))
	assertNoBranchHeadAt(t, f, rec, pinned)
}

// branchHeadAt returns the branch whose HEAD is commit, using the GC branch iterator, or "" if none.
func branchHeadAt(t *testing.T, f *spikeFixture, rec *graveler.RepositoryRecord, commit graveler.CommitID) graveler.BranchID {
	t.Helper()
	it, err := f.refMgr.GCBranchIterator(f.ctx, rec)
	require.NoError(t, err)
	defer it.Close()
	var head graveler.BranchID
	for it.Next() {
		if it.Value().CommitID == commit {
			head = it.Value().BranchID
		}
	}
	require.NoError(t, it.Err())
	return head
}

func assertNoBranchHeadAt(t *testing.T, f *spikeFixture, rec *graveler.RepositoryRecord, commit graveler.CommitID) {
	t.Helper()
	require.Empty(t, branchHeadAt(t, f, rec, commit), "commit %s must not be a GC starting point", commit)
}

// TestSpikeS2b_ArnMatchWildcardInResource pins the answer to S2b: auth.ArnMatch wildcard-matches
// the resource part and "*" spans "/", so a mid-resource wildcard selects pin branches.
func TestSpikeS2b_ArnMatchWildcardInResource(t *testing.T) {
	const pinPattern = "arn:lakefs:fs:::repository/*/branch/_lakefs_ds_pin_*"
	pinBranch := permissions.BranchArn("lakehouse-repo", "_lakefs_ds_pin_30f9eb1c")

	tests := []struct {
		name string
		src  string // policy resource
		dst  string // requested resource
		want bool
	}{
		{"pin wildcard matches a pin branch", pinPattern, pinBranch, true},
		{"pin wildcard matches a pin branch in another repo", pinPattern, permissions.BranchArn("ref-data", "_lakefs_ds_pin_abc"), true},
		{"pin wildcard does not match a regular branch", pinPattern, permissions.BranchArn("lakehouse-repo", "main"), false},
		{"pin wildcard does not match an object", pinPattern, permissions.ObjectArn("lakehouse-repo", "_lakefs_ds_pin_30f9eb1c"), false},
		{"tag wildcard does not match a branch ARN", "arn:lakefs:fs:::repository/*/tag/_lakefs_ds_pin_*", pinBranch, false},
		{"exact pin ARN matches itself", pinBranch, pinBranch, true},
		{"suffix wildcard inside a mid-resource wildcard", "arn:lakefs:fs:::repository/lakehouse-repo/branch/_lakefs_ds_pin_30f9*", pinBranch, true},
		{"mid-resource wildcard with a fixed repo name does not leak to another repo", "arn:lakefs:fs:::repository/lakehouse-repo/branch/_lakefs_ds_pin_*", permissions.BranchArn("lakehouse-repo-2", "_lakefs_ds_pin_30f9eb1c"), false},
		{"all-resources policy matches", permissions.All, pinBranch, true},
		{"service mismatch does not match", "arn:lakefs:datasets:::dataset/*", pinBranch, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, auth.ArnMatch(tt.src, tt.dst), "ArnMatch(%q, %q)", tt.src, tt.dst)
		})
	}

	// The same wildcard, evaluated through the real policy engine: the datasets-pins-protect deny
	// policy (design section 8.2) denies deleting a pin branch but leaves other branches alone.
	policy := &model.Policy{
		DisplayName: "datasets-pins-protect",
		Statement: model.Statements{{
			Effect:   model.StatementEffectDeny,
			Action:   []string{permissions.DeleteBranchAction},
			Resource: pinPattern,
		}},
	}
	check := func(branch string) auth.CheckResult {
		node := permissions.Node{
			Type: permissions.NodeTypeNode,
			Permission: permissions.Permission{
				Action:   permissions.DeleteBranchAction,
				Resource: permissions.BranchArn("lakehouse-repo", branch),
			},
		}
		return auth.CheckPermissions(t.Context(), node, "user1", []*model.Policy{policy}, &auth.MissingPermissions{})
	}
	require.Equal(t, auth.CheckDeny, check("_lakefs_ds_pin_30f9eb1c"), "S2b ANSWER: the pin deny policy blocks deleting pin branches")
	require.Equal(t, auth.CheckNeutral, check("main"), "the pin deny policy must not affect regular branches")
}
