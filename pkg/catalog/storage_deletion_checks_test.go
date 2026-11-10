package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
)

func TestNamespacesOverlap(t *testing.T) {
	cases := []struct {
		name           string
		storageID      string
		namespace      string
		otherStorageID string
		otherNamespace string
		want           bool
	}{
		{"equal", "", "mem://bucket/repo", "", "mem://bucket/repo", true},
		{"child", "", "mem://bucket/repo/child", "", "mem://bucket/repo", true},
		{"parent", "", "mem://bucket/repo", "", "mem://bucket/repo/child", true},
		{"sibling name prefix is not a segment", "", "mem://bucket/repo-metadata", "", "mem://bucket/repo", false},
		{"sibling name prefix is not a segment, reversed", "", "mem://bucket/repo", "", "mem://bucket/repo-metadata", false},
		{"different storage id", "sid-a", "mem://bucket/repo", "sid-b", "mem://bucket/repo", false},
		{"different scheme", "", "s3://bucket/repo", "", "gs://bucket/repo", false},
		{"trailing slash", "", "mem://bucket/repo/", "", "mem://bucket/repo", true},
		{"root contains everything", "", "mem://bucket", "", "mem://bucket/repo", true},
		{"root is contained by everything", "", "mem://bucket/repo", "", "mem://", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, namespacesOverlap(tc.storageID, tc.namespace, tc.otherStorageID, tc.otherNamespace))
			require.Equal(t, tc.want, namespacesOverlap(tc.otherStorageID, tc.otherNamespace, tc.storageID, tc.namespace),
				"overlap is symmetric")
		})
	}
}

func TestFindNamespaceConflict(t *testing.T) {
	ctx := t.Context()
	c := newStorageDeletionTestCatalog(t)

	createBare := func(repository, storageID, namespace string) {
		t.Helper()
		_, err := c.CreateBareRepository(ctx, repository, storageID, namespace, "main", false)
		require.NoError(t, err)
	}

	createBare("owner-equal", "", "mem://conflict/equal")
	createBare("owner-child", "", "mem://conflict/child")
	createBare("owner-parent", "", "mem://conflict/parent/deep")
	createBare("owner-sibling", "", "mem://conflict/x")
	createBare("owner-storage", "sid-a", "mem://conflict/storage")
	createBare("owner-indel", "", "mem://conflict/indel")
	setRepositoryState(t, c.KVStore, "owner-indel", graveler.RepositoryState_IN_DELETION)

	// A pending deletion record owns its namespace with no repository record left (R10).
	require.NoError(t, c.CreateRepositoryDeletion(ctx, &RepositoryDeletion{
		Repository:       "ghost-record",
		StorageID:        "",
		StorageNamespace: "mem://conflict/ghost",
		State:            RepositoryDeletionState_DELETING,
		Step:             RepositoryDeletionStep_OBJECTS,
	}))
	// A DONE record owns nothing (G5, R16).
	require.NoError(t, c.CreateRepositoryDeletion(ctx, &RepositoryDeletion{
		Repository:       "done-record",
		StorageNamespace: "mem://conflict/done",
		State:            RepositoryDeletionState_DONE,
		Step:             RepositoryDeletionStep_FINISHED,
	}))

	// Sanity: ListRepositories returns the IN_DELETION repository, so the scan sees it.
	it, err := c.Store.ListRepositories(ctx)
	require.NoError(t, err)
	t.Cleanup(it.Close)
	foundInDeletion := false
	for it.Next() {
		if it.Value().RepositoryID == "owner-indel" {
			foundInDeletion = true
			require.Equal(t, graveler.RepositoryState_IN_DELETION, it.Value().State)
		}
	}
	require.NoError(t, it.Err())
	require.True(t, foundInDeletion, "the repository iterator must return every state")

	cases := []struct {
		name    string
		targets []RepositoryDeletionTarget
		want    *NamespaceConflict
	}{
		{
			name:    "equal namespace",
			targets: []RepositoryDeletionTarget{{Repository: "new", StorageNamespace: "mem://conflict/equal"}},
			want:    &NamespaceConflict{Repository: "owner-equal", StorageNamespace: "mem://conflict/equal"},
		},
		{
			name:    "target inside owner",
			targets: []RepositoryDeletionTarget{{Repository: "new", StorageNamespace: "mem://conflict/child/deeper"}},
			want:    &NamespaceConflict{Repository: "owner-child", StorageNamespace: "mem://conflict/child"},
		},
		{
			name:    "target contains owner",
			targets: []RepositoryDeletionTarget{{Repository: "new", StorageNamespace: "mem://conflict/parent"}},
			want:    &NamespaceConflict{Repository: "owner-parent", StorageNamespace: "mem://conflict/parent/deep"},
		},
		{
			name:    "x and x-metadata do not conflict",
			targets: []RepositoryDeletionTarget{{Repository: "new", StorageNamespace: "mem://conflict/x-metadata"}},
			want:    nil,
		},
		{
			name:    "different storage id with the same path does not conflict",
			targets: []RepositoryDeletionTarget{{Repository: "new", StorageID: "sid-b", StorageNamespace: "mem://conflict/storage"}},
			want:    nil,
		},
		{
			name:    "IN_DELETION repository owns its namespace",
			targets: []RepositoryDeletionTarget{{Repository: "new", StorageNamespace: "mem://conflict/indel"}},
			want:    &NamespaceConflict{Repository: "owner-indel", StorageNamespace: "mem://conflict/indel"},
		},
		{
			name:    "pending deletion record owns its namespace",
			targets: []RepositoryDeletionTarget{{Repository: "new", StorageNamespace: "mem://conflict/ghost/child"}},
			want:    &NamespaceConflict{Repository: "ghost-record", StorageNamespace: "mem://conflict/ghost", FromDeletionRecord: true},
		},
		{
			name:    "DONE record is ignored",
			targets: []RepositoryDeletionTarget{{Repository: "new", StorageNamespace: "mem://conflict/done"}},
			want:    nil,
		},
		{
			name: "targets of the same request are excluded from each other",
			targets: []RepositoryDeletionTarget{
				{Repository: "owner-equal", StorageNamespace: "mem://conflict/equal"},
				{Repository: "owner-child", StorageNamespace: "mem://conflict/child"},
			},
			want: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := c.FindNamespaceConflict(ctx, tc.targets)
			require.NoError(t, err)
			if tc.want == nil {
				require.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			require.Equal(t, tc.want.Repository, got.Repository)
			require.Equal(t, tc.want.StorageNamespace, got.StorageNamespace)
			require.Equal(t, tc.want.StorageID, got.StorageID)
			require.Equal(t, tc.want.FromDeletionRecord, got.FromDeletionRecord)
		})
	}
}

func TestCheckRepositoryDeletionConflict(t *testing.T) {
	ctx := t.Context()
	c := newStorageDeletionTestCatalog(t)

	require.NoError(t, c.CreateRepositoryDeletion(ctx, &RepositoryDeletion{
		Repository:       "pending",
		StorageNamespace: "mem://pending-ns",
		State:            RepositoryDeletionState_PENDING,
	}))

	// The same repository name is refused while any non-DONE record exists (R11, R16).
	err := c.CheckRepositoryDeletionConflict(ctx, "pending", "", "mem://pending-ns")
	require.ErrorIs(t, err, ErrDeletionInProgress)

	// An overlapping namespace is refused, including a child of the pending namespace.
	err = c.CheckRepositoryDeletionConflict(ctx, "other", "", "mem://pending-ns/child")
	require.ErrorIs(t, err, ErrDeletionInProgress)

	// A sibling prefix is not an overlap (R10 decides on path segments).
	require.NoError(t, c.CheckRepositoryDeletionConflict(ctx, "other", "", "mem://pending-ns-metadata"))

	// A different storage ID with the same path is not an overlap.
	require.NoError(t, c.CheckRepositoryDeletionConflict(ctx, "other", "sid-b", "mem://pending-ns"))

	// A FAILED record is still pending: it blocks a create until it is retried or done.
	record, predicate, err := c.getRepositoryDeletionWithPredicate(ctx, "pending")
	require.NoError(t, err)
	record.State = RepositoryDeletionState_FAILED
	require.NoError(t, c.UpdateRepositoryDeletion(ctx, record, predicate))
	require.ErrorIs(t, c.CheckRepositoryDeletionConflict(ctx, "pending", "", "mem://pending-ns"), ErrDeletionInProgress)

	// Once DONE, both the name and the namespace are free again (G5, R16).
	record, predicate, err = c.getRepositoryDeletionWithPredicate(ctx, "pending")
	require.NoError(t, err)
	record.State = RepositoryDeletionState_DONE
	record.Step = RepositoryDeletionStep_FINISHED
	require.NoError(t, c.UpdateRepositoryDeletion(ctx, record, predicate))
	require.NoError(t, c.CheckRepositoryDeletionConflict(ctx, "pending", "", "mem://pending-ns"))
	require.NoError(t, c.CheckRepositoryDeletionConflict(ctx, "other", "", "mem://pending-ns/child"))
}

func TestCreateRepository_RefusedWhileDeletionPending(t *testing.T) {
	ctx := t.Context()
	c := newStorageDeletionTestCatalog(t)

	require.NoError(t, c.CreateRepositoryDeletion(ctx, &RepositoryDeletion{
		Repository:       "reused",
		StorageNamespace: "mem://reuse",
		State:            RepositoryDeletionState_DELETING,
		Step:             RepositoryDeletionStep_OBJECTS,
	}))

	// The same name is refused, metadata is not created.
	_, err := c.CreateRepository(ctx, "reused", "", "mem://reuse", "main", false)
	require.ErrorIs(t, err, ErrDeletionInProgress)
	_, err = c.GetRepository(ctx, "reused")
	require.ErrorIs(t, err, graveler.ErrNotFound)

	// A repository inside the pending namespace is refused too.
	_, err = c.CreateRepository(ctx, "reused-child", "", "mem://reuse/child", "main", false)
	require.ErrorIs(t, err, ErrDeletionInProgress)

	// CreateBareRepository is refused on the same rule (it skips the storage namespace check).
	_, err = c.CreateBareRepository(ctx, "reused-bare", "", "mem://reuse", "main", false)
	require.ErrorIs(t, err, ErrDeletionInProgress)
	_, err = c.CreateBareRepository(ctx, "reused", "", "mem://elsewhere", "main", false)
	require.ErrorIs(t, err, ErrDeletionInProgress)

	// A sibling prefix is allowed: it does not overlap.
	_, err = c.CreateBareRepository(ctx, "sibling", "", "mem://reuse-metadata", "main", false)
	require.NoError(t, err)

	// After DONE, the name and the namespace are allowed again (G5).
	record, predicate, err := c.getRepositoryDeletionWithPredicate(ctx, "reused")
	require.NoError(t, err)
	record.State = RepositoryDeletionState_DONE
	record.Step = RepositoryDeletionStep_FINISHED
	require.NoError(t, c.UpdateRepositoryDeletion(ctx, record, predicate))

	repo, err := c.CreateRepository(ctx, "reused", "", "mem://reuse", "main", false)
	require.NoError(t, err)
	require.Equal(t, "reused", repo.Name)
	require.Equal(t, "mem://reuse", repo.StorageNamespace)
}
