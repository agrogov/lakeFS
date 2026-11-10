package catalog

// Tests for the Metadata Search cascade of repository deletion with storage
// (REPO_DELETE_STORAGE_DESIGN.md §6, REPO_DELETE_STORAGE_SPEC.md R17–R19).
//
// The cascade itself is installed by pkg/icebergcatalog (its own test covers the decision); what is
// pinned here is the seam and what a request does with the companion it names: two deletion records,
// two deleted namespaces, the companion's namespace taken from its own record, and the two negative
// cases - a user's own `foo-metadata` and a request without delete_storage - that must leave the
// companion exactly as it was.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

// installMetadataCompanionCascade installs the shape of the Metadata Search cascade that
// pkg/icebergcatalog installs: the companion of R is R-metadata, and only when it exists and the
// init-commit proof accepts it. It is written out here rather than imported because
// pkg/icebergcatalog imports pkg/catalog; the decision itself is IsMetadataCompanion, the same
// function the repository listing uses.
func installMetadataCompanionCascade(t *testing.T, c *Catalog) {
	t.Helper()
	c.SetDeletionCascade(func(ctx context.Context, repository *Repository) (string, bool, error) {
		companion := repository.Name + metadataRepositorySuffix
		companionRepo, err := c.GetRepository(ctx, companion)
		if errors.Is(err, graveler.ErrNotFound) {
			return "", false, nil
		}
		if err != nil {
			return "", false, err
		}
		proven, err := c.IsMetadataCompanion(ctx, companion, companionRepo.DefaultBranch)
		if err != nil || !proven {
			return "", false, nil
		}
		return companion, true, nil
	})
	t.Cleanup(func() { c.SetDeletionCascade(nil) })
}

// commitMetadataInit writes the indexer's initialisation commit (§4.8) on the companion's default
// branch: the message and the marker the proof reads, on top of lakeFS's automatic creation root,
// which is the shape a companion created the ordinary way has.
func commitMetadataInit(t *testing.T, c *Catalog, repository string) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, c.CreateEntry(ctx, repository, "main", DBEntry{
		Path:            "system/init",
		PhysicalAddress: "system/init",
		AddressType:     AddressTypeRelative,
		CreationDate:    time.Now().UTC(),
	}))
	_, err := c.Commit(ctx, repository, "main", metadataSearchInitCommitMessage, "mds-service-user",
		Metadata{metadataSearchInitProperty: metadataSearchInitValue}, nil, nil, false)
	require.NoError(t, err)
}

// setRepositoryReadOnly flips the read_only flag in the repository record, the way an operator (or
// pkg/catalog's own repository settings) would.  The test catalog runs without the repository cache,
// so the next read sees it.
func setRepositoryReadOnly(t *testing.T, c *Catalog, repository string, readOnly bool) {
	t.Helper()
	ctx := t.Context()
	key := []byte(graveler.RepoPath(graveler.RepositoryID(repository)))
	data := graveler.RepositoryData{}
	_, err := kv.GetMsg(ctx, c.KVStore, graveler.RepositoriesPartition(), key, &data)
	require.NoError(t, err)
	data.ReadOnly = readOnly
	require.NoError(t, kv.SetMsg(ctx, c.KVStore, graveler.RepositoriesPartition(), key, &data))
}

// TestDeleteRepositoryWithStorage_CascadesProvenCompanion pins R17: a request for a data repository
// that owns a proven companion writes two records and leaves both namespaces empty.  The companion's
// namespace is an override - not R's namespace with "-metadata" appended - so the test also pins that
// the cascade reads it from the companion's record instead of deriving it.
func TestDeleteRepositoryWithStorage_CascadesProvenCompanion(t *testing.T) {
	ctx := t.Context()
	c := newStorageDeletionWorkerCatalog(t, nil)
	installMetadataCompanionCascade(t, c)

	const (
		dataRepository      = "data"
		companionRepository = "data-metadata"
		dataNamespace       = "mem://cascade/data"
		companionNamespace  = "mem://cascade/override/metadata"
	)
	_, err := c.CreateRepository(ctx, dataRepository, "", dataNamespace, "main", false)
	require.NoError(t, err)
	_, err = c.CreateRepository(ctx, companionRepository, "", companionNamespace, "main", false)
	require.NoError(t, err)
	commitMetadataInit(t, c, companionRepository)
	putNamespaceObject(t, c, "", dataNamespace, "committed/object")
	putNamespaceObject(t, c, "", dataNamespace, "_lakefs/dummy")
	putNamespaceObject(t, c, "", companionNamespace, "tables/object")

	records, err := c.DeleteRepositoryWithStorage(ctx, dataRepository, DeleteStorageOptions{RequestedBy: "deleter"})
	require.NoError(t, err)
	require.Len(t, records, 2)
	require.Equal(t, dataRepository, records[0].Repository)
	require.Empty(t, records[0].CascadeOf, "the repository the request named is not a cascade")
	require.Equal(t, dataNamespace, records[0].StorageNamespace)
	require.Equal(t, companionRepository, records[1].Repository)
	require.Equal(t, dataRepository, records[1].CascadeOf)
	require.Equal(t, companionNamespace, records[1].StorageNamespace,
		"the companion's namespace is read from its record, never derived from R's")

	startRepositoryDeletionWorker(t, c)
	waitForDeletionState(t, c, dataRepository, RepositoryDeletionState_DONE, 10*time.Second)
	waitForDeletionState(t, c, companionRepository, RepositoryDeletionState_DONE, 10*time.Second)

	require.Empty(t, namespaceObjects(t, c, dataNamespace), "the data repository's namespace is empty")
	require.Empty(t, namespaceObjects(t, c, companionNamespace), "the companion's namespace is empty")
	requireRepositoryGone(t, c, dataRepository)
	requireRepositoryGone(t, c, companionRepository)
}

// TestDeleteRepositoryWithStorage_DoesNotCascadeUserMetadataRepository pins the other half of R17: a
// repository that merely ends in "-metadata" - a user's own, without the init commit - is not a
// companion, so the request is the ordinary one and the user's repository keeps its objects.
func TestDeleteRepositoryWithStorage_DoesNotCascadeUserMetadataRepository(t *testing.T) {
	ctx := t.Context()
	c := newStorageDeletionWorkerCatalog(t, nil)
	installMetadataCompanionCascade(t, c)

	const (
		dataRepository     = "data"
		userRepository     = "data-metadata"
		dataNamespace      = "mem://cascade-user/data"
		userNamespace      = "mem://cascade-user/metadata"
		userObjectAddress  = "mem://cascade-user/metadata/object"
		userObjectRelative = "object"
	)
	_, err := c.CreateRepository(ctx, dataRepository, "", dataNamespace, "main", false)
	require.NoError(t, err)
	_, err = c.CreateRepository(ctx, userRepository, "", userNamespace, "main", false)
	require.NoError(t, err)
	putNamespaceObject(t, c, "", dataNamespace, "object")
	putNamespaceObject(t, c, "", userNamespace, userObjectRelative)

	records, err := c.DeleteRepositoryWithStorage(ctx, dataRepository, DeleteStorageOptions{})
	require.NoError(t, err)
	require.Len(t, records, 1, "a repository without the init commit is not a companion")
	require.Equal(t, dataRepository, records[0].Repository)

	startRepositoryDeletionWorker(t, c)
	waitForDeletionState(t, c, dataRepository, RepositoryDeletionState_DONE, 10*time.Second)

	_, err = c.GetRepository(ctx, userRepository)
	require.NoError(t, err, "the user's own data-metadata repository is untouched")
	require.Contains(t, namespaceObjects(t, c, userNamespace), userObjectAddress)
}

// TestDeleteRepositoryWithStorage_ReadOnlyCompanionRefusesTheRequest pins R19: the companion's
// refusal checks run with R's, before anything is written, so a read-only companion refuses the
// whole request - and the error names the repository that refused.
func TestDeleteRepositoryWithStorage_ReadOnlyCompanionRefusesTheRequest(t *testing.T) {
	ctx := t.Context()
	c := newStorageDeletionWorkerCatalog(t, nil)
	installMetadataCompanionCascade(t, c)

	const (
		dataRepository      = "data"
		companionRepository = "data-metadata"
		dataNamespace       = "mem://r19/data"
		companionNamespace  = "mem://r19/data-metadata"
	)
	_, err := c.CreateRepository(ctx, dataRepository, "", dataNamespace, "main", false)
	require.NoError(t, err)
	_, err = c.CreateRepository(ctx, companionRepository, "", companionNamespace, "main", false)
	require.NoError(t, err)
	commitMetadataInit(t, c, companionRepository)
	putNamespaceObject(t, c, "", dataNamespace, "object")
	putNamespaceObject(t, c, "", companionNamespace, "object")
	setRepositoryReadOnly(t, c, companionRepository, true)

	before := snapshotDeletionState(t, c, dataNamespace, companionNamespace)
	// force does not bypass R8, and it does not bypass the companion's R8 either (R4, R19).
	_, err = c.DeleteRepositoryWithStorage(ctx, dataRepository, DeleteStorageOptions{Force: true})
	require.ErrorIs(t, err, graveler.ErrReadOnlyRepository)
	require.Contains(t, err.Error(), companionRepository, "the response names the repository that refused (R19)")
	require.Equal(t, before, snapshotDeletionState(t, c, dataNamespace, companionNamespace),
		"a refused request changes nothing: neither the repositories nor the deletion records nor the objects")
}

// TestDeleteRepository_CompanionUntouchedWithoutDeleteStorage pins R18: a plain delete keeps today's
// behaviour, which for a data repository means its companion is left exactly as it is
// (METADATA_SEARCH_DESIGN.md:451).
func TestDeleteRepository_CompanionUntouchedWithoutDeleteStorage(t *testing.T) {
	ctx := t.Context()
	c := newStorageDeletionWorkerCatalog(t, nil)
	installMetadataCompanionCascade(t, c)

	const (
		dataRepository      = "data"
		companionRepository = "data-metadata"
		dataNamespace       = "mem://r18/data"
		companionNamespace  = "mem://r18/data-metadata"
	)
	_, err := c.CreateRepository(ctx, dataRepository, "", dataNamespace, "main", false)
	require.NoError(t, err)
	_, err = c.CreateRepository(ctx, companionRepository, "", companionNamespace, "main", false)
	require.NoError(t, err)
	commitMetadataInit(t, c, companionRepository)
	putNamespaceObject(t, c, "", companionNamespace, "tables/object")

	require.NoError(t, c.DeleteRepository(ctx, dataRepository))
	requireRepositoryGone(t, c, dataRepository)

	_, err = c.GetRepository(ctx, companionRepository)
	require.NoError(t, err, "without delete_storage the companion is not deleted")
	records, err := c.ListRepositoryDeletions(ctx)
	require.NoError(t, err)
	require.Empty(t, records, "without delete_storage no storage deletion is requested at all")
	require.Contains(t, namespaceObjects(t, c, companionNamespace), "mem://r18/data-metadata/tables/object",
		"the companion's objects are still there (the commit also wrote its _lakefs/ ranges)")
}

// TestCatalogIsMetadataCompanionMatchesTheListingFilter pins that the exported proof and the listing
// filter cannot disagree: they are one function, so a repository the listing hides is exactly one a
// cascade would delete.
func TestCatalogIsMetadataCompanionMatchesTheListingFilter(t *testing.T) {
	ctx := t.Context()
	c := newStorageDeletionWorkerCatalog(t, nil)

	_, err := c.CreateRepository(ctx, "proven-metadata", "", "mem://proof/proven", "main", false)
	require.NoError(t, err)
	commitMetadataInit(t, c, "proven-metadata")
	_, err = c.CreateRepository(ctx, "own-metadata", "", "mem://proof/own", "main", false)
	require.NoError(t, err)
	_, err = c.CreateRepository(ctx, "plain", "", "mem://proof/plain", "main", false)
	require.NoError(t, err)

	for _, test := range []struct {
		repository string
		want       bool
	}{
		{repository: "proven-metadata", want: true},
		{repository: "own-metadata", want: false},
		{repository: "plain", want: false},
	} {
		t.Run(test.repository, func(t *testing.T) {
			proven, err := c.IsMetadataCompanion(ctx, test.repository, "main")
			require.NoError(t, err)
			require.Equal(t, test.want, proven)
			hidden, err := c.isHiddenMetadataCompanion(ctx, test.repository, "main")
			require.NoError(t, err)
			require.Equal(t, proven, hidden, "the listing and the cascade share one proof")
		})
	}
}
