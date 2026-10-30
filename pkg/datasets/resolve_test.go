package datasets

// resolve_test.go covers §6.2 step 2 and §5.2 rule 4 on a real catalog: refs become
// commit IDs, and the three source checks (exists, is not a dataset, shares the
// storage ID) return typed errors the API can map to 400/422 (§10.4).

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
)

// TestResolver_ResolvesEveryRefTypeToACommit pins §5.2: branch, tag and commit refs are
// all stored as `type: commit`, and the input definition is not modified.
func TestResolver_ResolvesEveryRefTypeToACommit(t *testing.T) {
	f := newDatasetFixture(t)
	f.createRepository(t, "src-refs", "main", false)

	first := f.commitObjects(t, "src-refs", "main", testObject{key: "data/a.parquet", content: []byte("a")})
	second := f.commitObjects(t, "src-refs", "main", testObject{key: "data/b.parquet", content: []byte("b")})
	_, err := f.cat.CreateTag(f.ctx, "src-refs", "release-1", second.String())
	require.NoError(t, err, "create tag release-1")

	def := Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{
			{
				Target: "branch-item", Repository: "src-refs",
				Ref:  SourceRef{Type: RefTypeBranch, ID: "main"},
				Type: ItemTypeObject, Object: &PathSource{Path: "data/a.parquet"},
			},
			{
				Target: "tag-item", Repository: "src-refs",
				Ref:  SourceRef{Type: RefTypeTag, ID: "release-1"},
				Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/"},
			},
			{
				Target: "commit-item", Repository: "src-refs",
				Ref:  SourceRef{Type: RefTypeCommit, ID: first.String()},
				Type: ItemTypeObject, Object: &PathSource{Path: "data/a.parquet"},
			},
		},
	}
	require.NoError(t, Validate(&def, ValidateOptions{}))

	resolver := &Resolver{Catalog: f.cat}
	resolved, err := resolver.Resolve(f.ctx, &def)
	require.NoError(t, err)

	// Validate normalises and sorts the items by target, so index by target rather than by
	// the order they were written in.
	want := map[string]string{
		"branch-item": second.String(),
		"tag-item":    second.String(),
		"commit-item": first.String(),
	}
	for _, item := range resolved.Data {
		require.Equal(t, RefTypeCommit, item.Ref.Type, "item %q ref type", item.Target)
		require.Equal(t, want[item.Target], item.Ref.ID, "item %q ref id", item.Target)
	}

	// The submitted definition keeps its branch/tag refs: it is the caller's document.
	byTarget := make(map[string]DataItem, len(def.Data))
	for _, item := range def.Data {
		byTarget[item.Target] = item
	}
	require.Equal(t, RefTypeBranch, byTarget["branch-item"].Ref.Type)
	require.Equal(t, "main", byTarget["branch-item"].Ref.ID)
	require.Equal(t, RefTypeTag, byTarget["tag-item"].Ref.Type)
}

// TestResolver_RejectsUnknownSourceRepository pins the first half of §5.2 rule 4.
func TestResolver_RejectsUnknownSourceRepository(t *testing.T) {
	f := newDatasetFixture(t)
	def := Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{{
			Target: "t", Repository: "no-such-repo",
			Ref:  SourceRef{Type: RefTypeBranch, ID: "main"},
			Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/"},
		}},
	}

	resolver := &Resolver{Catalog: f.cat}
	_, err := resolver.Resolve(f.ctx, &def)
	require.ErrorIs(t, err, ErrSourceRepositoryNotFound)

	var sourceErr *SourceError
	require.ErrorAs(t, err, &sourceErr)
	require.Equal(t, 0, sourceErr.Item)
	require.Equal(t, "no-such-repo", sourceErr.Repository)
}

// TestResolver_RejectsNestedDataset pins the "no nesting in the MVP" half of §5.2 rule 4:
// a repository that carries the dataset marker of §6.1 cannot be a source.
func TestResolver_RejectsNestedDataset(t *testing.T) {
	f := newDatasetFixture(t)
	f.createRepository(t, "nested-src", "main", false)
	f.commitObjects(t, "nested-src", "main", testObject{key: "data/a.parquet", content: []byte("a")})
	require.NoError(t, f.cat.UpdateRepositoryMetadata(f.ctx, "nested-src", graveler.RepositoryMetadata{
		RepositoryMetadataIsDatasetKey: "true",
		RepositoryMetadataFormatKey:    "1",
	}))

	def := Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{{
			Target: "t", Repository: "nested-src",
			Ref:  SourceRef{Type: RefTypeBranch, ID: "main"},
			Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/"},
		}},
	}

	_, err := (&Resolver{Catalog: f.cat}).Resolve(f.ctx, &def)
	require.ErrorIs(t, err, ErrSourceIsDataset)
}

// TestResolver_RejectsStorageIDMismatch pins the 422 of §10.4: a source on another
// blockstore can never be materialised, because its entries are stored as full
// addresses that only the dataset's own blockstore can read (§6.3).
func TestResolver_RejectsStorageIDMismatch(t *testing.T) {
	f := newDatasetFixture(t)
	f.createRepository(t, "src-other-store", "main", false)
	f.commitObjects(t, "src-other-store", "main", testObject{key: "data/a.parquet", content: []byte("a")})

	def := Definition{
		FormatVersion: FormatVersion,
		Data: []DataItem{{
			Target: "t", Repository: "src-other-store",
			Ref:  SourceRef{Type: RefTypeBranch, ID: "main"},
			Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/"},
		}},
	}

	// The test blockstore is the single, default one, so its ID is "". Asking for
	// another storage ID is what an install with a dedicated datasets bucket does.
	_, err := (&Resolver{Catalog: f.cat, StorageID: "mem://dedicated-datasets"}).Resolve(f.ctx, &def)
	require.ErrorIs(t, err, ErrStorageIDMismatch)

	var sourceErr *SourceError
	require.ErrorAs(t, err, &sourceErr)
	require.Equal(t, "src-other-store", sourceErr.Repository)

	// The same definition resolves fine against the storage ID the sources actually use.
	_, err = (&Resolver{Catalog: f.cat}).Resolve(f.ctx, &def)
	require.NoError(t, err)
}

// TestResolver_RejectsUnknownRef pins that a stale commit, branch or tag is reported at
// resolve time rather than halfway through materialisation.
func TestResolver_RejectsUnknownRef(t *testing.T) {
	f := newDatasetFixture(t)
	f.createRepository(t, "src-stale", "main", false)
	f.commitObjects(t, "src-stale", "main", testObject{key: "data/a.parquet", content: []byte("a")})

	for _, ref := range []SourceRef{
		{Type: RefTypeBranch, ID: "no-such-branch"},
		{Type: RefTypeTag, ID: "no-such-tag"},
		{Type: RefTypeCommit, ID: "0123456789abcdef0123456789abcdef01234567"},
	} {
		t.Run(string(ref.Type)+"/"+ref.ID, func(t *testing.T) {
			def := Definition{
				FormatVersion: FormatVersion,
				Data: []DataItem{{
					Target: "t", Repository: "src-stale", Ref: ref,
					Type: ItemTypePrefix, Prefix: &PathSource{Path: "data/"},
				}},
			}
			_, err := (&Resolver{Catalog: f.cat}).Resolve(f.ctx, &def)
			require.True(t, errors.Is(err, ErrSourceRefNotFound), "want ErrSourceRefNotFound, got %v", err)
		})
	}
}

// TestResolver_RejectsMissingDefinition keeps Resolve's contract honest: it reports a
// missing document instead of panicking on it.
func TestResolver_RejectsMissingDefinition(t *testing.T) {
	f := newDatasetFixture(t)
	_, err := (&Resolver{Catalog: f.cat}).Resolve(f.ctx, nil)
	require.Error(t, err)
}

// TestSourceError_HTTPStatus pins the mapping the API uses (§10.4): a storage-ID
// mismatch is 422 (a deployment problem), the other source problems are 400, and the
// §9.3 source-read denial is a separate *PermissionError (403) returned by authz.go's
// Authorizer.CheckSources, never a *SourceError. Both implement HTTPStatusCode so the
// controller can map any resolve/publish error without type-switching.
func TestSourceError_HTTPStatus(t *testing.T) {
	require.Equal(t, http.StatusUnprocessableEntity,
		(&SourceError{Err: ErrStorageIDMismatch}).HTTPStatusCode())
	for _, err := range []error{
		ErrSourceRepositoryNotFound,
		ErrSourceIsDataset,
		ErrSourceRefNotFound,
		fmt.Errorf("%w: detail", ErrSourceKeyNotFound),
	} {
		require.Equal(t, http.StatusBadRequest, (&SourceError{Err: err}).HTTPStatusCode(), "%v", err)
	}

	// authz.go owns the 403 side of the same contract.
	require.Equal(t, http.StatusForbidden, (&PermissionError{}).HTTPStatusCode())
}
