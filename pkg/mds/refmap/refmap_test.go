package refmap

// Tests for the Metadata Search ref mapper (refmap.go, METADATA_SEARCH_DESIGN.md §6, task M5a).
//
// The fake below is a catalog that answers the two questions the mapping asks and nothing else:
// GetCommit resolves a ref the way lakeFS does (the test spells out what each ref resolves to, so
// a tag, a full ID, a short ID and an expression are all one entry in `commits`), and GetTag names
// the tag the indexer recorded in the companion repository. Every call is recorded, so a test can
// assert not only the answer but the absence of the lookup that would produce a wrong one - the
// ancestor fallback of §6.
//
// The last test is the wiring the design asks for: icebergcatalog.NewMetadataSearchResolver with
// this mapper answers a tag of R with the `mds-<id>` ref of R-metadata.

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
)

// fakeCatalog is a refResolver and a BranchLister over fixed answers. A nil map means "no such
// entry", which the fake reports with the graveler not-found error lakeFS reports.
type fakeCatalog struct {
	commits   map[string]string // repo@ref -> commit ID the ref names
	tags      map[string]string // repo@tagID -> commit ID the tag points at
	branches  map[string]string // repo@branch -> commit ID of its head
	commitErr map[string]error  // repo@ref -> error instead of an answer
	tagErr    map[string]error  // repo@tagID -> error instead of an answer
	nilCommit map[string]bool   // repo@ref -> answer with a nil commit and no error

	gotRefs []string // repo@ref passed to GetCommit, in call order
	gotTags []string // repo@tagID passed to GetTag, in call order
}

func refKey(repo, ref string) string { return repo + "@" + ref }

func (f *fakeCatalog) GetCommit(_ context.Context, repositoryID, reference string) (*catalog.CommitLog, error) {
	key := refKey(repositoryID, reference)
	f.gotRefs = append(f.gotRefs, key)
	if err := f.commitErr[key]; err != nil {
		return nil, err
	}
	if f.nilCommit[key] {
		return nil, nil
	}
	commitID, ok := f.commits[key]
	if !ok {
		return nil, graveler.ErrCommitNotFound
	}
	return &catalog.CommitLog{Reference: commitID}, nil
}

func (f *fakeCatalog) GetTag(_ context.Context, repositoryID, tagID string) (string, error) {
	key := refKey(repositoryID, tagID)
	f.gotTags = append(f.gotTags, key)
	if err := f.tagErr[key]; err != nil {
		return "", err
	}
	commitID, ok := f.tags[key]
	if !ok {
		return "", graveler.ErrTagNotFound
	}
	return commitID, nil
}

func (f *fakeCatalog) GetBranchReference(_ context.Context, repositoryID, branch string) (string, error) {
	commitID, ok := f.branches[refKey(repositoryID, branch)]
	if !ok {
		return "", graveler.ErrBranchNotFound
	}
	return commitID, nil
}

// listerOnly is a BranchLister that is not a refResolver: the wiring mistake New must survive.
type listerOnly struct{}

func (listerOnly) GetBranchReference(_ context.Context, _, _ string) (string, error) {
	return "", graveler.ErrBranchNotFound
}

func TestNew(t *testing.T) {
	t.Run("a catalog with GetCommit and GetTag installs a mapper", func(t *testing.T) {
		refs := &fakeCatalog{
			commits: map[string]string{refKey("lakehouse", "main"): "c1"},
			tags:    map[string]string{refKey("lakehouse-metadata", "mds-c1"): "c1"},
		}
		mapper := New(refs)
		require.NotNil(t, mapper)
		mapped, err := mapper.MapRef(t.Context(), "lakehouse", "main")
		require.NoError(t, err)
		require.Equal(t, "mds-c1", mapped)
	})

	t.Run("a branch lister that cannot resolve refs is refused", func(t *testing.T) {
		require.Nil(t, New(listerOnly{}), "no mapper keeps the documented default of §5.1")
	})

	t.Run("a nil branch lister is refused", func(t *testing.T) {
		require.Nil(t, New(nil), "no mapper keeps the documented default of §5.1")
	})
}

func TestMapRef(t *testing.T) {
	errBoom := errors.New("storage failure")

	tests := []struct {
		name string
		refs *fakeCatalog
		ref  string
		// wantTag is the mapped ref. A test with wantNoTable leaves it empty.
		wantTag string
		// wantNoTable is the §6 refusal: the ref names no indexed commit.
		wantNoTable bool
		// wantErr is an error returned as it is (not turned into NoSuchTableException).
		wantErr error
		// wantTagLookups is the exact list of GetTag lookups: the mapping asks about one tag and
		// never about an ancestor's.
		wantTagLookups []string
	}{
		{
			name: "a tag maps to the tag of the commit it names",
			refs: &fakeCatalog{
				commits: map[string]string{refKey("lakehouse", "v1.0"): "c1"},
				tags:    map[string]string{refKey("lakehouse-metadata", "mds-c1"): "c1"},
			},
			ref:            "v1.0",
			wantTag:        "mds-c1",
			wantTagLookups: []string{refKey("lakehouse-metadata", "mds-c1")},
		},
		{
			name: "a full commit ID maps to its tag",
			refs: &fakeCatalog{
				commits: map[string]string{refKey("lakehouse", "0123456789abcdef"): "0123456789abcdef"},
				tags:    map[string]string{refKey("lakehouse-metadata", "mds-0123456789abcdef"): "0123456789abcdef"},
			},
			ref:            "0123456789abcdef",
			wantTag:        "mds-0123456789abcdef",
			wantTagLookups: []string{refKey("lakehouse-metadata", "mds-0123456789abcdef")},
		},
		{
			name: "a short commit ID maps to the tag of the commit it abbreviates",
			refs: &fakeCatalog{
				commits: map[string]string{refKey("lakehouse", "0123456"): "0123456789abcdef"},
				tags:    map[string]string{refKey("lakehouse-metadata", "mds-0123456789abcdef"): "0123456789abcdef"},
			},
			ref:            "0123456",
			wantTag:        "mds-0123456789abcdef",
			wantTagLookups: []string{refKey("lakehouse-metadata", "mds-0123456789abcdef")},
		},
		{
			name: "a ref expression maps to the commit it selects",
			refs: &fakeCatalog{
				commits: map[string]string{refKey("lakehouse", "main~1"): "c0"},
				tags:    map[string]string{refKey("lakehouse-metadata", "mds-c0"): "c0"},
			},
			ref:            "main~1",
			wantTag:        "mds-c0",
			wantTagLookups: []string{refKey("lakehouse-metadata", "mds-c0")},
		},
		{
			name: "an unindexed commit is refused, never answered with an ancestor's tag",
			refs: &fakeCatalog{
				commits: map[string]string{refKey("lakehouse", "c2"): "c2"},
				// The ancestor c1 is indexed; c2 is not. Answering mds-c1 would return c1's rows.
				tags: map[string]string{refKey("lakehouse-metadata", "mds-c1"): "c1"},
			},
			ref:            "c2",
			wantNoTable:    true,
			wantTagLookups: []string{refKey("lakehouse-metadata", "mds-c2")},
		},
		{
			name: "a ref that names no commit is refused",
			refs: &fakeCatalog{
				commits: map[string]string{refKey("lakehouse", "main"): "c1"},
			},
			ref:         "v9.9",
			wantNoTable: true,
		},
		{
			name: "a missing companion repository is refused",
			refs: &fakeCatalog{
				commits: map[string]string{refKey("lakehouse", "v1.0"): "c1"},
				tagErr:  map[string]error{refKey("lakehouse-metadata", "mds-c1"): graveler.ErrRepositoryNotFound},
			},
			ref:            "v1.0",
			wantNoTable:    true,
			wantTagLookups: []string{refKey("lakehouse-metadata", "mds-c1")},
		},
		{
			name: "a nil commit from the resolver is refused",
			refs: &fakeCatalog{
				nilCommit: map[string]bool{refKey("lakehouse", "v1.0"): true},
			},
			ref:         "v1.0",
			wantNoTable: true,
		},
		{
			name: "a commit resolution failure that is not not-found is returned as it is",
			refs: &fakeCatalog{
				commitErr: map[string]error{refKey("lakehouse", "v1.0"): errBoom},
			},
			ref:     "v1.0",
			wantErr: errBoom,
		},
		{
			name: "a tag lookup failure that is not not-found is returned as it is",
			refs: &fakeCatalog{
				commits: map[string]string{refKey("lakehouse", "v1.0"): "c1"},
				tagErr:  map[string]error{refKey("lakehouse-metadata", "mds-c1"): errBoom},
			},
			ref:            "v1.0",
			wantErr:        errBoom,
			wantTagLookups: []string{refKey("lakehouse-metadata", "mds-c1")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapped, err := New(tt.refs).MapRef(t.Context(), "lakehouse", tt.ref)

			require.Equal(t, []string{refKey("lakehouse", tt.ref)}, tt.refs.gotRefs,
				"GetCommit is asked about the requested ref, unrewritten")
			require.Equal(t, tt.wantTagLookups, tt.refs.gotTags, "GetTag is asked about the companion repository only")

			switch {
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
				var noTable *icebergcatalog.NoSuchTableException
				require.NotErrorAs(t, err, &noTable, "a failure is not a missing table")
			case tt.wantNoTable:
				var noTable *icebergcatalog.NoSuchTableException
				require.ErrorAs(t, err, &noTable)
				require.Equal(t, "lakehouse", noTable.Repo)
				require.Equal(t, tt.ref, noTable.Ref)
				require.Equal(t, icebergcatalog.Namespace{icebergcatalog.SystemNamespace}, noTable.Namespace)
				require.Equal(t, icebergcatalog.ObjectMetadataTable, noTable.Table)
			default:
				require.NoError(t, err)
				require.Equal(t, tt.wantTag, mapped)
			}
			if tt.wantNoTable || tt.wantErr != nil {
				require.Empty(t, mapped, "a refused ref maps to nothing")
			}
		})
	}
}

// objectMetadataAddress is the virtual table of a data repository (§5.2).
func objectMetadataAddress(repo, ref string) icebergcatalog.Address {
	return icebergcatalog.Address{
		Repo:      repo,
		Ref:       ref,
		Namespace: icebergcatalog.Namespace{icebergcatalog.SystemNamespace},
		Table:     icebergcatalog.ObjectMetadataTable,
	}
}

func TestMetadataSearchResolverWiring(t *testing.T) {
	ctx := t.Context()

	t.Run("a tag of R resolves to the mds ref of R-metadata", func(t *testing.T) {
		refs := &fakeCatalog{
			commits: map[string]string{refKey("lakehouse", "v1.0"): "c1"},
			tags:    map[string]string{refKey("lakehouse-metadata", "mds-c1"): "c1"},
		}
		resolver := icebergcatalog.NewMetadataSearchResolver(refs, New(refs))
		addr := objectMetadataAddress("lakehouse", "v1.0")

		resolved, handled, err := resolver.Resolve(ctx, addr)
		require.NoError(t, err)
		require.True(t, handled)
		require.Equal(t, icebergcatalog.Address{
			Repo:      "lakehouse-metadata",
			Ref:       "mds-c1",
			Namespace: icebergcatalog.Namespace{icebergcatalog.SystemNamespace},
			Table:     icebergcatalog.ObjectMetadataTable,
		}, resolved.Target)
		require.Equal(t, addr, resolved.Authz, "authorization stays on the data repository (§5.3)")
	})

	t.Run("a branch of R is served by the branch of R-metadata without the mapper", func(t *testing.T) {
		refs := &fakeCatalog{
			branches: map[string]string{refKey("lakehouse", "main"): "c1"},
			commits:  map[string]string{refKey("lakehouse", "main"): "c1"},
			tags:     map[string]string{refKey("lakehouse-metadata", "mds-c1"): "c1"},
		}
		resolver := icebergcatalog.NewMetadataSearchResolver(refs, New(refs))

		resolved, handled, err := resolver.Resolve(ctx, objectMetadataAddress("lakehouse", "main"))
		require.NoError(t, err)
		require.True(t, handled)
		require.Equal(t, "main", resolved.Target.Ref)
		require.Equal(t, "lakehouse-metadata", resolved.Target.Repo)
		require.Empty(t, refs.gotRefs, "a branch needs no commit lookup")
	})

	t.Run("an unindexed tag of R is NoSuchTableException", func(t *testing.T) {
		refs := &fakeCatalog{
			commits: map[string]string{refKey("lakehouse", "v2.0"): "c2"},
			tags:    map[string]string{refKey("lakehouse-metadata", "mds-c1"): "c1"},
		}
		resolver := icebergcatalog.NewMetadataSearchResolver(refs, New(refs))

		_, handled, err := resolver.Resolve(ctx, objectMetadataAddress("lakehouse", "v2.0"))
		require.True(t, handled)
		var noTable *icebergcatalog.NoSuchTableException
		require.ErrorAs(t, err, &noTable)
		require.Equal(t, "v2.0", noTable.Ref)
	})

	t.Run("a mapper refused by New keeps the resolver's default refusal", func(t *testing.T) {
		resolver := icebergcatalog.NewMetadataSearchResolver(listerOnly{}, New(listerOnly{}))

		_, handled, err := resolver.Resolve(ctx, objectMetadataAddress("lakehouse", "v1.0"))
		require.True(t, handled)
		var noTable *icebergcatalog.NoSuchTableException
		require.ErrorAs(t, err, &noTable)
	})
}
