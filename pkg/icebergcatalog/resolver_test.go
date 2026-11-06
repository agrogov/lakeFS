package icebergcatalog

// Tests for the table-resolution hook (resolver.go, METADATA_SEARCH_DESIGN.md §5.1-5.4, task
// R10-1): the built-in virtual table of §5.2, the authorization rule of §5.3, the read-only system
// namespace of §5.4, the config switch of §5.5, and the "no registration, no change" promise the
// whole hook rests on.
//
// The HTTP half drives the router RoutesBuilder mounts - both addressing forms, the production
// middleware stack, real credentials - over a real catalog holding two repositories: the data
// repository of tables_test.go and its companion `lakehouse-metadata`, whose `system` namespace
// carries an `object_metadata` table the way the indexer leaves it. That is the fixture the design
// asks for: the virtual table is read out of a repository the caller has no grant on, and the
// caller's single permission check is on the data repository.
//
// The unit half exercises the three things the HTTP layer cannot reach directly: the resolver's own
// answer for every ref kind (§5.2), the default no-op of a store without a resolver, and the
// redirection of the two data-plane paths - credentials (creds.go) and remote signing (sign.go) -
// which both resolve the table to scope a session or a signature to the files that will be read.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	// resolverMetaRepo is the companion repository of tablesTestRepo (METADATA_SEARCH_DESIGN.md
	// §3.2), resolverMetaUUID the table uuid its object_metadata table carries.
	resolverMetaRepo = tablesTestRepo + MetadataRepositorySuffix
	resolverMetaUUID = "66666666-6666-6666-6666-666666666666"

	// resolverObjectMetadataTable is the virtual table's name, spelled out so a test reads as the
	// address a client sends.
	resolverObjectMetadataTable = "object_metadata"
)

// resolverCatalogReadStatements grants the read half of §7.2 on every catalog ARN and
// fs:ReadRepository on one repository. It is the shape §5.3 describes: catalog ARNs are
// branch-agnostic and repository-scoped, so "may browse R" is exactly this grant on R - and it says
// nothing about R-metadata.
func resolverCatalogReadStatements(repo string) model.Statements {
	return model.Statements{
		{
			Effect: model.StatementEffectAllow,
			Action: []string{
				permissions.CatalogListNamespacesAction,
				permissions.CatalogGetNamespaceAction,
				permissions.CatalogListTablesAction,
				permissions.CatalogReadTableAction,
				permissions.CatalogReadTableDataAction,
			},
			Resource: "arn:lakefs:catalog:::*",
		},
		{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.ReadRepositoryAction},
			Resource: permissions.RepoArn(repo),
		},
	}
}

// resolverCatalogWriteStatements is resolverCatalogReadStatements plus the write half of §7.2 on the
// same repository: the catalog write actions and fs:WriteObject on its reserved area.
func resolverCatalogWriteStatements(repo string) model.Statements {
	return append(resolverCatalogReadStatements(repo),
		model.Statement{
			Effect: model.StatementEffectAllow,
			Action: []string{
				permissions.CatalogCreateNamespaceAction,
				permissions.CatalogUpdateNamespaceAction,
				permissions.CatalogDeleteNamespaceAction,
				permissions.CatalogCreateTableAction,
				permissions.CatalogUpdateTableAction,
				permissions.CatalogDeleteTableAction,
				permissions.CatalogCreateViewAction,
				permissions.CatalogUpdateViewAction,
				permissions.CatalogDeleteViewAction,
			},
			Resource: "arn:lakefs:catalog:::*",
		},
		model.Statement{
			Effect:   model.StatementEffectAllow,
			Action:   []string{permissions.WriteObjectAction},
			Resource: permissions.ObjectArn(repo, icebergWriteObjectKey),
		})
}

// resolverCredential is one test principal's access key pair, because the fixture drives the
// catalog through the production authentication middleware: a catalog request carries credentials,
// not a test header.
type resolverCredential struct {
	accessKeyID     string
	secretAccessKey string
}

// resolverFixture is the catalog with Metadata Search enabled: the data repository of
// tables_test.go, its companion repository, and the router RoutesBuilder mounts when
// iceberg.metadata_search.enabled is true.
type resolverFixture struct {
	*tablesFixture

	// store is a second store over the same catalog, built with the built-in resolver. It is what
	// the credentials and signing redirection is asserted against (resolveScope, tableLocation).
	store *Store

	mux         *chi.Mux
	credentials map[string]resolverCredential

	// metaStorageNamespace is the storage namespace of resolverMetaRepo and metaLocation the
	// metadata.json of its object_metadata table. metaMetadataJSON is that document, with the
	// properties the fixture gave it (MDS-SEC-3).
	metaStorageNamespace string
	metaLocation         string
	metaMetadataJSON     string
}

// newResolverFixture builds the two-repository fixture and mounts the production catalog with
// Metadata Search enabled. The companion table carries the lakefs.mds.repository property the
// indexer writes, so it is a genuine index of tablesTestRepo.
func newResolverFixture(t *testing.T) *resolverFixture {
	t.Helper()
	return newResolverFixtureWithTableProperty(t, tablesTestRepo)
}

// newResolverFixtureWithTableProperty builds the same fixture with the lakefs.mds.repository
// property of the companion table set to repositoryProperty, or with no such property at all when
// repositoryProperty is empty: a genuine index, a table that names another repository, and a table
// that names none (MDS-SEC-3).
func newResolverFixtureWithTableProperty(t *testing.T, repositoryProperty string) *resolverFixture {
	t.Helper()
	return newResolverFixtureWithCompanion(t, repositoryProperty, true)
}

// newResolverFixtureWithoutInitCommit builds the forged-companion fixture: the table carries the
// right lakefs.mds.repository property, but the repository has no indexer initialisation commit.
func newResolverFixtureWithoutInitCommit(t *testing.T) *resolverFixture {
	t.Helper()
	return newResolverFixtureWithCompanion(t, tablesTestRepo, false)
}

// newResolverFixtureWithInitCommitter builds a companion repository whose init commit carries the
// marker and the message but is committed by committer: the ownership proof must not accept a
// commit written by anyone other than the indexer principal.
func newResolverFixtureWithInitCommitter(t *testing.T, committer string) *resolverFixture {
	t.Helper()
	return newResolverFixtureWithCompanionAndInit(t, tablesTestRepo, true, committer)
}

// newResolverFixtureWithCompanion builds the same fixture with the companion table's repository
// property set to repositoryProperty and, when initCommit is true, the indexer's initialisation
// commit on the mirrored branch.
func newResolverFixtureWithCompanion(t *testing.T, repositoryProperty string, initCommit bool) *resolverFixture {
	t.Helper()
	return newResolverFixtureWithCompanionAndInit(t, repositoryProperty, initCommit, MetadataSearchIndexerPrincipal)
}

// newResolverFixtureWithCompanionAndInit builds the fixture with an explicit init-commit committer.
func newResolverFixtureWithCompanionAndInit(t *testing.T, repositoryProperty string, initCommit bool, initCommitter string) *resolverFixture {
	t.Helper()
	base := newTablesFixture(t)
	ctx := base.ctx

	metaStorageNamespace := fmt.Sprintf("mem://iceberg-mds-%d-ns", tablesFixtureSeq.Add(1))
	_, err := base.catalog.CreateRepository(ctx, resolverMetaRepo, "", metaStorageNamespace,
		tablesTestRef, false)
	require.NoError(t, err, "create the companion repository")

	// The state the indexer leaves behind (§3.2): a system namespace marker and the
	// object_metadata table on the mirrored branch.
	metaNamespaceAddr := Address{Repo: resolverMetaRepo, Ref: tablesTestRef, Namespace: Namespace{SystemNamespace}}
	require.NoError(t, base.store.CreateNamespace(ctx, metaNamespaceAddr, map[string]string{"owner": "mds"}),
		"create the system namespace of the companion repository")

	metaLocation := strings.TrimRight(metaStorageNamespace, "/") + "/_iceberg/" +
		resolverMetaUUID + "/metadata/00001.metadata.json"
	properties := map[string]string{"owner": "data-team"}
	if repositoryProperty != "" {
		properties[MetadataSearchRepositoryProperty] = repositoryProperty
	}
	metaMetadataJSON := resolverMetadataWithProperties(t, resolverMetaUUID,
		strings.TrimRight(metaStorageNamespace, "/")+"/_iceberg/"+resolverMetaUUID, properties)
	putResolverObject(t, base.catalog, resolverMetaRepo, metaLocation, metaMetadataJSON)
	tableAddr := metaNamespaceAddr
	tableAddr.Table = resolverObjectMetadataTable
	require.NoError(t, base.store.CreateTable(ctx, tableAddr, TablePointer{
		MetadataLocation: metaLocation,
		TableUUID:        resolverMetaUUID,
		FormatVersion:    2,
	}), "create system.object_metadata in the companion repository")
	if initCommit {
		commitResolverInitAs(t, base.catalog, resolverMetaRepo, tablesTestRef, tablesTestRepo, initCommitter, false)
	}

	// alice may read the data repository (tablesReadStatements, fs on lakehouse only), carol may
	// write it, dana may list one namespace of it, bob may do nothing.
	base.authz.user(t, "mds-meta")
	base.authz.attachStatements(t, "meta-reader", "mds-meta", resolverCatalogReadStatements(resolverMetaRepo))
	base.authz.user(t, "mds-meta-writer")
	base.authz.attachStatements(t, "meta-writer", "mds-meta-writer", resolverCatalogWriteStatements(resolverMetaRepo))

	users := []string{"alice", "bob", "carol", "dana", "mds-meta", "mds-meta-writer"}
	credentials := make(map[string]resolverCredential, len(users))
	for i, username := range users {
		credential := resolverCredential{
			accessKeyID:     fmt.Sprintf("AKIAMDS%02d", i),
			secretAccessKey: fmt.Sprintf("metadata-search-test-secret-0000-%04d", i),
		}
		_, err := base.authz.svc.AddCredentials(ctx, username,
			credential.accessKeyID, credential.secretAccessKey)
		require.NoErrorf(t, err, "add credentials for %s", username)
		credentials[username] = credential
	}

	// RoutesBuilder reads iceberg.metadata_search.enabled through LoadConfig, and it is the switch
	// that registers the built-in resolver (§5.5).
	useConfig(t, "iceberg:\n  enabled: true\n  metadata_search:\n    enabled: true\n")

	kvStore, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	require.NoError(t, err, "open the server's KV store")
	t.Cleanup(kvStore.Close)

	deps := cmdpkg.ServerDeps{
		Config:        &config.ConfigImpl{},
		AuthService:   base.authz.svc,
		Authenticator: auth.NewBuiltinAuthenticator(base.authz.svc),
		Catalog:       base.catalog,
		BlockAdapter:  base.catalog.BlockAdapter,
		KVStore:       kvStore,
		Logger:        logging.ContextUnavailable(),
	}
	mux := coreRouter()
	require.NoError(t, RoutesBuilder(ctx, deps, mux), "mount the catalog")

	return &resolverFixture{
		tablesFixture:        base,
		store:                NewStore(base.catalog, base.catalog.BlockAdapter, WithTableResolver(NewMetadataSearchResolver(base.catalog, nil))),
		mux:                  mux,
		credentials:          credentials,
		metaStorageNamespace: metaStorageNamespace,
		metaLocation:         metaLocation,
		metaMetadataJSON:     metaMetadataJSON,
	}
}

// request sends one request to the mounted catalog as user, with an optional JSON body.
func (f *resolverFixture) request(t *testing.T, method, target, user, body string) *httptest.ResponseRecorder {
	t.Helper()
	credential, ok := f.credentials[user]
	require.Truef(t, ok, "no credentials for user %q", user)

	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	// The catalog is mounted at APIBasePath, like it is in the binary; a test names the catalog
	// path itself, so the mount prefix is added here.
	if !strings.HasPrefix(target, "/iceberg") {
		target = APIBasePath + target
	}
	req := httptest.NewRequest(method, target, reader)
	req.SetBasicAuth(credential.accessKeyID, credential.secretAccessKey)
	if body != "" {
		req.Header.Set("Content-Type", contentTypeJSON)
	}
	rec := httptest.NewRecorder()
	f.mux.ServeHTTP(rec, req)
	return rec
}

// resolverMetadataWithProperties renders the fixture's metadata document with its `properties`
// object replaced, so a test can give the companion table the lakefs.mds.repository property the
// indexer writes - or leave it out (MDS-SEC-3). The document is edited as raw JSON so that the
// snapshot IDs, which do not fit a float64, are passed through unchanged.
func resolverMetadataWithProperties(t *testing.T, tableUUID, tableLocation string, properties map[string]string) string {
	t.Helper()
	document := make(map[string]json.RawMessage)
	require.NoError(t, json.Unmarshal([]byte(tablesTestMetadata(tableUUID, tableLocation)), &document))
	rendered, err := json.Marshal(properties)
	require.NoError(t, err)
	document["properties"] = rendered
	encoded, err := json.Marshal(document)
	require.NoError(t, err)
	return string(encoded)
}

// putResolverObject writes content at the full identifier location of repository, the way the
// indexer (or any Iceberg writer) puts a metadata.json into the companion repository.
func putResolverObject(t *testing.T, cat *catalog.Catalog, repository, location, content string) {
	t.Helper()
	ctx := t.Context()
	repo, err := cat.GetRepository(ctx, repository)
	require.NoErrorf(t, err, "get repository %s", repository)
	_, err = cat.BlockAdapter.Put(ctx, block.ObjectPointer{
		StorageID:        repo.StorageID,
		StorageNamespace: repo.StorageNamespace,
		Identifier:       location,
		IdentifierType:   block.IdentifierTypeFull,
	}, int64(len(content)), strings.NewReader(content), block.PutOpts{})
	require.NoErrorf(t, err, "put %s", location)
}

// commitResolverInit writes the indexer's initialisation commit (METADATA_SEARCH_DESIGN.md §4.8)
// on a companion repository: message "mds: init" and metadata lakefs.mds.init=true, with the data
// repository it indexes. It commits everything staged before it, exactly as the indexer's initialise
// does when it creates the namespace and the table.
func commitResolverInit(t *testing.T, cat *catalog.Catalog, repository, branch, dataRepo string) {
	t.Helper()
	commitResolverInitAs(t, cat, repository, branch, dataRepo, MetadataSearchIndexerPrincipal, false)
}

// commitResolverInitAs is commitResolverInit with an explicit committer: the ownership proof reads
// the committer, so a forged init commit is a commit with the marker and the message written by
// someone else. allowEmpty lets a test place a different commit between the root and the init
// commit.
func commitResolverInitAs(t *testing.T, cat *catalog.Catalog, repository, branch, dataRepo, committer string, allowEmpty bool) {
	t.Helper()
	_, err := cat.Commit(t.Context(), repository, branch, MetadataSearchInitCommitMessage, committer,
		catalog.Metadata{
			MetadataSearchInitProperty:     MetadataSearchInitValue,
			"lakefs.mds.source-repository": dataRepo,
		}, nil, nil, allowEmpty)
	require.NoErrorf(t, err, "commit the initialisation of %s@%s", repository, branch)
}

// addResolverInitCommitRecord writes the indexer's initialisation commit as a commit record without
// a new meta-range and moves the branch to it, for a fixture whose repository lives at an s3://
// namespace the fixture's catalog block adapter cannot write. It is the same history the ownership
// proof reads: an mds: init commit on the first-parent chain above the automatic root.
func addResolverInitCommitRecord(t *testing.T, cat *catalog.Catalog, repository, branch, dataRepo string) {
	t.Helper()
	ctx := t.Context()
	head, err := cat.GetCommit(ctx, repository, branch)
	require.NoErrorf(t, err, "head of %s@%s", repository, branch)
	commit := graveler.Commit{
		Version:      graveler.CommitVersion(head.Version),
		Committer:    MetadataSearchIndexerPrincipal,
		Message:      MetadataSearchInitCommitMessage,
		MetaRangeID:  graveler.MetaRangeID(head.MetaRangeID),
		CreationDate: head.CreationDate,
		Parents:      graveler.CommitParents{graveler.CommitID(head.Reference)},
		Metadata: graveler.Metadata{
			MetadataSearchInitProperty:     MetadataSearchInitValue,
			"lakefs.mds.source-repository": dataRepo,
		},
		Generation: graveler.CommitGeneration(head.Generation),
	}
	commitID := ident.NewHexAddressProvider().ContentAddress(commit)
	err = cat.CreateCommitRecord(ctx, repository, commitID, int(commit.Version), commit.Committer,
		commit.Message, string(commit.MetaRangeID), commit.CreationDate.Unix(),
		[]string{head.Reference}, map[string]string(commit.Metadata), int32(commit.Generation))
	require.NoErrorf(t, err, "create the initialisation commit of %s@%s", repository, branch)
	require.NoErrorf(t, cat.HardResetBranch(ctx, repository, branch, commitID),
		"move %s@%s to its initialisation commit", repository, branch)
}

// virtualTarget is the /iceberg/api load/head URL of the virtual table on one ref of the data
// repository.
func virtualTarget(ref string) string {
	return tablesLoadTarget(nodes(tablesTestRepo, ref, SystemNamespace), resolverObjectMetadataTable, nil)
}

// virtualNamespaceTablesTarget is the /iceberg/api table listing URL of the system namespace of the
// data repository.
func virtualNamespaceTablesTarget(ref string) string {
	return tablesListTarget(nodes(tablesTestRepo, ref, SystemNamespace), nil)
}

// ---------------------------------------------------------------------------------------------
// The hook itself (§5.1, §5.2)
// ---------------------------------------------------------------------------------------------

// resolverBranchLister is a BranchLister with a fixed set of branches: the resolver only needs the
// branch/tag/commit distinction, not a repository.
type resolverBranchLister struct {
	branches map[string]bool
	err      error
}

func (l resolverBranchLister) GetBranchReference(_ context.Context, repo, branch string) (string, error) {
	if l.err != nil {
		return "", l.err
	}
	if l.branches[repo+"/"+branch] {
		return "commit-of-" + branch, nil
	}
	return "", fmt.Errorf("branch %q: %w", branch, graveler.ErrBranchNotFound)
}

// resolverRefMapper is a RefMapper that answers from a fixed map and can refuse a ref.
type resolverRefMapper struct {
	refs map[string]string
	err  error
}

func (m resolverRefMapper) MapRef(_ context.Context, _, ref string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	mapped, ok := m.refs[ref]
	if !ok {
		return "", &NoSuchTableException{Repo: "unused", Ref: ref, Namespace: Namespace{SystemNamespace}, Table: resolverObjectMetadataTable}
	}
	return mapped, nil
}

// resolverVirtualAddress is the virtual table of tablesTestRepo on one ref.
func resolverVirtualAddress(ref string) Address {
	return Address{
		Repo:      tablesTestRepo,
		Ref:       ref,
		Namespace: Namespace{SystemNamespace},
		Table:     resolverObjectMetadataTable,
	}
}

// TestMetadataSearchResolverRedirect pins the answer of the built-in resolver for every kind of
// address it is asked about (§5.2): a branch maps to the branch of the same name, a tag or a commit
// is the RefMapper's question, and everything that is not the virtual table is not handled at all.
func TestMetadataSearchResolverRedirect(t *testing.T) {
	branches := resolverBranchLister{branches: map[string]bool{
		tablesTestRepo + "/" + tablesTestRef: true,
	}}
	wantTarget := Address{
		Repo:      resolverMetaRepo,
		Ref:       tablesTestRef,
		Namespace: Namespace{SystemNamespace},
		Table:     resolverObjectMetadataTable,
	}

	t.Run("a branch maps to the branch of the same name", func(t *testing.T) {
		resolver := NewMetadataSearchResolver(branches, nil)
		resolved, handled, err := resolver.Resolve(t.Context(), resolverVirtualAddress(tablesTestRef))
		require.NoError(t, err)
		require.True(t, handled, "the virtual table is the resolver's to answer")
		require.Equal(t, wantTarget, resolved.Target)
		require.Equal(t, resolverVirtualAddress(tablesTestRef), resolved.Authz,
			"§5.3: the authorization address is the data repository, never the companion")
	})

	t.Run("a tag is refused while no RefMapper is registered", func(t *testing.T) {
		resolver := NewMetadataSearchResolver(branches, nil)
		_, handled, err := resolver.Resolve(t.Context(), resolverVirtualAddress(tablesTestTag))
		require.True(t, handled)
		var noTable *NoSuchTableException
		require.ErrorAs(t, err, &noTable, "a tag has no mirrored branch, and guessing one would answer the wrong table")
		require.Equal(t, tablesTestTag, noTable.Ref)
	})

	t.Run("a commit is refused while no RefMapper is registered", func(t *testing.T) {
		resolver := NewMetadataSearchResolver(branches, nil)
		_, _, err := resolver.Resolve(t.Context(), resolverVirtualAddress("a1b2c3d4e5f6"))
		var noTable *NoSuchTableException
		require.ErrorAs(t, err, &noTable)
	})

	t.Run("the RefMapper maps a tag", func(t *testing.T) {
		resolver := NewMetadataSearchResolver(branches, resolverRefMapper{
			refs: map[string]string{tablesTestTag: "indexed-" + tablesTestTag},
		})
		resolved, handled, err := resolver.Resolve(t.Context(), resolverVirtualAddress(tablesTestTag))
		require.NoError(t, err)
		require.True(t, handled)
		require.Equal(t, "indexed-"+tablesTestTag, resolved.Target.Ref)
		require.Equal(t, resolverMetaRepo, resolved.Target.Repo)
		require.Equal(t, resolverVirtualAddress(tablesTestTag), resolved.Authz)
	})

	t.Run("the RefMapper may refuse a ref", func(t *testing.T) {
		resolver := NewMetadataSearchResolver(branches, resolverRefMapper{
			err: &NoSuchTableException{
				Repo: tablesTestRepo, Ref: tablesTestTag,
				Namespace: Namespace{SystemNamespace}, Table: resolverObjectMetadataTable,
			},
		})
		_, handled, err := resolver.Resolve(t.Context(), resolverVirtualAddress(tablesTestTag))
		require.True(t, handled)
		var noTable *NoSuchTableException
		require.ErrorAs(t, err, &noTable)
	})

	t.Run("an address that is not the virtual table is not handled", func(t *testing.T) {
		resolver := NewMetadataSearchResolver(branches, nil)
		for _, addr := range []Address{
			{Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: Namespace{"inventory", "books"}, Table: "orders"},
			{Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: Namespace{SystemNamespace}, Table: "something_else"},
			{Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: Namespace{"system", "nested"}, Table: resolverObjectMetadataTable},
			{Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: Namespace{SystemNamespace}},
			// The companion repository itself: its system namespace is ordinary, which is what
			// lets the indexer write there (in the test fixture it already has).
			{Repo: resolverMetaRepo, Ref: tablesTestRef, Namespace: Namespace{SystemNamespace}, Table: resolverObjectMetadataTable},
			// A view in the system namespace is not a table: it has its own tree and endpoints.
			{Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: Namespace{SystemNamespace}, View: resolverObjectMetadataTable},
		} {
			_, handled, err := resolver.Resolve(t.Context(), addr)
			require.NoErrorf(t, err, "address %v", addr)
			require.Falsef(t, handled, "address %v must be left alone", addr)
		}
	})

	t.Run("a repository that does not exist is the missing warehouse it is", func(t *testing.T) {
		resolver := NewMetadataSearchResolver(resolverBranchLister{err: graveler.ErrRepositoryNotFound}, nil)
		_, _, err := resolver.Resolve(t.Context(), resolverVirtualAddress(tablesTestRef))
		require.True(t, isMissingWarehouse(err), "a read of an unknown repository must stay the 404 it was")
	})

	t.Run("a nil BranchLister treats every ref as a branch", func(t *testing.T) {
		resolver := NewMetadataSearchResolver(nil, nil)
		resolved, handled, err := resolver.Resolve(t.Context(), resolverVirtualAddress(tablesTestTag))
		require.NoError(t, err)
		require.True(t, handled)
		require.Equal(t, tablesTestTag, resolved.Target.Ref)
	})
}

// TestMetadataSearchResolverCapabilities pins the listing and read-only rules of §5.4 as the
// resolver states them, for the data repository and for the companion one.
func TestMetadataSearchResolverCapabilities(t *testing.T) {
	resolver := NewMetadataSearchResolver(nil, nil)

	require.Equal(t, []string{SystemNamespace},
		resolver.VirtualNamespaces(Address{Repo: tablesTestRepo, Ref: tablesTestRef}),
		"the ref level of a data repository lists the system namespace")
	require.Empty(t, resolver.VirtualNamespaces(Address{Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: Namespace{"inventory"}}),
		"a deeper namespace lists its markers and nothing else")
	require.Empty(t, resolver.VirtualNamespaces(Address{Repo: resolverMetaRepo, Ref: tablesTestRef}),
		"the companion repository lists its own markers")

	require.Equal(t, []string{resolverObjectMetadataTable},
		resolver.VirtualTables(resolverVirtualAddress(tablesTestRef)))
	require.Empty(t, resolver.VirtualTables(Address{Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: Namespace{"inventory"}}))
	require.Empty(t, resolver.VirtualTables(Address{Repo: resolverMetaRepo, Ref: tablesTestRef, Namespace: Namespace{SystemNamespace}}),
		"the companion repository's system namespace is an ordinary namespace, listed by its pointers")

	require.True(t, resolver.ReservedNamespace(resolverVirtualAddress(tablesTestRef)))
	require.True(t, resolver.ReservedNamespace(Address{Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: Namespace{SystemNamespace, "child"}}),
		"the whole subtree is reserved, not just its first level")
	require.False(t, resolver.ReservedNamespace(Address{Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: Namespace{"inventory"}}))
	require.False(t, resolver.ReservedNamespace(Address{Repo: resolverMetaRepo, Ref: tablesTestRef, Namespace: Namespace{SystemNamespace}}))
	require.False(t, resolver.ReservedNamespace(Address{}))

	require.True(t, IsMetadataRepository(resolverMetaRepo))
	require.False(t, IsMetadataRepository("metadata"), "a repository that is only the suffix is not a companion")
	require.Equal(t, resolverMetaRepo, MetadataRepository(tablesTestRepo))
}

// newNoResolverMount mounts the production catalog over the tables fixture with iceberg.enabled and
// no metadata_search section at all: the default of every installation that does not use the
// feature.
func newNoResolverMount(t *testing.T, base *tablesFixture) (http.Handler, map[string]resolverCredential) {
	t.Helper()
	ctx := t.Context()

	credentials := make(map[string]resolverCredential, 2)
	for i, username := range []string{"alice", "carol"} {
		credential := resolverCredential{
			accessKeyID:     fmt.Sprintf("AKIANOMDS%02d", i),
			secretAccessKey: fmt.Sprintf("no-metadata-search-secret-0000-%04d", i),
		}
		_, err := base.authz.svc.AddCredentials(ctx, username, credential.accessKeyID, credential.secretAccessKey)
		require.NoErrorf(t, err, "add credentials for %s", username)
		credentials[username] = credential
	}

	useConfig(t, "iceberg:\n  enabled: true\n")

	kvStore, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	require.NoError(t, err, "open the server's KV store")
	t.Cleanup(kvStore.Close)

	deps := cmdpkg.ServerDeps{
		Config:        &config.ConfigImpl{},
		AuthService:   base.authz.svc,
		Authenticator: auth.NewBuiltinAuthenticator(base.authz.svc),
		Catalog:       base.catalog,
		BlockAdapter:  base.catalog.BlockAdapter,
		KVStore:       kvStore,
		Logger:        logging.ContextUnavailable(),
	}
	mux := coreRouter()
	require.NoError(t, RoutesBuilder(ctx, deps, mux), "mount the catalog")
	return mux, credentials
}

// TestMetadataSearchDefaultNoop is the promise the hook is built on: with no resolver registered the
// catalog behaves exactly as it did before it existed. Nothing redirects, nothing is listed, nothing
// is reserved - and the virtual address is the 404 it always was.
func TestMetadataSearchDefaultNoop(t *testing.T) {
	f := newTablesFixture(t)
	mux, credentials := newNoResolverMount(t, f)
	request := func(t *testing.T, method, target, user string, body string) *httptest.ResponseRecorder {
		t.Helper()
		credential, ok := credentials[user]
		require.Truef(t, ok, "no credentials for user %q", user)
		var reader io.Reader
		if body != "" {
			reader = strings.NewReader(body)
		}
		req := httptest.NewRequest(method, APIBasePath+target, reader)
		req.SetBasicAuth(credential.accessKeyID, credential.secretAccessKey)
		if body != "" {
			req.Header.Set("Content-Type", contentTypeJSON)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}

	t.Run("the store resolves every address to itself", func(t *testing.T) {
		for _, addr := range []Address{
			resolverVirtualAddress(tablesTestRef),
			{Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: Namespace{"inventory", "books"}, Table: "orders"},
		} {
			resolved, err := f.store.Resolve(f.ctx, addr)
			require.NoError(t, err)
			require.Equal(t, addr, resolved.Target)
			require.Equal(t, addr, resolved.Authz)
			require.NoError(t, f.store.RequireWritableNamespace(addr), "no namespace is reserved")
		}
		require.Nil(t, f.store.Resolver())
		require.Empty(t, f.store.VirtualTables(resolverVirtualAddress(tablesTestRef)))
		require.Empty(t, f.store.VirtualNamespaces(Address{Repo: tablesTestRepo, Ref: tablesTestRef}))
	})

	t.Run("the virtual table is not there", func(t *testing.T) {
		requireEnvelope(t, request(t, http.MethodGet, virtualTarget(tablesTestRef), "alice", ""),
			http.StatusNotFound, ErrTypeNoSuchTable)
		requireEnvelope(t, request(t, http.MethodHead, virtualTarget(tablesTestRef), "alice", ""),
			http.StatusNotFound, ErrTypeNoSuchTable)
		// The system namespace has no marker either, so its listing is the 404 a namespace without
		// one always answers (§5).
		requireEnvelope(t, request(t, http.MethodGet, virtualNamespaceTablesTarget(tablesTestRef), "alice", ""),
			http.StatusNotFound, ErrTypeNoSuchNamespace)
	})

	t.Run("the system namespace is not listed", func(t *testing.T) {
		rec := request(t, http.MethodGet, listTarget([]string{tablesTestRepo, tablesTestRef}, nil), "alice", "")
		for _, namespace := range decodeListNamespaces(t, rec).Namespaces {
			require.NotEqualf(t, Namespace{tablesTestRepo, tablesTestRef, SystemNamespace}, namespace,
				"nothing is virtual while no resolver is registered")
		}
	})

	t.Run("the system namespace is writable like any other", func(t *testing.T) {
		// No resolver: system is an ordinary namespace, so creating a marker one level below it is
		// the create it always was - 200, not the 403 of §5.4.
		body := fmt.Sprintf(`{"namespace":[%q,%q,%q,%q]}`, tablesTestRepo, tablesTestRef, SystemNamespace, "child")
		require.Equal(t, http.StatusOK,
			request(t, http.MethodPost, "/v1/"+CatalogPrefix+"/namespaces", "carol", body).Code)
	})
}

// ---------------------------------------------------------------------------------------------
// The virtual table over HTTP (§5.2, §5.3)
// ---------------------------------------------------------------------------------------------

// TestMetadataSearchVirtualTableRead covers the read half: the load and head of the virtual table,
// its listing, and the promise of §5.3 that the caller's only permission is on the data repository.
func TestMetadataSearchVirtualTableRead(t *testing.T) {
	f := newResolverFixture(t)

	t.Run("a branch load reads the companion repository's table", func(t *testing.T) {
		rec := f.request(t, http.MethodGet, virtualTarget(tablesTestRef), "alice", "")
		body := decodeLoadTable(t, rec)
		require.Equal(t, f.metaLocation, body.MetadataLocation,
			"the table is the one the indexer wrote into the companion repository")
		require.True(t, strings.HasPrefix(body.MetadataLocation, strings.TrimRight(f.metaStorageNamespace, "/")+"/"),
			"the metadata lives in the companion repository's storage namespace")
		require.JSONEq(t, f.metaMetadataJSON, string(mustMarshalRaw(t, body.Metadata)))

		// HEAD is the same authorized read, so it cannot report a table the load would refuse.
		require.Equal(t, http.StatusNoContent,
			f.request(t, http.MethodHead, virtualTarget(tablesTestRef), "alice", "").Code)
	})

	t.Run("the relative_to mount answers the same table", func(t *testing.T) {
		target := warehouseTarget(tablesTestRepo+"."+tablesTestRef,
			tablesLoadTarget(nodes(SystemNamespace), resolverObjectMetadataTable, nil))
		body := decodeLoadTable(t, f.request(t, http.MethodGet, target, "alice", ""))
		require.Equal(t, f.metaLocation, body.MetadataLocation)
	})

	t.Run("a caller without access to the data repository is refused", func(t *testing.T) {
		// mds-meta may read the companion repository and nothing else: the virtual table must not
		// hand it the data repository's metadata.
		requireEnvelope(t, f.request(t, http.MethodGet, virtualTarget(tablesTestRef), "mds-meta", ""),
			http.StatusForbidden, ErrorTypeForbidden)
		requireEnvelope(t, f.request(t, http.MethodHead, virtualTarget(tablesTestRef), "bob", ""),
			http.StatusForbidden, ErrorTypeForbidden)
	})

	t.Run("the caller needs no access to the companion repository", func(t *testing.T) {
		// alice holds fs:ReadRepository on the data repository only (resolverCatalogReadStatements
		// is the shape of its grant), so reading the companion repository's table directly is
		// refused while the virtual table above was served to her.
		direct := tablesLoadTarget(nodes(resolverMetaRepo, tablesTestRef, SystemNamespace), resolverObjectMetadataTable, nil)
		requireEnvelope(t, f.request(t, http.MethodGet, direct, "alice", ""),
			http.StatusForbidden, ErrorTypeForbidden)
	})

	t.Run("a tag is not indexed and answers NoSuchTable", func(t *testing.T) {
		requireEnvelope(t, f.request(t, http.MethodGet, virtualTarget(tablesTestTag), "alice", ""),
			http.StatusNotFound, ErrTypeNoSuchTable)
	})

	t.Run("the snapshots query parameter applies to the redirected document", func(t *testing.T) {
		body := decodeLoadTable(t, f.request(t, http.MethodGet,
			tablesLoadTarget(nodes(tablesTestRepo, tablesTestRef, SystemNamespace), resolverObjectMetadataTable,
				map[string][]string{"snapshots": {SnapshotsRefs}}), "alice", ""))
		require.Equal(t, f.metaLocation, body.MetadataLocation)
		require.ElementsMatch(t, []int64{tablesTestTagSnapshot, tablesTestCurrentSnapshot},
			snapshotIDsOf(t, body.Metadata), "refs mode filters the redirected document, not another one")
	})

	t.Run("the system namespace and its table are listed", func(t *testing.T) {
		rec := f.request(t, http.MethodGet, listTarget([]string{tablesTestRepo, tablesTestRef}, nil), "alice", "")
		require.Contains(t, decodeListNamespaces(t, rec).Namespaces,
			Namespace{tablesTestRepo, tablesTestRef, SystemNamespace})

		listing := decodeListTables(t, f.request(t, http.MethodGet, virtualNamespaceTablesTarget(tablesTestRef), "alice", ""))
		require.Equal(t, []string{resolverObjectMetadataTable}, tableNamesOf(listing))
		require.Equal(t, Namespace{tablesTestRepo, tablesTestRef, SystemNamespace},
			listing.Identifiers[0].Namespace, "the identifier is the full address a client can load")
	})

	t.Run("the companion repository's system namespace is listed from its pointers", func(t *testing.T) {
		listing := decodeListTables(t, f.request(t, http.MethodGet,
			tablesListTarget(nodes(resolverMetaRepo, tablesTestRef, SystemNamespace), nil), "mds-meta-writer", ""))
		require.Equal(t, []string{resolverObjectMetadataTable}, tableNamesOf(listing))
	})

	t.Run("a caller that may not list the namespace does not see the table", func(t *testing.T) {
		// dana may list only inventory.books (catalog:TableName condition), so the system namespace
		// is not hers to list: the filter refuses rather than returning an empty page (§7.2).
		requireEnvelope(t, f.request(t, http.MethodGet, virtualNamespaceTablesTarget(tablesTestRef), "dana", ""),
			http.StatusForbidden, ErrorTypeForbidden)
	})

	t.Run("the metrics report resolves the redirected table", func(t *testing.T) {
		rec := f.request(t, http.MethodPost,
			tableMetricsTarget(nodes(tablesTestRepo, tablesTestRef, SystemNamespace), resolverObjectMetadataTable),
			"alice", `{"report-type":"scan-report"}`)
		// The metrics sink's §7.2 row is catalog:UpdateTable, which alice does not hold; the point
		// of this subtest is that an authorized caller reaches the redirected table (carol), and an
		// unauthorized one is refused before anything is resolved.
		requireEnvelope(t, rec, http.StatusForbidden, ErrorTypeForbidden)
		require.Equal(t, http.StatusNoContent, f.request(t, http.MethodPost,
			tableMetricsTarget(nodes(tablesTestRepo, tablesTestRef, SystemNamespace), resolverObjectMetadataTable),
			"carol", `{"report-type":"scan-report"}`).Code)
	})
}

// TestMetadataSearchCompanionRepositoryProof is MDS-SEC-3: a companion repository is only R's
// index when its table says so. The indexer writes lakefs.mds.repository = R into the
// object_metadata table it creates (METADATA_SEARCH_DESIGN.md §3.1), and the catalog checks that
// property before it serves the redirected table. Anything else - a table that names another
// repository, or a table with no property at all - is the 404 NoSuchTableException an unindexed
// ref answers: whoever created R-metadata before the indexer did must not be able to show every
// reader of R attacker-chosen rows, because the redirected read runs with the server's identity.
func TestMetadataSearchCompanionRepositoryProof(t *testing.T) {
	t.Run("a companion table that names another repository is not served", func(t *testing.T) {
		f := newResolverFixtureWithTableProperty(t, "another-repo")
		requireEnvelope(t, f.request(t, http.MethodGet, virtualTarget(tablesTestRef), "alice", ""),
			http.StatusNotFound, ErrTypeNoSuchTable)
		requireEnvelope(t, f.request(t, http.MethodHead, virtualTarget(tablesTestRef), "alice", ""),
			http.StatusNotFound, ErrTypeNoSuchTable)
		// The data-plane paths resolve the same table, so a vended session or a signature must not
		// be scoped to a location the virtual table does not read.
		_, err := newTableScopeResolver(f.store)(t.Context(), resolverVirtualAddress(tablesTestRef),
			func(context.Context, Address) error { return nil })
		var noTable *NoSuchTableException
		require.ErrorAs(t, err, &noTable)
	})

	t.Run("a companion table with no repository property is not served", func(t *testing.T) {
		f := newResolverFixtureWithTableProperty(t, "")
		requireEnvelope(t, f.request(t, http.MethodGet, virtualTarget(tablesTestRef), "alice", ""),
			http.StatusNotFound, ErrTypeNoSuchTable)
		requireEnvelope(t, f.request(t, http.MethodHead, virtualTarget(tablesTestRef), "alice", ""),
			http.StatusNotFound, ErrTypeNoSuchTable)
	})

	t.Run("a companion table with the property but no init commit is not served", func(t *testing.T) {
		f := newResolverFixtureWithoutInitCommit(t)
		requireEnvelope(t, f.request(t, http.MethodGet, virtualTarget(tablesTestRef), "alice", ""),
			http.StatusNotFound, ErrTypeNoSuchTable)
		requireEnvelope(t, f.request(t, http.MethodHead, virtualTarget(tablesTestRef), "alice", ""),
			http.StatusNotFound, ErrTypeNoSuchTable)
		// A forged property is not proof for the data plane either.
		_, err := newTableScopeResolver(f.store)(t.Context(), resolverVirtualAddress(tablesTestRef),
			func(context.Context, Address) error { return nil })
		var noTable *NoSuchTableException
		require.ErrorAs(t, err, &noTable)
	})

	t.Run("an init commit written by a non-indexer is not served", func(t *testing.T) {
		// The marker and the message are not proof of who wrote the commit: anyone with
		// fs:CreateCommit on R-metadata can copy them. The genuine indexer principal is mds-indexer.
		f := newResolverFixtureWithInitCommitter(t, "mallory")
		requireEnvelope(t, f.request(t, http.MethodGet, virtualTarget(tablesTestRef), "alice", ""),
			http.StatusNotFound, ErrTypeNoSuchTable)
		requireEnvelope(t, f.request(t, http.MethodHead, virtualTarget(tablesTestRef), "alice", ""),
			http.StatusNotFound, ErrTypeNoSuchTable)
		_, err := newTableScopeResolver(f.store)(t.Context(), resolverVirtualAddress(tablesTestRef),
			func(context.Context, Address) error { return nil })
		var noTable *NoSuchTableException
		require.ErrorAs(t, err, &noTable)
	})

	t.Run("an init commit that is not the first commit above the root is not served", func(t *testing.T) {
		// Position is part of the proof: an mds: init commit by the indexer, but with another
		// commit between it and the repository's automatic root commit, is not the initialisation
		// commit the indexer writes when it creates R-metadata.
		f := newResolverFixtureWithoutInitCommit(t)
		_, err := f.catalog.Commit(f.ctx, resolverMetaRepo, tablesTestRef, "mds: bootstrap",
			"operator", nil, nil, nil, false)
		require.NoError(t, err, "commit the table before the forged init commit")
		commitResolverInitAs(t, f.catalog, resolverMetaRepo, tablesTestRef, tablesTestRepo,
			MetadataSearchIndexerPrincipal, true)
		requireEnvelope(t, f.request(t, http.MethodGet, virtualTarget(tablesTestRef), "alice", ""),
			http.StatusNotFound, ErrTypeNoSuchTable)
		requireEnvelope(t, f.request(t, http.MethodHead, virtualTarget(tablesTestRef), "alice", ""),
			http.StatusNotFound, ErrTypeNoSuchTable)
	})

	t.Run("a companion table that names the data repository and has the init commit is served", func(t *testing.T) {
		f := newResolverFixture(t)
		require.Equal(t, http.StatusOK, f.request(t, http.MethodGet, virtualTarget(tablesTestRef), "alice", "").Code)
	})
}

// resolverCommitChain is a BranchLister and commitLogLister with a deterministic first-parent
// history. It lets the cache tests count commit-log reads without materializing a catalog: the root
// is commits[0], the indexer's init commit is commits[1], and every later commit is a normal child
// of the one before it. calls is read and reset by the test that owns the chain, which runs its
// assertions sequentially.
type resolverCommitChain struct {
	commits []*catalog.CommitLog
	heads   map[string]int
	calls   int //nolint:unused // read by TestMetadataSearchInitCommitCache
}

func newResolverCommitChain(n int) *resolverCommitChain {
	if n < 2 {
		n = 2
	}
	chain := &resolverCommitChain{heads: make(map[string]int, 1)}
	for i := 0; i < n; i++ {
		commit := &catalog.CommitLog{
			Reference: fmt.Sprintf("commit-%05d", i),
			Committer: "author",
			Message:   fmt.Sprintf("commit %d", i),
		}
		if i > 0 {
			commit.Parents = []string{chain.commits[i-1].Reference}
		}
		if i == 1 {
			commit.Committer = MetadataSearchIndexerPrincipal
			commit.Message = MetadataSearchInitCommitMessage
			commit.Metadata = catalog.Metadata{MetadataSearchInitProperty: MetadataSearchInitValue}
		}
		chain.commits = append(chain.commits, commit)
	}
	return chain
}

// appendCommit adds one first-parent child of the current tip.
func (c *resolverCommitChain) appendCommit() {
	i := len(c.commits)
	commit := &catalog.CommitLog{
		Reference: fmt.Sprintf("commit-%05d", i),
		Committer: "author",
		Message:   fmt.Sprintf("commit %d", i),
		Parents:   []string{c.commits[i-1].Reference},
	}
	c.commits = append(c.commits, commit)
}

func (c *resolverCommitChain) setHead(repository, ref string, index int) {
	c.heads[repository+"/"+ref] = index
}

func (c *resolverCommitChain) GetBranchReference(_ context.Context, repository, branch string) (string, error) {
	index, ok := c.heads[repository+"/"+branch]
	if !ok {
		return "", fmt.Errorf("branch %q: %w", branch, graveler.ErrBranchNotFound)
	}
	return c.commits[index].Reference, nil
}

// ListCommits mimics catalog.ListCommits page semantics: FromReference is skipped, the page starts
// at its first parent, and hasMore reports that older commits remain.
func (c *resolverCommitChain) ListCommits(_ context.Context, repository, ref string, params catalog.LogParams) ([]*catalog.CommitLog, bool, error) {
	c.calls++
	index, ok := c.heads[repository+"/"+ref]
	if !ok {
		return nil, false, nil
	}
	if params.FromReference != "" {
		found := false
		for i := index; i >= 0; i-- {
			if c.commits[i].Reference == params.FromReference {
				index = i - 1
				found = true
				break
			}
		}
		if !found {
			index = -1
		}
	}
	amount := params.Amount
	if amount <= 0 {
		amount = metadataSearchInitPageSize
	}
	page := make([]*catalog.CommitLog, 0, amount)
	for i := index; i >= 0 && len(page) < amount; i-- {
		page = append(page, c.commits[i])
	}
	return page, index-len(page) >= 0, nil
}

// TestMetadataSearchInitCommitCache pins MDS-SEC-3's cost model: the init commit is found once per
// repository, and a second read - the same head or one new commit - does not walk the history again.
// The commit-log call count is the observable; before the per-repository cache it grew with the
// length of the branch on every new head.
func TestMetadataSearchInitCommitCache(t *testing.T) {
	for _, n := range []int{300, 4096} {
		t.Run(fmt.Sprintf("history of %d commits", n), func(t *testing.T) {
			chain := newResolverCommitChain(n)
			chain.setHead("repo", tablesTestRef, len(chain.commits)-1)
			resolver := NewMetadataSearchResolver(chain, nil)
			target := Address{Repo: "repo", Ref: tablesTestRef}

			found, err := resolver.firstParentCarriesInitCommit(t.Context(), target)
			require.NoError(t, err)
			require.True(t, found, "the genuine init commit directly above the root is proof")
			require.Positive(t, chain.calls, "the first read has to walk the history")

			chain.calls = 0
			found, err = resolver.firstParentCarriesInitCommit(t.Context(), target)
			require.NoError(t, err)
			require.True(t, found)
			require.LessOrEqual(t, chain.calls, 1, "an unchanged head is answered from the cached witness")

			chain.appendCommit()
			chain.setHead("repo", tablesTestRef, len(chain.commits)-1)
			chain.calls = 0
			found, err = resolver.firstParentCarriesInitCommit(t.Context(), target)
			require.NoError(t, err)
			require.True(t, found)
			require.LessOrEqual(t, chain.calls, 1,
				"a new head is verified against a cached witness, not by re-walking the whole history")
		})
	}
}

// TestMetadataSearchInitCommitCacheBound pins the cache bounds: many heads of one repository are
// one repository entry with a bounded witness list, and many repositories do not grow the cache
// without limit.
func TestMetadataSearchInitCommitCacheBound(t *testing.T) {
	const heads = 24
	chain := newResolverCommitChain(heads + 2)
	resolver := NewMetadataSearchResolver(chain, nil)
	for i := 0; i < heads; i++ {
		ref := fmt.Sprintf("branch-%02d", i)
		chain.setHead("repo", ref, len(chain.commits)-1-i)
		found, err := resolver.firstParentCarriesInitCommit(t.Context(), Address{Repo: "repo", Ref: ref})
		require.NoError(t, err)
		require.Truef(t, found, "branch %s descends from the init commit", ref)
	}
	require.Len(t, resolver.initCache, 1, "the cache is keyed by repository, not by head")
	entry := resolver.initCache["repo"]
	require.NotNil(t, entry)
	require.LessOrEqual(t, len(entry.heads), metadataSearchInitCacheMaxHeads,
		"the verified-head witnesses of one repository are bounded")

	const repositories = metadataSearchInitCacheMaxRepositories + 1
	tiny := newResolverCommitChain(3)
	resolver = NewMetadataSearchResolver(tiny, nil)
	for i := 0; i < repositories; i++ {
		repository := fmt.Sprintf("repo-%04d", i)
		tiny.setHead(repository, tablesTestRef, len(tiny.commits)-1)
		found, err := resolver.firstParentCarriesInitCommit(t.Context(), Address{Repo: repository, Ref: tablesTestRef})
		require.NoError(t, err)
		require.Truef(t, found, "repository %s descends from the init commit", repository)
	}
	require.LessOrEqual(t, len(resolver.initCache), metadataSearchInitCacheMaxRepositories,
		"old repository entries are evicted at the cache bound")
}

// mustMarshalRaw re-encodes a decoded metadata document so it can be compared with require.JSONEq.
func mustMarshalRaw(t *testing.T, document map[string]json.RawMessage) []byte {
	t.Helper()
	raw, err := json.Marshal(document)
	require.NoError(t, err)
	return raw
}

// ---------------------------------------------------------------------------------------------
// The reserved namespace (§5.4)
// ---------------------------------------------------------------------------------------------

// TestMetadataSearchReservedNamespace covers §5.4 on the data repository: every catalog write into
// system answers 403 ForbiddenException for a caller that may write the repository - and the same
// writes succeed in the companion repository, where system is an ordinary namespace.
func TestMetadataSearchReservedNamespace(t *testing.T) {
	f := newResolverFixture(t)

	// A valid create request, so a refusal cannot be a validation error in disguise.
	createTableBody := fmt.Sprintf(`{"name":%q,"schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"x","required":true,"type":"long"}]}}`,
		"clients_table")

	t.Run("create table", func(t *testing.T) {
		requireEnvelope(t, f.request(t, http.MethodPost,
			tablesCreateTarget(nodes(tablesTestRepo, tablesTestRef, SystemNamespace)), "carol", createTableBody),
			http.StatusForbidden, ErrorTypeForbidden)
	})

	t.Run("register", func(t *testing.T) {
		body := fmt.Sprintf(`{"name":%q,"metadata-location":%q}`, "clients_table", f.metaLocation)
		requireEnvelope(t, f.request(t, http.MethodPost,
			"/v1/"+CatalogPrefix+"/namespaces/"+nodes(tablesTestRepo, tablesTestRef, SystemNamespace)+"/register",
			"carol", body),
			http.StatusForbidden, ErrorTypeForbidden)
	})

	t.Run("drop table", func(t *testing.T) {
		requireEnvelope(t, f.request(t, http.MethodDelete,
			tablesLoadTarget(nodes(tablesTestRepo, tablesTestRef, SystemNamespace), resolverObjectMetadataTable, nil),
			"carol", ""),
			http.StatusForbidden, ErrorTypeForbidden)
		// Nothing was dropped: the virtual table still loads.
		require.Equal(t, http.StatusOK, f.request(t, http.MethodGet, virtualTarget(tablesTestRef), "alice", "").Code)
	})

	t.Run("commit", func(t *testing.T) {
		body := `{"requirements":[],"updates":[{"action":"set-properties","updates":{"owner":"mallory"}}]}`
		requireEnvelope(t, f.request(t, http.MethodPost,
			tablesLoadTarget(nodes(tablesTestRepo, tablesTestRef, SystemNamespace), resolverObjectMetadataTable, nil),
			"carol", body),
			http.StatusForbidden, ErrorTypeForbidden)
	})

	t.Run("create view", func(t *testing.T) {
		body := `{"name":"clients_view"}`
		requireEnvelope(t, f.request(t, http.MethodPost,
			"/v1/"+CatalogPrefix+"/namespaces/"+nodes(tablesTestRepo, tablesTestRef, SystemNamespace)+"/views",
			"carol", body),
			http.StatusForbidden, ErrorTypeForbidden)
	})

	t.Run("create namespace", func(t *testing.T) {
		body := fmt.Sprintf(`{"namespace":[%q,%q,%q,%q]}`, tablesTestRepo, tablesTestRef, SystemNamespace, "child")
		requireEnvelope(t, f.request(t, http.MethodPost, "/v1/"+CatalogPrefix+"/namespaces", "carol", body),
			http.StatusForbidden, ErrorTypeForbidden)
	})

	t.Run("update namespace properties", func(t *testing.T) {
		body := `{"updates":{"owner":"mallory"}}`
		requireEnvelope(t, f.request(t, http.MethodPost,
			"/v1/"+CatalogPrefix+"/namespaces/"+nodes(tablesTestRepo, tablesTestRef, SystemNamespace)+"/properties",
			"carol", body),
			http.StatusForbidden, ErrorTypeForbidden)
	})

	t.Run("drop namespace", func(t *testing.T) {
		requireEnvelope(t, f.request(t, http.MethodDelete,
			"/v1/"+CatalogPrefix+"/namespaces/"+nodes(tablesTestRepo, tablesTestRef, SystemNamespace), "carol", ""),
			http.StatusForbidden, ErrorTypeForbidden)
	})

	t.Run("the companion repository's system namespace stays writable", func(t *testing.T) {
		// The indexer writes system.object_metadata in R-metadata directly, so system there is an
		// ordinary namespace: a create answers 200, and a create of the existing table answers the
		// 409 a duplicate name always answers - never the 403 of §5.4.
		body := fmt.Sprintf(`{"name":%q,"schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"x","required":true,"type":"long"}]}}`,
			"indexer_scratch")
		require.Equal(t, http.StatusOK, f.request(t, http.MethodPost,
			tablesCreateTarget(nodes(resolverMetaRepo, tablesTestRef, SystemNamespace)), "mds-meta-writer", body).Code)

		duplicate := fmt.Sprintf(`{"name":%q,"schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"x","required":true,"type":"long"}]}}`,
			resolverObjectMetadataTable)
		requireEnvelope(t, f.request(t, http.MethodPost,
			tablesCreateTarget(nodes(resolverMetaRepo, tablesTestRef, SystemNamespace)), "mds-meta-writer", duplicate),
			http.StatusConflict, ErrTypeAlreadyExists)
	})

	t.Run("a caller without the write grant never reaches the reservation", func(t *testing.T) {
		// bob holds nothing: the answer is the permission refusal, not a description of the
		// namespace.
		requireEnvelope(t, f.request(t, http.MethodPost,
			tablesCreateTarget(nodes(tablesTestRepo, tablesTestRef, SystemNamespace)), "bob", createTableBody),
			http.StatusForbidden, ErrorTypeForbidden)
	})
}

// ---------------------------------------------------------------------------------------------
// The data-plane paths and the authorization address (§5.1, §5.3)
// ---------------------------------------------------------------------------------------------

// TestMetadataSearchDataPlaneRedirect pins that the two §8 paths that resolve a table - vended
// credentials and remote signing - resolve it through the hook too: a session or a signature for
// the virtual table is scoped to where the table actually lives, and the caller was authorized on
// the data repository alone.
func TestMetadataSearchDataPlaneRedirect(t *testing.T) {
	f := newResolverFixture(t)
	ctx := t.Context()
	virtual := resolverVirtualAddress(tablesTestRef)

	var authorized []Address
	authorize := func(_ context.Context, addr Address) error {
		authorized = append(authorized, addr)
		return nil
	}

	t.Run("credentials are scoped to the companion repository's table", func(t *testing.T) {
		authorized = nil
		scope, err := newTableScopeResolver(f.store)(ctx, virtual, authorize)
		require.NoError(t, err)
		require.Empty(t, authorized,
			"§5.3: the redirect is not a second authorization question - the requested address was already checked")
		require.Equal(t, strings.TrimRight(f.metaStorageNamespace, "/")+"/_iceberg/"+resolverMetaUUID, scope.Root,
			"the session covers the files of the table that will be read, which is the companion's")
		require.Equal(t, strings.TrimRight(f.metaStorageNamespace, "/")+"/_iceberg/"+resolverMetaUUID, scope.Location)
	})

	t.Run("signing resolves the companion repository's table location", func(t *testing.T) {
		authorized = nil
		adapter := &signingAdapter{blockstoreType: block.BlockstoreTypeS3, preSignSupported: true}
		service := NewRemoteSigningService(f.store, NewAuthorizer(f.authz.svc), adapter, f.catalog, &Config{Prefix: CatalogPrefix})
		location, err := service.tableLocation(ctx, virtual, authorize)
		require.NoError(t, err)
		require.Equal(t, strings.TrimRight(f.metaStorageNamespace, "/")+"/_iceberg/"+resolverMetaUUID, location)
		require.Empty(t, authorized, "the requested address was already authorized by the caller")
	})

	t.Run("a resolver that names another authorization address is checked on it", func(t *testing.T) {
		// A custom resolver whose target and authorization address are both elsewhere: the catalog
		// authorizes the address the caller named and, in addition, the authorization address. That
		// is what keeps a resolver from granting more than the caller already had.
		store := NewStore(f.catalog, f.catalog.BlockAdapter, WithTableResolver(resolverRedirectStub{
			target: Address{Repo: resolverMetaRepo, Ref: tablesTestRef, Namespace: Namespace{SystemNamespace}, Table: resolverObjectMetadataTable},
			authz:  Address{Repo: resolverMetaRepo, Ref: tablesTestRef, Namespace: Namespace{SystemNamespace}, Table: resolverObjectMetadataTable},
		}))
		authorized = nil
		resolved, err := store.ResolveAuthorized(ctx, virtual, authorize)
		require.NoError(t, err)
		require.Equal(t, []Address{resolved.Authz}, authorized,
			"the caller authorized the requested address already; the redirect's own address is the extra question")
	})

	t.Run("a resolver that answers an incomplete target is a server error", func(t *testing.T) {
		store := NewStore(f.catalog, f.catalog.BlockAdapter, WithTableResolver(resolverRedirectStub{
			target: Address{Repo: tablesTestRepo, Ref: tablesTestRef},
		}))
		_, err := store.Resolve(ctx, virtual)
		require.ErrorIs(t, err, ErrInvalidResolution)
	})
}

// resolverRedirectStub is a TableResolver that handles every address with a fixed answer, for the
// tests of the store's generic half.
type resolverRedirectStub struct {
	target Address
	authz  Address
}

func (s resolverRedirectStub) Resolve(context.Context, Address) (ResolvedTable, bool, error) {
	return ResolvedTable{Target: s.target, Authz: s.authz}, true, nil
}

// TestMetadataSearchRefMapperSeam pins the seam through which the indexer installs its RefMapper
// (resolver.go): the built-in resolver is registered with it when it is set, and with none when it
// is not. The test restores the variable, so it cannot leak into another test's mount.
func TestMetadataSearchRefMapperSeam(t *testing.T) {
	previous := MetadataSearchRefMapper
	t.Cleanup(func() { MetadataSearchRefMapper = previous })

	MetadataSearchRefMapper = nil
	require.Nil(t, metadataSearchRefMapper(nil))

	installed := resolverRefMapper{refs: map[string]string{"v1": "indexed-v1"}}
	MetadataSearchRefMapper = func(BranchLister) RefMapper { return installed }

	mapper := metadataSearchRefMapper(nil)
	require.NotNil(t, mapper)
	mapped, err := mapper.MapRef(t.Context(), tablesTestRepo, "v1")
	require.NoError(t, err)
	require.Equal(t, "indexed-v1", mapped)

	// A resolver built with that mapper resolves a tag, which the default (no mapper) refuses.
	branches := resolverBranchLister{branches: map[string]bool{tablesTestRepo + "/" + tablesTestRef: true}}
	resolved, handled, err := NewMetadataSearchResolver(branches, mapper).Resolve(t.Context(), resolverVirtualAddress("v1"))
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, "indexed-v1", resolved.Target.Ref)
}

// TestMetadataSearchErrorMapping pins the status and the Iceberg error type of a reserved-namespace
// refusal, so the 403 of §5.4 cannot drift into a 404 or a 500.
func TestMetadataSearchErrorMapping(t *testing.T) {
	err := &ReservedNamespaceError{Repo: tablesTestRepo, Ref: tablesTestRef, Namespace: Namespace{SystemNamespace}}
	require.True(t, errors.Is(err, ErrReservedNamespace))
	require.Equal(t, http.StatusForbidden, err.HTTPStatusCode())
	require.Equal(t, ErrorTypeForbidden, err.ErrorType())
	require.Contains(t, err.Error(), SystemNamespace)

	rec := httptest.NewRecorder()
	(&tableService{}).writeError(rec, err)
	requireEnvelope(t, rec, http.StatusForbidden, ErrorTypeForbidden)

	rec = httptest.NewRecorder()
	(&namespaceService{}).writeError(rec, err)
	requireEnvelope(t, rec, http.StatusForbidden, ErrorTypeForbidden)
}
