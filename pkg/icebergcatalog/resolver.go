package icebergcatalog

// The table-resolution hook (METADATA_SEARCH_DESIGN.md §5.1-5.4, task R10-1).
//
// Metadata Search puts the system metadata of a data repository R into an ordinary companion
// repository R-metadata, one Iceberg table per mirrored branch, and lets a user query it from the
// root of the catalog:
//
//	system.metadata.<R>.<ref>.objects  →  <R>-metadata.<mapped ref>.system.object_metadata
//
// The root tree - its nodes, its listings, its permissions and its reservation - is the section
// "The root `system.metadata` tree" at the end of this file. It is the only thing the lakeFS
// catalog (`/iceberg/api` and `relative_to`) serves: there is no `system` namespace, no virtual
// table and no reservation below `<repo>.<ref>` (METADATA_SEARCH_REALIGN.md §2, RA3).
//
// The identifier of the older guide, `<R>.<ref>.system.object_metadata`, still resolves - to the
// same companion table - but only in the guide's deprecated Metadata Search catalog at
// `/mds/iceberg/api`, which is a catalog of its own (NewDeprecatedMetadataSearchResolver). It is
// never an alias of the lakeFS catalog and its clients' addresses are ordinary addresses of the
// lakeFS catalog: there they mean what they say, and `system.object_metadata` of a data repository
// is the 404 of a table nobody created.
//
// The catalog is the only place that knows those mappings, so it is the only place that has to
// change: this file is a hook, not a feature. A TableResolver is registered by the server
// (WithTableResolver); while none is registered the catalog behaves exactly as it did before this
// file existed - every address resolves to itself, nothing is listed, no namespace is reserved.
//
// # The hook
//
// Resolve is asked about one parsed Address (ident.go) before the catalog reads it. A resolver
// answers one of three things:
//
//   - not handled (handled == false): the address means what it always meant;
//   - handled: read Target instead, and check the caller's permission against Authz;
//   - an error, which is answered to the client as it is. A resolver that cannot answer a
//     well-formed address - the table does not exist on that ref, the companion repository was
//     never indexed - returns a *NoSuchTableException, i.e. the 404 the client's load or head
//     would get for a table that is not there.
//
// Both addresses in a ResolvedTable are complete Addresses, so a resolver can move a read across
// repositories, refs, namespaces and table names in one step. Authorization is deliberately a
// separate field: the metadata table of §5.2 is read by a caller who has no access to R-metadata at
// all, and the rule of §5.3 is one catalog:ReadTable (or catalog:ReadTableData) check on the
// *data* repository. The redirected read itself runs with the server's own identity - the catalog
// reads metadata.json through its block adapter, not with the caller's credentials - which is what
// makes that possible without granting the caller anything on R-metadata.
//
// # Order of authorization and resolution
//
// Every read path authorizes the address the caller named *before* it asks a resolver anything,
// exactly as it did before this hook existed (see "Load, head, drop and the metrics sink are all
// authorized before the pointer is resolved" in tables.go). A resolver is therefore never told
// about an address by a caller who may not read it, and resolution cannot be used to probe a
// repository the caller has no grant on. When the resolver answers with an Authz address that is
// not the address the caller named, the catalog authorizes that address too (Store.ResolveAuthorized),
// so a resolver can never grant more than the caller already had. The built-in resolver below
// answers Authz == the requested address, which is the single data-repository check of §5.3.
//
// # The virtual table (§5.2)
//
// MetadataSearchResolver implements the one virtual table the design fixes, at the address the
// current guide names (or, built by NewDeprecatedMetadataSearchResolver, at the address of the
// older one):
//
//	system.metadata.<R>.<ref>.objects → <R>-metadata.<mapped ref>.system.object_metadata
//	R.<ref>.system.object_metadata    → <R>-metadata.<mapped ref>.system.object_metadata  (deprecated catalog only)
//
// <ref> is resolved by the table's own rules:
//
//   - a branch of R maps to the branch of the same name in R-metadata. That is the identity the
//     indexer mirrors, and it needs no lookup beyond "is this a branch?";
//   - a tag or a commit is not a branch, so it has no mirrored branch of its own: it goes to the
//     RefMapper, which maps it through the index's own record of the source commit
//     (lakefs.mds.source-commit, §5.2). A resolver built without a RefMapper refuses those refs
//     with NoSuchTableException rather than guessing - the table of a tag is not the table of the
//     branch the tag names.
//
// R-metadata is an ordinary repository: its own system namespace is not virtual, and this resolver
// never redirects it (see IsMetadataRepository). That is what lets the indexer write
// R-metadata's system.object_metadata as a normal table.
//
// # The reserved namespace (§5.4)
//
// The only reserved namespace left is the root `system` repository of RA1, and it belongs to the
// root tree, not to any data repository: every catalog write into it - create, commit, register,
// rename, drop, and namespace create, update and drop - is refused with 403 ForbiddenException
// (*ReservedNamespaceError). Nothing below `<repo>.<ref>` is reserved any more
// (METADATA_SEARCH_REALIGN.md §2, RA3): a client may address `system` in its own repository like
// any other namespace name, and nothing the catalog does is hidden behind it.
//
// The deprecated catalog has no reservation at all: it serves the old identifier and nothing else,
// so there is no tree of its own to protect.
//
// The listing capability stays because the deprecated catalog needs it: it lists the `system`
// namespace of a data repository and the `object_metadata` table inside it, so an upstream client's
// browse keeps answering what it answered before (G5). Both listings run the ordinary
// authorization filter afterwards, so a caller without a grant on the namespace does not see them
// either.
//
// # What the hook does not do
//
// Nothing else in the catalog changes. A resolver cannot make a table writable through a virtual
// address (writes never resolve: the root tree refuses them first, and the deprecated catalog
// mounts read operations only), cannot change what a commit compares, and cannot reach the
// catalog's staging area except by naming an address the caller could have named. The interface is
// a read path, and the four methods of VirtualTableSource are listing and refusal rules - not data.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
)

const (
	// SystemNamespace is the root repository of the Metadata Search tree, and the name of the
	// physical namespace of the companion repository. At the root it is reserved (every write is
	// refused); inside a data repository it is an ordinary namespace name like any other
	// (METADATA_SEARCH_REALIGN.md §2, RA3).
	SystemNamespace = "system"

	// ObjectMetadataTable is the name of the table the indexer writes: the system metadata of every
	// object of the data repository, indexed per branch (§3.1). Inside a data repository the name
	// is not reserved: the table is served at the root tree as `objects`, and only the deprecated
	// catalog still answers it at `<R>.<ref>.system.object_metadata`.
	ObjectMetadataTable = "object_metadata"

	// MetadataNamespace and ObjectsTable name the root tree of the current Metadata Search guide
	// (fetched 2026-11-09, METADATA_SEARCH_REALIGN.md G1): the tables are addressed at the root of
	// the lakeFS catalog as
	//
	//	system.metadata.<data repo>.<ref>.objects
	//
	// i.e. in the reserved root repository `system`, on its `metadata` ref, one namespace per data
	// repository and, inside it, one namespace per ref. Neither name belongs to a data repository:
	// there is no `system` namespace inside a repository any more (the old identifier lives only in
	// the deprecated separate catalog, G5).
	MetadataNamespace = "metadata"

	// ObjectsTable is the table name of the root tree: one table per data repository ref.
	ObjectsTable = "objects"

	// MetadataRepositorySuffix is the suffix that names the companion repository of a data
	// repository R: R-metadata (§3.2). It is the one naming convention the catalog and the
	// indexer share, so it is declared here, in the package both use.
	MetadataRepositorySuffix = "-metadata"

	// MetadataSearchRepositoryProperty is the table property that says which data repository a
	// Metadata Search index describes (METADATA_SEARCH_DESIGN.md §3.1). The indexer writes it into
	// every system.object_metadata table it creates, with the name of the data repository as the
	// value; the catalog reads it before it serves a resolved virtual table, because the name
	// R-metadata alone does not prove who wrote the table (MDS-SEC-3).
	//
	// The literal is restated here rather than imported: it is written by
	// pkg/mds/table/schema.go (PropertyRepository) on the Metadata Search branch, and this package
	// must not import pkg/mds - the dependency runs the other way (the indexer imports this
	// package for MetadataRepository and Address). The two spellings are pinned together by the
	// design §3.1, not by a Go identifier.
	MetadataSearchRepositoryProperty = "lakefs.mds.repository"

	// MetadataSearchInitProperty, MetadataSearchInitValue and MetadataSearchInitCommitMessage are
	// the initialisation marker the indexer writes on R-metadata (METADATA_SEARCH_DESIGN.md §4.8).
	// They are the second proof of ownership, and the one a forger cannot fake without writing
	// R-metadata's history: the table property alone lives in metadata.json, which anyone who can
	// create that repository can write (MDS-SEC-3).
	//
	// The literals are restated here rather than imported: pkg/mds/indexer/repo.go
	// (MetadataKeyInit, MetadataKeyInitValue) and pkg/mds/indexer/indexer.go (InitCommitMessage)
	// write them, and this package must not import pkg/mds - the dependency runs the other way.
	// The design §4.8 pins the two spellings together.
	MetadataSearchInitProperty      = "lakefs.mds.init"
	MetadataSearchInitValue         = "true"
	MetadataSearchInitCommitMessage = "mds: init"

	metadataSearchInitPageSize = 256

	// metadataSearchInitCacheMaxRepositories bounds the positive ownership cache. An entry is one
	// repository whose init commit was found; the oldest entry is evicted when the cache is full,
	// so a process that sees many repositories (or recreated ones) does not grow without bound.
	metadataSearchInitCacheMaxRepositories = 1024

	// metadataSearchInitCacheMaxHeads bounds the verified-head witnesses kept per repository. The
	// init commit itself is the immutable fact; the witnesses only let a new head stop walking as
	// soon as it reaches a history that was already proven to contain that commit.
	metadataSearchInitCacheMaxHeads = 8
)

// commitLogLister is the lakeFS commit-history read the ownership proof needs. *catalog.Catalog
// implements it; the resolver reaches it through the BranchLister it was registered with, so a
// caller that only knows how to answer "is this a branch?" still resolves addresses, but cannot
// prove a companion table is the indexer's.
type commitLogLister interface {
	ListCommits(ctx context.Context, repositoryID string, ref string, params catalog.LogParams) ([]*catalog.CommitLog, bool, error)
}

// ErrInvalidResolution is returned when a registered TableResolver answers with a target that is
// not a table address. It is a server-side misconfiguration (a resolver bug), never a property of
// the request, so the HTTP layer answers 500 instead of turning it into a client error.
var ErrInvalidResolution = errors.New("invalid table resolution")

// ResolvedTable is what a TableResolver answers for one address: where the table is read from and
// which address the caller's permission is checked against.
type ResolvedTable struct {
	// Target is the address the catalog reads: the repository, ref, namespace and table whose
	// pointer and metadata.json serve the request. The redirected read runs with the server's
	// identity - the catalog's own catalog and block adapter - so the caller needs no lakeFS
	// access to this address.
	Target Address

	// Authz is the address the caller must be allowed to read (METADATA_SEARCH_DESIGN.md §5.3:
	// the data repository, never the companion repository). An empty Authz means "the address the
	// caller named", which is also what a resolver that does not move a read answers.
	Authz Address
}

// TableResolver is the extension point the server registers to redirect table reads
// (METADATA_SEARCH_DESIGN.md §5.1). The catalog asks it about a parsed address on every table read
// path - load, HEAD, the table listing, credentials, remote signing and metrics, on both addressing
// forms - and about nothing else.
//
// The bool reports whether the resolver handled the address. A resolver that handles an address
// owns it: the catalog reads Target and checks Authz instead of reading the address itself. A
// resolver that does not handle it must leave the address alone (false, nil).
//
// Resolve is called after the caller was authorized for the address it names, so it must not be
// used to disclose anything the catalog itself would not. It may do I/O: the built-in resolver asks
// the catalog whether a ref is a branch.
//
// A resolver whose redirect is only valid if the target table proves it belongs there implements
// ResolvedTableVerifier as well; the built-in Metadata Search resolver does, because a repository
// whose name ends in -metadata is not necessarily R's index (MDS-SEC-3).
type TableResolver interface {
	Resolve(ctx context.Context, addr Address) (ResolvedTable, bool, error)
}

// ResolvedTableVerifier is the optional half of a TableResolver whose target has to prove that it
// is the table the resolver meant to serve. Store.Resolve loads the target and asks the verifier
// before it hands the resolution to the read path; a resolver that does not implement it is
// trusted with the target it answered (the hook's original contract).
//
// It is a separate interface because only a resolver that moves a read *across repositories* has a
// question the address alone cannot answer. The built-in Metadata Search resolver answers R's
// virtual table with R-metadata's system.object_metadata, and the repository name is the only
// thing that connects the two: a repository someone else created under that name would otherwise
// be served, with the server's own identity, to every reader of R (MDS-SEC-3).
type ResolvedTableVerifier interface {
	// VerifyResolved reports whether tbl, loaded from target, is the table the resolver serves for
	// requested. A *NoSuchTableException hides it as a table that is not there, which is the
	// answer the virtual address gives when its index does not exist.
	VerifyResolved(ctx context.Context, requested, target Address, tbl *Table) error
}

// RefMapper maps a ref of a data repository to a ref of its companion repository
// (METADATA_SEARCH_DESIGN.md §5.2). The design's default behaviour - a branch maps to the branch of
// the same name - is what MetadataSearchResolver does without a mapper; a mapper exists for the
// refs that have no such branch, i.e. commits and tags, which the indexer resolves through its own
// record of the source commit.
//
// MapRef returns a *NoSuchTableException to refuse a ref the index knows nothing about: the client
// then gets the 404 an unindexed ref deserves.
type RefMapper interface {
	// MapRef maps ref of dataRepo to a ref of dataRepo + MetadataRepositorySuffix.
	MapRef(ctx context.Context, dataRepo, ref string) (metadataRef string, err error)
}

// VirtualTableSource is the optional half of a TableResolver that owns a namespace of a data
// repository rather than a single address: it declares the names a listing adds and the names
// clients may not write.
//
// It is separate from TableResolver because both are rules about addresses the resolver does *not*
// redirect. Only the deprecated catalog's resolver implements it: the lakeFS catalog has nothing
// virtual, listed or reserved below `<repo>.<ref>` (METADATA_SEARCH_REALIGN.md §2, RA3).
type VirtualTableSource interface {
	// VirtualNamespaces returns the child namespaces the resolver adds below addr, a data
	// repository ref (ListNamespaces with parent <repo><sep><ref>). Empty for an address that is
	// not such a parent.
	VirtualNamespaces(addr Address) []string

	// VirtualTables returns the table names the resolver adds to addr.Namespace of a data
	// repository (ListTables). Empty for a namespace the resolver does not own.
	VirtualTables(addr Address) []string

	// ReservedNamespace reports whether addr.Namespace is owned by the resolver on a data
	// repository - and so read-only for clients (§5.4). The indexer's own writes address the
	// companion repository, where this is false.
	ReservedNamespace(addr Address) bool
}

// MetadataSearchRefMapper is the seam through which the Metadata Search index - a package of its
// own, which imports this one - installs its RefMapper for the built-in resolver
// (METADATA_SEARCH_DESIGN.md §5.2, milestone M4). It is a package variable rather than a parameter
// of this package's API because the dependency runs one way: this package must not import the
// indexer, and the indexer needs a BranchLister (the lakeFS catalog) to build its mapper. A
// package that installs one assigns it before the server mounts the catalog; nil keeps the
// documented default of this release, in which a tag or a commit is answered NoSuchTableException
// because the index mirrors branches only.
var MetadataSearchRefMapper func(branches BranchLister) RefMapper

// metadataSearchRefMapper returns the RefMapper the built-in resolver is registered with: the
// mapper the index installed, if any, and nil otherwise.
func metadataSearchRefMapper(branches BranchLister) RefMapper {
	if MetadataSearchRefMapper == nil {
		return nil
	}
	return MetadataSearchRefMapper(branches)
}

// ReservedNamespaceError is the 403 of a client write into a namespace a resolver reserved
// (METADATA_SEARCH_DESIGN.md §5.4). It is answered as 403 ForbiddenException: the caller may write
// the repository, just not this namespace.
type ReservedNamespaceError struct {
	// Repo and Ref are the address the write named.
	Repo string
	Ref  string
	// Namespace is the reserved namespace. It is never empty.
	Namespace Namespace
}

// Error implements error. It names what is reserved and why, because a client cannot correct a
// request otherwise - the namespace is not a typo, it is a namespace this catalog owns.
func (e *ReservedNamespaceError) Error() string {
	return fmt.Sprintf("namespace %q of %s@%s is reserved for catalog-managed metadata: it is read-only for clients",
		e.Namespace.Dotted(), e.Repo, e.Ref)
}

// Unwrap reports the sentinel a caller can classify the refusal with.
func (e *ReservedNamespaceError) Unwrap() error { return ErrReservedNamespace }

// ErrorType is the Iceberg error type of the refusal: 403 ForbiddenException, like every other
// refusal of the catalog.
func (e *ReservedNamespaceError) ErrorType() string { return ErrorTypeForbidden }

// HTTPStatusCode is the status of the refusal, 403.
func (e *ReservedNamespaceError) HTTPStatusCode() int { return http.StatusForbidden }

// ErrReservedNamespace is wrapped by *ReservedNamespaceError.
var ErrReservedNamespace = errors.New("the namespace is reserved and read-only")

// MetadataRepository returns the name of the companion repository of dataRepo (§3.2).
func MetadataRepository(dataRepo string) string {
	return dataRepo + MetadataRepositorySuffix
}

// IsMetadataRepository reports whether repo is the companion repository of a data repository, i.e.
// whether its system namespace is ordinary. The naming convention of §3.2 is the only thing that
// distinguishes the two kinds of repository, so it is what this predicate encodes: a repository
// whose name ends with -metadata is a companion, and a data repository that itself ends with
// -metadata would therefore have a companion named <repo>-metadata-metadata and keep an ordinary
// system namespace of its own. That is a documented consequence of the convention, not a special
// case the catalog can resolve.
func IsMetadataRepository(repo string) bool {
	return strings.HasSuffix(repo, MetadataRepositorySuffix) && len(repo) > len(MetadataRepositorySuffix)
}

// MetadataSearchResolver is the built-in TableResolver of METADATA_SEARCH_DESIGN.md §5.2. In the
// lakeFS catalog it serves system.metadata.R.<ref>.objects out of R-metadata and reserves the root
// system tree (§5.4, METADATA_SEARCH_REALIGN.md RA1); built by NewDeprecatedMetadataSearchResolver
// it serves R.<ref>.system.object_metadata out of R-metadata instead and lists it where the older
// guide said it lived, for the separate /mds/iceberg/api catalog only (G5, RA3).
//
// Register it only while the Metadata Search feature is enabled (iceberg.metadata_search.enabled,
// §5.5): the resolver is what makes the metadata table and the reserved root namespace exist, so a
// catalog that does not register it is exactly the catalog of every other release.
type MetadataSearchResolver struct {
	// branches tells a branch from a tag or a commit. A nil lister treats every ref as a branch,
	// which is the identity mapping of §5.2; the production wiring passes the catalog.
	branches BranchLister

	// mapper maps a tag or a commit to a ref of the companion repository (§5.2). A nil mapper
	// refuses those refs with NoSuchTableException instead of guessing.
	mapper RefMapper

	// deprecated serves the guide's deprecated catalog: the per-repository identifier
	// R.<ref>.system.object_metadata, its listing, and nothing of the root tree. It is never active
	// in the lakeFS catalog (METADATA_SEARCH_REALIGN.md G5, RA3).
	deprecated bool

	// initMu guards initCache, the found initialisation commit of each repository and the heads
	// whose first-parent history was already proven to contain it. The init commit is immutable
	// once found; a new head is verified incrementally, by walking back only until it reaches the
	// init commit or one of the verified heads, instead of walking the whole history again.
	initMu    sync.Mutex
	initCache map[string]*metadataSearchInitCacheEntry
	initOrder []string
}

// metadataSearchInitCacheEntry is one repository's ownership proof: the init commit the indexer
// wrote, plus a bounded set of heads known to descend from it. commitID is immutable for the life
// of the entry.
type metadataSearchInitCacheEntry struct {
	commitID string
	heads    []string
}

// NewMetadataSearchResolver builds the built-in resolver of the lakeFS catalog. branches answers
// "is this ref a branch?" (the catalog implements it); mapper is the optional RefMapper that gives
// tags and commits a meaning in the companion repository. Both may be nil: no lister means every
// ref is treated as a branch, no mapper means a tag or a commit is not indexed and therefore
// NoSuchTable.
func NewMetadataSearchResolver(branches BranchLister, mapper RefMapper) *MetadataSearchResolver {
	return &MetadataSearchResolver{
		branches:  branches,
		mapper:    mapper,
		initCache: make(map[string]*metadataSearchInitCacheEntry),
	}
}

// NewDeprecatedMetadataSearchResolver builds the resolver of the guide's deprecated, separate
// Metadata Search catalog at /mds/iceberg/api (METADATA_SEARCH_REALIGN.md G5, RA3). It is the same
// resolver with the old per-repository address in place of the root tree:
//
//	R.<ref>.system.object_metadata  →  <R>-metadata.<mapped ref>.system.object_metadata
//
// It serves load, head and list of that one identifier and resolves nothing else. Because the
// deprecated catalog is a catalog of its own, the identifier is not an alias of anything in the
// lakeFS catalog: the same address there is an ordinary address and answers 404.
func NewDeprecatedMetadataSearchResolver(branches BranchLister, mapper RefMapper) *MetadataSearchResolver {
	resolver := NewMetadataSearchResolver(branches, mapper)
	resolver.deprecated = true
	return resolver
}

// Resolve answers the address of §5.2 and leaves every other address alone.
//
// In the lakeFS catalog the only address it answers is the root tree's table,
// system.metadata.<data repo>.<ref>.objects; in the deprecated catalog the only address it answers
// is <data repo>.<ref>.system.object_metadata. Everything else - including the other form - is not
// handled (false, nil), so the rest of the catalog is untouched: R-metadata itself is one of those
// addresses, which is what lets the indexer own its system namespace, and so is any address below
// `<repo>.<ref>` of a data repository, which the lakeFS catalog treats like every other address
// (METADATA_SEARCH_REALIGN.md §2, RA3).
//
// For the handled address:
//
//   - a branch of the data repository maps to the branch of the same name in R-metadata. The
//     authorization address is the data repository the caller named: one catalog:ReadTable check
//     on it, and nothing on R-metadata (§5.3).
//   - a tag or a commit goes to the RefMapper. Without one it is refused with
//     NoSuchTableException: the index mirrors branches, and the snapshot a tag or a commit names is
//     the RefMapper's question, not this resolver's (§5.2).
//
// A repository that does not exist, or any other failure to resolve the ref, is returned as it is,
// so the client gets the 404 NoSuchWarehouseException (or the 500) the same address would answer
// without the hook.
func (r *MetadataSearchResolver) Resolve(ctx context.Context, addr Address) (ResolvedTable, bool, error) {
	if r.deprecated {
		return r.resolveDataRepository(ctx, addr)
	}
	dataRepo, ref, ok := systemObjectsAddress(addr)
	if !ok {
		return ResolvedTable{}, false, nil
	}
	// The root tree's table: system.metadata.<R>.<ref>.objects → the companion table of R.
	mapped, err := r.objectsRef(ctx, dataRepo, ref, addr)
	if err != nil {
		return ResolvedTable{}, true, err
	}
	authz, _ := systemTreeAuthz(addr)
	return ResolvedTable{
		Target: Address{
			Repo:      MetadataRepository(dataRepo),
			Ref:       mapped,
			Namespace: Namespace{SystemNamespace},
			Table:     ObjectMetadataTable,
		},
		Authz: authz,
	}, true, nil
}

// resolveDataRepository answers the deprecated catalog's address, <R>.<ref>.system.object_metadata,
// and leaves everything else alone (G5).
func (r *MetadataSearchResolver) resolveDataRepository(ctx context.Context, addr Address) (ResolvedTable, bool, error) {
	if !isObjectMetadataAddress(addr) {
		return ResolvedTable{}, false, nil
	}
	ref, err := r.objectsRef(ctx, addr.Repo, addr.Ref, addr)
	if err != nil {
		return ResolvedTable{}, true, err
	}
	return ResolvedTable{
		Target: Address{
			Repo:      MetadataRepository(addr.Repo),
			Ref:       ref,
			Namespace: Namespace{SystemNamespace},
			Table:     ObjectMetadataTable,
		},
		Authz: addr,
	}, true, nil
}

// objectsRef maps the ref of the data repository onto a ref of the companion repository: the same
// name for a branch (§5.2), the RefMapper's answer for a tag or a commit. It is the mapping of both
// addressing forms: the deprecated catalog's per-repository table and the root system tree name the
// same companion table, so they must not be able to disagree.
//
// A ref the catalog cannot look up is returned as the missing warehouse it is, never silently
// treated as "not a branch": the difference decides between 404 NoSuchWarehouseException and 404
// NoSuchTableException, and only the catalog can tell them apart. A nil BranchLister assumes every
// ref is a branch: without a lister there is nothing to ask, and the identity mapping is the
// documented default.
//
// requested is the address the client named; it only names the table a 404 is about, so a caller of
// either addressing form gets the answer for its own address.
func (r *MetadataSearchResolver) objectsRef(ctx context.Context, dataRepo, ref string, requested Address) (string, error) {
	if r.branches == nil {
		return ref, nil
	}
	_, err := r.branches.GetBranchReference(ctx, dataRepo, ref)
	switch {
	case err == nil:
		return ref, nil
	case errors.Is(err, graveler.ErrBranchNotFound), errors.Is(err, graveler.ErrInvalidBranchID):
		// A tag or a commit: no branch of that name exists, so there is no mirrored branch to read
		// and the RefMapper decides whether the index has a table for it.
	default:
		return "", err
	}
	if r.mapper == nil {
		// No mapper: a tag or a commit has no mirrored branch, and returning the branch of the same
		// name would answer a question nobody asked. §5.2: NoSuchTableException.
		return "", &NoSuchTableException{
			Repo:      requested.Repo,
			Ref:       requested.Ref,
			Namespace: requested.Namespace,
			Table:     requested.Table,
		}
	}
	mapped, err := r.mapper.MapRef(ctx, dataRepo, ref)
	if err != nil {
		// A *NoSuchTableException is the mapper's refusal of an unindexed ref and is answered as
		// the 404 it is; anything else is a failure to resolve and stays one.
		return "", err
	}
	if mapped == "" {
		return "", fmt.Errorf("%w: the ref mapper returned an empty ref for %q of %q",
			ErrInvalidResolution, ref, dataRepo)
	}
	return mapped, nil
}

// VerifyResolved implements ResolvedTableVerifier: R-metadata is R's index only when the table
// says so and carries the indexer's initialisation commit. Two independent facts have to hold,
// because either one alone can be written by whoever created R-metadata:
//
//   - the table property lakefs.mds.repository names R (§3.1); and
//   - the served ref's first-parent history carries the initialisation commit (§4.8): message
//     "mds: init" and metadata lakefs.mds.init=true, positioned as the first commit directly above
//     the repository's root commit - or as that root commit itself, for a companion repository
//     created bare, which is the form the indexer accepts there too. That commit is written by the
//     indexer after it creates the table, so it also commits the table and makes the repository's
//     history the state a forger cannot reproduce by editing metadata.json.
//
// The committer is deliberately not part of the proof (METADATA_SEARCH_REALIGN.md X4): the guide
// says the sync may run "as another user instead" with MDSCatalogAccess, so a check pinned to one
// committer name would refuse a legitimate sync. The marker and the position are what the indexer
// writes; who it authenticated as is an operator decision, not a fact about the table.
//
// A repository someone else created under the companion name can copy the property, but it has no
// initialisation commit on the history the resolver is about to serve. It is answered as an absent
// table: R-metadata may be an ordinary repository, and serving it would show every reader of R
// attacker-chosen rows under the server's own identity (§5.2). The indexer refuses such a
// repository permanently (§4.8), so the catalog has to refuse it too.
//
// The answer is NoSuchTableException for the address the caller named, which is the 404 the design
// promises for an unindexed ref: from the caller's side there is no object_metadata table on that
// ref of R, and there is nothing else it could mean.
func (r *MetadataSearchResolver) VerifyResolved(ctx context.Context, requested, target Address, tbl *Table) error {
	// The tree's address carries the data repository as a namespace level, so the property is
	// compared with the repository the address names, not with the reserved root repository.
	dataRepo := requested.Repo
	if repo, _, ok := systemObjectsAddress(requested); ok {
		dataRepo = repo
	}
	if tbl == nil || tbl.Metadata == nil ||
		tbl.Metadata.Properties()[MetadataSearchRepositoryProperty] != dataRepo {
		return noSuchMetadataTable(requested)
	}
	carries, err := r.firstParentCarriesInitCommit(ctx, target)
	if err != nil {
		return err
	}
	if !carries {
		return noSuchMetadataTable(requested)
	}
	return nil
}

// noSuchMetadataTable is the 404 of a virtual table whose companion cannot prove it is R's index.
func noSuchMetadataTable(requested Address) error {
	return &NoSuchTableException{
		Repo:      requested.Repo,
		Ref:       requested.Ref,
		Namespace: requested.Namespace,
		Table:     requested.Table,
	}
}

// firstParentCarriesInitCommit reports whether the first-parent history of target.Ref carries the
// indexer's initialisation commit (§4.8). The full walk is newest first and stops at the root, so
// its cost is the length of the served history in the worst case; the init commit is then cached
// per repository, and a later head is verified incrementally: the walk stops as soon as it reaches
// that immutable commit or a previously verified head. A branch that only gained commits pays for
// the new commits, not for the whole history again.
//
// The init commit is the first commit directly above the repository's root commit, or the root
// commit itself when the companion repository was created bare (§3.3, §4.8). That is a position, not
// just a message: an mds: init commit somewhere higher in history is not the commit the indexer wrote
// when it initialised the repository. The bare form is the one other position a root can hold, and it
// is not weaker evidence: a commit with no parents is a repository's first commit, so it cannot be
// added on top of a history the catalog already trusts; it appears when the repository is created
// (pkg/catalog/catalog.go, CreateBareRepository) or as the first commit of an empty branch, and the
// committer fact then still requires the indexer principal, which the commit API takes from the
// authenticated user and not from the request (pkg/api/controller.go, Commit). The residual of §5.1
// (CreateCommitRecord names any committer, on either form) is not widened by accepting it. It is the
// shape the indexer accepts (pkg/mds/indexer/repo.go, initScan.accepted), and a companion the indexer
// indexes must be one the catalog serves.
//
// A resolver built with a BranchLister that cannot read commit history fails closed: the property
// proves nothing without the commit, so the virtual table is the 404 an unindexed ref answers.
func (r *MetadataSearchResolver) firstParentCarriesInitCommit(ctx context.Context, target Address) (bool, error) {
	lister, ok := r.branches.(commitLogLister)
	if !ok {
		return false, nil
	}

	initCommitID, verifiedHeads, cached := r.cachedInitCommit(target.Repo)
	if !cached {
		found, initID, head, err := r.findInitCommit(ctx, lister, target)
		if err != nil {
			return false, err
		}
		if !found {
			return false, nil
		}
		r.rememberInitCommit(target.Repo, initID, head)
		return true, nil
	}

	found, head, err := r.cachedInitCommitOnHistory(ctx, lister, target, initCommitID, verifiedHeads)
	if err != nil {
		return false, err
	}
	if !found {
		// The immutable init commit is not on this history: this branch is not an index of the
		// data repository, even if it carries a look-alike marker somewhere above the root.
		return false, nil
	}
	r.rememberVerifiedHead(target.Repo, initCommitID, head)
	return true, nil
}

// findInitCommit walks the first-parent history from target.Ref and returns the commit that carries
// the indexer's marker, its committer and its position, when there is one. The position is the first
// commit directly above the root, or the root itself for a companion repository created bare. The
// candidate is held from one commit to the next: when the root arrives, the previous commit is the
// only one that can be "directly above" it, and the root itself is the other. A root that carries
// the marker is not held back for a later page: the root has no parent, so the walk ends there.
func (r *MetadataSearchResolver) findInitCommit(ctx context.Context, lister commitLogLister, target Address) (found bool, initCommitID, head string, err error) {
	after := ""
	var previous *catalog.CommitLog
	for {
		page, hasMore, err := r.initCommitPage(ctx, lister, target, after)
		if err != nil {
			return false, "", "", err
		}
		if len(page) == 0 {
			return false, "", "", nil
		}
		if head == "" {
			head = page[0].Reference
		}
		for _, commit := range page {
			if len(commit.Parents) == 0 {
				// The root. The bare companion's initialisation commit is this commit itself;
				// otherwise the commit visited immediately before it is the only one that can be
				// "directly above" it, and no ancestor can carry the marker (§4.8).
				if isMetadataSearchInitCommit(commit) {
					return true, commit.Reference, head, nil
				}
				if previous != nil && len(previous.Parents) == 1 && previous.Parents[0] == commit.Reference &&
					isMetadataSearchInitCommit(previous) {
					return true, previous.Reference, head, nil
				}
				return false, "", "", nil
			}
			previous = commit
		}
		if !hasMore {
			return false, "", "", nil
		}
		last := page[len(page)-1].Reference
		if last == "" || last == after {
			return false, "", "", fmt.Errorf("%w: the first-parent log of %q@%s did not advance",
				ErrInvalidResolution, target.Repo, target.Ref)
		}
		after = last
	}
}

// cachedInitCommitOnHistory verifies a cached init commit against a new (or repeated) head without
// re-walking the whole history: it stops at the init commit itself or at any head already proven to
// descend from it. The returned head is the branch head the walk started from, which becomes a new
// witness when the answer is true.
func (r *MetadataSearchResolver) cachedInitCommitOnHistory(ctx context.Context, lister commitLogLister, target Address, initCommitID string, verifiedHeads []string) (bool, string, error) {
	after := ""
	head := ""
	for {
		page, hasMore, err := r.initCommitPage(ctx, lister, target, after)
		if err != nil {
			return false, "", err
		}
		if len(page) == 0 {
			return false, "", nil
		}
		if head == "" {
			head = page[0].Reference
		}
		for _, commit := range page {
			if commit.Reference == initCommitID || slices.Contains(verifiedHeads, commit.Reference) {
				return true, head, nil
			}
			if len(commit.Parents) == 0 {
				return false, head, nil
			}
		}
		if !hasMore {
			return false, head, nil
		}
		last := page[len(page)-1].Reference
		if last == "" || last == after {
			return false, "", fmt.Errorf("%w: the first-parent log of %q@%s did not advance",
				ErrInvalidResolution, target.Repo, target.Ref)
		}
		after = last
	}
}

// initCommitPage reads one page of the first-parent log, starting after the page's last commit.
func (r *MetadataSearchResolver) initCommitPage(ctx context.Context, lister commitLogLister, target Address, after string) ([]*catalog.CommitLog, bool, error) {
	params := catalog.LogParams{FirstParent: true, Amount: metadataSearchInitPageSize}
	if after != "" {
		params.FromReference = after
	}
	page, hasMore, err := lister.ListCommits(ctx, target.Repo, target.Ref, params)
	if err != nil {
		return nil, false, fmt.Errorf("read the first-parent log of %q@%s: %w", target.Repo, target.Ref, err)
	}
	return page, hasMore, nil
}

// isMetadataSearchInitCommit reports whether commit carries the indexer's initialisation marker.
// The metadata marker and the message are both required, so a commit that merely copied one of
// them is not mistaken for the indexer's. The committer is not checked: the guide lets the sync run
// as another user with MDSCatalogAccess (X4).
func isMetadataSearchInitCommit(commit *catalog.CommitLog) bool {
	return commit != nil &&
		commit.Message == MetadataSearchInitCommitMessage &&
		commit.Metadata[MetadataSearchInitProperty] == MetadataSearchInitValue
}

// cachedInitCommit returns the immutable init commit of one repository and a copy of the heads that
// were already verified to descend from it.
func (r *MetadataSearchResolver) cachedInitCommit(repo string) (commitID string, heads []string, ok bool) {
	r.initMu.Lock()
	defer r.initMu.Unlock()
	entry, ok := r.initCache[repo]
	if !ok || entry == nil {
		return "", nil, false
	}
	return entry.commitID, append([]string(nil), entry.heads...), true
}

// rememberInitCommit records the init commit found for a repository, together with the head whose
// history proved it. The init commit is immutable once the entry exists.
func (r *MetadataSearchResolver) rememberInitCommit(repo, commitID, head string) {
	r.initMu.Lock()
	defer r.initMu.Unlock()
	if r.initCache == nil {
		r.initCache = make(map[string]*metadataSearchInitCacheEntry)
	}
	entry, ok := r.initCache[repo]
	if !ok || entry == nil {
		r.evictInitCacheLocked()
		entry = &metadataSearchInitCacheEntry{commitID: commitID}
		r.initCache[repo] = entry
		r.initOrder = append(r.initOrder, repo)
	} else if entry.commitID != commitID {
		return
	}
	addInitVerifiedHead(entry, head)
}

// rememberVerifiedHead adds one head to a repository's already known init commit. It never changes
// the commit itself, and it does nothing when the cache entry was replaced or evicted meanwhile.
func (r *MetadataSearchResolver) rememberVerifiedHead(repo, commitID, head string) {
	r.initMu.Lock()
	defer r.initMu.Unlock()
	entry, ok := r.initCache[repo]
	if !ok || entry == nil || entry.commitID != commitID {
		return
	}
	addInitVerifiedHead(entry, head)
}

// addInitVerifiedHead records head as a witness, keeping the per-repository list bounded.
func addInitVerifiedHead(entry *metadataSearchInitCacheEntry, head string) {
	if head == "" {
		return
	}
	for _, verified := range entry.heads {
		if verified == head {
			return
		}
	}
	entry.heads = append([]string{head}, entry.heads...)
	if len(entry.heads) > metadataSearchInitCacheMaxHeads {
		entry.heads = entry.heads[:metadataSearchInitCacheMaxHeads]
	}
}

// evictInitCacheLocked drops the oldest repository entry while the cache is at its bound. The
// caller holds initMu.
func (r *MetadataSearchResolver) evictInitCacheLocked() {
	for len(r.initCache) >= metadataSearchInitCacheMaxRepositories && len(r.initOrder) > 0 {
		oldest := r.initOrder[0]
		r.initOrder = r.initOrder[1:]
		delete(r.initCache, oldest)
	}
}

// VirtualNamespaces implements VirtualTableSource: the deprecated catalog's client sees system as
// the one virtual child namespace of a data repository ref, as it did before the guide moved the
// tables to the root (G5). The parent of a listing is an Address with a repository and a ref and no
// namespace levels; a deeper parent lists what its markers say and nothing else.
//
// The lakeFS catalog adds nothing: ListNamespaces on a data repository answers its markers only
// (METADATA_SEARCH_REALIGN.md §2, RA3).
func (r *MetadataSearchResolver) VirtualNamespaces(addr Address) []string {
	if !r.deprecated || !isDataRepositoryAddress(addr) || addr.Ref == "" || len(addr.Namespace) != 0 {
		return nil
	}
	return []string{SystemNamespace}
}

// VirtualTables implements VirtualTableSource: in the deprecated catalog the system namespace of a
// data repository lists object_metadata even though no pointer entry exists for it (G5). It lists
// nothing in the lakeFS catalog.
func (r *MetadataSearchResolver) VirtualTables(addr Address) []string {
	if !r.deprecated || !isDataRepositoryAddress(addr) || addr.Namespace.Dotted() != SystemNamespace {
		return nil
	}
	return []string{ObjectMetadataTable}
}

// ReservedNamespace implements VirtualTableSource: the whole root `system` repository is read-only
// for clients (METADATA_SEARCH_REALIGN.md RA1). Nothing below `<repo>.<ref>` is reserved: the
// deprecated catalog has no reservation at all, and in a data repository `system` is an ordinary
// namespace name whose contents the catalog serves like any other (RA3).
func (r *MetadataSearchResolver) ReservedNamespace(addr Address) bool {
	return !r.deprecated && addr.Repo == SystemNamespace
}

// isDataRepositoryAddress reports whether addr names a repository that can have a companion
// repository: not the reserved root repository and not a companion itself. It is the question the
// deprecated catalog's listing rules ask, because only there does a data repository still own a
// virtual namespace.
func isDataRepositoryAddress(addr Address) bool {
	return addr.Repo != "" && addr.Repo != SystemNamespace && !IsMetadataRepository(addr.Repo)
}

// isObjectMetadataAddress reports whether addr is the deprecated catalog's table: the system
// namespace, the object_metadata table, a data repository and a ref - and not a view, which has its
// own namespace tree and its own endpoints. The lakeFS catalog never answers an address this
// predicate accepts: it is an ordinary address there (RA3).
func isObjectMetadataAddress(addr Address) bool {
	if addr.HasView() || addr.Table != ObjectMetadataTable {
		return false
	}
	if !isDataRepositoryAddress(addr) || addr.Ref == "" {
		return false
	}
	return addr.Namespace.Dotted() == SystemNamespace
}

// Resolver returns the registered TableResolver, or nil while no hook is registered (the default:
// every read is exactly the read it was before this file existed).
func (s *Store) Resolver() TableResolver { return s.resolver }

// Resolve returns the address the catalog must read addr from, and the address the caller's
// permission is checked against. With no resolver registered both are addr itself, which is the
// behaviour of every code path that predates this hook.
//
// The target a resolver returns is validated here rather than in each of the six read paths: a
// resolution that does not name a repository, a ref, a namespace and a table could only be served
// as a 404 for an address nobody asked for, so it is reported as the server-side misconfiguration
// it is.
//
// A resolver that also implements ResolvedTableVerifier gets its target loaded and checked here
// (MDS-SEC-3): a redirect is served only if the table it names proves it is the right one. The load
// is the store's ordinary table read - the same pointer and metadata.json the read path would read
// next - so the check cannot pass on a table the read would not get, and a failing check answers
// before anything is served.
func (s *Store) Resolve(ctx context.Context, addr Address) (ResolvedTable, error) {
	identity := ResolvedTable{Target: addr, Authz: addr}
	if s.resolver == nil {
		return identity, nil
	}
	resolved, handled, err := s.resolver.Resolve(ctx, addr)
	if err != nil {
		return ResolvedTable{}, err
	}
	if !handled {
		return identity, nil
	}
	if resolved.Authz.Repo == "" {
		// An omitted authorization address means the one the caller named: the redirect changes
		// where the table is read, not who may read it.
		resolved.Authz = addr
	}
	target := resolved.Target
	if target.Repo == "" || target.Ref == "" || len(target.Namespace) == 0 || target.Table == "" || target.View != "" {
		return ResolvedTable{}, fmt.Errorf("%w: the resolver returned an incomplete target for %s@%s",
			ErrInvalidResolution, addr.Repo, addr.Ref)
	}
	if verifier, ok := s.resolver.(ResolvedTableVerifier); ok {
		tbl, err := s.GetTable(ctx, target)
		if err != nil {
			return ResolvedTable{}, err
		}
		if err := verifier.VerifyResolved(ctx, addr, target, tbl); err != nil {
			return ResolvedTable{}, err
		}
	}
	return resolved, nil
}

// AuthorizeFunc authorizes one catalog address for the caller of the request in flight. The catalog
// passes the same decision it already made for the requested address, so a redirected read is
// authorized without re-stating what the operation or the caller is.
type AuthorizeFunc func(ctx context.Context, addr Address) error

// ResolveAuthorized resolves addr for a caller who was already authorized for it, and authorizes
// the resolver's authorization address when it is a different one.
//
// The order - authorize, then resolve - is the catalog's own (see the file comment): a resolver is
// never asked about an address a caller may not read, and a resolver cannot widen a caller's
// permissions by naming an Authz address it already has access to. The built-in virtual table
// answers the same address it was asked about, so it costs no second decision (§5.3: one check on
// the data repository).
func (s *Store) ResolveAuthorized(ctx context.Context, addr Address, authorize AuthorizeFunc) (ResolvedTable, error) {
	resolved, err := s.Resolve(ctx, addr)
	if err != nil {
		return ResolvedTable{}, err
	}
	if authorize == nil || sameAddress(resolved.Authz, addr) {
		return resolved, nil
	}
	if err := authorize(ctx, resolved.Authz); err != nil {
		return ResolvedTable{}, err
	}
	return resolved, nil
}

// VirtualNamespaces returns the child namespaces the registered resolver adds to the listing of
// addr. It is empty while no resolver - or no VirtualTableSource - is registered.
func (s *Store) VirtualNamespaces(addr Address) []string {
	source, ok := s.virtualSource()
	if !ok {
		return nil
	}
	return source.VirtualNamespaces(addr)
}

// VirtualTables returns the table names the registered resolver adds to the listing of
// addr.Namespace. It is empty while no resolver - or no VirtualTableSource - is registered.
func (s *Store) VirtualTables(addr Address) []string {
	source, ok := s.virtualSource()
	if !ok {
		return nil
	}
	return source.VirtualTables(addr)
}

// RequireWritableNamespace returns *ReservedNamespaceError when addr.Namespace is reserved by the
// registered resolver, and nil otherwise (§5.4). Every catalog write that names a namespace calls
// it after it authorized the caller: a reserved namespace is refused to a caller who may write the
// repository, which is the only caller that gets far enough to read the message. The root `system`
// repository of RA1 is the only reserved address; nothing below `<repo>.<ref>` is.
func (s *Store) RequireWritableNamespace(addr Address) error {
	source, ok := s.virtualSource()
	if !ok || !source.ReservedNamespace(addr) {
		return nil
	}
	return &ReservedNamespaceError{Repo: addr.Repo, Ref: addr.Ref, Namespace: addr.Namespace}
}

// virtualSource is the registered resolver's VirtualTableSource capability, if it has one.
func (s *Store) virtualSource() (VirtualTableSource, bool) {
	if s.resolver == nil {
		return nil, false
	}
	source, ok := s.resolver.(VirtualTableSource)
	return source, ok
}

// sameAddress reports whether two addresses name the same address. Namespace is a slice, so the
// structs are not comparable; every resolver in this package answers Authz with the very slice it
// was handed, and a resolver that rebuilds it still compares equal here.
func sameAddress(a, b Address) bool {
	return a.Repo == b.Repo && a.Ref == b.Ref && a.Table == b.Table && a.View == b.View &&
		slices.Equal(a.Namespace, b.Namespace)
}

// mergeListings merges the names a listing discovered with the names a resolver adds, keeping the
// listing in name order. It is the one place the two listings (child namespaces and tables) add
// virtual entries, so a listing cannot accidentally put them before or after the walked ones.
//
// With nothing to add the discovered names are returned as they are, not copied and not re-sorted:
// that is what keeps the catalog's listing byte-for-byte identical while no resolver is registered.
func mergeListings(discovered, virtual []string) []string {
	if len(virtual) == 0 {
		return discovered
	}
	merged := make([]string, 0, len(discovered)+len(virtual))
	merged = append(merged, discovered...)
	merged = append(merged, virtual...)
	slices.Sort(merged)
	return merged
}

// ---------------------------------------------------------------------------------------------
// The root `system.metadata` tree (METADATA_SEARCH_REALIGN.md RA1: G1, G2, G6, G7, G8)
// ---------------------------------------------------------------------------------------------
//
// The current guide addresses the metadata tables at the root of the lakeFS catalog:
//
//	system.metadata.<data repo>.<ref>.objects
//
// The address is an ordinary catalog address (ident.go): `system` is the repository level,
// `metadata` the ref level, the next level is the data repository R and the level after it is R's
// ref; `objects` is the table. There is no `system` namespace inside any data repository - this
// tree is the only one, and every write into it is refused (ReservedNamespace).
//
// The physical table stays where the guide puts it: on each matching branch of R-metadata, as the
// ordinary table `<R>-metadata.<mapped ref>.system.object_metadata`. Resolve sends a table read
// there, exactly like the old per-repository address did.
//
// The rest of this section is the tree's own vocabulary, shared by the resolver and the listing and
// data-plane handlers of the catalog:
//
//   - the predicates below say which node an address names;
//   - systemTreeAuthz maps a node onto the catalog ARN the caller's permission is checked against.
//     It is the guide's ARN (G7): table/<R>/system/object_metadata for the table and
//     namespace/<R>/system for the nodes above it. The tree's own address is never authorized - it
//     must not be, or the guide's grant alone would not be enough;
//   - the Store methods at the end answer which nodes exist, derived from the companion repository
//     and its branches. The whole tree is marker-less: no entry says a node exists, so existence is
//     the state of R-metadata, and the ownership proof is the load-time check (VerifyResolved).

// SystemNamespaceNotFoundException is the 404 of a node of the root system tree that does not exist.
// It is answered as 404 NoSuchNamespaceException, the status an absent marker-less namespace has.
type SystemNamespaceNotFoundException struct {
	// Node is the dotted address of the node, e.g. "system.metadata.lakehouse.main".
	Node string
}

// Error implements error.
func (e *SystemNamespaceNotFoundException) Error() string {
	return fmt.Sprintf("no such namespace %q", e.Node)
}

// isSystemTreeRoot reports whether addr is the root of the tree, the reserved `system` repository.
func isSystemTreeRoot(addr Address) bool {
	return addr.Repo == SystemNamespace && addr.Ref == "" && len(addr.Namespace) == 0 &&
		!addr.HasTable() && !addr.HasView()
}

// isSystemMetadataLevel reports whether addr is the `system.metadata` level of the tree.
func isSystemMetadataLevel(addr Address) bool {
	return addr.Repo == SystemNamespace && addr.Ref == MetadataNamespace && len(addr.Namespace) == 0 &&
		!addr.HasTable() && !addr.HasView()
}

// isDataRepositoryName reports whether name can be the data repository level of the tree: the
// reserved root name is the tree itself and a -metadata name is a companion, not a data repository.
func isDataRepositoryName(name string) bool {
	return name != SystemNamespace && !IsMetadataRepository(name)
}

// systemObjectsAddress reports whether addr is the root tree's table,
// system.metadata.<R>.<ref>.objects, and returns the data repository and ref it names.
//
// A companion repository's own name is not a data repository (a repository whose name ends in
// -metadata has no companion of its own), and neither is the reserved root name, so those refs are
// not addresses of the tree. Everything else is decided by the load: an unindexed data repository
// has no companion table and answers the 404 an absent table answers.
func systemObjectsAddress(addr Address) (dataRepo, ref string, ok bool) {
	if addr.Repo != SystemNamespace || addr.Ref != MetadataNamespace ||
		len(addr.Namespace) != 2 || addr.Table != ObjectsTable || addr.View != "" {
		return "", "", false
	}
	dataRepo, ref = addr.Namespace[0], addr.Namespace[1]
	if !isDataRepositoryName(dataRepo) {
		return "", "", false
	}
	return dataRepo, ref, true
}

// dataRepositoryRef returns the data repository and ref a tree address is about: the two namespace
// levels below `system.metadata`. It is the mapping the Q-I4 branch check needs, because the tree
// address's own repository level is the reserved root and has no branch.
func dataRepositoryRef(addr Address) (dataRepo, ref string, ok bool) {
	if addr.Repo != SystemNamespace || addr.Ref != MetadataNamespace || len(addr.Namespace) < 2 {
		return "", "", false
	}
	return addr.Namespace[0], addr.Namespace[1], true
}

// branchCheckAddress is the address a branch question is asked about: the data repository and ref
// for a tree address, and the address itself everywhere else.
func branchCheckAddress(addr Address) Address {
	if dataRepo, ref, ok := dataRepositoryRef(addr); ok {
		return Address{Repo: dataRepo, Ref: ref}
	}
	return addr
}

// systemTreeAuthz maps a root-tree address onto the catalog address the caller's permission is
// checked against, and reports whether addr belongs to the tree at all.
//
// The mapping is the one the guide's permissions name (METADATA_SEARCH_REALIGN.md G7): the data
// repository R, its `system` namespace and the table name `object_metadata`, even though the tree's
// table is called `objects`. That is deliberate: the ARNs of the tree are the ARNs of the physical
// table, so one policy grants the old per-repository address, the new root-tree address and the
// data-plane session of both. The nodes above the table check the same namespace ARN.
//
// No ref is resolved here: catalog ARNs are branch-agnostic (authz.go), so the ref level only has to
// be a non-empty level for RequiredNode. The tree uses the ref it names, or `metadata` - the tree's
// own level above the ref - when the node is the data repository itself.
func systemTreeAuthz(addr Address) (Address, bool) {
	if addr.Repo != SystemNamespace || addr.Ref != MetadataNamespace || len(addr.Namespace) == 0 {
		return Address{}, false
	}
	dataRepo := addr.Namespace[0]
	ref := MetadataNamespace
	if len(addr.Namespace) > 1 {
		ref = addr.Namespace[1]
	}
	authz := Address{Repo: dataRepo, Ref: ref, Namespace: Namespace{SystemNamespace}}
	switch {
	case addr.HasView():
		// The tree has no views: a view address is not part of it.
		return Address{}, false
	case addr.Table == ObjectsTable && len(addr.Namespace) == 2:
		authz.Table = ObjectMetadataTable
	case addr.Table != "":
		// A table name the tree does not have. It is not an address of the tree, and mapping it onto
		// the namespace ARN would pair a table operation with a namespace address, so it is left to
		// the ordinary path (and its own 403/404).
		return Address{}, false
	}
	return authz, true
}

// authzAddress is the address a request's permission is checked against: the data repository's
// catalog ARN for a root-tree address, and the address itself everywhere else.
func authzAddress(addr Address) Address {
	if authz, ok := systemTreeAuthz(addr); ok {
		return authz
	}
	return addr
}

// systemNamespaceAuthz is the catalog address of R's `system` namespace for a permission check that
// is not about one ref: the nodes above the ref level and the listings. The ARN is branch-agnostic,
// so the ref level is the tree's own `metadata` and is never resolved.
func systemNamespaceAuthz(dataRepo string) Address {
	return Address{Repo: dataRepo, Ref: MetadataNamespace, Namespace: Namespace{SystemNamespace}}
}

// systemTreeNamespace is the namespace a client named, in the shape of the spec's Namespace type:
// the tree's own levels followed by the levels below them. It differs from fullNamespace only at the
// root, which has no ref level.
func systemTreeNamespace(addr Address) Namespace {
	levels := make(Namespace, 0, macroLevels+len(addr.Namespace))
	levels = append(levels, addr.Repo)
	if addr.Ref != "" {
		levels = append(levels, addr.Ref)
	}
	levels = append(levels, addr.Namespace...)
	return levels
}

// metadataTreeLister is the lakeFS listing surface the root tree needs: the repositories, so the
// companion repositories can be found, and their branches, so the synced refs can be named.
// *catalog.Catalog implements it; a resolver registered with a lister that does not leaves the tree
// with no listings and no nodes, which is the fail-closed direction.
type metadataTreeLister interface {
	ListRepositories(ctx context.Context, limit int, prefix, searchString, after string, opts ...catalog.ListRepositoriesOptionsFunc) ([]*catalog.Repository, bool, error)
	ListBranches(ctx context.Context, repositoryID string, prefix string, limit int, after string, opts ...graveler.ListOptionsFunc) ([]*catalog.Branch, bool, error)
}

// SystemMetadataRepository is one data repository the root tree lists: its name and the companion
// branches whose `objects` table resolves, in name order.
type SystemMetadataRepository struct {
	// Name is the data repository R (not the companion repository).
	Name string

	// Branches are R's synced branches: the branches of R-metadata that hold
	// system.object_metadata. Tags and commits are never listed.
	Branches []string
}

// hasSystemTree reports whether this store's registered resolver is the root system tree of the
// lakeFS catalog, i.e. the built-in Metadata Search resolver in its non-deprecated form. The
// deprecated catalog's resolver is a separate catalog and has no tree; a custom resolver does not
// create the tree either: it is the built-in one's capability.
func (s *Store) hasSystemTree() bool {
	resolver, ok := s.resolver.(*MetadataSearchResolver)
	return ok && !resolver.deprecated
}

// systemTreeLister returns the listing surface of the resolver the tree is served by.
func (s *Store) systemTreeLister() (metadataTreeLister, bool) {
	resolver, ok := s.resolver.(*MetadataSearchResolver)
	if !ok {
		return nil, false
	}
	lister, ok := resolver.branches.(metadataTreeLister)
	return lister, ok
}

// SystemMetadataExists reports whether at least one metadata repository exists, i.e. whether the
// root tree has any content. It is what makes `system` visible in the repository listing and what
// makes the two levels above the data repositories exist; there is no server flag any more
// (METADATA_SEARCH_REALIGN.md X5), the state of the repositories is the answer.
//
// "A metadata repository" is a companion repository R-metadata whose data repository R also exists:
// a stray -metadata name is not an index of anything.
func (s *Store) SystemMetadataExists(ctx context.Context) (bool, error) {
	repositories, err := s.listAllRepositories(ctx)
	if err != nil {
		return false, err
	}
	names := make(map[string]struct{}, len(repositories))
	for _, repository := range repositories {
		names[repository.Name] = struct{}{}
	}
	for _, repository := range repositories {
		if !IsMetadataRepository(repository.Name) {
			continue
		}
		if dataRepo := strings.TrimSuffix(repository.Name, MetadataRepositorySuffix); isDataRepositoryName(dataRepo) {
			if _, ok := names[dataRepo]; ok {
				return true, nil
			}
		}
	}
	return false, nil
}

// SystemMetadataRepositories returns every data repository whose `objects` table resolves on at
// least one branch, in name order. It is what `system.metadata` lists; the caller still has to be
// authorized for each repository, which the listing does per name.
func (s *Store) SystemMetadataRepositories(ctx context.Context) ([]SystemMetadataRepository, error) {
	repositories, err := s.listAllRepositories(ctx)
	if err != nil {
		return nil, err
	}
	names := make(map[string]struct{}, len(repositories))
	for _, repository := range repositories {
		names[repository.Name] = struct{}{}
	}
	indexed := make([]SystemMetadataRepository, 0, len(repositories))
	for _, repository := range repositories {
		if !IsMetadataRepository(repository.Name) {
			continue
		}
		dataRepo := strings.TrimSuffix(repository.Name, MetadataRepositorySuffix)
		if _, ok := names[dataRepo]; !ok || !isDataRepositoryName(dataRepo) {
			continue
		}
		branches, err := s.SystemMetadataBranches(ctx, dataRepo)
		if err != nil {
			return nil, err
		}
		if len(branches) == 0 {
			continue
		}
		indexed = append(indexed, SystemMetadataRepository{Name: dataRepo, Branches: branches})
	}
	sort.Slice(indexed, func(i, j int) bool { return indexed[i].Name < indexed[j].Name })
	return indexed, nil
}

// SystemMetadataBranches returns R's synced branches: the branches of R-metadata that hold the
// system.object_metadata table, in name order. Tags and commits are never listed, and a data
// repository with no companion has no synced branch.
//
// A branch counts as synced when its table pointer exists. The pointer is the one entry that makes
// a table exist (§6.1), the same existence the drop and the metrics sink use; whether the table also
// proves it is R's index is the load-time check (VerifyResolved), which a listing does not have to
// pay for every branch of every repository.
func (s *Store) SystemMetadataBranches(ctx context.Context, dataRepo string) ([]string, error) {
	branches, err := s.listAllBranches(ctx, MetadataRepository(dataRepo))
	if err != nil {
		if isMissingWarehouse(err) {
			return nil, nil
		}
		return nil, err
	}
	synced := make([]string, 0, len(branches))
	for _, branch := range branches {
		exists, err := s.systemObjectsPointerExists(ctx, dataRepo, branch.Name)
		if err != nil {
			return nil, err
		}
		if exists {
			synced = append(synced, branch.Name)
		}
	}
	sort.Strings(synced)
	return synced, nil
}

// SystemObjectsExists reports whether `system.metadata.<R>.<ref>.objects` resolves: the ref is
// mapped by the resolver's ref rules (branch → itself, tag or commit → the RefMapper's answer) and
// the companion table must exist on the mapped ref.
func (s *Store) SystemObjectsExists(ctx context.Context, dataRepo, ref string) (bool, error) {
	resolver, ok := s.resolver.(*MetadataSearchResolver)
	if !ok {
		return false, nil
	}
	requested := Address{
		Repo:      SystemNamespace,
		Ref:       MetadataNamespace,
		Namespace: Namespace{dataRepo, ref},
		Table:     ObjectsTable,
	}
	mapped, err := resolver.objectsRef(ctx, dataRepo, ref, requested)
	if err != nil {
		var noTable *NoSuchTableException
		if errors.As(err, &noTable) || isMissingWarehouse(err) {
			return false, nil
		}
		return false, err
	}
	return s.systemObjectsPointerExists(ctx, dataRepo, mapped)
}

// systemObjectsPointerExists reports whether dataRepo's objects table exists on mappedRef of its
// companion repository. It reads the pointer entry, the one thing that makes a table exist.
func (s *Store) systemObjectsPointerExists(ctx context.Context, dataRepo, mappedRef string) (bool, error) {
	if s.catalog == nil {
		return false, nil
	}
	addr := Address{
		Repo:      MetadataRepository(dataRepo),
		Ref:       mappedRef,
		Namespace: Namespace{SystemNamespace},
		Table:     ObjectMetadataTable,
	}
	pointerPath, err := tablePointerPath(addr)
	if err != nil {
		return false, err
	}
	_, err = s.catalog.GetEntry(ctx, addr.Repo, addr.Ref, pointerPath, catalog.GetEntryParams{})
	switch {
	case err == nil:
		return true, nil
	case isMissingWarehouse(err):
		return false, nil
	default:
		return false, err
	}
}

// listAllRepositories walks the whole repository listing of the tree's lister.
func (s *Store) listAllRepositories(ctx context.Context) ([]*catalog.Repository, error) {
	lister, ok := s.systemTreeLister()
	if !ok {
		return nil, nil
	}
	return walkRepositories(ctx, lister)
}

// listAllBranches walks the whole branch listing of repo on the tree's lister.
func (s *Store) listAllBranches(ctx context.Context, repo string) ([]*catalog.Branch, error) {
	lister, ok := s.systemTreeLister()
	if !ok {
		return nil, nil
	}
	return walkBranches(ctx, lister, repo)
}

// walkRepositories concatenates the pages of a repository listing that the catalog's own
// iterators order by name.
func walkRepositories(ctx context.Context, lister metadataTreeLister) ([]*catalog.Repository, error) {
	var (
		repositories []*catalog.Repository
		after        string
	)
	for {
		page, hasMore, err := lister.ListRepositories(ctx, catalog.ListRepositoriesLimitMax, "", "", after)
		if err != nil {
			return nil, err
		}
		repositories = append(repositories, page...)
		if !hasMore || len(page) == 0 {
			return repositories, nil
		}
		after = page[len(page)-1].Name
	}
}

// walkBranches concatenates the pages of the branch listing of repo.
func walkBranches(ctx context.Context, lister metadataTreeLister, repo string) ([]*catalog.Branch, error) {
	var (
		branches []*catalog.Branch
		after    string
	)
	for {
		page, hasMore, err := lister.ListBranches(ctx, repo, "", catalog.ListBranchesLimitMax, after)
		if err != nil {
			return nil, err
		}
		branches = append(branches, page...)
		if !hasMore || len(page) == 0 {
			return branches, nil
		}
		after = page[len(page)-1].Name
	}
}
