package icebergcatalog

// The table-resolution hook (METADATA_SEARCH_DESIGN.md §5.1-5.4, task R10-1).
//
// Metadata Search puts the system metadata of a data repository R into an ordinary companion
// repository R-metadata, one Iceberg table per mirrored branch, and lets a user query it as if it
// lived in R:
//
//	R.<ref>.system.object_metadata  →  <R>-metadata.<mapped ref>.system.object_metadata
//
// The catalog is the only place that knows that mapping, so it is the only place that has to
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
// separate field: the virtual table of §5.2 is read by a caller who has no access to R-metadata at
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
// MetadataSearchResolver implements the one virtual table the design fixes:
//
//	R.<ref>.system.object_metadata → <R>-metadata.<mapped ref>.system.object_metadata
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
// On a *data* repository the system namespace belongs to the resolver, not to clients: it has no
// markers and no pointers a client wrote, and a client that created a table or a namespace there
// would either shadow the virtual table or write into a namespace nothing reads. So on a data
// repository every catalog write into system is refused with 403 ForbiddenException
// (*ReservedNamespaceError) - create, commit, register, drop, rename, and namespace create and
// drop - while in R-metadata itself system is an ordinary namespace.
//
// The same capability answers what a listing shows (§5.4): ListNamespaces on a data repository
// reports system, and ListTables in it reports object_metadata, only while a resolver that declares
// them is registered. Both listings run the ordinary authorization filter afterwards, so a caller
// without a grant on the namespace does not see them either.
//
// # What the hook does not do
//
// Nothing else in the catalog changes. A resolver cannot make a table writable through the virtual
// address (writes never resolve: the reserved namespace refuses them first), cannot change what a
// commit compares, and cannot reach the catalog's staging area except by naming an address the
// caller could have named. The interface is a read path, and the four methods of
// VirtualTableSource are listing and refusal rules - not data.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/treeverse/lakefs/pkg/graveler"
)

const (
	// SystemNamespace is the namespace the virtual metadata-search table lives in. On a data
	// repository it is reserved for the resolver (§5.4); in the companion repository it is an
	// ordinary namespace.
	SystemNamespace = "system"

	// ObjectMetadataTable is the name of the virtual table: the system metadata of every object
	// of the data repository, indexed per branch (§3.1).
	ObjectMetadataTable = "object_metadata"

	// MetadataRepositorySuffix is the suffix that names the companion repository of a data
	// repository R: R-metadata (§3.2). It is the one naming convention the catalog and the
	// indexer share, so it is declared here, in the package both use.
	MetadataRepositorySuffix = "-metadata"
)

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
type TableResolver interface {
	Resolve(ctx context.Context, addr Address) (ResolvedTable, bool, error)
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

// VirtualTableSource is the optional half of a TableResolver that owns a namespace of the data
// repository rather than a single address: it declares the names a listing adds and the namespace
// clients may not write.
//
// It is separate from TableResolver because both are rules about addresses the resolver does *not*
// redirect: the namespace of §5.4 and its table are listed without a pointer entry, and a write to
// them is refused instead of resolved. A resolver that only redirects reads does not have to
// implement it, and the catalog then lists and refuses nothing.
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

// MetadataSearchResolver is the built-in TableResolver of METADATA_SEARCH_DESIGN.md §5.2: it
// serves R.<ref>.system.object_metadata out of R-metadata, and reserves the system namespace of
// every data repository (§5.4).
//
// Register it only while the Metadata Search feature is enabled (iceberg.metadata_search.enabled,
// §5.5): the resolver is what makes the virtual table and the reserved namespace exist, so a
// catalog that does not register it is exactly the catalog of every other release.
type MetadataSearchResolver struct {
	// branches tells a branch from a tag or a commit. A nil lister treats every ref as a branch,
	// which is the identity mapping of §5.2; the production wiring passes the catalog.
	branches BranchLister

	// mapper maps a tag or a commit to a ref of the companion repository (§5.2). A nil mapper
	// refuses those refs with NoSuchTableException instead of guessing.
	mapper RefMapper
}

// NewMetadataSearchResolver builds the built-in resolver. branches answers "is this ref a branch?"
// (the catalog implements it); mapper is the optional RefMapper that gives tags and commits a
// meaning in the companion repository. Both may be nil: no lister means every ref is treated as a
// branch, no mapper means a tag or a commit is not indexed and therefore NoSuchTable.
func NewMetadataSearchResolver(branches BranchLister, mapper RefMapper) *MetadataSearchResolver {
	return &MetadataSearchResolver{branches: branches, mapper: mapper}
}

// Resolve answers the virtual-table address of §5.2 and leaves every other address alone.
//
// The three answers:
//
//   - an address that is not <data repo>.<ref>.system.object_metadata is not handled (false, nil),
//     so the rest of the catalog is untouched. R-metadata itself is one of those addresses, which
//     is what lets the indexer own its system namespace.
//   - a branch of the data repository maps to the branch of the same name in R-metadata. The
//     authorization address is the address the caller named: one catalog:ReadTable check on the
//     data repository, and nothing on R-metadata (§5.3).
//   - a tag or a commit goes to the RefMapper. Without one it is refused with
//     NoSuchTableException: the index mirrors branches, and the snapshot a tag or a commit names is
//     the RefMapper's question, not this resolver's (§5.2).
//
// A repository that does not exist, or any other failure to resolve the ref, is returned as it is,
// so the client gets the 404 NoSuchWarehouseException (or the 500) the same address would answer
// without the hook.
func (r *MetadataSearchResolver) Resolve(ctx context.Context, addr Address) (ResolvedTable, bool, error) {
	if !isObjectMetadataAddress(addr) {
		return ResolvedTable{}, false, nil
	}
	ref, err := r.metadataRef(ctx, addr)
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

// metadataRef maps the ref of the data repository onto a ref of the companion repository: the same
// name for a branch (§5.2), the RefMapper's answer for a tag or a commit.
//
// A ref the catalog cannot look up is returned as the missing warehouse it is, never silently
// treated as "not a branch": the difference decides between 404 NoSuchWarehouseException and 404
// NoSuchTableException, and only the catalog can tell them apart. A nil BranchLister assumes every
// ref is a branch: without a lister there is nothing to ask, and the identity mapping is the
// documented default.
func (r *MetadataSearchResolver) metadataRef(ctx context.Context, addr Address) (string, error) {
	if r.branches == nil {
		return addr.Ref, nil
	}
	_, err := r.branches.GetBranchReference(ctx, addr.Repo, addr.Ref)
	switch {
	case err == nil:
		return addr.Ref, nil
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
			Repo:      addr.Repo,
			Ref:       addr.Ref,
			Namespace: addr.Namespace,
			Table:     addr.Table,
		}
	}
	mapped, err := r.mapper.MapRef(ctx, addr.Repo, addr.Ref)
	if err != nil {
		// A *NoSuchTableException is the mapper's refusal of an unindexed ref and is answered as
		// the 404 it is; anything else is a failure to resolve and stays one.
		return "", err
	}
	if mapped == "" {
		return "", fmt.Errorf("%w: the ref mapper returned an empty ref for %q of %q",
			ErrInvalidResolution, addr.Ref, addr.Repo)
	}
	return mapped, nil
}

// VirtualNamespaces implements VirtualTableSource: a data repository ref lists system as its one
// virtual child namespace (§5.4). The parent of a listing is an Address with a repository and a ref
// and no namespace levels; a deeper parent lists what its markers say and nothing else.
func (r *MetadataSearchResolver) VirtualNamespaces(addr Address) []string {
	if !reservesSystemNamespace(addr) || addr.Ref == "" || len(addr.Namespace) != 0 {
		return nil
	}
	return []string{SystemNamespace}
}

// VirtualTables implements VirtualTableSource: the system namespace of a data repository lists
// object_metadata even though no pointer entry exists for it (§5.4).
func (r *MetadataSearchResolver) VirtualTables(addr Address) []string {
	if !reservesSystemNamespace(addr) || addr.Namespace.Dotted() != SystemNamespace {
		return nil
	}
	return []string{ObjectMetadataTable}
}

// ReservedNamespace implements VirtualTableSource: system is read-only for clients on a data
// repository (§5.4). In the companion repository it is an ordinary namespace, which is what lets the
// indexer create, commit and drop tables there.
func (r *MetadataSearchResolver) ReservedNamespace(addr Address) bool {
	return reservesSystemNamespace(addr) && len(addr.Namespace) > 0 && addr.Namespace[0] == SystemNamespace
}

// reservesSystemNamespace reports whether addr names a data repository, i.e. one that has a
// companion repository and whose system namespace therefore belongs to the resolver. An empty
// repository is not one.
func reservesSystemNamespace(addr Address) bool {
	return addr.Repo != "" && !IsMetadataRepository(addr.Repo)
}

// isObjectMetadataAddress reports whether addr is the virtual table of §5.2: the system namespace,
// the object_metadata table, a repository and a ref - and not a view, which has its own namespace
// tree and its own endpoints.
func isObjectMetadataAddress(addr Address) bool {
	if addr.HasView() || addr.Table != ObjectMetadataTable {
		return false
	}
	if addr.Repo == "" || addr.Ref == "" || IsMetadataRepository(addr.Repo) {
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
// repository, which is the only caller that gets far enough to read the message.
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
