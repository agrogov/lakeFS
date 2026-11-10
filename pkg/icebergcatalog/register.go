package icebergcatalog

// Adopting an existing table: POST /v1/{prefix}/namespaces/{namespace}/register
// (ICEBERG_CATALOG_PLAN.md §6.1, §6.2, §6.3, §13, §7.2 row "create / register / commit / drop",
// §11 I-2).
//
// One operation lives here:
//
//	POST /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/register
//	    RegisterTableRequest in, LoadTableResult out
//
// Register is how a table that was written *outside* the catalog - by an Iceberg client using its
// own FileIO, by another catalog, by a Spark job pointed straight at the object store - becomes
// addressable through lakeFS. The client has already written metadata.json and the files it
// references; it sends the name it wants and the location of that metadata.json, and the catalog
// adopts the document instead of creating one:
//
//  1. the request is authorized exactly like a create (OpCreateTable, §7.2): catalog:CreateTable on
//     the table ARN - the catalog action is the whole permission, no fs: grant is needed
//     (authz.go) - plus the Q-I4 rule that a write needs a branch - a tag or a commit is
//     403 "ref is read-only";
//  2. the namespace must exist on the branch (its marker is what creates it, §6.1); otherwise 404
//     NoSuchNamespaceException, as for createTable;
//  3. the table must not exist yet: a pointer that is already staged or committed is 409
//     AlreadyExistsException (§2.1 - the client must not retry, the name is taken);
//  4. metadata.json is read through the block adapter, from a location that must be inside the
//     repository's storage namespace (the §13 control, see below) and parsed with iceberg-go;
//  5. format version 3 is refused with 400 (§1.2: v1 serves format versions 1 and 2);
//  6. the files the document's snapshots reference are staged as lakeFS entries with
//     StageTableFiles (§6.3, files.go), so a registered table is as GC-safe as a created one, and
//     `lakectl diff` and merge see its contents;
//  7. the pointer is staged with the assert-create condition of Store.CreateTable - a
//     compare-and-swap on an absent value, i.e. If-None-Match: * (§6.2) - and the response is the
//     LoadTableResult of the spec's registerTable, built from the bytes that were read.
//
// The pointer names the metadata file the *client* wrote. Nothing is rewritten: register adopts a
// document, it does not produce one. That is why the response carries the request's
// metadata-location unchanged and the exact bytes that were read, so what a later GET returns is
// the file the client has in its hands, not a re-encoding of it.
//
// # The foreign-location check is a security control (§13)
//
// The server reads metadata.json, and the manifests behind it, with its *own* storage credentials.
// A metadata-location outside the repository's storage namespace would therefore make one catalog
// request read - and, once the snapshots are staged, publish - objects in a bucket this repository
// does not own. So the location has to be strictly inside that namespace, and the remainder has to
// be a clean path (no empty, "." or ".." segment): anything else is 400 BadRequestException and
// nothing is read at all. StageTableFiles applies the second half of the same rule for the files a
// snapshot references, and additionally requires the table's own location to be inside
// <storage_namespace>/_iceberg/ - the catalog's table area (location.go, §6.3). A table whose files
// live outside that area is therefore not registrable in v1: it can be read only if something else
// staged its pointer, and its files could not be protected from GC anyway (§6.3, §13).
//
// # Ordering, and why it is safe
//
// The files are staged *before* the pointer: a pointer that outlived a failed file-staging step
// would make a table exist whose data files no lakeFS entry references, which is exactly the
// GC-unsafe state §6.3 exists to prevent. The other order would be worse in the only case that
// matters: when the pointer write loses the compare-and-swap, the entries staged by the loser are
// deterministic (one key per file, §6.3) and point at real objects, so a retry overwrites them and
// the uncommitted-GC grace protects them until it does. Nothing is committed here either - register
// stages, like every other catalog write, and becomes part of the repository on the next explicit
// lakeFS commit (§6.2, §14 Q-I2).
//
// The `overwrite` member of RegisterTableRequest is accepted and deliberately has no effect: a
// table is never replaced by a register in v1, because the pointer is staged with a
// compare-and-swap on an absent value (§6.2) and an existing table's document may be read by any
// branch, tag or commit. A request that asks to overwrite an existing table is answered 409 with a
// message that says so, which is the truth a client can act on: drop the table first.
//
// Clean-room note: written from ICEBERG_CATALOG_PLAN.md, the Apache Iceberg REST OpenAPI spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml, registerTable,
// components.schemas.RegisterTableRequest and LoadTableResult) and the Apache Iceberg table format
// spec (https://github.com/apache/iceberg/blob/main/format/spec.md, "Table Metadata") only. No code
// from lakeFS Enterprise or from upstream lakeFS >= 1.87 (BSL) was used.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/block"
)

const (
	// registerPattern serves RegisterTable (spec: registerTable). It sits next to the tables
	// collection of a namespace rather than on a table, because the table does not exist yet: the
	// name arrives in the body (RegisterTableRequest.name).
	registerPattern = "/v1/{prefix}/namespaces/{namespace}/register"

	// registerTableEndpoint is the wire form advertised in GET /v1/config once the route above is
	// mounted. Like every other entry it keeps the literal {prefix} placeholder, so a client can
	// match it byte for byte (§2.1, §15.3).
	registerTableEndpoint = "POST /v1/{prefix}/namespaces/{namespace}/register"
)

// ErrInvalidRegisterTable is wrapped by every refusal of a register request that is not already an
// identifier, a permission or an existing table: a missing or unreadable metadata-location, a
// location outside the repository's storage namespace (§13), a document that cannot be parsed, a
// format version this catalog does not serve (§1.2), and a set of table files that cannot be
// staged. The HTTP layer maps it to 400 BadRequestException: the request is refused as sent, and a
// retry with the same body cannot succeed.
var ErrInvalidRegisterTable = errors.New("invalid iceberg register table request")

// registerTableRequest is RegisterTableRequest (spec: components.schemas.RegisterTableRequest):
// the name to adopt, the metadata.json to adopt, and the optional overwrite flag.
//
// metadata-location is a fully qualified URI - the location of the file the client wrote - not a
// lakeFS path: the server reads it through the block adapter, and the pointer stores it as
// iceberg.metadata-location, which is what a client resolves with its own FileIO (§6.1).
type registerTableRequest struct {
	// Name is the table name to create. It is in the body because register addresses a namespace.
	Name string `json:"name"`

	// MetadataLocation is the physical URI of the metadata.json to adopt.
	MetadataLocation string `json:"metadata-location"`

	// Overwrite asks the catalog to replace an existing table. v1 accepts the member and never
	// overwrites: the pointer is staged with a compare-and-swap on an absent value (§6.2), so an
	// existing table is 409 whatever this flag says (file comment).
	Overwrite bool `json:"overwrite,omitempty"`
}

// registerService serves the register operation. It is its own service so that mounting it is one
// entry in the route table (http.go) and so that GET /v1/config advertises it only when it is
// mounted, like the namespace, table and data-plane services.
//
// It embeds the table service rather than re-deriving its surroundings: register answers with a
// LoadTableResult, mounts under the same {prefix}/{namespace} path parameters, and shares four
// things with the table endpoints that must not be able to diverge - the prefix check (Q-I3), the
// caller of a request, the Q-I4 branch resolution of a write, and the error mapping of §2.1
// (writeError). Its Routes() is its own, so embedding the table service does not mount its routes.
type registerService struct {
	*tableService
}

// NewRegisterService builds the register surface. The arguments are the table service's, because
// register needs the same three things: the catalog state (Store), the §7.2 authorization matrix
// (Authorizer) and the lakeFS reads a write path needs (TableLister).
func NewRegisterService(store *Store, authz *Authorizer, lister TableLister, cfg *Config) *registerService {
	return &registerService{tableService: NewTableService(store, authz, lister, cfg)}
}

// Routes is this file's contribution to the catalog route table (server.ownRoutes in http.go):
// exactly one operation, so mounting register cannot mount anything else.
func (s *registerService) Routes() []route {
	return []route{
		{
			method:   http.MethodPost,
			pattern:  registerPattern,
			handler:  s.handleRegisterTable,
			endpoint: registerTableEndpoint,
		},
	}
}

// handleRegisterTable is POST /v1/{prefix}/namespaces/{namespace}/register (spec: registerTable):
// adopt the metadata.json the request names as a new table on the addressed branch.
//
// The order is the create's (tables.go handleCreateTable), for the same reasons: the body carries
// the table name, so the catalog:TableName ARN of §7.2 cannot be built before the body is read -
// which leaks nothing, because an unauthorized caller gets the same 403 whatever name it sends -
// and everything after that is validate, authorize, resolve, act.
func (s *registerService) handleRegisterTable(w http.ResponseWriter, r *http.Request) {
	if !s.checkPrefix(w, r) {
		return
	}
	addr, ok := s.namespaceAddress(w, r)
	if !ok {
		return
	}

	var request registerTableRequest
	if !decodeJSONBody(w, r, &request) {
		return
	}
	name, err := decodeTableName(request.Name)
	if err != nil {
		s.writeRegisterError(w, err)
		return
	}
	addr.Table = name

	caller, ok := s.caller(w, r)
	if !ok {
		return
	}
	isBranch, err := s.isBranchRef(r.Context(), addr)
	if err != nil {
		s.writeRegisterError(w, err)
		return
	}
	// The §7.2 row of register is catalog:CreateTable, the row of create, including the Q-I4
	// branch rule: a register stages a pointer and table files through the catalog's own
	// CreateEntry, which needs no fs: permission of the caller.
	if err := s.authz.Authorize(r.Context(), caller, Request{
		Op:          OpCreateTable,
		Address:     addr,
		RefIsBranch: isBranch,
	}); err != nil {
		s.writeRegisterError(w, err)
		return
	}
	if !s.requireWritableNamespace(w, addr) {
		return
	}

	result, err := s.registerTable(r.Context(), addr, request)
	if err != nil {
		s.writeRegisterError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// registerTable adopts one metadata.json as the table addr, once the request is parsed and
// authorized.
//
// Every check that can refuse the request runs before anything is written, and the file staging
// runs before the pointer (file comment): a failure therefore either leaves nothing behind or
// leaves deterministic entries that a retry overwrites and the uncommitted-GC grace protects.
func (s *registerService) registerTable(ctx context.Context, addr Address, request registerTableRequest) (*loadTableResult, error) {
	location := strings.TrimSpace(request.MetadataLocation)
	if location == "" {
		return nil, fmt.Errorf("%w: the request carries no metadata-location", ErrInvalidRegisterTable)
	}

	// §6.1: a namespace exists when its marker does, and register - like createTable - answers 404
	// NoSuchNamespaceException for one that does not. The marker is read on the same ref the pointer
	// is staged on, so it sees the branch's staging area too.
	if _, err := s.store.GetNamespace(ctx, addr); err != nil {
		return nil, err
	}

	// The name must be free. This is a cheaper answer than the compare-and-swap below and it comes
	// before the file staging, so an obviously doomed register reads no metadata file and stages
	// nothing; the compare-and-swap of Store.CreateTable is still what decides a race.
	if err := s.requireAbsentTable(ctx, addr, request.Overwrite); err != nil {
		return nil, err
	}

	metadata, metadataJSON, err := s.registerMetadata(ctx, addr, location)
	if err != nil {
		return nil, err
	}

	// Format version 3 is a v1 non-goal (§1.2), exactly as on the create and commit paths. The gate
	// is here, before anything is staged or written, so the reason reaches the client.
	version := metadata.Version()
	if version != 1 && version != 2 {
		return nil, fmt.Errorf("%w: format version %d is not supported: this catalog serves format versions 1 and 2",
			ErrInvalidRegisterTable, version)
	}
	// The pointer carries the table UUID that metadata.json also carries (iceberg.table-uuid, §6.1),
	// because it is what names the table's managed location and its staged files. The format spec
	// makes table-uuid optional in a v1 document, so a document without one is refused here, as the
	// unreadable request it is, rather than surfacing as a pointer that cannot be encoded.
	if metadata.TableUUID() == uuid.Nil {
		return nil, fmt.Errorf("%w: the metadata document carries no table-uuid", ErrInvalidRegisterTable)
	}

	// §6.3: the files the adopted metadata references become lakeFS entries, with Base nil because
	// nothing was registered for this table before - every snapshot of the document is new to the
	// catalog. The metadata file itself is registered too, at the location the client wrote it to
	// and with the size that was read, because the pointer names it from entry user metadata rather
	// than from an entry address and nothing else would make the committed GC retain it.
	if _, err := s.store.StageTableFiles(ctx, addr, StageTableFilesRequest{
		Next:          metadata,
		MetadataFiles: []TableMetadataFile{{Location: location, Size: int64(len(metadataJSON))}},
		MaxFiles:      s.store.MaxStagedFiles(),
	}); err != nil {
		if isRefusedTableFiles(err) {
			// A snapshot outside the table root or a document above iceberg.max_staged_files:
			// the request cannot be served as sent (§13), and nothing was staged.
			return nil, fmt.Errorf("%w: the table's files cannot be registered: %w", ErrInvalidRegisterTable, err)
		}
		// Anything else is a storage failure. Nothing was swapped, so the outcome is knowable:
		// the table was not registered.
		return nil, err
	}

	pointer := TablePointer{
		MetadataLocation: location,
		TableUUID:        metadata.TableUUID().String(),
		FormatVersion:    version,
	}
	if err := s.store.CreateTable(ctx, addr, pointer); err != nil {
		// ErrAlreadyExists means a concurrent register won between the check above and this write;
		// the loser's staged entries are deterministic and collectable (§6.3).
		return nil, err
	}

	return &loadTableResult{
		// The pointer that was just staged names the file the client wrote: register adopts a
		// document, it does not produce one, so the location is the request's.
		MetadataLocation: location,
		// The exact bytes read, so the response is the document a later load returns.
		Metadata: json.RawMessage(metadataJSON),
		Config:   map[string]string{},
	}, nil
}

// requireAbsentTable returns nil when addr has no table pointer on its ref yet, and an
// ErrAlreadyExists error when it has.
//
// Existence is the pointer entry (§6.1): a register must not be stopped by a document it could not
// read, and a name is taken exactly when something already points at one. The read resolves the
// staging area first, like every other catalog read, so a pointer staged but not yet committed on
// this branch is taken too - that is the same branch the new pointer would be staged on.
//
// overwrite does not change the answer; the message says why, because it is the one member of the
// request whose effect a client may otherwise guess wrong (file comment).
func (s *registerService) requireAbsentTable(ctx context.Context, addr Address, overwrite bool) error {
	_, err := s.requireTablePointer(ctx, addr)
	var noTable *NoSuchTableException
	switch {
	case err == nil:
		note := ""
		if overwrite {
			note = "; overwrite is not supported, so an existing table is never replaced by register"
		}
		return fmt.Errorf("%w: table %q in namespace %q already exists on %s@%s%s",
			ErrAlreadyExists, addr.Table, addr.Namespace.Dotted(), addr.Repo, addr.Ref, note)
	case errors.As(err, &noTable):
		return nil
	default:
		return err
	}
}

// registerMetadata reads and parses the metadata.json a register request names.
//
// The repository is resolved first, because the storage namespace it carries is what makes the
// location check possible: the server reads this file with its own credentials, so a location
// outside the repository's storage namespace is refused with 400 before a single byte is fetched
// (§13). The remainder of the location is validated as a clean relative path, so a location that
// only *looks* inside - "…/ns/../other-bucket/x", "…/ns//x" - is refused as well.
//
// The errors are classified for the client: a document that cannot be parsed and an object that is
// not there are both the client's 400 (it named the file), while a repository that does not exist
// stays the 404 NoSuchWarehouseException the rest of the catalog answers.
func (s *registerService) registerMetadata(ctx context.Context, addr Address, location string) (table.Metadata, []byte, error) {
	repo, err := s.store.catalog.GetRepository(ctx, addr.Repo)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve repository %q: %w", addr.Repo, err)
	}
	if err := requireMetadataLocationInside(repo.StorageNamespace, location); err != nil {
		return nil, nil, err
	}

	metadata, metadataJSON, err := s.store.loadMetadata(ctx, addr.Repo, location)
	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidPointer):
			// loadMetadata reports a document it cannot parse as ErrInvalidPointer; on the create
			// and commit paths that is a server-side data error, but here the document is the
			// client's, so it is the 400 it would be for a malformed request body.
			return nil, nil, fmt.Errorf("%w: the metadata-location does not name a readable table metadata document: %w",
				ErrInvalidRegisterTable, err)
		case errors.Is(err, block.ErrDataNotFound):
			return nil, nil, fmt.Errorf("%w: there is no object at the metadata-location %q",
				ErrInvalidRegisterTable, location)
		default:
			return nil, nil, err
		}
	}
	return metadata, metadataJSON, nil
}

// requireMetadataLocationInside returns nil when location is a file inside the repository's storage
// namespace, and an ErrInvalidRegisterTable error when it is not.
//
// This is the §13 control ("client writes outside the table location") applied to the file register
// reads rather than to the files a commit stages: the catalog fetches this document, and then the
// manifests it names, with the server's credentials, so "inside the repository's storage namespace"
// is the smallest scope in which that read is the owner's own data. The check is on the *namespace*,
// not on the table area <storage_namespace>/_iceberg/: register may adopt a table whose location is
// elsewhere in the repository, and StageTableFiles is what then enforces the table-area rule for
// the files themselves (§6.3).
//
// The location must name a file: the namespace itself, a trailing slash and any path that is not
// clean (an empty, "." or ".." segment, a control character) are refused, because they either name
// a directory or could reach outside the namespace while carrying it as a string prefix.
func requireMetadataLocationInside(storageNamespace, location string) error {
	namespace := strings.TrimRight(strings.TrimSpace(storageNamespace), "/")
	if namespace == "" {
		return fmt.Errorf("%w: the repository has no storage namespace to read a metadata file from",
			ErrInvalidRegisterTable)
	}
	relative, ok := strings.CutPrefix(location, namespace+"/")
	if !ok {
		return fmt.Errorf("%w: the metadata-location %q is not inside the repository's storage namespace %q",
			ErrInvalidRegisterTable, location, namespace)
	}
	if err := validateTableRelativePath(relative); err != nil {
		// The location carries the namespace as a string prefix but does not resolve to a file
		// under it, so it is refused like a foreign one, with the reason attached.
		return fmt.Errorf("%w: the metadata-location %q is not a path inside the repository's storage namespace %q: %w",
			ErrInvalidRegisterTable, location, namespace, err)
	}
	return nil
}

// writeRegisterError maps the typed errors of register onto the catalog envelope and the statuses
// of §2.1:
//
//   - an ErrInvalidRegisterTable error (a missing or foreign metadata-location, an unreadable or
//     unparseable document, a document without a table-uuid, an unsupported format version, files
//     that cannot be staged): 400 BadRequestException, because retrying the same body cannot help;
//   - ErrAlreadyExists, which writeError answers 409 AlreadyExistsException - the §7.2 row of the
//     create whose name is taken;
//   - *ReadOnlyRefError / *PermissionDeniedError: 403; NoSuchNamespaceException and a missing
//     warehouse: 404; a protected branch or an immutable repository: 403;
//   - everything else is the table path's answer (writeError), so an unexpected failure is a 500
//     with no detail.
func (s *registerService) writeRegisterError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrInvalidRegisterTable), isRefusedTableFiles(err):
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest, err.Error())
	default:
		s.writeError(w, err)
	}
}
