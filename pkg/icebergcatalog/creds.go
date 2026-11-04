package icebergcatalog

// Credential vending for table data access (ICEBERG_CATALOG_PLAN.md §8 "Data-plane access",
// §2.1 "Access delegation" and §7.2 "Authorization").
//
// §8 lists three ways a client can reach the table files it is told about. The default is that
// the client brings its own storage credentials; the other two let the catalog speak for the
// storage on the client's behalf. This file implements the second one, vended credentials: the
// catalog calls STS AssumeRole and answers with a session whose inline session policy is scoped
// to one table's prefix, so a client with no object-store grant of its own can read (and, for a
// write session, write) exactly the files of the table it was authorized for.
//
//	GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials
//	    → LoadCredentialsResponse{storage-credentials: [{prefix, config}]}
//
// The endpoint is the spec's loadCredentials operation, which is in the REST OpenAPI spec's
// endpoint inventory (components.paths./v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials,
// operation loadCredentials) and which the stub in ICEBERG_SPIKE_S-I4.md §2 advertised to both
// clients. The credentials themselves travel as the spec's StorageCredential objects: `prefix`
// is the storage prefix the credentials apply to, and `config` carries the FileIO properties S3
// clients already understand (s3.access-key-id, s3.secret-access-key, s3.session-token, and the
// millisecond expiry of s3.session-token-expires-at-ms, so a client can refresh before the
// session dies).
//
// Scope. The session policy is the narrowest that works (§8, and the note of task I4-2): the
// object ARNs are `<table root>/*`, i.e. <storage_namespace>/_iceberg/<table-uuid>/, never the
// storage namespace. The only bucket-level action is s3:ListBucket, and it is restricted by an
// s3:prefix StringLike condition on the same prefix, because Iceberg readers and maintenance
// jobs list a table's directory rather than only the files metadata names. A table whose
// metadata.json points outside its own root is not vended at all: the root is derived from the
// table UUID and the repository's storage namespace, so a registered table that lives elsewhere
// must not borrow a prefix the catalog would then be trusting.
//
// Authorization is the §7.2 row for vended data access: catalog:ReadTableData for a read session
// and catalog:WriteTableData for a write session, on the table ARN, plus the fs:ReadRepository
// every catalog request needs (and fs:WriteObject on the reserved area for a write), which the
// Authorizer adds itself. A write session additionally needs a branch ref (Q-I4). The
// authorization decision is taken before the table is resolved, so a caller without the data
// action learns nothing about the table.
//
// Best effort (§8). Vending is a convenience layered on the default mode, not a requirement: if
// STS fails, is unreachable, or answers without credentials, the request still succeeds and the
// answer simply carries no credentials, which leaves the client on its own storage credentials.
// Only the authorization decision and the resolution of the table itself can fail the request.
//
// Advertisement. The blockstore, not the URL, decides whether vending exists: it is S3 only
// (§8: "S3 only; other blockstores fall back to the client's own credentials"), so
// VendedCredentialsSupported requires the S3 adapter, and the setting must name a role
// (CredentialsVendingConfig.Configured). While either is false the service contributes no route,
// so GET /v1/config neither advertises the credentials endpoint nor lets a client call it.
//
// Clean-room note (plan §2, DATASETS_DESIGN.md §2): written from the Apache Iceberg REST OpenAPI
// spec (Apache-2.0),
// https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
// (loadCredentials, LoadCredentialsResponse, StorageCredential, LoadTableResult.storage-credentials,
// the `data-access` parameter), the Apache Iceberg table spec and the public lakeFS documentation
// (https://docs.lakefs.io/iceberg/, including the blockstore.s3.credentials_vending setting names
// and their defaults) only. No code from lakeFS Enterprise, and nothing from upstream lakeFS >=
// 1.87 (BSL), was used.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	// credentialsPattern is the vended-credentials route, relative to the catalog mount
	// (APIBasePath). It is the spec's loadCredentials operation.
	credentialsPattern = "/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials" //nolint:gosec // a URL path, not a credential

	// credentialsEndpoint is the wire form advertised in GET /v1/config once the route is
	// mounted, i.e. only when the blockstore is S3 and role_arn is configured.
	credentialsEndpoint = "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials" //nolint:gosec // a URL path, not a credential

	// The FileIO properties of a vended S3 session, as the Iceberg REST spec and the rest-catalog
	// documentation name them. A client merges them into its FileIO configuration, so the names
	// are part of the protocol rather than an internal detail.
	accessKeyIDProperty             = "s3.access-key-id"
	secretAccessKeyProperty         = "s3.secret-access-key"
	sessionTokenProperty            = "s3.session-token"
	sessionTokenExpiresAtMsProperty = "s3.session-token-expires-at-ms"

	// roleSessionNamePrefix, roleSessionNameMinLength and roleSessionNameMaxLength bound
	// RoleSessionName of the AssumeRole call. STS accepts 2-64 characters from [\w+=,.@-] and every
	// session of this catalog starts with the same prefix, so a principal can tell a catalog
	// session apart in CloudTrail.
	roleSessionNamePrefix    = "lakefs-iceberg"
	roleSessionNameMinLength = 2
	roleSessionNameMaxLength = 64

	// defaultSTSRegion is the region used when the server's AWS configuration names none. STS
	// needs a region to sign with; us-east-1 is the one every S3-compatible STS implements
	// (MinIO ignores the value, and an AWS role is region-independent for AssumeRole).
	defaultSTSRegion = "us-east-1"

	// CredentialVendingUnsupportedMessage is the 501 answer when this deployment cannot vend
	// credentials. Vending is S3 only in OSS (§8), so the message names the requirement rather
	// than leaving a client to guess why the endpoint is off: a non-S3 blockstore (Azure, GCS)
	// has no vending at all, and an S3 blockstore needs a role.
	CredentialVendingUnsupportedMessage = "credential vending is not enabled for this catalog: it requires an S3 blockstore and blockstore.s3.credentials_vending.role_arn" //nolint:gosec // an error message naming a setting, not a credential
)

// The refusals a credentials request can produce. They are sentinels so the handler and the tests
// classify an answer without matching on message text.
var (
	// ErrCredentialVendingDisabled is a credentials request to a catalog that does not vend. The
	// route is not mounted in that case, so this is a programming error rather than a client
	// error, and it fails closed.
	ErrCredentialVendingDisabled = errors.New("icebergcatalog: credential vending is not configured")

	// ErrInvalidCredentialRequest is a credentials request for an operation that is not a data
	// access operation, or one whose session cannot be scoped. It is a 400 BadRequestException.
	ErrInvalidCredentialRequest = errors.New("invalid credential vending request")

	// ErrCredentialScope is a table whose physical location is not under its own root, so no
	// session could be scoped to it. It is best effort: the answer carries no credentials
	// instead of failing the request (§8).
	ErrCredentialScope = errors.New("the table location is not inside the table prefix")

	// ErrCredentialVendingFailed wraps every failure of the STS exchange. Like ErrCredentialScope
	// it is best effort: it is logged and answered with no credentials, never with an error.
	ErrCredentialVendingFailed = errors.New("credential vending failed")

	// errNoTableScope is a programming error: the service was built without a store, so it cannot
	// resolve the table prefix a session is scoped to.
	errNoTableScope = errors.New("icebergcatalog: no table scope resolver configured")

	// errNoAuthorizer is a programming error: the service was built without the §7.2 authorizer, so
	// it cannot decide whether the caller may have the table's data at all.
	errNoAuthorizer = errors.New("icebergcatalog: no authorizer configured")
)

// STSAssumer is the one operation of the AWS STS client this service uses.
// *sts.Client implements it, and so does a fake in the tests: the session policy, the duration
// and the external ID are properties of the request, so they are asserted without a network call.
type STSAssumer interface {
	AssumeRole(ctx context.Context, params *sts.AssumeRoleInput, optFns ...func(*sts.Options)) (*sts.AssumeRoleOutput, error)
}

// StorageCredential is StorageCredential of the Iceberg REST OpenAPI spec: the storage prefix a
// set of credentials applies to and the FileIO properties to use there. config is required by the
// spec and is never nil, so a client can merge it unconditionally.
type StorageCredential struct {
	Prefix string            `json:"prefix,omitempty"`
	Config map[string]string `json:"config"`
}

// LoadCredentialsResponse is LoadCredentialsResponse of the spec, the body of
// GET …/tables/{table}/credentials. storage-credentials is required and always present; it is
// empty when vending failed (§8 best effort) so that a client falls back to its own credentials
// instead of receiving an error.
type LoadCredentialsResponse struct {
	StorageCredentials []StorageCredential `json:"storage-credentials"`
}

// dataAccess is the kind of session a credentials request asks for: a read session
// (catalog:ReadTableData) or a write session (catalog:WriteTableData).
type dataAccess int

const (
	// accessRead is a session that can read the table's files.
	accessRead dataAccess = iota

	// accessWrite is a session that can read and write the table's files. It is the session a
	// client needs before it uploads data files or rewrites metadata.
	accessWrite
)

// String is the value used in the session name and in log fields.
func (a dataAccess) String() string {
	if a == accessWrite {
		return "write"
	}
	return "read"
}

// dataAccessFor maps a data operation of the §7.2 matrix onto the session kind it vends. Any other
// operation is refused: a handler cannot accidentally give a read session to an operation that
// was authorized for something else.
func dataAccessFor(op Operation) (dataAccess, bool) {
	switch op {
	case OpReadTableData:
		return accessRead, true
	case OpWriteTableData:
		return accessWrite, true
	default:
		return accessRead, false
	}
}

// tableDataScope is what the catalog knows about a table's physical storage: the root the session
// is scoped to and the location the table's metadata.json names. The two are compared before
// anything is vended (see the file comment).
type tableDataScope struct {
	// Root is the table's physical root, <storage_namespace>/_iceberg/<table-uuid> (§6.3, Q-I5).
	Root string

	// Location is metadata.json's `location`: where this table's files actually are.
	Location string
}

// credentialService serves GET …/tables/{table}/credentials and vends the sessions the write path
// needs. It is built by NewCredentialService, and like remoteSigningService it exposes exactly the
// surface the route table needs - Routes and Enabled - so mounting vending is one entry in
// server.ownRoutes (http.go).
type credentialService struct {
	// scope resolves the table's physical root and location for an address, authorizing the
	// addresses the table resolver names on the way (resolver.go): the redirect of a virtual table
	// happens here, so a vended session is scoped to the table the resolver read - the companion
	// repository's table - while the caller was authorized on the data repository (§5.3).
	scope func(ctx context.Context, addr Address, authorize AuthorizeFunc) (tableDataScope, error)

	// createdScope is the scope of a table a create has just decided, from its repository, UUID
	// and metadata location: a staged create is not readable, so scope cannot answer for it.
	createdScope func(ctx context.Context, repository, tableUUID, location string) (tableDataScope, error)

	// authz is the §7.2 authorizer.
	authz *Authorizer

	// store is the catalog state: the reserved namespaces a write session is never vended for
	// (§5.4). A nil store reserves nothing.
	store *Store

	// refs resolves whether a ref is a branch, which decides whether a credentials request may get a
	// write session (Q-I4). A nil refs treats every ref as read-only, so the session is a read one.
	refs BranchLister

	// cfg is the vended-credentials setting: role_arn, session_duration, external_id, endpoint.
	cfg CredentialsVendingConfig

	// logger reports a best-effort vending failure. A nil logger is replaced by
	// logging.ContextUnavailable, so a test does not have to provide one.
	logger logging.Logger

	// prefix is overrides.prefix of GET /v1/config.
	prefix string

	// enabled reports whether the blockstore can vend (S3) and role_arn was configured.
	enabled bool

	// mu guards the lazily built STS client, so that the first credentials request builds it
	// once and a client that fails to build (a bad endpoint, an unreachable IMDS) is retried on
	// the next request instead of disabling vending until restart.
	mu      sync.Mutex
	assumer STSAssumer
}

// NewCredentialService builds the vended-credentials surface for a catalog store. refs resolves
// branches, so that a credentials request on a branch can get a write session. assumer is the
// STS client; a nil assumer means "build one from the server's own AWS identity on first use",
// which is what the production wiring uses (an in-process fake is passed by the tests). logger
// reports best-effort failures; a nil logger discards them.
//
// The service stays disabled - no route, no advertisement - while the configured blockstore is
// not S3 (§8: vending is S3 only) or no role is configured. cfg may be nil, which disables it.
func NewCredentialService(store *Store, authz *Authorizer, refs BranchLister, cfg *Config, assumer STSAssumer, logger logging.Logger) *credentialService {
	if logger == nil {
		logger = logging.ContextUnavailable()
	}
	var (
		vending CredentialsVendingConfig
		adapter block.Adapter
		scope   func(context.Context, Address, AuthorizeFunc) (tableDataScope, error)
		created func(context.Context, string, string, string) (tableDataScope, error)
	)
	if cfg != nil {
		vending = cfg.CredentialsVending
	}
	if store != nil {
		adapter = store.block
		scope = newTableScopeResolver(store)
		created = newCreatedTableScopeResolver(store)
	}
	prefix := CatalogPrefix
	if cfg != nil && cfg.Prefix != "" {
		prefix = cfg.Prefix
	}
	return &credentialService{
		scope:        scope,
		createdScope: created,
		authz:        authz,
		store:        store,
		refs:         refs,
		cfg:          vending,
		logger:       logger,
		prefix:       prefix,
		enabled:      vending.Configured() && VendedCredentialsSupported(adapter),
		assumer:      assumer,
	}
}

// VendedCredentialsSupported reports whether the blockstore can vend credentials at all: S3 only,
// because the session policy this file writes is an S3 policy (§8). Every other blockstore keeps
// the default mode, the client's own credentials.
func VendedCredentialsSupported(adapter block.Adapter) bool {
	return adapter != nil && adapter.BlockstoreType() == block.BlockstoreTypeS3
}

// Enabled reports whether this catalog vends credentials.
func (s *credentialService) Enabled() bool {
	return s != nil && s.enabled
}

// Routes is this file's contribution to the catalog route table (server.ownRoutes in http.go).
// The endpoint is advertised by the same table entry, so GET /v1/config lists the credentials
// endpoint exactly when vending is configured: an installation without a role, or with a
// blockstore that is not S3, contributes no route and no endpoint.
func (s *credentialService) Routes() []route {
	if !s.Enabled() {
		return nil
	}
	return []route{{
		method:   http.MethodGet,
		pattern:  credentialsPattern,
		handler:  s.handleLoadCredentials,
		endpoint: credentialsEndpoint,
	}}
}

// handleLoadCredentials is GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials.
//
// The session is the one TableCredentials picks: a write session on a branch for a caller who may
// write the table's data, a read session otherwise. iceberg-go refreshes the session of a table it
// loaded or created through this endpoint, so a writer must get a write session here too.
//
// The order is the one remote signing uses: prefix, delegation, identifier, caller,
// authorization, table resolution, vending. A best-effort vending failure is not an error here:
// the answer is a 200 with an empty storage-credentials list, which leaves the client on its own
// credentials (§8).
func (s *credentialService) handleLoadCredentials(w http.ResponseWriter, r *http.Request) {
	if !s.Enabled() {
		writeIcebergError(w, http.StatusNotImplemented, ErrTypeNotImplemented,
			CredentialVendingUnsupportedMessage)
		return
	}
	if !s.checkPrefix(w, r) {
		return
	}

	// Honour X-Iceberg-Access-Delegation: vended-credentials. An absent header is a client calling
	// the credentials endpoint directly (allowed); a header that lists mechanisms but not
	// vended-credentials is a client that asked for something else, so vending is refused rather
	// than done behind its back - the same rule remote signing applies to remote-signing.
	delegation := strings.Join(r.Header.Values(AccessDelegationHeader), ",")
	if delegation != "" && !delegationRequests(delegation, VendedCredentialsDelegation) {
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest,
			"the client did not request vended credentials in "+AccessDelegationHeader)
		return
	}

	addr, err := ParseTable(chi.URLParam(r, "namespace"), chi.URLParam(r, "table"))
	if err != nil {
		s.writeError(w, err)
		return
	}
	caller, ok := s.caller(w, r)
	if !ok {
		return
	}
	isBranch, err := s.refIsBranch(r.Context(), addr)
	if err != nil {
		s.writeError(w, err)
		return
	}
	credentials, err := s.TableCredentials(r.Context(), caller, addr, isBranch)
	if err != nil {
		s.writeError(w, err)
		return
	}
	if credentials == nil {
		credentials = []StorageCredential{}
	}
	writeJSON(w, http.StatusOK, LoadCredentialsResponse{StorageCredentials: credentials})
}

// Vend returns the storage credentials of one table for caller: a session scoped to the table's
// prefix, gated by req.Op - catalog:ReadTableData for a read session, catalog:WriteTableData for a
// write session (§7.2).
//
// The answer distinguishes the two kinds of failure deliberately:
//
//   - an authorization denial, or a failure to resolve the table (a missing table or repository),
//     is returned as an error: the request cannot be served and the client must be told why;
//   - anything after that - a table whose location is outside its root, an unreachable STS, a
//     session STS refuses - is best effort (§8): it is logged and the result is a nil slice, so
//     the caller answers "no credentials" and the client keeps using its own.
//
// Vend is the entry point of the write path as well: a create or a commit that wants the client to
// write the data files itself calls it with OpWriteTableData and RefIsBranch set, which is also
// what makes the Q-I4 read-only-ref refusal apply.
func (s *credentialService) Vend(ctx context.Context, caller Caller, req Request) ([]StorageCredential, error) {
	if !s.Enabled() {
		return nil, ErrCredentialVendingDisabled
	}
	access, ok := dataAccessFor(req.Op)
	if !ok {
		return nil, fmt.Errorf("%w: operation %q is not a data access operation",
			ErrInvalidCredentialRequest, string(req.Op))
	}
	// A service built without an authorizer fails closed: the zero value of the field must never
	// turn into "no decision needed".
	if s.authz == nil {
		return nil, &AuthorizationError{Username: caller.Username, Operation: req.Op,
			Err: errNoAuthorizer}
	}
	if err := s.authz.Authorize(ctx, caller, req); err != nil {
		return nil, err
	}
	// The table is resolved here, after the decision above and through the registered table
	// resolver (resolver.go): a virtual table is vended from where it actually lives - the
	// companion repository - while the caller was authorized on the data repository. A different
	// authorization address is checked inside the resolver, so a redirect can never grant more
	// than the caller had.
	scope, err := s.tableScope(ctx, req.Address, func(ctx context.Context, authzAddr Address) error {
		return s.authz.Authorize(ctx, caller, Request{
			Op:          req.Op,
			Address:     authzAddr,
			RefIsBranch: req.RefIsBranch,
		})
	})
	if err != nil {
		return nil, err
	}
	return s.vendScope(ctx, caller, req.Address, scope, access), nil
}

// TableCredentials vends the session a client gets for a table that exists on addr.Ref: a write
// session when the ref is a branch, the namespace is not reserved (§5.4) and the caller is allowed
// catalog:WriteTableData, and a read session otherwise. A write denial falls back to the read
// session; any other failure, and a read denial, is Vend's.
//
// The reserved-namespace check is what keeps a write session away from a virtual table: the
// resolver reads it from the companion repository, and a caller who may write the data repository
// must not get credentials that write the companion's files.
func (s *credentialService) TableCredentials(ctx context.Context, caller Caller, addr Address, isBranch bool) ([]StorageCredential, error) {
	if isBranch && s.writableNamespace(addr) {
		credentials, err := s.Vend(ctx, caller, Request{Op: OpWriteTableData, Address: addr, RefIsBranch: true})
		if !isWriteRefusal(err) {
			return credentials, err
		}
	}
	return s.Vend(ctx, caller, Request{Op: OpReadTableData, Address: addr, RefIsBranch: isBranch})
}

// VendsFor reports whether a LoadTableResult answered to a client that sent delegation - the raw
// X-Iceberg-Access-Delegation header - carries storage-credentials: vending is on and the client
// listed vended-credentials (§2.1: the server must not choose a mechanism the client did not
// request). The table handlers ask before they resolve anything a session needs.
func (s *credentialService) VendsFor(delegation string) bool {
	return s.Enabled() && delegationRequests(delegation, VendedCredentialsDelegation)
}

// LoadTableCredentials is the storage-credentials member of a LoadTableResult for a table that
// exists on addr.Ref, or nil when no session could be vended. The load itself was authorized
// already, so nothing here fails it; a reason not to vend is logged.
//
// It is the member iceberg-go needs: the client enters its vended-credentials mode only when the
// load answer carries storage-credentials, and uses the credentials endpoint only to refresh them.
func (s *credentialService) LoadTableCredentials(ctx context.Context, caller Caller, addr Address, isBranch bool) []StorageCredential {
	credentials, err := s.TableCredentials(ctx, caller, addr, isBranch)
	if err != nil {
		s.logNotVending(ctx, caller, addr, "table", err)
		return nil
	}
	return credentials
}

// CreatedTableCredentials is LoadTableCredentials for the answer of a create, whose table may not
// be readable yet (a staged create writes no pointer). Its scope therefore comes from what the
// create just decided - the table UUID and the location of its metadata.json - instead of from a
// read of the table. The create was authorized on a branch, so the session is a write session
// when the caller is allowed catalog:WriteTableData and a read session otherwise.
func (s *credentialService) CreatedTableCredentials(ctx context.Context, caller Caller, addr Address, tableUUID, location string) []StorageCredential {
	if !s.Enabled() || s.createdScope == nil || s.authz == nil {
		return nil
	}
	scope, err := s.createdScope(ctx, addr.Repo, tableUUID, location)
	if err != nil {
		s.logNotVending(ctx, caller, addr, "created table", err)
		return nil
	}
	op := OpWriteTableData
	err = s.authz.Authorize(ctx, caller, Request{Op: op, Address: addr, RefIsBranch: true})
	if isWriteRefusal(err) {
		op = OpReadTableData
		err = s.authz.Authorize(ctx, caller, Request{Op: op, Address: addr, RefIsBranch: true})
	}
	if err != nil {
		s.logNotVending(ctx, caller, addr, "created table", err)
		return nil
	}
	access, _ := dataAccessFor(op)
	return s.vendScope(ctx, caller, addr, scope, access)
}

// writableNamespace reports whether a client may write addr's namespace (§5.4). A service built
// without a store has no resolver, so nothing is reserved.
func (s *credentialService) writableNamespace(addr Address) bool {
	return s.store == nil || s.store.RequireWritableNamespace(addr) == nil
}

// refIsBranch reports whether addr.Ref is a branch, like tableService.isBranchRef: a ref that is
// not a valid branch is "not a branch", and a missing repository is the error it is.
func (s *credentialService) refIsBranch(ctx context.Context, addr Address) (bool, error) {
	if s.refs == nil {
		return false, nil
	}
	_, err := s.refs.GetBranchReference(ctx, addr.Repo, addr.Ref)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, graveler.ErrBranchNotFound), errors.Is(err, graveler.ErrInvalidBranchID):
		return false, nil
	default:
		return false, err
	}
}

// isWriteRefusal reports whether err is the authorizer's refusal of a write session - a denial or
// the Q-I4 read-only ref - which a caller answers with a read session instead.
func isWriteRefusal(err error) bool {
	return errors.Is(err, ErrPermissionDenied) || errors.Is(err, ErrRefReadOnly)
}

// logNotVending records why a LoadTableResult carries no storage credentials.
func (s *credentialService) logNotVending(ctx context.Context, caller Caller, addr Address, what string, err error) {
	s.logger.WithContext(ctx).WithError(err).WithFields(logging.Fields{
		logging.UserFieldKey: caller.Username,
		"repository":         addr.Repo,
		"ref":                addr.Ref,
		"namespace":          addr.Namespace.Dotted(),
		"table":              addr.Table,
	}).Debug("iceberg catalog: no storage credentials for the " + what)
}

// vendScope runs the STS exchange for an authorized request whose scope is known. Every failure is
// best effort (§8): it is logged, and the answer is no credentials.
func (s *credentialService) vendScope(ctx context.Context, caller Caller, addr Address, scope tableDataScope, access dataAccess) []StorageCredential {
	credentials, err := s.assume(ctx, caller, addr, scope, access)
	if err != nil {
		// §8 best effort: the client falls back to its own credentials. The reason stays in the
		// log, and no part of it - the role, the STS error, the table - reaches the client.
		s.logger.WithContext(ctx).WithError(err).WithFields(logging.Fields{
			logging.UserFieldKey: caller.Username,
			"repository":         addr.Repo,
			"ref":                addr.Ref,
			"namespace":          addr.Namespace.Dotted(),
			"table":              addr.Table,
			"access":             access.String(),
		}).Warn("iceberg catalog: not vending storage credentials")
		return nil
	}
	return credentials
}

// assume runs the STS exchange of one session: the narrow policy, the duration, the optional
// external ID, and the credentials in the FileIO form a client decodes.
func (s *credentialService) assume(ctx context.Context, caller Caller, addr Address, scope tableDataScope, access dataAccess) ([]StorageCredential, error) {
	root, err := parseS3Location(scope.Root)
	if err != nil {
		return nil, fmt.Errorf("%w: the table root %q: %w", ErrCredentialVendingFailed, scope.Root, err)
	}
	location, err := parseS3Location(scope.Location)
	if err != nil {
		return nil, fmt.Errorf("%w: the table location %q: %w", ErrCredentialVendingFailed, scope.Location, err)
	}
	// The session is scoped to the table root, so a table that lives outside it would be handed
	// credentials that do not cover its own files - or, worse, a prefix it does not own. Either
	// way it is not vended (ErrCredentialScope) and the client keeps its own credentials.
	if !root.contains(location) {
		return nil, fmt.Errorf("%w: table location %q is not under the table root %q",
			ErrCredentialScope, location.uri, root.uri)
	}
	policy, err := sessionPolicy(root, access)
	if err != nil {
		return nil, err
	}
	assumer, err := s.stsAssumer(ctx)
	if err != nil {
		return nil, err
	}

	durationSeconds, err := sessionDurationSeconds(s.cfg.SessionDuration)
	if err != nil {
		return nil, err
	}
	input := &sts.AssumeRoleInput{
		RoleArn:         aws.String(s.cfg.RoleARN),
		RoleSessionName: aws.String(sessionName(caller, addr)),
		Policy:          aws.String(policy),
		DurationSeconds: aws.Int32(durationSeconds),
	}
	if s.cfg.ExternalID != "" {
		input.ExternalId = aws.String(s.cfg.ExternalID)
	}
	output, err := assumer.AssumeRole(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("%w: assume %s: %w", ErrCredentialVendingFailed, s.cfg.RoleARN, err)
	}
	if output == nil || output.Credentials == nil {
		return nil, fmt.Errorf("%w: STS answered without credentials", ErrCredentialVendingFailed)
	}
	accessKeyID := aws.ToString(output.Credentials.AccessKeyId)
	secretAccessKey := aws.ToString(output.Credentials.SecretAccessKey)
	if accessKeyID == "" || secretAccessKey == "" {
		return nil, fmt.Errorf("%w: STS answered with an empty access key", ErrCredentialVendingFailed)
	}
	// The properties are exactly the ones an Iceberg S3 FileIO reads; s3.region is left out so
	// that the client keeps the region it already configured (the session is not region-bound).
	config := map[string]string{
		accessKeyIDProperty:     accessKeyID,
		secretAccessKeyProperty: secretAccessKey,
	}
	if token := aws.ToString(output.Credentials.SessionToken); token != "" {
		config[sessionTokenProperty] = token
	}
	if output.Credentials.Expiration != nil {
		config[sessionTokenExpiresAtMsProperty] = fmt.Sprintf("%d", output.Credentials.Expiration.UnixMilli())
	}
	return []StorageCredential{{Prefix: root.uri, Config: config}}, nil
}

// stsAssumer returns the STS client, building one from the server's own AWS identity on first
// use when the service was built without one.
func (s *credentialService) stsAssumer(ctx context.Context) (STSAssumer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.assumer != nil {
		return s.assumer, nil
	}
	client, err := newSTSClient(ctx, s.cfg)
	if err != nil {
		return nil, err
	}
	s.assumer = client
	return client, nil
}

// newSTSClient builds the STS client the catalog assumes its role with.
//
// The credentials are the server's own: the same default AWS chain the S3 blockstore uses
// (environment, shared config, ECS/EC2 instance role, IRSA). They are deliberately not the
// caller's - a catalog client authenticates to lakeFS, not to AWS - which is why vending is only
// offered to an installation that runs with an identity allowed to assume role_arn. The endpoint
// override exists for an STS that is not AWS's: MinIO's, or a private endpoint.
func newSTSClient(ctx context.Context, cfg CredentialsVendingConfig) (STSAssumer, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: load the server's AWS configuration: %w", ErrCredentialVendingFailed, err)
	}
	if awsCfg.Region == "" {
		awsCfg.Region = defaultSTSRegion
	}
	return sts.NewFromConfig(awsCfg, func(options *sts.Options) {
		if cfg.Endpoint != "" {
			options.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	}), nil
}

// sessionPolicy renders the inline session policy of one session: the narrowest document that
// lets an Iceberg client work on this table and nothing else (§8, task note of I4-2).
//
// It is the table root, never the storage namespace. Object actions are granted on
// <root>/* only; the one bucket-level action, s3:ListBucket, is restricted by an s3:prefix
// StringLike condition to the same prefix, because a reader lists the table's directory (and a
// maintenance job lists it before deleting expired files) rather than the bucket. s3:GetBucketLocation
// is the one bucket action that cannot be prefix-restricted and is required by every S3 client.
// A write session is a read session plus the write actions, because a commit reads the manifests
// and data files it re-points.
//
// Only Allow statements are written: a session policy is intersected with the role's own
// identity policy, so the result can never grant more than the role has, and adding an explicit
// Deny could only take away what the prefix already excludes.
func sessionPolicy(root s3Location, access dataAccess) (string, error) {
	if root.bucket == "" || root.key == "" {
		return "", fmt.Errorf("%w: %q is not a table prefix", ErrInvalidCredentialRequest, root.uri)
	}
	bucketARN := "arn:aws:s3:::" + root.bucket
	objectARN := bucketARN + "/" + root.key + "/*"

	objectActions := []string{"s3:GetObject", "s3:GetObjectVersion"}
	bucketActions := []string{"s3:GetBucketLocation", "s3:ListBucket"}
	if access == accessWrite {
		objectActions = append(objectActions,
			"s3:AbortMultipartUpload",
			"s3:DeleteObject",
			"s3:ListMultipartUploadParts",
			"s3:PutObject",
		)
	}

	document := policyDocument{
		Version: policyDocumentVersion,
		Statement: []policyStatement{
			{
				Effect:   policyEffectAllow,
				Action:   objectActions,
				Resource: []string{objectARN},
			},
			{
				Effect:   policyEffectAllow,
				Action:   bucketActions,
				Resource: []string{bucketARN},
				Condition: map[string]map[string][]string{
					// The listing is limited to the table's own directory: both the directory
					// itself and everything below it, which is what a recursive listing asks for.
					"s3:prefix": {"StringLike": {root.key + "/", root.key + "/*"}},
				},
			},
		},
	}
	policy, err := json.Marshal(document)
	if err != nil {
		return "", fmt.Errorf("%w: encode the session policy: %w", ErrCredentialVendingFailed, err)
	}
	return string(policy), nil
}

const (
	// policyDocumentVersion and policyEffectAllow are the IAM policy syntax constants of the
	// session policy (IAM policy elements, Version 2012-10-17).
	policyDocumentVersion = "2012-10-17"
	policyEffectAllow     = "Allow"
)

// policyDocument, policyStatement and its fields are the IAM policy document of the session,
// modelled so that it is marshalled in a stable order and can be asserted by a test.
type policyDocument struct {
	Version   string            `json:"Version"`
	Statement []policyStatement `json:"Statement"`
}

type policyStatement struct {
	Effect    string                         `json:"Effect"`
	Action    []string                       `json:"Action"`
	Resource  []string                       `json:"Resource"`
	Condition map[string]map[string][]string `json:"Condition,omitempty"`
}

// sessionDurationSeconds is DurationSeconds of the AssumeRole call: the configured session_duration
// in seconds, checked against the STS bounds again because a service can be built without going
// through Config.Validate (a test, or a caller that assembled the setting itself). STS rejects a
// duration outside [15m, 12h], which would turn every request into a best-effort "no credentials".
func sessionDurationSeconds(duration time.Duration) (int32, error) {
	seconds := duration.Truncate(time.Second)
	if seconds < MinimumVendedSessionDuration || seconds > MaximumVendedSessionDuration {
		return 0, fmt.Errorf("%w: session duration %s is outside [%s, %s]",
			ErrInvalidCredentialRequest, duration, MinimumVendedSessionDuration, MaximumVendedSessionDuration)
	}
	return int32(seconds / time.Second), nil //nolint:gosec // bounded above: 12h is 43200 seconds
}

// sessionName is RoleSessionName of the AssumeRole call: the user, the table and the ref, so that
// a session in CloudTrail is attributable to a lakeFS caller. The value is sanitized to the
// character set STS accepts and truncated to its length limit.
func sessionName(caller Caller, addr Address) string {
	parts := []string{roleSessionNamePrefix, caller.Username, addr.Repo, addr.Ref, addr.Namespace.Dotted(), addr.Table}
	name := sanitizeSessionName(strings.Join(parts, "-"))
	if len(name) > roleSessionNameMaxLength {
		name = name[:roleSessionNameMaxLength]
	}
	if len(name) < roleSessionNameMinLength {
		name = roleSessionNamePrefix
	}
	return name
}

// sanitizeSessionName maps every character STS does not accept onto '-', and "-" onto "-". The
// allowed set is [\w+=,.@-], i.e. letters, digits and underscore plus + = , . @ -
// (https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html, RoleSessionName).
func sanitizeSessionName(name string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '_', r == '+', r == '=', r == ',', r == '.', r == '@', r == '-':
			return r
		default:
			return '-'
		}
	}, name)
}

// tableScope resolves the scope of req's table, authorizing any address the table resolver names
// on the way.
func (s *credentialService) tableScope(ctx context.Context, addr Address, authorize AuthorizeFunc) (tableDataScope, error) {
	if s.scope == nil {
		return tableDataScope{}, errNoTableScope
	}
	return s.scope(ctx, addr, authorize)
}

// newTableScopeResolver builds the production table scope resolver out of the catalog store: the
// table's root is <storage_namespace>/_iceberg/<table-uuid> (§6.3, Q-I5), and the location it is
// compared with is metadata.json's `location`, the same field remote signing trusts.
//
// The table is read through the registered table resolver (resolver.go), so a vended session is
// scoped to the prefix of the table that was actually read - for the Metadata Search virtual table,
// the table in the companion repository - and the caller needed no access to that repository
// (§5.3). Authorization of the resolved address, when it differs from the requested one, is the
// callback's: it runs before the read.
func newTableScopeResolver(store *Store) func(ctx context.Context, addr Address, authorize AuthorizeFunc) (tableDataScope, error) {
	return func(ctx context.Context, addr Address, authorize AuthorizeFunc) (tableDataScope, error) {
		resolved, err := store.ResolveAuthorized(ctx, addr, authorize)
		if err != nil {
			return tableDataScope{}, err
		}
		table, err := store.GetTable(ctx, resolved.Target)
		if err != nil {
			return tableDataScope{}, err
		}
		// The root and the repository are those of the table that was read, which is the target of
		// the resolution and not necessarily the address the client named: a vended session must
		// cover the files of the table it will read.
		target := resolved.Target
		tableUUID, err := uuid.Parse(table.Pointer.TableUUID)
		if err != nil {
			return tableDataScope{}, fmt.Errorf("%w: table %q has no usable UUID: %w",
				ErrInvalidPointer, target.Table, err)
		}
		if table.Metadata == nil {
			return tableDataScope{}, fmt.Errorf("%w: metadata.json has no location", ErrInvalidPointer)
		}
		repo, err := store.catalog.GetRepository(ctx, target.Repo)
		if err != nil {
			return tableDataScope{}, fmt.Errorf("resolve repository %q: %w", target.Repo, err)
		}
		root, err := TableRoot(repo.StorageNamespace, tableUUID)
		if err != nil {
			return tableDataScope{}, err
		}
		return tableDataScope{Root: root, Location: table.Metadata.Location()}, nil
	}
}

// newCreatedTableScopeResolver builds the scope of a table the create path has just decided: the
// root of its UUID under the repository's storage namespace, and the location its metadata.json
// names. Nothing is read from the ref, because a staged create has no pointer there.
func newCreatedTableScopeResolver(store *Store) func(ctx context.Context, repository, tableUUID, location string) (tableDataScope, error) {
	return func(ctx context.Context, repository, tableUUID, location string) (tableDataScope, error) {
		id, err := uuid.Parse(tableUUID)
		if err != nil {
			return tableDataScope{}, fmt.Errorf("%w: table uuid %q: %w", ErrInvalidPointer, tableUUID, err)
		}
		repo, err := store.catalog.GetRepository(ctx, repository)
		if err != nil {
			return tableDataScope{}, fmt.Errorf("resolve repository %q: %w", repository, err)
		}
		root, err := TableRoot(repo.StorageNamespace, id)
		if err != nil {
			return tableDataScope{}, err
		}
		return tableDataScope{Root: root, Location: location}, nil
	}
}

// checkPrefix validates the {prefix} path segment against the catalog's configured prefix
// (§14 Q-I3), exactly like remoteSigningService.checkPrefix.
func (s *credentialService) checkPrefix(w http.ResponseWriter, r *http.Request) bool {
	prefix := chi.URLParam(r, "prefix")
	if prefix == s.prefix {
		return true
	}
	writeIcebergError(w, http.StatusNotFound, ErrTypeNoSuchWarehouse,
		fmt.Sprintf("prefix %q is not the catalog prefix %q", prefix, s.prefix))
	return false
}

// caller builds the authorization caller of a request, like remoteSigningService.caller.
func (s *credentialService) caller(w http.ResponseWriter, r *http.Request) (Caller, bool) {
	user, err := auth.GetUser(r.Context())
	if err != nil {
		writeIcebergError(w, http.StatusUnauthorized, ErrTypeNotAuthorized, authFailureMessage(http.StatusUnauthorized))
		return Caller{}, false
	}
	return Caller{
		Username: user.Username,
		ClientIP: httputil.ExtractClientIP(r.Header, r.RemoteAddr),
	}, true
}

// writeError maps the typed errors of a credentials request onto the catalog envelope. A denial
// is 403, a malformed request is 400, a missing table or warehouse is 404, and an undecided
// authorization or an unexpected failure is 500. Best-effort vending failures never reach this
// function: Vend turns them into "no credentials" (§8).
func (s *credentialService) writeError(w http.ResponseWriter, err error) {
	var (
		denied       *PermissionDeniedError
		readOnly     *ReadOnlyRefError
		authzFailure *AuthorizationError
		noTable      *NoSuchTableException
	)
	switch {
	case errors.Is(err, ErrInvalidCredentialRequest):
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest, err.Error())
	case errors.Is(err, ErrInvalidIdentifier):
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest, err.Error())
	case errors.Is(err, ErrCredentialVendingDisabled):
		writeIcebergError(w, http.StatusNotImplemented, ErrTypeNotImplemented, err.Error())
	case errors.As(err, &readOnly):
		writeIcebergError(w, http.StatusForbidden, ErrorTypeForbidden, readOnly.Message())
	case errors.As(err, &denied):
		writeIcebergError(w, http.StatusForbidden, ErrorTypeForbidden, denied.Error())
	case errors.As(err, &authzFailure):
		writeIcebergError(w, http.StatusInternalServerError, ErrTypeInternalServerError,
			"the authorization decision failed")
	case errors.As(err, &noTable):
		writeIcebergError(w, http.StatusNotFound, errTypeNoSuchTable, noTable.Error())
	case isMissingWarehouse(err):
		writeIcebergError(w, http.StatusNotFound, ErrTypeNoSuchWarehouse,
			"the repository or ref named by the request does not exist")
	default:
		writeIcebergError(w, http.StatusInternalServerError, ErrTypeInternalServerError,
			"the catalog could not serve the request")
	}
}
