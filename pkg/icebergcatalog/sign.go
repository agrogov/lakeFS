package icebergcatalog

// Remote signing for table data access (ICEBERG_CATALOG_PLAN.md §8 "Data-plane access",
// §2.1 "Access delegation" and §7.2 "Authorization").
//
// Iceberg clients normally read and write table files with their own object-store credentials.
// When the server advertises remote signing, the client sends every storage request it would
// otherwise sign itself to
//
//	POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/sign
//
// with a RemoteSignRequest, and uses the signed URL the catalog returns. The catalog therefore
// signs with the blockstore's own credentials - the same credentials lakeFS uses for its own
// reads and writes - which is what makes remote signing work for a client that has no direct
// object-store grant.
//
// The signing endpoint is a capability with a sharp edge: a presigned URL is a bearer capability
// for the object it names. Two checks keep it scoped to the table the caller is authorized for:
//
//  1. the requested URI must resolve, lexically, to an S3 URI under the table's location. The
//     location is the `location` field of the table's current metadata.json. The comparison is a
//     path comparison on a normalized URI, never a string prefix: `..` segments, encoded
//     traversal, another bucket, a trailing sibling (`<location>-other/`) and a non-S3 scheme are
//     all refused before the blockstore is asked to sign anything.
//  2. the caller needs catalog:ReadTableData for a read (GET, HEAD) and catalog:WriteTableData
//     for a write (PUT, POST, DELETE, PATCH, OPTIONS), exactly the §7.2 row for vended data
//     access. A write additionally needs a branch ref (Q-I4), so a tag or commit ref is refused
//     as read-only. The Authorizer also adds fs:ReadRepository and, for a write,
//     fs:WriteObject on the reserved _lakefs_iceberg/ area.
//
// Advertisement. The blockstore, not the URL, decides whether remote signing exists: it is
// enabled when the configured blockstore is S3 and its pre-signer is available
// (RemoteSigningSupported; the S3 adapter reports both through GetStorageNamespaceInfo). While
// it is disabled the routes table and the table-config property are both empty, so the endpoint
// is neither mounted nor advertised. When it is enabled, TableConfig returns the spec's
// `s3.remote-signing-enabled: "true"` property for a LoadTableResult, and only when the client
// actually asked for remote signing in X-Iceberg-Access-Delegation (§2.1: the server may ignore
// the header and must not advertise a mechanism the client did not request).
//
// Signing methods. The block adapter interface exposes a read presigner (GET) and a write
// presigner (PUT), which is what lakeFS already uses for its own presigned URLs. Those are the
// two methods a remote-signing S3 client needs for table data. The other methods of the spec's
// RemoteSignRequest enum are recognized for authorization (so a write method can never fall back
// to a read grant and so it is still refused on a tag), and then refused with 400 because the
// adapter cannot sign them. This is deliberate: returning a GET URL labelled HEAD (or a PUT URL
// labelled POST) would be a signature the storage service rejects, which is worse than an
// explicit refusal.
//
// Clean-room note (plan §2, DATASETS_DESIGN.md §2): written from the Apache Iceberg REST OpenAPI
// spec (Apache-2.0),
// https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
// (RemoteSignRequest, RemoteSignResult, RemoteSigningConfig and the `data-access` parameter),
// the Apache Iceberg table spec and the public lakeFS documentation only. No code from lakeFS
// Enterprise, and nothing from upstream lakeFS >= 1.87 (BSL), was used.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/httputil"
)

const (
	// AccessDelegationHeader is the spec's `data-access` request header
	// (X-Iceberg-Access-Delegation). Its value is a comma-separated list of mechanisms the client
	// supports: vended-credentials, remote-signing or none (§2.1).
	AccessDelegationHeader = "X-Iceberg-Access-Delegation"

	// RemoteSigningDelegation is the value of AccessDelegationHeader that asks the server for
	// remote signing.
	RemoteSigningDelegation = "remote-signing"

	// VendedCredentialsDelegation is the value of AccessDelegationHeader that asks the server to
	// vend storage credentials for the table (§8, creds.go). Both clients in the spike send it:
	// PyIceberg on every request, DuckDB on the table-scoped ones (ICEBERG_SPIKE_S-I4.md §4.1).
	VendedCredentialsDelegation = "vended-credentials" //nolint:gosec // a protocol value of the data-access header, not a credential

	// RemoteSigningEnabledProperty is the LoadTableResult config property that tells an S3 client
	// to send its requests to the signing endpoint (§8). It is only set while
	// RemoteSigningSupported reports true and the client requested remote signing.
	RemoteSigningEnabledProperty = "s3.remote-signing-enabled"

	// RemoteSigningUnsupportedMessage is the 501 answer when this deployment cannot sign. Remote
	// signing is S3 only in OSS (§8), so the message names the requirement: an Azure or GCS
	// blockstore has no remote signing, and an S3 blockstore needs a working pre-signer.
	RemoteSigningUnsupportedMessage = "remote signing is not enabled for this catalog: it requires an S3 blockstore with a pre-signer"

	// remoteSigningPattern is the signing route, relative to the catalog mount (APIBasePath).
	remoteSigningPattern = "/v1/{prefix}/namespaces/{namespace}/tables/{table}/sign"

	// remoteSigningEndpoint is the wire form advertised in GET /v1/config once the route is
	// mounted. It is the spec's operation path with the catalog's {prefix} segment.
	remoteSigningEndpoint = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/sign"

	// errTypeNoSuchTable is the Iceberg error type of a missing table, 404. The table read path
	// (I1-9) owns the full table surface; sign.go only needs the type for the lookup it performs.
	errTypeNoSuchTable = "NoSuchTableException"
)

// The refusals of a signing request. They are sentinels so the handler and the tests classify a
// refusal without matching on message text; the handler maps them to the Iceberg error envelope.
var (
	// ErrInvalidSignRequest is a malformed signing request: an unknown method, a non-S3 scheme,
	// a query string, a `..` segment or a missing URI. It is a 400 BadRequestException.
	ErrInvalidSignRequest = errors.New("invalid remote signing request")

	// ErrForeignSignURI is a well-formed S3 URI that is not under the table location. It is a
	// 403 ForbiddenException: the caller may read the table's data, but not an arbitrary object.
	ErrForeignSignURI = errors.New("the requested URI is not under the table location")

	// ErrUnsupportedSigningMethod is a method the block adapter cannot presign. It is a 400
	// BadRequestException; see the file comment for why pretending to sign it would be worse.
	ErrUnsupportedSigningMethod = errors.New("the blockstore cannot sign this method")

	// errNoTableLocator is a programming error: the service was built without a store, so it
	// cannot resolve the table location the URI check needs.
	errNoTableLocator = errors.New("icebergcatalog: no table location loader configured")
)

// RemoteSignRequest is RemoteSignRequest of the Iceberg REST OpenAPI spec, the body of
// POST …/tables/{table}/sign. The spec requires region, uri, method and headers. lakeFS signs its
// own request entirely from the method and the URI, so region, headers, properties and body are
// decoded (a client is allowed to send them) but do not change the signature; body is in
// particular not republished because no supported method carries a signed body.
type RemoteSignRequest struct {
	Region     string              `json:"region"`
	URI        string              `json:"uri"`
	Method     string              `json:"method"`
	Headers    map[string][]string `json:"headers"`
	Properties map[string]string   `json:"properties,omitempty"`
	Body       string              `json:"body,omitempty"`
	Provider   string              `json:"provider,omitempty"`
}

// RemoteSignResult is RemoteSignResult of the spec: the signed URI and any headers the client
// must send with it. The block adapter's presigner returns a self-contained S3 URL, so Headers is
// always present and empty rather than absent (both are required fields in the spec).
type RemoteSignResult struct {
	URI     string              `json:"uri"`
	Headers map[string][]string `json:"headers"`
}

// RefResolver answers whether a ref is a branch. It is the one thing a write-side signing request
// needs beyond the table address: Q-I4 refuses writes on a tag or a commit. *catalog.Catalog
// implements GetBranchReference, which is the same lookup the commit path uses.
type RefResolver interface {
	GetBranchReference(ctx context.Context, repositoryID string, branch string) (string, error)
}

// remoteSigningService serves POST …/tables/{table}/sign and holds the two advertisement hooks:
// Routes for the catalog route table and TableConfig for a LoadTableResult.
//
// It is built by NewRemoteSigningService and, like namespaceService, exposes exactly the surface
// the future route-table change needs: Routes, TableConfig and Enabled. Tests in this package
// build it directly with a fixed table location and a fake block adapter.
type remoteSigningService struct {
	// locate resolves the current table location (metadata.json's `location`) for an address,
	// authorizing the addresses the table resolver names on the way (resolver.go): a virtual table
	// is signed from where it actually lives - the companion repository - while the caller was
	// authorized on the data repository (§5.3).
	locate func(ctx context.Context, addr Address, authorize AuthorizeFunc) (string, error)

	// authz is the §7.2 authorizer.
	authz *Authorizer

	// adapter is the blockstore whose own credentials sign the request.
	adapter block.Adapter

	// refs resolves whether a write addresses a branch (Q-I4). A nil resolver makes every write
	// fail closed as read-only: the zero value of a request must never grant a write.
	refs RefResolver

	// prefix is overrides.prefix of GET /v1/config.
	prefix string

	// enabled mirrors s3.remote-signing-enabled: the blockstore can sign and the client-facing
	// advertisement is on.
	enabled bool
}

// NewRemoteSigningService builds the remote-signing surface for a catalog store. adapter is the
// blockstore that signs; when it is nil the store's own adapter is used. cfg supplies the prefix
// (an empty configuration falls back to CatalogPrefix, like server.prefix in http.go).
func NewRemoteSigningService(store *Store, authz *Authorizer, adapter block.Adapter, refs RefResolver, cfg *Config) *remoteSigningService {
	var locate func(ctx context.Context, addr Address, authorize AuthorizeFunc) (string, error)
	if store != nil {
		locate = func(ctx context.Context, addr Address, authorize AuthorizeFunc) (string, error) {
			resolved, err := store.ResolveAuthorized(ctx, addr, authorize)
			if err != nil {
				return "", err
			}
			table, err := store.GetTable(ctx, resolved.Target)
			if err != nil {
				return "", err
			}
			if table.Metadata == nil {
				return "", fmt.Errorf("%w: metadata.json has no location", ErrInvalidPointer)
			}
			return table.Metadata.Location(), nil
		}
		if adapter == nil {
			adapter = store.block
		}
	}
	prefix := CatalogPrefix
	if cfg != nil && cfg.Prefix != "" {
		prefix = cfg.Prefix
	}
	return &remoteSigningService{
		locate:  locate,
		authz:   authz,
		adapter: adapter,
		refs:    refs,
		prefix:  prefix,
		enabled: RemoteSigningSupported(adapter),
	}
}

// RemoteSigningSupported reports whether the blockstore can sign remote Iceberg requests. It is
// the server-side meaning of s3.remote-signing-enabled: the blockstore must be S3 (the only
// provider this implementation signs for, §8) and its pre-signer must be available. A disabled
// pre-signer (blockstore.s3.disable_pre_signed) therefore switches the endpoint off rather than
// advertising an endpoint that can only answer 500.
func RemoteSigningSupported(adapter block.Adapter) bool {
	if adapter == nil || adapter.BlockstoreType() != block.BlockstoreTypeS3 {
		return false
	}
	info := adapter.GetStorageNamespaceInfo("")
	return info != nil && info.PreSignSupport
}

// Enabled reports whether this catalog advertises remote signing.
func (s *remoteSigningService) Enabled() bool {
	return s != nil && s.enabled && s.adapter != nil
}

// Routes is this file's contribution to the catalog route table (server.ownRoutes in http.go).
// The endpoint is advertised by the same table entry, so it exists in GET /v1/config exactly
// when the service is enabled: a disabled blockstore contributes no route and no endpoint.
func (s *remoteSigningService) Routes() []route {
	if !s.Enabled() {
		return nil
	}
	return []route{{
		method:   http.MethodPost,
		pattern:  remoteSigningPattern,
		handler:  s.handleSign,
		endpoint: remoteSigningEndpoint,
	}}
}

// TableConfig returns the LoadTableResult config properties that turn remote signing on for a
// client, or nil when it must not be advertised. delegation is the raw
// X-Iceberg-Access-Delegation header of the load-table request; remote signing is advertised only
// when the client listed it (§2.1: the server may choose any or none of the requested
// mechanisms, but it must not choose one the client did not request).
func (s *remoteSigningService) TableConfig(delegation string) map[string]string {
	if !s.Enabled() || !delegationRequestsRemoteSigning(delegation) {
		return nil
	}
	return map[string]string{RemoteSigningEnabledProperty: "true"}
}

// delegationRequestsRemoteSigning reports whether a comma-separated X-Iceberg-Access-Delegation
// value names remote-signing. Comparison is case-insensitive and whitespace-tolerant, so a client
// that sends "vended-credentials, remote-signing" is understood.
func delegationRequestsRemoteSigning(header string) bool {
	return delegationRequests(header, RemoteSigningDelegation)
}

// delegationRequests reports whether a comma-separated X-Iceberg-Access-Delegation value names
// one mechanism of §2.1. The header is a client capability list, so every mechanism of it has to
// be matched the same way; creds.go matches VendedCredentialsDelegation with this function.
func delegationRequests(header, mechanism string) bool {
	for _, requested := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(requested), mechanism) {
			return true
		}
	}
	return false
}

// handleSign is POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/sign.
//
// The order is prefix, delegation, identifier, authorization, method, table lookup, location
// check, signing: a caller learns nothing about the table (or its location) before the §7.2
// decision, and the blockstore is never asked for a URL that has not passed the location check.
func (s *remoteSigningService) handleSign(w http.ResponseWriter, r *http.Request) {
	if !s.Enabled() {
		writeIcebergError(w, http.StatusNotImplemented, ErrTypeNotImplemented,
			RemoteSigningUnsupportedMessage)
		return
	}
	if !s.checkPrefix(w, r) {
		return
	}

	// Honour X-Iceberg-Access-Delegation: remote-signing. An absent header is the client calling
	// the signing endpoint without a negotiation (allowed); a header that lists mechanisms but
	// not remote-signing is a client that asked for something else, so signing is refused rather
	// than silently done.
	delegation := strings.Join(r.Header.Values(AccessDelegationHeader), ",")
	if delegation != "" && !delegationRequestsRemoteSigning(delegation) {
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest,
			"the client did not request remote signing in "+AccessDelegationHeader)
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

	var request RemoteSignRequest
	if !decodeJSONBody(w, r, &request) {
		return
	}
	method := strings.ToUpper(strings.TrimSpace(request.Method))
	op, ok := signingOperation(method)
	if !ok {
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest,
			fmt.Sprintf("method %q is not supported by remote signing", request.Method))
		return
	}
	if request.Provider != "" && !strings.EqualFold(request.Provider, "s3") {
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest,
			fmt.Sprintf("provider %q is not supported; remote signing is S3 only", request.Provider))
		return
	}

	refIsBranch := false
	if op == OpWriteTableData {
		refIsBranch, err = s.refIsBranch(r.Context(), addr)
		if err != nil {
			s.writeError(w, err)
			return
		}
	}
	if err := s.authz.Authorize(r.Context(), caller, Request{
		Op: op, Address: addr, RefIsBranch: refIsBranch,
	}); err != nil {
		s.writeError(w, err)
		return
	}

	mode, ok := signingMode(method)
	if !ok {
		// The method is recognized (and was authorized as a write above), but the block adapter
		// has no presigner for it. See the file comment.
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest,
			fmt.Sprintf("%s: %s", method, ErrUnsupportedSigningMethod))
		return
	}

	// The location is the one of the table the resolver reads (resolver.go), not necessarily the
	// one of the address the caller named; the signature is therefore scoped to the files of the
	// table that will actually be read.
	location, err := s.tableLocation(r.Context(), addr, func(ctx context.Context, authzAddr Address) error {
		return s.authz.Authorize(ctx, caller, Request{Op: op, Address: authzAddr, RefIsBranch: refIsBranch})
	})
	if err != nil {
		s.writeError(w, err)
		return
	}
	requested, err := parseS3Location(request.URI)
	if err != nil {
		s.writeError(w, err)
		return
	}
	tableLocation, err := parseS3Location(location)
	if err != nil {
		// The table location comes from metadata.json, so an unusable one is a server-side data
		// error, not the client's request.
		writeIcebergError(w, http.StatusInternalServerError, ErrTypeInternalServerError,
			"the table location is not a valid S3 URI")
		return
	}
	if !tableLocation.contains(requested) {
		writeIcebergError(w, http.StatusForbidden, ErrorTypeForbidden, ErrForeignSignURI.Error())
		return
	}

	signed, _, err := s.adapter.GetPreSignedURL(r.Context(), block.ObjectPointer{
		Identifier:     requested.uri,
		IdentifierType: block.IdentifierTypeFull,
	}, mode, "")
	if err != nil {
		if errors.Is(err, block.ErrOperationNotSupported) {
			writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest,
				"the blockstore does not support pre-signing")
			return
		}
		writeIcebergError(w, http.StatusInternalServerError, ErrTypeInternalServerError,
			"the catalog could not sign the request")
		return
	}
	if signed == "" {
		writeIcebergError(w, http.StatusInternalServerError, ErrTypeInternalServerError,
			"the catalog could not sign the request")
		return
	}
	writeJSON(w, http.StatusOK, RemoteSignResult{
		URI:     signed,
		Headers: map[string][]string{},
	})
}

// signingOperation classifies a RemoteSignRequest method onto the §7.2 data actions: GET and
// HEAD read table data, every other method the spec allows writes it. Classifying before the
// method-support check is deliberate: DELETE must still require WriteTableData (and a branch)
// even though this implementation cannot sign it.
func signingOperation(method string) (Operation, bool) {
	switch method {
	case http.MethodGet, http.MethodHead:
		return OpReadTableData, true
	case http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodPatch, http.MethodOptions:
		return OpWriteTableData, true
	default:
		return "", false
	}
}

// signingMode is the block adapter presign mode of a method. Only the two operations lakeFS's
// block adapter can presign are supported (see the file comment).
func signingMode(method string) (block.PreSignMode, bool) {
	switch method {
	case http.MethodGet:
		return block.PreSignModeRead, true
	case http.MethodPut:
		return block.PreSignModeWrite, true
	default:
		return 0, false
	}
}

// refIsBranch reports whether addr.Ref is a branch. A tag or a commit is a missing branch and is
// therefore read-only for a write (Q-I4); any other failure to resolve is returned as a failure
// to decide, never as "not a branch", so a transient error cannot change the answer to a 403.
func (s *remoteSigningService) refIsBranch(ctx context.Context, addr Address) (bool, error) {
	if s.refs == nil {
		return false, nil
	}
	_, err := s.refs.GetBranchReference(ctx, addr.Repo, addr.Ref)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, graveler.ErrBranchNotFound):
		return false, nil
	default:
		return false, err
	}
}

// tableLocation resolves the current location of the table at addr, authorizing any address the
// table resolver names on the way.
func (s *remoteSigningService) tableLocation(ctx context.Context, addr Address, authorize AuthorizeFunc) (string, error) {
	if s.locate == nil {
		return "", errNoTableLocator
	}
	return s.locate(ctx, addr, authorize)
}

// checkPrefix validates the {prefix} path segment against the catalog's configured prefix
// (§14 Q-I3), exactly like namespaceService.checkPrefix.
func (s *remoteSigningService) checkPrefix(w http.ResponseWriter, r *http.Request) bool {
	prefix := chi.URLParam(r, "prefix")
	if prefix == s.prefix {
		return true
	}
	writeIcebergError(w, http.StatusNotFound, ErrTypeNoSuchWarehouse,
		fmt.Sprintf("prefix %q is not the catalog prefix %q", prefix, s.prefix))
	return false
}

// caller builds the authorization caller of a request, like namespaceService.caller.
func (s *remoteSigningService) caller(w http.ResponseWriter, r *http.Request) (Caller, bool) {
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

// writeError maps the typed errors of a signing request onto the catalog envelope. A refusal is
// 403, a malformed request or an unsupported method is 400, a missing table or warehouse is 404,
// and an undecided authorization or an unexpected failure is 500.
func (s *remoteSigningService) writeError(w http.ResponseWriter, err error) {
	var (
		denied       *PermissionDeniedError
		readOnly     *ReadOnlyRefError
		authzFailure *AuthorizationError
		noTable      *NoSuchTableException
	)
	switch {
	case errors.Is(err, ErrInvalidSignRequest):
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest, err.Error())
	case errors.Is(err, ErrInvalidIdentifier):
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest, err.Error())
	case errors.Is(err, ErrForeignSignURI):
		writeIcebergError(w, http.StatusForbidden, ErrorTypeForbidden, err.Error())
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

// s3Location is a resolved S3 URI: one bucket and a normalized key. It is the only form the
// location check compares; no raw string from the request is ever used as a prefix.
type s3Location struct {
	bucket string
	key    string
	uri    string
}

// parseS3Location resolves a storage URI to its lexical S3 form. It accepts the s3, s3a and s3n
// schemes (Spark and Hadoop spell the same bucket that way) and normalizes all three to s3://.
//
// The checks are the security boundary of the signing endpoint, so they are deliberately strict:
//
//   - userinfo, a port, a query string, a fragment, a fragment-like opaque URI and a non-S3
//     scheme are refused; an S3 object URI has none of them;
//   - the path is taken from url.Path, i.e. after percent-decoding, so `%2e%2e` is seen as `..`
//     and refused; a backslash (a separator on Windows, a literal in an S3 key) is refused;
//   - every segment is checked for `.` and `..` before path.Clean runs, so the clean can only
//     collapse duplicate separators, never resolve a traversal;
//   - control characters are refused, so a URI cannot smuggle a request-splitting byte into the
//     signed URL.
func parseS3Location(raw string) (s3Location, error) {
	if strings.TrimSpace(raw) == "" {
		return s3Location{}, fmt.Errorf("%w: the URI is empty", ErrInvalidSignRequest)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return s3Location{}, fmt.Errorf("%w: %v", ErrInvalidSignRequest, err)
	}
	switch strings.ToLower(u.Scheme) {
	case "s3", "s3a", "s3n":
	default:
		return s3Location{}, fmt.Errorf("%w: scheme %q is not s3", ErrInvalidSignRequest, u.Scheme)
	}
	if u.Opaque != "" || u.User != nil || u.Host == "" {
		return s3Location{}, fmt.Errorf("%w: %q does not name an S3 bucket", ErrInvalidSignRequest, raw)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return s3Location{}, fmt.Errorf("%w: %q carries a query or a fragment", ErrInvalidSignRequest, raw)
	}
	if strings.ContainsAny(u.Host, ":/\\") {
		return s3Location{}, fmt.Errorf("%w: %q has an invalid bucket name", ErrInvalidSignRequest, u.Host)
	}

	key := strings.TrimPrefix(u.Path, "/")
	if strings.Contains(key, "\\") {
		return s3Location{}, fmt.Errorf("%w: %q contains a backslash", ErrInvalidSignRequest, raw)
	}
	if strings.IndexFunc(key, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
		return s3Location{}, fmt.Errorf("%w: %q contains a control character", ErrInvalidSignRequest, raw)
	}
	if key != "" {
		for _, segment := range strings.Split(key, "/") {
			if segment == "." || segment == ".." {
				return s3Location{}, fmt.Errorf("%w: %q contains a %q path segment", ErrInvalidSignRequest, raw, segment)
			}
		}
		key = path.Clean(key)
		if key == "." {
			key = ""
		}
	}

	normalized := (&url.URL{Scheme: "s3", Host: u.Host, Path: "/" + key}).String()
	if key == "" {
		normalized = (&url.URL{Scheme: "s3", Host: u.Host}).String()
	}
	return s3Location{bucket: u.Host, key: key, uri: normalized}, nil
}

// contains reports whether other is the table location itself or an object below it. The suffix
// comparison is on a path boundary, so a table at .../uuid does not contain .../uuid-other/....
func (l s3Location) contains(other s3Location) bool {
	if l.bucket != other.bucket {
		return false
	}
	if l.key == "" {
		return true
	}
	return other.key == l.key || strings.HasPrefix(other.key, l.key+"/")
}
