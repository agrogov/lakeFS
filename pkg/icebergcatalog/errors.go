package icebergcatalog

import (
	"encoding/json"
	"net/http"
)

// The catalog answers every error in the Iceberg REST error envelope
// (ICEBERG_CATALOG_PLAN.md §2.1):
//
//	{"error": {"message": "...", "type": "...Exception", "code": 404}}
//
// The shape, the required fields and the meaning of `type` and `code` follow
// components.schemas.ErrorModel of the Apache Iceberg REST OpenAPI spec (Apache-2.0; this
// package is a clean-room implementation, see doc.go):
//
//	https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
//
// The envelope is not cosmetic: a client decodes every non-2xx body as this model and reports
// its message, so a body in any other shape (the core lakeFS {"message": ...}, chi's plain-text
// 404, a 501 from an http.Error) leaves the user with no reason at all. That is why even the
// disabled-catalog placeholder and the authentication middleware's answers are rendered here.

// ErrorModel is the error object of the envelope (spec: components.schemas.ErrorModel).
// Message, Type and Code are required by the spec; Stack is optional and deliberately not
// sent - it carries a server-side stack trace, which leaks implementation detail (§12).
type ErrorModel struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    int    `json:"code"`
}

// ErrorResponse is the body of every catalog error.
type ErrorResponse struct {
	Error ErrorModel `json:"error"`
}

// The error types this server answers with. The values are the exception names the spec's
// ErrorModel examples use; a client matches on them, so they are part of the protocol rather
// than an internal detail.
const (
	// ErrTypeBadRequest is a malformed request or body (400).
	ErrTypeBadRequest = "BadRequestException"

	// ErrTypeNotAuthorized is a missing or wrong credential (401). §7.1: the answer carries no
	// detail about why the credential was rejected.
	ErrTypeNotAuthorized = "NotAuthorizedException"

	// ErrTypeForbidden is an authenticated caller the policy does not allow (403). It is also
	// the answer for a write against a tag or a commit, which is read-only (§14 Q-I4).
	ErrTypeForbidden = "ForbiddenException"

	// ErrTypeNotFound is a path under the catalog mount that names no catalog operation
	// (404). The spec names no generic 404 model - only the NoSuch*Exception family - so this
	// follows the same naming for the one 404 that is about the request rather than a resource.
	ErrTypeNotFound = "NotFoundException"

	// ErrTypeNoSuchWarehouse is a warehouse the URL or the identifier cannot name (404):
	// a relative_to path that does not have the <warehouse>/api shape (§5), and a prefix that
	// is not the catalog's constant (§14 Q-I3).
	ErrTypeNoSuchWarehouse = "NoSuchWarehouseException"

	// ErrTypeMethodNotAllowed is a catalog path with a method it does not serve (405). Like
	// ErrTypeNotFound this is not one of the spec's named models, so it follows their naming.
	ErrTypeMethodNotAllowed = "MethodNotAllowedException"

	// ErrTypeCommitFailed is a commit whose requirements do not hold (409): the pointer moved
	// under the client, or an update conflicts (§6.2, I2-3).
	ErrTypeCommitFailed = "CommitFailedException"

	// ErrTypeRequestTooLarge is a body over iceberg.max_request_body (413). The spec has no
	// named model for 413; this keeps the spec's *Exception naming so clients that surface the
	// type do not print an empty reason.
	ErrTypeRequestTooLarge = "RequestTooLargeException"

	// ErrTypeNotImplemented is a catalog that is not enabled on this server (501).
	ErrTypeNotImplemented = "NotImplementedException"

	// ErrTypeInternalServerError is an unexpected failure (500). It carries no detail: the
	// cause goes to the server log, not to the client (§12).
	ErrTypeInternalServerError = "InternalServerError"
)

// errorTypeByStatus is the error type the catalog answers with when a caller has no more
// specific one to give. The statuses are the ones the catalog can return
// (ICEBERG_CATALOG_PLAN.md §12); a named resource failure passes its own type explicitly
// (NoSuchTableException for a missing table, CommitFailedException for a lost race in I2-3).
func errorTypeByStatus(status int) string {
	switch status {
	case http.StatusBadRequest:
		return ErrTypeBadRequest
	case http.StatusUnauthorized:
		return ErrTypeNotAuthorized
	case http.StatusForbidden:
		return ErrTypeForbidden
	case http.StatusNotFound:
		return ErrTypeNotFound
	case http.StatusMethodNotAllowed:
		return ErrTypeMethodNotAllowed
	case http.StatusConflict:
		// The spec's 409 model is CommitFailedException: on this catalog a conflict is a lost
		// compare-and-swap of a table pointer (§6.2).
		return ErrTypeCommitFailed
	case http.StatusRequestEntityTooLarge:
		return ErrTypeRequestTooLarge
	case http.StatusNotImplemented:
		return ErrTypeNotImplemented
	default:
		return ErrTypeInternalServerError
	}
}

// errorBody renders one error envelope. An empty errType means the default of the status
// (errorTypeByStatus), and an empty message is replaced by the status text, because both are
// required fields of the spec's model.
func errorBody(status int, errType, message string) []byte {
	if errType == "" {
		errType = errorTypeByStatus(status)
	}
	if message == "" {
		message = http.StatusText(status)
	}
	data, err := json.Marshal(ErrorResponse{Error: ErrorModel{Message: message, Type: errType, Code: status}})
	if err != nil {
		// Unreachable: ErrorModel holds two strings and an int. The fallback is still a valid
		// envelope because a client cannot decode anything else.
		return []byte(`{"error":{"message":"internal error","type":"InternalServerError","code":500}}`)
	}
	return data
}

// writeIcebergError writes one catalog error: the status, and the envelope body that describes
// it. Every catalog response that is not a success goes through here or through writeJSON, so
// no answer can leave the server in a shape a client does not parse.
func writeIcebergError(w http.ResponseWriter, status int, errType, message string) {
	w.Header().Set("Content-Type", contentTypeJSON)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(errorBody(status, errType, message))
}

// authFailureMessage is the message of a 401 or 403 produced by the authentication middleware.
// It is deliberately constant: §7.1 and the §12 security tests require that a rejected
// credential learns nothing about why it was rejected - not whether the access key exists, not
// whether the secret was close, not which of the four schemes matched.
func authFailureMessage(status int) string {
	if status == http.StatusForbidden {
		return "the credentials do not permit this operation"
	}
	return "authentication required"
}

// authErrorWriter answers the authentication middleware's rejection in the catalog's error
// envelope.
//
// The catalog reuses the core api.GenericAuthMiddleware, which reports a failed authentication
// with the core lakeFS body {"message": ...} and logs the cause. An Iceberg client reads the
// reason of a failure out of the {"error": {...}} envelope, so the status alone is not enough.
//
// The wrapper is in force only until the middleware calls the next handler (downstream): from
// that point the response belongs to the catalog's own handlers, which already answer in the
// envelope, so their answers - including their own 401 from requireUser and their 403 from
// authz - pass through untouched.
type authErrorWriter struct {
	http.ResponseWriter

	// downstream is set once the middleware accepted the request and called the next handler.
	downstream bool
	// replaced is set when this wrapper wrote the envelope in place of the middleware's body.
	replaced bool
}

// WriteHeader intercepts the middleware's 401 and 403 answers and renders the envelope instead.
// The core writer sets Content-Type before WriteHeader, so the headers are overwritten here
// rather than only added.
func (w *authErrorWriter) WriteHeader(status int) {
	if w.downstream || (status != http.StatusUnauthorized && status != http.StatusForbidden) {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	w.replaced = true
	header := w.Header()
	header.Set("Content-Type", contentTypeJSON)
	header.Set("X-Content-Type-Options", "nosniff")
	header.Del("Content-Length")
	w.ResponseWriter.WriteHeader(status)
	_, _ = w.ResponseWriter.Write(errorBody(status, "", authFailureMessage(status)))
}

// Write discards the middleware's body once the envelope was written, and passes everything
// else through.
func (w *authErrorWriter) Write(p []byte) (int, error) {
	if w.replaced {
		return len(p), nil
	}
	return w.ResponseWriter.Write(p)
}

// Unwrap exposes the underlying writer to http.ResponseController, so an outer handler can
// still reach optional interfaces (flushing, hijacking) of the real ResponseWriter.
func (w *authErrorWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
