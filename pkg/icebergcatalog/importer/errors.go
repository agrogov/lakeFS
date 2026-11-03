package importer

// Error vocabulary of the importer. Every error the package returns wraps one of these sentinels,
// so a caller can tell a bad option from a source-catalog failure, and a catalog failure carries
// the Iceberg error envelope of the server that produced it.

import (
	"errors"
	"fmt"
	"net/http"
)

var (
	// ErrInvalidOptions is wrapped by a refusal of the import request itself: a missing source
	// URI, both --namespace and --all, a malformed --source-prop, and so on. Nothing is read.
	ErrInvalidOptions = errors.New("invalid iceberg import options")

	// ErrSource is wrapped by every failure that belongs to the source catalog: an unreachable
	// endpoint, a rejected credential, a malformed response.
	ErrSource = errors.New("iceberg source catalog")

	// ErrTarget is wrapped by every failure that belongs to the lakeFS catalog.
	ErrTarget = errors.New("lakeFS iceberg catalog")

	// ErrInvalidMetadata is wrapped when a table's metadata document cannot be used: it is not
	// JSON, it has no format version or location, or its format version is not one this catalog
	// serves.
	ErrInvalidMetadata = errors.New("invalid iceberg table metadata")

	// ErrTableCopy is wrapped by a copy-mode failure: a file outside the source table location, an
	// unreadable file, a failed upload.
	ErrTableCopy = errors.New("copy iceberg table files")

	// ErrUnsupportedFile is wrapped when the source FileIO cannot read a location's scheme.
	ErrUnsupportedFile = errors.New("unsupported iceberg file location")

	// ErrNotFound is wrapped by a catalog client for a 404. The target client uses it to answer
	// "the table does not exist yet".
	ErrNotFound = errors.New("not found")
)

// CatalogError is an Iceberg REST error response, as the protocol's envelope carries it:
//
//	{"error": {"message": "...", "type": "NoSuchTableException", "code": 404}}
//
// Catalog names the side of the import that produced it ("source" or "target"), so a failure of a
// multi-catalog run says which catalog to look at. A non-JSON body is kept (truncated) in Message.
type CatalogError struct {
	Catalog    string
	Method     string
	URL        string
	StatusCode int
	ErrorType  string
	Message    string
}

func (e *CatalogError) Error() string {
	detail := e.Message
	if detail == "" {
		detail = http.StatusText(e.StatusCode)
	}
	if e.ErrorType != "" {
		return fmt.Sprintf("%s catalog: %s %s: %d %s: %s", e.Catalog, e.Method, e.URL, e.StatusCode, e.ErrorType, detail)
	}
	return fmt.Sprintf("%s catalog: %s %s: %d: %s", e.Catalog, e.Method, e.URL, e.StatusCode, detail)
}

// catalogErrorEnvelope is components.schemas.ErrorResponse of the Iceberg REST OpenAPI spec.
type catalogErrorEnvelope struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    int    `json:"code"`
	} `json:"error"`
}

// isStatus reports whether err is a *CatalogError with one of statuses.
func isStatus(err error, statuses ...int) bool {
	var catalogErr *CatalogError
	if !errors.As(err, &catalogErr) {
		return false
	}
	for _, status := range statuses {
		if catalogErr.StatusCode == status {
			return true
		}
	}
	return false
}

// isNotFound reports whether err is a 404 of a catalog, whatever its error type.
func isNotFound(err error) bool { return isStatus(err, http.StatusNotFound) }

// isConflict reports whether err is a 409 of a catalog, whatever its error type.
func isConflict(err error) bool { return isStatus(err, http.StatusConflict) }
