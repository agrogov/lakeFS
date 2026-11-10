package icebergcatalog

// The HTTP half of the commit path: POST /v1/{prefix}/namespaces/{ns}/tables/{table}
// (ICEBERG_CATALOG_PLAN.md §2.1 "Commit", §6.2 "Atomic commit", §7.2 "Authorization", §11 I-2).
//
// One operation lives here:
//
//	POST /v1/{prefix}/namespaces/{repo<sep>ref<sep>ns…}/tables/{table}
//	    CommitTableRequest in, LoadTableResult out
//
// It is the endpoint that makes the Iceberg commit path reachable: Store.Commit (commit.go) has
// resolved the base pointer, validated the request's requirements, applied its updates, written a
// new metadata.json and swapped the pointer atomically - but until this route existed nothing
// called it, so a client could create a table and never write to it. The route is advertised in
// GET /v1/config through the route table of tables.go, which is what tells PyIceberg and DuckDB
// that the operation exists (§2.1, §15.3).
//
// The handler is deliberately thin, because every decision of substance belongs to a layer that is
// already tested on its own:
//
//   - authorization is authorizeTableWrite with OpUpdateTable, the §7.2 row for "create / register
//     / commit / properties / metrics": catalog:UpdateTable on the table ARN, which is the whole
//     requirement (authz.go: catalog permissions supersede object permissions). That helper also
//     resolves the ref, so the Q-I4 rule - a commit needs a branch, a tag or a commit ID is 403
//     "ref is read-only" - is enforced before the body is read, and an unauthorized caller cannot
//     tell a tag from a branch;
//   - decoding is DecodeTableCommitRequest, so a body this catalog cannot model fails as the 400 it
//     is instead of half-way through a commit;
//   - the commit itself is Store.Commit, including the requirement check (409), the compare-and-swap
//     of the pointer (409) and the unknown-outcome mapping of a failure after the metadata write
//     (500 CommitStateUnknownException).
//
// The order is authorization, then body, then commit. The spec's identifier repeats the address the
// URL already carries, so nothing in the body is needed to build the table ARN - unlike createTable,
// which carries the name in the body - and authentication (the router) plus authorization therefore
// both finish before a byte of the body is parsed.
//
// The response is the LoadTableResult of §6.2 step 7: metadata-location and metadata, exactly the
// two members PyIceberg requires of a commit response (§15.3, "both required"). metadata is the
// bytes this commit wrote, not a re-marshalled model, and config is the empty map a v1 catalog vends
// (phase I-4). The 200 carries the table as it is now, so a client that commits does not have to
// load it again.
//
// Status codes are chosen for the exception class the client raises (§2.1, §15.3): 409 means
// CommitFailedException - refresh and retry - and 500 means CommitStateUnknownException, where the
// commit may have landed and the client must re-read rather than assume failure. Nothing about an
// unknown outcome is ever reported as 409.
//
// Clean-room note: written from ICEBERG_CATALOG_PLAN.md, the Apache Iceberg REST OpenAPI spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml, commitTable,
// components.schemas.CommitTableRequest, CommitTableResponse and LoadTableResult) and the public
// lakeFS documentation only. No code from lakeFS Enterprise or from upstream lakeFS >= 1.87 (BSL)
// was used.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// commitTableEndpoint is the wire form of the commit, advertised in GET /v1/config once the route
// of tables.go is mounted. Like every other entry it keeps the literal {prefix} placeholder: a
// client matches these strings byte for byte (§2.1, §15.3).
const commitTableEndpoint = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}"

// handleCommitTable is POST /v1/{prefix}/namespaces/{namespace}/tables/{table} (spec: commitTable):
// apply one Iceberg commit to the table and answer with the new LoadTableResult.
//
// The commit is staged on the branch, never auto-committed (§6.2 "why stage instead of
// auto-commit"): the client's append, overwrite or property change is visible to subsequent catalog
// reads immediately and becomes part of the repository on the next explicit lakeFS commit, like any
// other change on the branch.
func (s *tableService) handleCommitTable(w http.ResponseWriter, r *http.Request) {
	// Parse, authorize and resolve the ref first: the write needs a branch (Q-I4), and a caller
	// without catalog:UpdateTable learns nothing about the body it sent.
	addr, ok := s.authorizeTableWrite(w, r, OpUpdateTable)
	if !ok {
		return
	}
	if !s.requireWritableNamespace(w, addr) {
		return
	}
	body, ok := s.readCommitBody(w, r)
	if !ok {
		return
	}
	request, err := DecodeTableCommitRequest(body)
	if err != nil {
		s.writeCommitError(w, err)
		return
	}

	result, err := s.store.Commit(r.Context(), addr, request)
	if err != nil {
		s.writeCommitError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, loadTableResult{
		// The pointer that was just staged names the file this commit wrote.
		MetadataLocation: result.Pointer.MetadataLocation,
		// The exact bytes of the new metadata.json, so the response is the document a later load
		// returns instead of a re-encoding of the parsed model.
		Metadata: json.RawMessage(result.MetadataJSON),
		Config:   map[string]string{},
	})
}

// readCommitBody reads the commit request body, bounded by iceberg.max_request_body (§10, §12).
//
// The router's limitRequestBody middleware has already bounded the body, and the bound is applied
// again here for two reasons: a service mounted without that middleware (a unit test that mounts
// Routes() on a bare router) must still refuse an unbounded body, and the 413 answer belongs to this
// handler - readCommitBody is what turns a *http.MaxBytesError into RequestTooLargeException instead
// of the generic 500 an unexpected read failure would become.
func (s *tableService) readCommitBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	limit := s.maxBody
	if limit <= 0 {
		limit = DefaultMaxRequestBody
	}
	if r.Body == nil {
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest, "the request body is empty")
		return nil, false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeIcebergError(w, http.StatusRequestEntityTooLarge, ErrTypeRequestTooLarge,
				fmt.Sprintf("request body exceeds the server limit of %d bytes", maxBytesErr.Limit))
			return nil, false
		}
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest, "cannot read the request body")
		return nil, false
	}
	return body, true
}

// writeCommitError maps the typed errors of the commit path onto the catalog envelope and the
// statuses of §2.1. The four cases are the ones Store.Commit documents:
//
//   - *CommitFailedError: a requirement of the request does not hold, or the pointer swap lost its
//     compare-and-swap. 409 CommitFailedException, the signal to refresh and retry;
//   - *CommitStateUnknownError: metadata.json was written and the swap then failed for another
//     reason, so the commit may have landed. 500 CommitStateUnknownException, never 409 - a client
//     that reads 409 assumes the commit did not happen (§2.1, §15.3);
//   - *BadRequestError: a malformed body, an update the table forbids, an unsupported format
//     version, or set-location. 400, because nothing about this path is worth retrying as-is;
//   - everything else is the read half's error (a missing table is 404 NoSuchTableException, a
//     corrupt pointer is a server-side data error) and is mapped by writeError like the load path.
func (s *tableService) writeCommitError(w http.ResponseWriter, err error) {
	var (
		failed       *CommitFailedError
		stateUnknown *CommitStateUnknownError
		rejected     *BadRequestError
	)
	switch {
	case errors.As(err, &failed):
		writeIcebergError(w, http.StatusConflict, ErrTypeCommitFailed, commitFailedMessage(failed))
	case errors.As(err, &stateUnknown):
		// The message says what the client has to do, not which file was written: the outcome is
		// unknown, so re-read the table instead of retrying the same commit.
		writeIcebergError(w, http.StatusInternalServerError, ErrorTypeCommitStateUnknown,
			"the commit may or may not have been applied: re-read the table before retrying")
	case errors.As(err, &rejected):
		writeIcebergError(w, http.StatusBadRequest, ErrTypeBadRequest, rejected.Message)
	default:
		s.writeError(w, err)
	}
}

// commitFailedMessage is the client-facing reason of a 409. It names the requirement that failed
// when there is one - a client debugging "assert-ref-snapshot-id" needs to know which requirement
// was stale - and otherwise reports the lost race, which is the other way a commit fails with 409.
func commitFailedMessage(err *CommitFailedError) string {
	if err.Requirement != "" {
		return fmt.Sprintf("the requirement %q is not satisfied by the current table", err.Requirement)
	}
	return "the table was changed by a concurrent commit"
}
