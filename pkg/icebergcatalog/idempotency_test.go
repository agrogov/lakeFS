package icebergcatalog

// Tests for Idempotency-Key support (ICEBERG_CATALOG_PLAN.md §2.1, §11 I5-3). The three cases the
// ledger names are all here:
//
//   - a replay returns the same body and does not run the operation again;
//   - an expired key runs the operation again;
//   - a request without the header is unaffected.
//
// They are joined by the two rules the spec is explicit about and the ledger repeats: a stored
// 409 is replayed as a 409 (never swallowed into a success), a 5xx is not stored, and
// idempotency-key-lifetime is advertised only when there is storage behind it.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	_ "github.com/treeverse/lakefs/pkg/kv/mem"
)

// idempotencyTestPath is the mutating route the tests mount. A POST is enough: the server wraps
// every method that is not a read (http.go isMutatingMethod).
const idempotencyTestPath = "/v1/mutate"

// newIdempotencyKV opens a private in-memory KV for one test, so records cannot leak between
// tests through a shared partition.
func newIdempotencyKV(t *testing.T) kv.Store {
	t.Helper()
	store, err := kv.Open(t.Context(), kvparams.Config{Type: "mem"})
	require.NoError(t, err)
	t.Cleanup(store.Close)
	return store
}

// idempotencyTestHandler counts its executions, so a replay is observable as calls not growing.
// A dynamic body carries the call number: if an expired or uncached key re-executes, the second
// response differs from the first.
type idempotencyTestHandler struct {
	calls   int
	status  int
	body    string
	dynamic bool
}

func (h *idempotencyTestHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	h.calls++
	status := h.status
	if status == 0 {
		status = http.StatusOK
	}
	body := h.body
	if h.dynamic {
		body = fmt.Sprintf(`{"call":%d}`, h.calls)
	}
	w.Header().Set("Content-Type", contentTypeJSON)
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

// newIdempotencyRouter mounts handler as a mutating route on the production middleware stack,
// with store as the Idempotency-Key store. store may be nil, which is how a server without a KV
// is expressed.
func newIdempotencyRouter(t *testing.T, store *IdempotencyStore, handler http.Handler) http.Handler {
	t.Helper()
	cfg := &Config{MaxRequestBody: DefaultMaxRequestBody}
	s := testServer(cfg, route{
		method:  http.MethodPost,
		pattern: idempotencyTestPath,
		handler: handler.ServeHTTP,
	})
	s.idempotency = store
	router, err := newRouter(authenticated, cfg, s)
	require.NoError(t, err)
	return router
}

// doIdempotent sends one mutating request, with the Idempotency-Key header when key is non-empty.
func doIdempotent(t *testing.T, router http.Handler, key string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, idempotencyTestPath, nil)
	if key != "" {
		req.Header.Set(IdempotencyKeyHeader, key)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// TestIdempotencyReplayReturnsSameBody is the core promise: the second request with the same key
// returns the stored final response, byte for byte, without re-running the operation.
func TestIdempotencyReplayReturnsSameBody(t *testing.T) {
	store := NewIdempotencyStore(newIdempotencyKV(t), time.Hour)
	handler := &idempotencyTestHandler{dynamic: true}
	router := newIdempotencyRouter(t, store, handler)

	first := doIdempotent(t, router, "0d1f22e2-79b0-7cc3-98c4-dc0c0c07398f")
	require.Equal(t, http.StatusOK, first.Code, "body: %s", first.Body.String())
	require.Equal(t, `{"call":1}`, first.Body.String())
	require.Equal(t, contentTypeJSON, first.Header().Get("Content-Type"))

	replay := doIdempotent(t, router, "0d1f22e2-79b0-7cc3-98c4-dc0c0c07398f")
	require.Equal(t, first.Code, replay.Code)
	require.Equal(t, first.Body.String(), replay.Body.String(), "the stored body is replayed exactly")
	require.Equal(t, contentTypeJSON, replay.Header().Get("Content-Type"))
	require.Equal(t, 1, handler.calls, "the operation must not run twice")

	// A different key is a different operation and must execute.
	other := doIdempotent(t, router, "1a2b3c4d-0000-7000-8000-000000000000")
	require.Equal(t, `{"call":2}`, other.Body.String())
	require.Equal(t, 2, handler.calls)
}

// TestIdempotencyExpiredKeyReExecutes pins the TTL: once the lifetime passed, the stored record is
// no longer replayed and the operation runs again. The clock is injected, so the test does not
// sleep.
func TestIdempotencyExpiredKeyReExecutes(t *testing.T) {
	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
	store := NewIdempotencyStore(newIdempotencyKV(t), time.Minute)
	store.now = func() time.Time { return now }
	handler := &idempotencyTestHandler{dynamic: true}
	router := newIdempotencyRouter(t, store, handler)

	first := doIdempotent(t, router, "2b3c4d5e-0000-7000-8000-000000000000")
	require.Equal(t, `{"call":1}`, first.Body.String())
	require.Equal(t, 1, handler.calls)

	// Still inside the window: replay.
	now = now.Add(30 * time.Second)
	withinWindow := doIdempotent(t, router, "2b3c4d5e-0000-7000-8000-000000000000")
	require.Equal(t, `{"call":1}`, withinWindow.Body.String())
	require.Equal(t, 1, handler.calls)

	// Past the window: the key is free again and the operation re-executes.
	now = now.Add(2 * time.Minute)
	afterExpiry := doIdempotent(t, router, "2b3c4d5e-0000-7000-8000-000000000000")
	require.Equal(t, http.StatusOK, afterExpiry.Code)
	require.Equal(t, `{"call":2}`, afterExpiry.Body.String())
	require.Equal(t, 2, handler.calls)

	// The re-execution stored a fresh record, so the next request replays it.
	replay := doIdempotent(t, router, "2b3c4d5e-0000-7000-8000-000000000000")
	require.Equal(t, `{"call":2}`, replay.Body.String())
	require.Equal(t, 2, handler.calls)
}

// TestIdempotencyMissingKeyUnaffected: without the header every request executes, exactly as
// before idempotency existed.
func TestIdempotencyMissingKeyUnaffected(t *testing.T) {
	store := NewIdempotencyStore(newIdempotencyKV(t), time.Hour)
	handler := &idempotencyTestHandler{dynamic: true}
	router := newIdempotencyRouter(t, store, handler)

	first := doIdempotent(t, router, "")
	second := doIdempotent(t, router, "")

	require.Equal(t, `{"call":1}`, first.Body.String())
	require.Equal(t, `{"call":2}`, second.Body.String())
	require.Equal(t, 2, handler.calls, "a request without a key is never cached")
}

// TestIdempotencyStoredConflictStaysAConflict: a 409 is a deterministic terminal answer, so it is
// stored and replayed - as a 409. Turning it into a success would tell the client a commit it
// lost actually won (§2.1, §11 I5-3 notes).
func TestIdempotencyStoredConflictStaysAConflict(t *testing.T) {
	store := NewIdempotencyStore(newIdempotencyKV(t), time.Hour)
	const conflictBody = `{"error":{"message":"commit failed","type":"CommitFailedException","code":409}}`
	handler := &idempotencyTestHandler{status: http.StatusConflict, body: conflictBody}
	router := newIdempotencyRouter(t, store, handler)

	first := doIdempotent(t, router, "3c4d5e6f-0000-7000-8000-000000000000")
	requireEnvelope(t, first, http.StatusConflict, ErrTypeCommitFailed)
	require.Equal(t, conflictBody, first.Body.String())

	replay := doIdempotent(t, router, "3c4d5e6f-0000-7000-8000-000000000000")
	require.Equal(t, http.StatusConflict, replay.Code, "a stored 409 must never become a success")
	require.Equal(t, conflictBody, replay.Body.String())
	require.Equal(t, 1, handler.calls)
}

// TestIdempotencyDoesNotStoreServerErrors: the spec says a 5xx is not finalized - the client
// cannot know whether the operation happened - so it must not become a cached answer.
func TestIdempotencyDoesNotStoreServerErrors(t *testing.T) {
	store := NewIdempotencyStore(newIdempotencyKV(t), time.Hour)
	handler := &idempotencyTestHandler{
		status: http.StatusInternalServerError,
		body:   `{"error":{"message":"commit state unknown","type":"CommitStateUnknownException","code":500}}`,
	}
	router := newIdempotencyRouter(t, store, handler)

	first := doIdempotent(t, router, "4d5e6f70-0000-7000-8000-000000000000")
	require.Equal(t, http.StatusInternalServerError, first.Code)
	second := doIdempotent(t, router, "4d5e6f70-0000-7000-8000-000000000000")
	require.Equal(t, http.StatusInternalServerError, second.Code)
	require.Equal(t, 2, handler.calls, "a 5xx must not be stored and replayed")
}

// TestIdempotencyAdvertisedOnlyWhenWired is the §2.1 promise: idempotency-key-lifetime is present
// in GET /v1/config only when responses are actually stored, and it is an ISO-8601 duration.
func TestIdempotencyAdvertisedOnlyWhenWired(t *testing.T) {
	cfg := &Config{MaxRequestBody: DefaultMaxRequestBody}

	t.Run("wired storage advertises its lifetime", func(t *testing.T) {
		s := newServer(cfg)
		s.idempotency = NewIdempotencyStore(newIdempotencyKV(t), DefaultIdempotencyKeyLifetime)
		router, err := newRouter(authenticated, cfg, s)
		require.NoError(t, err)

		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, configPath, nil))
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

		var response CatalogConfig
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.Equal(t, "PT30M", response.IdempotencyKeyLifetime)
	})

	t.Run("no storage sends no lifetime", func(t *testing.T) {
		s := newServer(cfg)
		router, err := newRouter(authenticated, cfg, s)
		require.NoError(t, err)

		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, configPath, nil))
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		require.NotContains(t, rec.Body.String(), "idempotency-key-lifetime",
			"advertising a lifetime without storage would promise replay that does not happen")
	})
}

// TestIdempotencyISO8601Duration pins the wire format of the advertised value
// (CatalogConfig idempotency-key-lifetime, format: duration, example PT30M).
func TestIdempotencyISO8601Duration(t *testing.T) {
	for _, tc := range []struct {
		duration time.Duration
		want     string
	}{
		{duration: 30 * time.Minute, want: "PT30M"},
		{duration: time.Hour, want: "PT1H"},
		{duration: 90 * time.Minute, want: "PT1H30M"},
		{duration: 24 * time.Hour, want: "PT24H"},
		{duration: time.Minute + 15*time.Second, want: "PT1M15S"},
		{duration: 0, want: ""},
		{duration: -time.Minute, want: ""},
	} {
		require.Equal(t, tc.want, iso8601Duration(tc.duration), "duration %s", tc.duration)
	}
}
