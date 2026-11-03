package icebergcatalog

// Idempotency-Key support for the mutating catalog operations (ICEBERG_CATALOG_PLAN.md §2.1
// "Idempotency-Key", §11 I-5).
//
// The Apache Iceberg REST protocol makes Idempotency-Key optional: a client may retry a write
// with the same key, and a server that advertises `idempotency-key-lifetime` in GET /v1/config
// promises to remember the final response of the first submission for at least that window. The
// spec (open-api/rest-catalog-open-api.yaml, components.parameters.idempotency-key) is explicit
// about what "final" means:
//
//	Finalize & replay: 200, 201, 204, and deterministic terminal 4xx (including 409 such as
//	AlreadyExists, NamespaceNotEmpty, etc.)
//	Do not finalize (not stored/replayed): 5xx
//
// This file implements exactly that promise and nothing wider:
//
//   - a response is stored only when the operation finalized, i.e. the status is below 500. A
//     409 is a final answer and is replayed as a 409 - it is never turned into a success;
//   - the record carries its own expiry, because kv.Store has no native TTL, and an expired
//     record is treated as absent so the operation runs again;
//   - the middleware is mounted only when a KV store and a positive lifetime are wired, and
//     GET /v1/config advertises `idempotency-key-lifetime` from the same object (http.go), so a
//     client is never told about storage that does not exist (§2.1, §11 I5-3 notes).
//
// The record is scoped by the authenticated principal and by the routed operation (method and
// chi route pattern), not only by the key. The spec requires a key to be unique per operation;
// scoping makes an accidental reuse across two operations or two users a separate record rather
// than a replay of somebody else's response, without changing the behaviour of a genuine retry.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	// IdempotencyKeyHeader is the request header of components.parameters.idempotency-key. Its
	// value is opaque to the server: the spec asks clients for a UUIDv7, but a server that
	// rejected anything else would break clients that use another opaque token.
	IdempotencyKeyHeader = "Idempotency-Key"

	// DefaultIdempotencyKeyLifetime is how long a stored response is replayed. The spec's own
	// example advertises PT30M (open-api/rest-catalog-open-api.yaml, CatalogConfig
	// idempotency-key-lifetime), and half an hour comfortably covers the retry loop of a client
	// whose first attempt timed out.
	DefaultIdempotencyKeyLifetime = 30 * time.Minute

	// idempotencyPartitionKey is the KV partition of the stored responses, following the plain
	// partition-name convention of the other stores in this repository (_datasets, actions).
	idempotencyPartitionKey = "iceberg_catalog_idempotency"
)

// IdempotencyStore keeps the final response of a mutating catalog operation in KV, keyed by the
// client's Idempotency-Key, and replays it until its lifetime ends.
//
// A nil *IdempotencyStore is not enabled and never wraps a handler (Enabled is nil-safe), which
// is how http.go expresses "the server has no KV store": GET /v1/config then omits
// idempotency-key-lifetime instead of advertising storage that does not exist.
type IdempotencyStore struct {
	// kv is the store the responses live in. Always non-nil on an enabled store.
	kv kv.Store

	// lifetime is how long each stored response stays replayable. It is also the value
	// GET /v1/config advertises, so the advertisement cannot outlive the storage.
	lifetime time.Duration

	// now is the clock, a field so tests can expire a key without sleeping. It defaults to
	// time.Now in NewIdempotencyStore.
	now func() time.Time
}

// NewIdempotencyStore builds the store over the server's KV. A nil kv returns nil: there is
// nowhere to keep a response, so the catalog must not claim idempotency at all. A non-positive
// lifetime falls back to DefaultIdempotencyKeyLifetime, so a caller cannot accidentally advertise
// a zero-width window.
func NewIdempotencyStore(store kv.Store, lifetime time.Duration) *IdempotencyStore {
	if store == nil {
		return nil
	}
	if lifetime <= 0 {
		lifetime = DefaultIdempotencyKeyLifetime
	}
	return &IdempotencyStore{kv: store, lifetime: lifetime, now: time.Now}
}

// Enabled reports whether this store can actually replay a response: a nil store cannot.
func (s *IdempotencyStore) Enabled() bool {
	return s != nil && s.kv != nil && s.lifetime > 0
}

// Lifetime is the configured replay window.
func (s *IdempotencyStore) Lifetime() time.Duration {
	if s == nil {
		return 0
	}
	return s.lifetime
}

// ISO8601Lifetime is the value of GET /v1/config `idempotency-key-lifetime`, or an empty string
// when the store is not enabled. The spec requires an ISO-8601 duration (CatalogConfig
// idempotency-key-lifetime, format: duration), e.g. PT30M for the default.
func (s *IdempotencyStore) ISO8601Lifetime() string {
	if !s.Enabled() {
		return ""
	}
	return iso8601Duration(s.lifetime)
}

// middleware wraps next so that a request carrying an Idempotency-Key either replays the stored
// final response or runs once and stores its own. A request without the header is passed through
// untouched, so the header is purely additive (§2.1, "Optional").
func (s *IdempotencyStore) middleware(next http.Handler) http.Handler {
	if !s.Enabled() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimSpace(r.Header.Get(IdempotencyKeyHeader))
		if key == "" {
			next.ServeHTTP(w, r)
			return
		}
		recordKey := s.recordKey(r, key)
		if record := s.load(r.Context(), recordKey); record != nil {
			replay(w, record)
			return
		}
		recorder := &idempotencyRecorder{ResponseWriter: w}
		next.ServeHTTP(recorder, r)
		s.save(r.Context(), recordKey, recorder)
	})
}

// recordKey derives the KV key of one (principal, operation, Idempotency-Key) triple. Hashing
// bounds the key length and keeps arbitrary client input out of the KV key space. The routed
// pattern - rather than the raw path - is what identifies the operation, so the same logical
// request is one record even if a client reaches it through a different prefix; the raw path is
// only a fallback for a handler that is not mounted through chi.
func (s *IdempotencyStore) recordKey(r *http.Request, key string) []byte {
	principal := ""
	if user, err := auth.GetUser(r.Context()); err == nil && user != nil {
		principal = user.Username
	}
	operation := r.URL.Path
	if routeContext := chi.RouteContext(r.Context()); routeContext != nil {
		if pattern := routeContext.RoutePattern(); pattern != "" {
			operation = pattern
		}
	}
	digest := sha256.Sum256([]byte(strings.Join([]string{principal, r.Method, operation, key}, "\x00")))
	return []byte(hex.EncodeToString(digest[:]))
}

// load returns the stored record of key, or nil when there is none or it has expired. An expired
// record is removed so the next store of the same key is not refused by the "absent only" write,
// and a KV failure is logged and treated as a miss: failing the write closed would turn a
// storage hiccup into an outage of every mutation, while the operation itself is still the
// client's idempotency fallback.
func (s *IdempotencyStore) load(ctx context.Context, key []byte) *idempotencyRecord {
	value, err := s.kv.Get(ctx, []byte(idempotencyPartitionKey), key)
	if err != nil {
		if !errors.Is(err, kv.ErrNotFound) {
			logging.FromContext(ctx).WithError(err).
				Warn("Iceberg idempotency: cannot read a stored response; executing the request")
		}
		return nil
	}
	var record idempotencyRecord
	if err := json.Unmarshal(value.Value, &record); err != nil {
		logging.FromContext(ctx).WithError(err).
			Warn("Iceberg idempotency: cannot decode a stored response; executing the request")
		return nil
	}
	if !record.expired(s.now()) {
		return &record
	}
	if err := s.kv.Delete(ctx, []byte(idempotencyPartitionKey), key); err != nil {
		logging.FromContext(ctx).WithError(err).
			Warn("Iceberg idempotency: cannot remove an expired response")
	}
	return nil
}

// save stores the response of a finalized operation. A 5xx is not final (the spec says it is not
// stored or replayed), and neither is a response that never reached a handler: the caller cannot
// have seen it, so a retry must not read it as an answer. The write is conditional on the key
// still being absent, so a concurrent retry that finished first keeps its record instead of being
// overwritten by a later, unrelated result.
func (s *IdempotencyStore) save(ctx context.Context, key []byte, recorder *idempotencyRecorder) {
	status := recorder.statusCode()
	if status >= http.StatusInternalServerError {
		return
	}
	record := idempotencyRecord{
		Status:    status,
		Header:    recorder.Header().Clone(),
		Body:      recorder.body.Bytes(),
		ExpiresAt: s.now().Add(s.lifetime),
	}
	data, err := json.Marshal(record)
	if err != nil {
		logging.FromContext(ctx).WithError(err).
			Warn("Iceberg idempotency: cannot encode the response to store")
		return
	}
	err = s.kv.SetIf(ctx, []byte(idempotencyPartitionKey), key, data, nil)
	if err != nil && !errors.Is(err, kv.ErrPredicateFailed) {
		logging.FromContext(ctx).WithError(err).
			Warn("Iceberg idempotency: cannot store the response")
	}
}

// idempotencyRecord is one stored final response. Body is a byte slice so json.Marshal emits it
// as base64; Header is the response header map, kept so the replay carries the content type and
// any other header the handler set.
type idempotencyRecord struct {
	Status    int         `json:"status"`
	Header    http.Header `json:"header,omitempty"`
	Body      []byte      `json:"body,omitempty"`
	ExpiresAt time.Time   `json:"expires_at"`
}

// expired reports whether the record's window has ended. A zero ExpiresAt is expired: a record
// without an expiry can never honour an advertised lifetime, so it must not be replayed.
func (r idempotencyRecord) expired(now time.Time) bool {
	return !r.ExpiresAt.After(now)
}

// replay writes a stored response exactly as it was produced: the same status, headers and body.
// It is the reason a stored 409 stays a 409 (§11 I5-3 notes: never turn a conflict into success).
func replay(w http.ResponseWriter, record *idempotencyRecord) {
	header := w.Header()
	for name, values := range record.Header {
		header[name] = values
	}
	w.WriteHeader(record.Status)
	if len(record.Body) > 0 {
		_, _ = w.Write(record.Body)
	}
}

// idempotencyRecorder is a pass-through ResponseWriter that keeps the status and body of one
// response so they can be stored after the handler returns. Header writes go straight to the
// wrapped writer, so the client sees the response while it is being captured.
type idempotencyRecorder struct {
	http.ResponseWriter

	status  int
	written bool
	body    bytes.Buffer
}

// WriteHeader records the status once - the first call is the one that takes effect - and forwards
// it.
func (w *idempotencyRecorder) WriteHeader(status int) {
	if !w.written {
		w.status = status
		w.written = true
	}
	w.ResponseWriter.WriteHeader(status)
}

// Write records the body and forwards it. A handler that writes without calling WriteHeader is
// answered 200 by net/http; the recorder does the same so the stored status matches the wire.
func (w *idempotencyRecorder) Write(p []byte) (int, error) {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	w.body.Write(p)
	return w.ResponseWriter.Write(p)
}

// statusCode is the status the client received; 200 when the handler wrote nothing at all.
func (w *idempotencyRecorder) statusCode() int {
	if !w.written {
		return http.StatusOK
	}
	return w.status
}

// Unwrap exposes the underlying writer to http.ResponseController, so an outer handler can still
// reach optional interfaces (flushing, hijacking) of the real ResponseWriter.
func (w *idempotencyRecorder) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// iso8601Duration renders d as the ISO-8601 duration the CatalogConfig field requires, e.g.
// PT30M, PT1H30M or PT24H. The spec's example spells a day as PT24H as well, so hours are not
// folded into days; sub-second precision is dropped, because a replay window is not a stopwatch.
func iso8601Duration(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	total := int64(d.Round(time.Second) / time.Second)
	hours := total / int64(time.Hour/time.Second)
	minutes := (total % int64(time.Hour/time.Second)) / int64(time.Minute/time.Second)
	seconds := total % int64(time.Minute/time.Second)

	var b strings.Builder
	b.WriteString("PT")
	if hours > 0 {
		fmt.Fprintf(&b, "%dH", hours)
	}
	if minutes > 0 {
		fmt.Fprintf(&b, "%dM", minutes)
	}
	if seconds > 0 || (hours == 0 && minutes == 0) {
		fmt.Fprintf(&b, "%dS", seconds)
	}
	return b.String()
}
