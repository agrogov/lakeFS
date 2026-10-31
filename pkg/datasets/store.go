package datasets

// This file is the KV persistence layer of the datasets control plane
// (DATASETS_DESIGN.md §5.3 record layout, §5.4 version semantics and concurrency,
// §8.2 pin refcounts). It holds the record types, the key layout and the
// conditional-write helpers, and it talks to kv.Store only: no catalog, no
// blockstore, no HTTP.
//
// Clean-room note (§2): written from DATASETS_DESIGN.md and the public lakeFS
// documentation only. No code, SQL, protobuf or UI assets from lakeFS Enterprise,
// and nothing from upstream lakeFS >= 1.87, were used. Values are JSON, never
// protobuf, so nothing here involves make gen-proto.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/kv"
)

// KV layout (§5.3).
const (
	// PartitionKey is the dedicated KV partition that holds every dataset
	// control-plane record. It is a single partition of its own, never the auth
	// partition ("auth", pkg/auth/model) and never a graveler repository partition,
	// and it can never be mistaken for the control-plane path of a dataset because
	// the dataset name pattern (§5.2 rule 1) admits neither "_" nor an upper-case
	// letter. Keys inside it are additionally namespaced by the record-type prefixes
	// below, so a backend that ignored partitions still could not collide.
	PartitionKey = "_datasets"

	datasetKeyPrefix     = "datasets"
	versionKeyPrefix     = "versions"
	proposalKeyPrefix    = "proposals"
	metadataKeyKeyPrefix = "metadata_keys"
	pinKeyPrefix         = "pins"
	grantKeyPrefix       = "grants"
	taskKeyPrefix        = "tasks"

	// versionKeyWidth is the zero padding of the version number in
	// versions/<name>/<n:010d>, chosen so that lexicographic key order equals
	// numeric version order (§5.3). Ten digits covers every int32 version.
	versionKeyWidth = 10
)

// MaxInlineDefinitionBytes is the §5.3 large-definition threshold: a VersionRecord
// whose JSON grows past it stores DefinitionRef - the dataset repository commit and
// the path of the definition object inside it - instead of the inline definition,
// because a KV item is limited to 400 KB on DynamoDB (§5.3, §14).
const MaxInlineDefinitionBytes = 256 * 1024

// DefinitionPath is the path of the definition object that materialisation writes
// into every version commit of the dataset repository (§6.4). It is the Path of a
// DefinitionRef; it lives under the reserved target prefix, which user targets may
// not use.
const DefinitionPath = ReservedTargetPrefix + "/definition.json"

// DatasetRecord.State values (§5.3, plus "degraded" from §10.2). The state machine
// is the CAS target of §5.4.
const (
	DatasetStateCreating   = "creating"
	DatasetStateActive     = "active"
	DatasetStatePublishing = "publishing"
	DatasetStateDeleting   = "deleting"
	DatasetStateDegraded   = "degraded"
)

// ProposalRecord.Status values (§5.6).
const (
	ProposalStatusOpen   = "open"
	ProposalStatusClosed = "closed"
	ProposalStatusMerged = "merged"
)

// Grant principal types (§9.2, §10.1).
const (
	PrincipalTypeUser  = "user"
	PrincipalTypeGroup = "group"
)

// TaskRecord.Status values (§6.5, §10.1 Task).
const (
	TaskStatusPending   = "pending"
	TaskStatusRunning   = "running"
	TaskStatusCompleted = "completed"
	TaskStatusFailed    = "failed"
)

// TaskKindPublish is the Kind of a TaskRecord that runs a publish asynchronously into
// the goroutine of §6.5.
const TaskKindPublish = "publish"

// pinCASRetries bounds the read-modify-write attempts of the pin reference helpers.
// Losing a pin reference would let GC collect data a dataset still serves (§8.2),
// so a concurrent update is retried rather than dropped.
const pinCASRetries = 16

var (
	// ErrInvalidRecordKey reports a record whose key cannot be built: a dataset name
	// that fails §5.2 rule 1, an empty or slash-carrying key segment, or a version
	// number below 1.
	ErrInvalidRecordKey = errors.New("invalid dataset record key")

	// ErrRecordMismatch reports a stored record whose identity fields do not match
	// the key it was read from, i.e. corrupted or mis-written KV state.
	ErrRecordMismatch = errors.New("dataset record does not match its key")

	// ErrDefinitionRefUnavailable reports a VersionRecord that is too large to store
	// inline but carries no dataset repository commit to point DefinitionRef at.
	ErrDefinitionRefUnavailable = errors.New("definition_ref requires the dataset repository commit")

	// ErrNilRecord reports a nil record passed to a Store write method.
	ErrNilRecord = errors.New("nil datasets record")
)

// DatasetRecord is the head record of a dataset, stored at datasets/<name> (§5.3).
// It is the single CAS target of the publish state machine (§5.4): every write to
// it goes through Store.PutDatasetIf or Store.DeleteDataset, never through a plain
// Set.
type DatasetRecord struct {
	Name           string    `json:"name"`
	Description    string    `json:"description"`
	CreatedBy      string    `json:"created_by"`
	CreatedAt      time.Time `json:"created_at"`
	LatestVersion  int       `json:"latest_version"` // 0 = no version published yet
	State          string    `json:"state"`          // creating | active | publishing | deleting | degraded
	PendingVersion int       `json:"pending_version,omitempty"`
	StorageID      string    `json:"storage_id"`
	StorageNS      string    `json:"storage_namespace"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// ItemStat is the footprint of one data item in a published version (§10.2
// DatasetStats.items).
type ItemStat struct {
	Target  string `json:"target"`
	Objects int64  `json:"objects"`
	Bytes   int64  `json:"bytes"`
}

// ItemStats is the footprint of a whole version (§5.3 VersionRecord.Stats, §10.2
// DatasetStats): totals plus the per-item counts the UI shows.
type ItemStats struct {
	Objects int64      `json:"objects"`
	Bytes   int64      `json:"bytes"`
	Items   []ItemStat `json:"items,omitempty"`
}

// DefinitionRef locates the definition object of a version inside the dataset
// repository (§5.3 large definitions). The service reads it back with the catalog
// when a VersionRecord carries no inline definition.
type DefinitionRef struct {
	CommitID string `json:"commit_id"` // commit in the dataset repository
	Path     string `json:"path"`      // DefinitionPath
}

// VersionRecord is one immutable published version, stored at
// versions/<name>/<n:010d> (§5.3).
//
// Definition is inline unless the encoded record would pass
// MaxInlineDefinitionBytes; in that case DefinitionRef is set instead and
// Definition is the zero value, which is why it is tagged omitzero (§5.3 large
// definitions).
type VersionRecord struct {
	Dataset       string         `json:"dataset"`
	Version       int            `json:"version"`
	Definition    Definition     `json:"definition,omitzero"`
	DefinitionRef *DefinitionRef `json:"definition_ref,omitempty"`
	Message       string         `json:"message"`
	Publisher     string         `json:"publisher"`
	PublishedAt   time.Time      `json:"published_at"`
	CommitID      string         `json:"commit_id"` // commit in the dataset repository
	MetaRangeID   string         `json:"metarange_id"`
	ProposalID    string         `json:"proposal_id,omitempty"`
	Stats         ItemStats      `json:"stats"`
}

// HasInlineDefinition reports whether the record carries its definition itself. It
// is false for a record that points at the version's definition object through
// DefinitionRef: the service then loads the definition from the dataset repository
// with the catalog (§5.3).
func (r VersionRecord) HasInlineDefinition() bool {
	return r.DefinitionRef == nil
}

// ProposalRecord is a dataset pull request, stored at proposals/<name>/<id> (§5.6).
//
// Proposals are not graveler pull requests, and must not be turned into them: merging one
// publishes the proposal's definition as a version, and a dataset version is a single-parent
// commit on `latest` (§6.2 step 9), which a graveler merge commit would break. A graveler pull
// request would also have to merge through the caller's own write permission, and nobody -
// least of all a reviewer - holds one on a read-only, immutability-guarded dataset repository
// (§9.4). The rationale is spelled out in proposals.go.
type ProposalRecord struct {
	ID            string     `json:"id"`           // ULID
	Dataset       string     `json:"dataset"`      //
	BaseVersion   int        `json:"base_version"` // version the proposal was made against
	Definition    Definition `json:"definition"`   // resolved
	Title         string     `json:"title"`
	Description   string     `json:"description"`
	Author        string     `json:"author"`
	Status        string     `json:"status"` // open | closed | merged
	CreatedAt     time.Time  `json:"created_at"`
	ClosedAt      *time.Time `json:"closed_at,omitempty"`
	MergedVersion int        `json:"merged_version,omitempty"`
	PreviewBranch string     `json:"preview_branch"` // "_proposal-<id>" in the dataset repository
}

// GrantRecord is the KV mirror of a dataset access grant, stored at
// grants/<name>/<principal-type>/<principal> (§5.3). The authoritative grant is the
// managed policy of §9.2; the record exists so that GET /datasets/{ds}/access can
// list grants without reading the auth store.
type GrantRecord struct {
	Dataset       string    `json:"dataset"`
	PrincipalType string    `json:"principal_type"` // user | group
	PrincipalID   string    `json:"principal_id"`
	PolicyName    string    `json:"policy_name,omitempty"` // managed read policy of §9.2
	GrantedBy     string    `json:"granted_by"`
	GrantedAt     time.Time `json:"granted_at"`
}

// TaskRecord is the control-plane record of one asynchronous operation (§6.5, §10.1
// GET /datasets/{ds}/tasks/{task}), stored at tasks/<name>/<id>.
//
// The record is the observable side of the goroutine that runs a publish above
// datasets.sync_publish_max_objects: the API answers 202 with it, the client polls it,
// and it is the only trace a node restart leaves behind besides the dataset record
// itself, which stays in state "publishing" for the reconciler to resume or roll back
// (§12.3 item 1). Unlike a DatasetRecord or a VersionRecord it is mutable (pending ->
// running -> completed|failed), so it is written with a plain Set.
type TaskRecord struct {
	ID      string `json:"id"`      // ULID
	Dataset string `json:"dataset"` //
	Kind    string `json:"kind"`    // publish
	Status  string `json:"status"`  // pending | running | completed | failed
	Version int    `json:"version,omitempty"`
	// Error is the sanitised failure of a failed task (SEC-8). It is never the raw error:
	// this record is readable by every holder of dataset:ReadDataset, and a raw error carries
	// block and physical addresses. The full error goes to the server log.
	Error     *TaskError `json:"error,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// TaskError codes (§10.1 Task, SEC-8). They are stable identifiers a client may switch on; the
// message is a fixed, server-authored sentence, not text derived from an error.
const (
	// TaskErrorCodePublishFailed is a publish that failed for an unspecified reason.
	TaskErrorCodePublishFailed = "publish_failed"
	// TaskErrorCodePermissionDenied is a publish the caller was not allowed to complete.
	TaskErrorCodePermissionDenied = "permission_denied"
	// TaskErrorCodeSourceUnavailable is a publish whose sources are no longer readable.
	TaskErrorCodeSourceUnavailable = "source_unavailable"
)

// TaskError is the sanitised failure of an asynchronous task (SEC-8): a code and a fixed
// message. The raw error is logged server-side, never stored on the record.
type TaskError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Error implements error, so a TaskError can be logged or compared like one.
func (e *TaskError) Error() string {
	if e == nil {
		return ""
	}
	return e.Code + ": " + e.Message
}

// PinRecord is the GC pin of one (source repository, commit) pair, stored at
// pins/<repo>/<commit> (§5.3, §8.2). References holds the set of "<dataset>@v<n>"
// strings that keep the pin alive; when the set becomes empty the record and the
// hidden pin branch are removed.
type PinRecord struct {
	Repository string   `json:"repository"`
	CommitID   string   `json:"commit_id"`
	References []string `json:"references"` // sorted, unique
}

// PinReference renders the "<dataset>@v<n>" memo stored in a PinRecord (§5.3).
func PinReference(dataset string, version int) string {
	return fmt.Sprintf("%s@v%d", dataset, version)
}

// Store is the KV persistence layer of the datasets control plane (§5.3). It is a
// thin, typed layer over kv.Store and owns the key layout, so the service never
// formats a KV key itself.
//
// Error contract, which the HTTP layer relies on (§10.4):
//   - reads return kv.ErrNotFound unchanged, for 404;
//   - conditional writes return kv.ErrPredicateFailed unchanged, for 409.
//
// The zero Store is unusable: build one with NewStore.
type Store struct {
	store kv.Store

	// now is the clock used for records owned by the store (the reconcile lease);
	// production uses the wall clock and tests replace it through the service.
	now func() time.Time

	// putProposalFn and deleteVersionFn are test hooks, nil in production: they let a
	// test inject the one write that fails in the window a repair covers - the proposal
	// write that follows a merge (BUG-8) and a step of finishDelete (BUG-7) - without a
	// second KV implementation. Each falls through to the real write when nil.
	putProposalFn   func(ctx context.Context, rec, previous *ProposalRecord) error
	deleteVersionFn func(ctx context.Context, dataset string, version int) error
}

// NewStore returns a Store backed by kvs. The Store does not own kvs and never
// closes it.
func NewStore(kvs kv.Store) *Store {
	return &Store{store: kvs, now: time.Now}
}

// clock reports the store's clock, falling back to the wall clock when the store was
// built without one.
func (s *Store) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// ----------------------------------------------------------------------------
// Key layout (§5.3)
//
// The builders validate their input, because a bad segment would otherwise turn
// into a key that silently lists under another dataset. They are unexported: the
// key layout is an implementation detail of this file, and nothing outside it
// should reason about KV keys.
// ----------------------------------------------------------------------------

// datasetKey returns datasets/<name>.
func datasetKey(name string) ([]byte, error) {
	if err := checkDatasetName(name); err != nil {
		return nil, err
	}
	return []byte(kv.FormatPath(datasetKeyPrefix, name)), nil
}

// versionKey returns versions/<name>/<n:010d>, zero padded so that lexicographic
// key order equals numeric version order (§5.3).
func versionKey(dataset string, version int) ([]byte, error) {
	if err := checkDatasetName(dataset); err != nil {
		return nil, err
	}
	if version < 1 {
		return nil, fmt.Errorf("%w: version must be >= 1, got %d", ErrInvalidRecordKey, version)
	}
	return []byte(kv.FormatPath(versionKeyPrefix, dataset, fmt.Sprintf("%0*d", versionKeyWidth, version))), nil
}

// versionsPrefix returns the key prefix of every version of dataset.
func versionsPrefix(dataset string) ([]byte, error) {
	if err := checkDatasetName(dataset); err != nil {
		return nil, err
	}
	return []byte(kv.FormatPath(versionKeyPrefix, dataset, "")), nil
}

// proposalKey returns proposals/<name>/<id>.
func proposalKey(dataset, id string) ([]byte, error) {
	if err := checkDatasetName(dataset); err != nil {
		return nil, err
	}
	if err := checkSegment("proposal id", id); err != nil {
		return nil, err
	}
	return []byte(kv.FormatPath(proposalKeyPrefix, dataset, id)), nil
}

// proposalsPrefix returns the key prefix of every proposal of dataset.
func proposalsPrefix(dataset string) ([]byte, error) {
	if err := checkDatasetName(dataset); err != nil {
		return nil, err
	}
	return []byte(kv.FormatPath(proposalKeyPrefix, dataset, "")), nil
}

// metadataKeyKey returns metadata_keys/<key>. Metadata keys are global (they apply
// to every dataset, §5.5), so they are not namespaced by dataset name.
func metadataKeyKey(name string) ([]byte, error) {
	if !IsValidMetadataKeyName(name) {
		return nil, fmt.Errorf("%w: metadata key name %q must match %s",
			ErrInvalidRecordKey, name, metadataKeyNameRe.String())
	}
	return []byte(kv.FormatPath(metadataKeyKeyPrefix, name)), nil
}

// metadataKeysPrefix returns the key prefix of every metadata key.
func metadataKeysPrefix() []byte {
	return []byte(metadataKeyKeyPrefix + kv.PathDelimiter)
}

// pinKey returns pins/<repo>/<commit>.
func pinKey(repository, commitID string) ([]byte, error) {
	if err := checkSegment("repository", repository); err != nil {
		return nil, err
	}
	if err := checkSegment("commit id", commitID); err != nil {
		return nil, err
	}
	return []byte(kv.FormatPath(pinKeyPrefix, repository, commitID)), nil
}

// pinsPrefix returns the key prefix of every pin of repository.
func pinsPrefix(repository string) ([]byte, error) {
	if err := checkSegment("repository", repository); err != nil {
		return nil, err
	}
	return []byte(kv.FormatPath(pinKeyPrefix, repository, "")), nil
}

// grantKey returns grants/<name>/<principal-type>/<principal>.
func grantKey(dataset, principalType, principalID string) ([]byte, error) {
	if err := checkDatasetName(dataset); err != nil {
		return nil, err
	}
	if err := checkPrincipalType(principalType); err != nil {
		return nil, err
	}
	if err := checkSegment("principal id", principalID); err != nil {
		return nil, err
	}
	return []byte(kv.FormatPath(grantKeyPrefix, dataset, principalType, principalID)), nil
}

// grantsPrefix returns the key prefix of every grant of dataset.
func grantsPrefix(dataset string) ([]byte, error) {
	if err := checkDatasetName(dataset); err != nil {
		return nil, err
	}
	return []byte(kv.FormatPath(grantKeyPrefix, dataset, "")), nil
}

// taskKey returns tasks/<name>/<id>.
func taskKey(dataset, id string) ([]byte, error) {
	if err := checkDatasetName(dataset); err != nil {
		return nil, err
	}
	if err := checkSegment("task id", id); err != nil {
		return nil, err
	}
	return []byte(kv.FormatPath(taskKeyPrefix, dataset, id)), nil
}

// tasksPrefix returns the key prefix of every task of dataset.
func tasksPrefix(dataset string) ([]byte, error) {
	if err := checkDatasetName(dataset); err != nil {
		return nil, err
	}
	return []byte(kv.FormatPath(taskKeyPrefix, dataset, "")), nil
}

// checkDatasetName rejects every name that §5.2 rule 1 would reject, including the
// reserved control-plane names. The returned error is both a key error and a
// ValidationError, so a caller can treat it as a bad request (§10.4).
func checkDatasetName(name string) error {
	problems := ValidateName(name)
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrInvalidRecordKey, &ValidationError{Problems: problems})
}

// checkSegment rejects an empty key segment and one that would add path separators
// to a KV key.
func checkSegment(field, value string) error {
	if value == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidRecordKey, field)
	}
	if strings.Contains(value, kv.PathDelimiter) {
		return fmt.Errorf("%w: %s %q must not contain %q", ErrInvalidRecordKey, field, value, kv.PathDelimiter)
	}
	return nil
}

// checkPrincipalType rejects a grant principal type other than user or group (§9.2).
func checkPrincipalType(principalType string) error {
	switch principalType {
	case PrincipalTypeUser, PrincipalTypeGroup:
		return nil
	default:
		return fmt.Errorf("%w: principal type %q must be %q or %q",
			ErrInvalidRecordKey, principalType, PrincipalTypeUser, PrincipalTypeGroup)
	}
}

// ----------------------------------------------------------------------------
// Low-level helpers
// ----------------------------------------------------------------------------

// put stores value at key unconditionally.
func (s *Store) put(ctx context.Context, key, value []byte) error {
	return s.store.Set(ctx, []byte(PartitionKey), key, value)
}

// get reads key, returning kv.ErrNotFound unchanged.
func (s *Store) get(ctx context.Context, key []byte) ([]byte, error) {
	res, err := s.store.Get(ctx, []byte(PartitionKey), key)
	if err != nil {
		return nil, err
	}
	return res.Value, nil
}

// delete removes key; deleting a missing key is not an error.
func (s *Store) delete(ctx context.Context, key []byte) error {
	return s.store.Delete(ctx, []byte(PartitionKey), key)
}

// decode unmarshals a stored JSON record, naming the record kind and key in the
// error so a corrupted value is diagnosable.
func decodeRecord[T any](kind string, key, value []byte) (*T, error) {
	var rec T
	if err := json.Unmarshal(value, &rec); err != nil {
		return nil, fmt.Errorf("datasets: decode %s at %q: %w", kind, key, err)
	}
	return &rec, nil
}

// scanPrefix calls fn for every entry of PartitionKey whose key starts with prefix,
// in key order, and stops at the first key outside the prefix.
func (s *Store) scanPrefix(ctx context.Context, prefix []byte, fn func(key, value []byte) error) error {
	it, err := s.store.Scan(ctx, []byte(PartitionKey), kv.ScanOptions{KeyStart: prefix})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Next() {
		entry := it.Entry()
		if entry == nil {
			break
		}
		if !bytes.HasPrefix(entry.Key, prefix) {
			break
		}
		if err := fn(entry.Key, entry.Value); err != nil {
			return err
		}
	}
	return it.Err()
}

// ----------------------------------------------------------------------------
// Dataset records (§5.3, §5.4)
// ----------------------------------------------------------------------------

// GetDataset reads the record of name, returning kv.ErrNotFound unchanged when the
// dataset does not exist.
//
// The predicate is the one kv.Store.Get returned with the record. Pass it to
// PutDatasetIf or DeleteDataset to write conditioned on the record as read. It must
// never be rebuilt from the record: some stores compare it opaquely (CosmosDB uses an
// ETag, pkg/kv/cosmosdb/store.go).
func (s *Store) GetDataset(ctx context.Context, name string) (*DatasetRecord, kv.Predicate, error) {
	key, err := datasetKey(name)
	if err != nil {
		return nil, nil, err
	}
	res, err := s.store.Get(ctx, []byte(PartitionKey), key)
	if err != nil {
		return nil, nil, err
	}
	rec, err := decodeRecord[DatasetRecord]("dataset record", key, res.Value)
	if err != nil {
		return nil, nil, err
	}
	if rec.Name != name {
		return nil, nil, fmt.Errorf("%w: dataset key %q holds record of %q", ErrRecordMismatch, key, rec.Name)
	}
	return rec, res.Predicate, nil
}

// ListDatasets returns every dataset record in name order. It is the read behind GET
// /datasets (§10.1), which filters the result in memory: the record set is small
// (datasets are curated, not created per object) and the filters combine a name
// prefix, a free-text search and metadata of the latest version, which a key range
// cannot express.
func (s *Store) ListDatasets(ctx context.Context) ([]*DatasetRecord, error) {
	prefix := []byte(datasetKeyPrefix + kv.PathDelimiter)
	records := make([]*DatasetRecord, 0)
	err := s.scanPrefix(ctx, prefix, func(key, value []byte) error {
		// The reconcile lease (§12.3) lives at datasets/_lease, under the same prefix as the
		// dataset records. It is a lease, not a dataset, so it is skipped by name rather than
		// decoded (its value would look like a record with an empty name and state).
		if bytes.Equal(key, []byte(ReconcileLeaseKey)) {
			return nil
		}
		rec, err := decodeRecord[DatasetRecord]("dataset record", key, value)
		if err != nil {
			return err
		}
		records = append(records, rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.SliceStable(records, func(i, j int) bool { return records[i].Name < records[j].Name })
	return records, nil
}

// PutDatasetIf writes rec only if the stored record still matches the caller's view of
// it, which is the §5.4 step 3 CAS of the publish state machine: read the record,
// derive the next state from it, write it back conditioned on the record that was read.
//
// previous is the record as this caller last read (GetDataset) or last wrote (the value
// passed to the previous PutDatasetIf), and predicate is the kv.Predicate returned with
// it. A nil previous means "the record must not exist yet", which is how CreateDataset
// is expressed. A nil predicate falls back to a read-compare-write (setIfUnchanged) for
// a caller that holds a record it wrote rather than one it read.
//
// kv.ErrPredicateFailed is returned unchanged: the caller lost the race and the HTTP
// layer maps it to 409 Conflict (§5.4, §14 "two concurrent publishes"). The stored
// record is left untouched.
func (s *Store) PutDatasetIf(ctx context.Context, rec, previous *DatasetRecord, predicate kv.Predicate) error {
	if rec == nil {
		return fmt.Errorf("datasets: %w", ErrNilRecord)
	}
	key, err := datasetKey(rec.Name)
	if err != nil {
		return err
	}
	value, err := datasetValue(rec)
	if err != nil {
		return err
	}
	if previous == nil {
		return s.store.SetIf(ctx, []byte(PartitionKey), key, value, nil)
	}
	if previous.Name != rec.Name {
		return fmt.Errorf("%w: previous record is dataset %q, new record is %q",
			ErrRecordMismatch, previous.Name, rec.Name)
	}
	if predicate != nil {
		// The predicate is the one kv.Store.Get returned for the record this caller read,
		// passed through unchanged: a store may compare it opaquely (CosmosDB uses an
		// ETag, pkg/kv/cosmosdb/store.go), so rebuilding it from the record never matches.
		return s.store.SetIf(ctx, []byte(PartitionKey), key, value, predicate)
	}
	expected, err := datasetValue(previous)
	if err != nil {
		return err
	}
	return s.setIfUnchanged(ctx, key, value, expected, canonicalValue[DatasetRecord])
}

// setIfUnchanged writes value at key only if the stored record is still the one
// expected (compared in canonical encoding, see canonicalValue).
//
// The SetIf predicate is the one kv.Store.Get returned for the value just compared,
// never one rebuilt from a record: a store may use an opaque predicate (CosmosDB uses
// an ETag, pkg/kv/cosmosdb/store.go), and the others compare bytes. Because the
// predicate guards exactly the value that was compared, the read-compare-write is
// atomic. A record that vanished in between is a lost race, like any other change.
func (s *Store) setIfUnchanged(ctx context.Context, key, value, expected []byte, canon func([]byte) ([]byte, error)) error {
	res, err := s.store.Get(ctx, []byte(PartitionKey), key)
	if errors.Is(err, kv.ErrNotFound) {
		return kv.ErrPredicateFailed
	}
	if err != nil {
		return err
	}
	current, err := canon(res.Value)
	if err != nil {
		return err
	}
	// Canonicalize the expected side the same way, so the comparison does not depend
	// on how the caller encoded previous.
	want, err := canon(expected)
	if err != nil {
		return err
	}
	if !bytes.Equal(current, want) {
		return kv.ErrPredicateFailed
	}
	return s.store.SetIf(ctx, []byte(PartitionKey), key, value, res.Predicate)
}

// canonicalValue decodes a stored record and encodes it again, so that two encodings
// of the same record compare equal even when the record has map fields.
func canonicalValue[T any](raw []byte) ([]byte, error) {
	var rec T
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, fmt.Errorf("datasets: decode record for compare: %w", err)
	}
	out, err := json.Marshal(&rec)
	if err != nil {
		return nil, fmt.Errorf("datasets: encode record for compare: %w", err)
	}
	return out, nil
}

// CreateDataset writes a new dataset record. It fails with kv.ErrPredicateFailed if a
// record with that name already exists, so two concurrent creates cannot both win.
func (s *Store) CreateDataset(ctx context.Context, rec *DatasetRecord) error {
	return s.PutDatasetIf(ctx, rec, nil, nil)
}

// DeleteDataset removes the record of name.
//
// previous bounds the delete the way PutDatasetIf bounds a write: pass the record as
// last read with the kv.Predicate returned by GetDataset, or a nil previous to delete
// unconditionally. kv.Store has no conditional delete, so the predicate is validated
// with a conditional write of the current value followed by the delete; on conflict the
// record is left untouched and kv.ErrPredicateFailed is returned. A nil predicate
// compares re-encoded values instead. A record that is already gone is treated as
// deleted. A delete that loses the check/delete race is finished by the reconciler
// (§6.6, §12.3).
func (s *Store) DeleteDataset(ctx context.Context, name string, previous *DatasetRecord, predicate kv.Predicate) error {
	key, err := datasetKey(name)
	if err != nil {
		return err
	}
	if previous != nil && previous.Name != name {
		return fmt.Errorf("%w: previous record is dataset %q, delete is for %q",
			ErrRecordMismatch, previous.Name, name)
	}
	if previous == nil && predicate == nil {
		return s.delete(ctx, key)
	}
	if predicate != nil {
		return s.deleteIf(ctx, key, predicate)
	}
	res, err := s.store.Get(ctx, []byte(PartitionKey), key)
	if errors.Is(err, kv.ErrNotFound) {
		return kv.ErrPredicateFailed
	}
	if err != nil {
		return err
	}
	want, err := datasetValue(previous)
	if err != nil {
		return err
	}
	current, err := canonicalValue[DatasetRecord](res.Value)
	if err != nil {
		return err
	}
	if want, err = canonicalValue[DatasetRecord](want); err != nil {
		return err
	}
	if !bytes.Equal(current, want) {
		return kv.ErrPredicateFailed
	}
	// Delete under the predicate that guarded the value just compared, so a writer that
	// lands between the read and the delete keeps its record.
	return s.deleteIf(ctx, key, res.Predicate)
}

// deleteIf removes key only if its stored value still matches predicate, the
// kv.Predicate that kv.Store.Get returned for it. kv.Store has no conditional delete,
// so the predicate is checked by writing the current value back unchanged: SetIf fails
// with kv.ErrPredicateFailed when the predicate is stale, and is a no-op otherwise. A
// missing key is already deleted and is not an error. A writer that wins between that
// check and the delete leaves the record for the reconciler to finish (§6.6, §12.3).
func (s *Store) deleteIf(ctx context.Context, key []byte, predicate kv.Predicate) error {
	res, err := s.store.Get(ctx, []byte(PartitionKey), key)
	if errors.Is(err, kv.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := s.store.SetIf(ctx, []byte(PartitionKey), key, res.Value, predicate); err != nil {
		return err
	}
	return s.delete(ctx, key)
}

// deleteIfUnchanged removes key only if its stored value is still expected, compared in
// canonical encoding (see canonicalValue). It is the delete form of setIfUnchanged: the
// caller has just written expected and must not delete a value that a concurrent writer
// replaced in between. A missing key is already deleted and is not an error; a changed
// value is reported as kv.ErrPredicateFailed and the record is left alone.
func (s *Store) deleteIfUnchanged(ctx context.Context, key, expected []byte, canon func([]byte) ([]byte, error)) error {
	res, err := s.store.Get(ctx, []byte(PartitionKey), key)
	if errors.Is(err, kv.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	current, err := canon(res.Value)
	if err != nil {
		return err
	}
	want, err := canon(expected)
	if err != nil {
		return err
	}
	if !bytes.Equal(current, want) {
		return kv.ErrPredicateFailed
	}
	// Delete under the predicate that guarded the value just compared, so a writer that
	// lands between the read and the delete keeps its record.
	return s.deleteIf(ctx, key, res.Predicate)
}

// datasetValue encodes a dataset record as stored. It is what PutDatasetIf and
// DeleteDataset compare a nil predicate against; it is never itself used as a
// kv.Predicate.
func datasetValue(rec *DatasetRecord) ([]byte, error) {
	value, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("datasets: encode dataset record %q: %w", rec.Name, err)
	}
	return value, nil
}

// ----------------------------------------------------------------------------
// Version records (§5.3, §5.3 large definitions)
// ----------------------------------------------------------------------------

// PutVersion creates the version record of a published version and returns the
// record as stored. Version records are immutable after write (§5.3), so a second
// write of the same dataset/version fails with kv.ErrPredicateFailed instead of
// overwriting history; the reconciler writes only missing records (§12.3).
//
// Large definitions (§5.3): the encoded record is measured with the definition
// inline. If it exceeds MaxInlineDefinitionBytes, the stored record keeps
// DefinitionRef - derived from rec.CommitID and DefinitionPath unless rec already
// carries one - and drops the inline definition. The returned record is that stored
// form, so a caller never has to guess which of the two it wrote.
func (s *Store) PutVersion(ctx context.Context, rec VersionRecord) (*VersionRecord, error) {
	key, err := versionKey(rec.Dataset, rec.Version)
	if err != nil {
		return nil, err
	}
	stored, value, err := encodeVersionRecord(rec)
	if err != nil {
		return nil, err
	}
	if err := s.store.SetIf(ctx, []byte(PartitionKey), key, value, nil); err != nil {
		return nil, err
	}
	return stored, nil
}

// encodeVersionRecord applies the §5.3 large-definition switch and returns the
// record as stored together with its encoding.
func encodeVersionRecord(rec VersionRecord) (*VersionRecord, []byte, error) {
	// An explicitly supplied DefinitionRef wins: the caller already wrote the
	// definition object, so keeping a copy inline would only duplicate it.
	if rec.DefinitionRef != nil {
		rec.Definition = Definition{}
	}
	value, err := json.Marshal(rec)
	if err != nil {
		return nil, nil, fmt.Errorf("datasets: encode version record %s@v%d: %w", rec.Dataset, rec.Version, err)
	}
	if len(value) <= MaxInlineDefinitionBytes || !hasInlineDefinition(rec) {
		return &rec, value, nil
	}
	if rec.CommitID == "" {
		return nil, nil, fmt.Errorf("%w: dataset %q version %d is %d bytes",
			ErrDefinitionRefUnavailable, rec.Dataset, rec.Version, len(value))
	}
	stored := rec
	stored.Definition = Definition{}
	stored.DefinitionRef = &DefinitionRef{CommitID: rec.CommitID, Path: DefinitionPath}
	value, err = json.Marshal(stored)
	if err != nil {
		return nil, nil, fmt.Errorf("datasets: encode version record %s@v%d: %w", rec.Dataset, rec.Version, err)
	}
	return &stored, value, nil
}

// hasInlineDefinition reports whether rec would store a definition inline, i.e.
// whether moving it to DefinitionRef can shrink the record.
func hasInlineDefinition(rec VersionRecord) bool {
	return rec.DefinitionRef == nil && !definitionIsZero(rec.Definition)
}

// definitionIsZero reports whether d is the zero Definition, the value an encoded
// VersionRecord omits (json tag omitzero, §5.3).
func definitionIsZero(d Definition) bool {
	return d.FormatVersion == 0 && d.Description == "" && len(d.Metadata) == 0 && len(d.Data) == 0
}

// GetVersion reads the record of one version, returning kv.ErrNotFound unchanged
// when there is no such version.
func (s *Store) GetVersion(ctx context.Context, dataset string, version int) (*VersionRecord, error) {
	key, err := versionKey(dataset, version)
	if err != nil {
		return nil, err
	}
	value, err := s.get(ctx, key)
	if err != nil {
		return nil, err
	}
	rec, err := decodeRecord[VersionRecord]("version record", key, value)
	if err != nil {
		return nil, err
	}
	if rec.Dataset != dataset || rec.Version != version {
		return nil, fmt.Errorf("%w: version key %q holds record %q@v%d",
			ErrRecordMismatch, key, rec.Dataset, rec.Version)
	}
	return rec, nil
}

// DeleteVersion removes the record of one version. Deleting a missing version is not
// an error, which is what makes the delete sequence of §6.6 tolerant and re-runnable.
// Version records are immutable while they exist; this is only called when the dataset
// itself is deleted.
func (s *Store) DeleteVersion(ctx context.Context, dataset string, version int) error {
	if s.deleteVersionFn != nil {
		return s.deleteVersionFn(ctx, dataset, version)
	}
	return s.deleteVersion(ctx, dataset, version)
}

// deleteVersion is the real DeleteVersion, the fall-through of the injection point.
func (s *Store) deleteVersion(ctx context.Context, dataset string, version int) error {
	key, err := versionKey(dataset, version)
	if err != nil {
		return err
	}
	return s.delete(ctx, key)
}

// ListVersions returns every version record of dataset, newest first (§10.1
// "Newest first"). An unknown dataset yields an empty slice and no error.
func (s *Store) ListVersions(ctx context.Context, dataset string) ([]*VersionRecord, error) {
	prefix, err := versionsPrefix(dataset)
	if err != nil {
		return nil, err
	}
	versions := make([]*VersionRecord, 0)
	err = s.scanPrefix(ctx, prefix, func(key, value []byte) error {
		rec, err := decodeRecord[VersionRecord]("version record", key, value)
		if err != nil {
			return err
		}
		versions = append(versions, rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Keys are zero padded (§5.3), so the scan already returns versions in order;
	// sorting on the record makes the contract independent of the key encoding.
	sort.SliceStable(versions, func(i, j int) bool { return versions[i].Version > versions[j].Version })
	return versions, nil
}

// ----------------------------------------------------------------------------
// Proposal records (§5.3, §5.6)
// ----------------------------------------------------------------------------

// GetProposal reads one proposal, returning kv.ErrNotFound unchanged when it does
// not exist.
func (s *Store) GetProposal(ctx context.Context, dataset, id string) (*ProposalRecord, error) {
	key, err := proposalKey(dataset, id)
	if err != nil {
		return nil, err
	}
	value, err := s.get(ctx, key)
	if err != nil {
		return nil, err
	}
	return decodeRecord[ProposalRecord]("proposal record", key, value)
}

// PutProposalIf writes a proposal record only if the stored record still equals
// previous (the record as last read by GetProposal), so a merge racing a close or an
// update cannot overwrite it (DATASETS_REVIEW BUG-8). A nil previous means "the
// proposal must not exist yet", which is how a proposal is created.
//
// kv.ErrPredicateFailed is returned unchanged when the caller lost the race. The
// predicate handed to SetIf comes from kv.Get (see setIfUnchanged).
func (s *Store) PutProposalIf(ctx context.Context, rec, previous *ProposalRecord) error {
	if s.putProposalFn != nil {
		return s.putProposalFn(ctx, rec, previous)
	}
	return s.putProposalIf(ctx, rec, previous)
}

// putProposalIf is the real PutProposalIf, the fall-through of the injection point.
func (s *Store) putProposalIf(ctx context.Context, rec, previous *ProposalRecord) error {
	if rec == nil {
		return fmt.Errorf("datasets: %w", ErrNilRecord)
	}
	key, err := proposalKey(rec.Dataset, rec.ID)
	if err != nil {
		return err
	}
	value, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("datasets: encode proposal record %q: %w", rec.ID, err)
	}
	if previous == nil {
		return s.store.SetIf(ctx, []byte(PartitionKey), key, value, nil)
	}
	if previous.Dataset != rec.Dataset || previous.ID != rec.ID {
		return fmt.Errorf("%w: previous record is proposal %q/%q, new record is %q/%q",
			ErrRecordMismatch, previous.Dataset, previous.ID, rec.Dataset, rec.ID)
	}
	expected, err := json.Marshal(previous)
	if err != nil {
		return fmt.Errorf("datasets: encode proposal record %q: %w", previous.ID, err)
	}
	return s.setIfUnchanged(ctx, key, value, expected, canonicalValue[ProposalRecord])
}

// PutProposal writes a proposal record unconditionally. Service code uses
// PutProposalIf; this remains for tests and tools that seed records.
func (s *Store) PutProposal(ctx context.Context, rec *ProposalRecord) error {
	if rec == nil {
		return fmt.Errorf("datasets: %w", ErrNilRecord)
	}
	key, err := proposalKey(rec.Dataset, rec.ID)
	if err != nil {
		return err
	}
	value, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("datasets: encode proposal record %q: %w", rec.ID, err)
	}
	return s.put(ctx, key, value)
}

// DeleteProposal removes a proposal record; deleting a missing proposal is not an
// error.
func (s *Store) DeleteProposal(ctx context.Context, dataset, id string) error {
	key, err := proposalKey(dataset, id)
	if err != nil {
		return err
	}
	return s.delete(ctx, key)
}

// ListProposals returns every proposal of dataset, newest first. An unknown dataset
// yields an empty slice and no error.
func (s *Store) ListProposals(ctx context.Context, dataset string) ([]*ProposalRecord, error) {
	prefix, err := proposalsPrefix(dataset)
	if err != nil {
		return nil, err
	}
	proposals := make([]*ProposalRecord, 0)
	err = s.scanPrefix(ctx, prefix, func(key, value []byte) error {
		rec, err := decodeRecord[ProposalRecord]("proposal record", key, value)
		if err != nil {
			return err
		}
		proposals = append(proposals, rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.SliceStable(proposals, func(i, j int) bool {
		if !proposals[i].CreatedAt.Equal(proposals[j].CreatedAt) {
			return proposals[i].CreatedAt.After(proposals[j].CreatedAt)
		}
		return proposals[i].ID > proposals[j].ID // ULIDs: descending is newest first
	})
	return proposals, nil
}

// ----------------------------------------------------------------------------
// Metadata keys (§5.3, §5.5)
// ----------------------------------------------------------------------------

// GetMetadataKey reads one metadata key, returning kv.ErrNotFound unchanged when it
// is not defined.
func (s *Store) GetMetadataKey(ctx context.Context, name string) (*MetadataKey, error) {
	key, err := metadataKeyKey(name)
	if err != nil {
		return nil, err
	}
	value, err := s.get(ctx, key)
	if err != nil {
		return nil, err
	}
	return decodeRecord[MetadataKey]("metadata key", key, value)
}

// PutMetadataKey writes a metadata key definition. Metadata keys are global (§5.5),
// so this is not namespaced by dataset name. Callers validate the definition with
// ValidateMetadataKey or ValidateKeyUpdate first; the store does not.
//
// previous is the definition as last read (GetMetadataKey): the write is then a CAS
// against it and returns kv.ErrPredicateFailed when another client changed the key.
// A nil previous means "the key must not exist yet", which is how a create is
// expressed (§5.5).
func (s *Store) PutMetadataKey(ctx context.Context, key MetadataKey, previous *MetadataKey) error {
	path, err := metadataKeyKey(key.Name)
	if err != nil {
		return err
	}
	value, err := json.Marshal(key)
	if err != nil {
		return fmt.Errorf("datasets: encode metadata key %q: %w", key.Name, err)
	}
	if previous == nil {
		return s.store.SetIf(ctx, []byte(PartitionKey), path, value, nil)
	}
	if previous.Name != key.Name {
		return fmt.Errorf("%w: previous metadata key is %q, new key is %q",
			ErrRecordMismatch, previous.Name, key.Name)
	}
	expected, err := json.Marshal(previous)
	if err != nil {
		return fmt.Errorf("datasets: encode previous metadata key %q: %w", key.Name, err)
	}
	return s.setIfUnchanged(ctx, path, value, expected, canonicalValue[MetadataKey])
}

// DeleteMetadataKey removes a metadata key definition. Existing metadata values are
// not touched: they simply become free-form (§5.5).
//
// previous bounds the delete like PutMetadataKey: pass the definition as last read, or
// nil to delete unconditionally. A concurrent update leaves the key untouched and
// returns kv.ErrPredicateFailed.
func (s *Store) DeleteMetadataKey(ctx context.Context, name string, previous *MetadataKey) error {
	key, err := metadataKeyKey(name)
	if err != nil {
		return err
	}
	if previous != nil {
		if previous.Name != name {
			return fmt.Errorf("%w: previous metadata key is %q, delete is for %q",
				ErrRecordMismatch, previous.Name, name)
		}
		expected, err := json.Marshal(previous)
		if err != nil {
			return fmt.Errorf("datasets: encode previous metadata key %q: %w", name, err)
		}
		current, err := s.get(ctx, key)
		if errors.Is(err, kv.ErrNotFound) {
			return kv.ErrPredicateFailed
		}
		if err != nil {
			return err
		}
		current, err = canonicalValue[MetadataKey](current)
		if err != nil {
			return err
		}
		if expected, err = canonicalValue[MetadataKey](expected); err != nil {
			return err
		}
		if !bytes.Equal(current, expected) {
			return kv.ErrPredicateFailed
		}
	}
	return s.delete(ctx, key)
}

// ListMetadataKeys returns every metadata key, sorted by name, as the value slice
// MetadataKeysValidator expects (§5.5).
func (s *Store) ListMetadataKeys(ctx context.Context) ([]MetadataKey, error) {
	keys := make([]MetadataKey, 0)
	err := s.scanPrefix(ctx, metadataKeysPrefix(), func(key, value []byte) error {
		rec, err := decodeRecord[MetadataKey]("metadata key", key, value)
		if err != nil {
			return err
		}
		keys = append(keys, *rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.SliceStable(keys, func(i, j int) bool { return keys[i].Name < keys[j].Name })
	return keys, nil
}

// ----------------------------------------------------------------------------
// GC pins (§5.3, §8.2)
// ----------------------------------------------------------------------------

// GetPin reads the pin of one (repository, commit), returning kv.ErrNotFound
// unchanged when it does not exist.
//
// The predicate is the one kv.Store.Get returned with the record; AddPinReference and
// RemovePinReference condition their compare-and-swap on it.
func (s *Store) GetPin(ctx context.Context, repository, commitID string) (*PinRecord, kv.Predicate, error) {
	key, err := pinKey(repository, commitID)
	if err != nil {
		return nil, nil, err
	}
	res, err := s.store.Get(ctx, []byte(PartitionKey), key)
	if err != nil {
		return nil, nil, err
	}
	rec, err := decodeRecord[PinRecord]("pin record", key, res.Value)
	if err != nil {
		return nil, nil, err
	}
	return rec, res.Predicate, nil
}

// PutPin writes a pin record, normalising References to a sorted, duplicate-free
// set. Use AddPinReference and RemovePinReference for reference changes: they apply
// the update as a compare-and-swap, which this unconditional write does not.
func (s *Store) PutPin(ctx context.Context, rec *PinRecord) error {
	if rec == nil {
		return fmt.Errorf("datasets: %w", ErrNilRecord)
	}
	key, err := pinKey(rec.Repository, rec.CommitID)
	if err != nil {
		return err
	}
	normalised := *rec
	normalised.References = normaliseReferences(rec.References)
	value, err := json.Marshal(normalised)
	if err != nil {
		return fmt.Errorf("datasets: encode pin record %s/%s: %w", rec.Repository, rec.CommitID, err)
	}
	return s.put(ctx, key, value)
}

// DeletePin removes the pin record of (repository, commit); deleting a missing pin
// is not an error. It does not delete the hidden pin branch: that is the caller's
// job when RemovePinReference reports that the set became empty (§8.2).
func (s *Store) DeletePin(ctx context.Context, repository, commitID string) error {
	key, err := pinKey(repository, commitID)
	if err != nil {
		return err
	}
	return s.delete(ctx, key)
}

// DeletePinIf removes the pin record of (repository, commit) only if it is still the record
// that predicate guards, the kv.Predicate that GetPin returned for it. A record a concurrent
// AddPinReference changed is left untouched and kv.ErrPredicateFailed is returned, so
// orphan cleanup never deletes a pin that just gained a reference (BUG-4). Deleting a
// missing pin is not an error. Like DeletePin it does not delete the hidden pin branch.
func (s *Store) DeletePinIf(ctx context.Context, repository, commitID string, predicate kv.Predicate) error {
	key, err := pinKey(repository, commitID)
	if err != nil {
		return err
	}
	return s.deleteIf(ctx, key, predicate)
}

// AddPinReference adds ref - a PinReference value, "<dataset>@v<n>" - to the pin of
// (repository, commit) and returns the resulting record. It creates the pin when
// there is none, and adding a reference twice is a no-op.
//
// Publish pins its sources before it writes data (§6.2 step 5, §8.2), so this is the
// call that protects a dataset version from the source repository's GC.
//
// The update is a compare-and-swap with bounded retries: a pin has many writers (one
// per dataset version that references the same commit) and losing a reference would
// let GC collect objects a dataset still serves.
func (s *Store) AddPinReference(ctx context.Context, repository, commitID, ref string) (*PinRecord, error) {
	if err := checkSegment("pin reference", ref); err != nil {
		return nil, err
	}
	key, err := pinKey(repository, commitID)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < pinCASRetries; attempt++ {
		current, predicate, err := s.readPin(ctx, key)
		if err != nil {
			return nil, err
		}
		next := PinRecord{Repository: repository, CommitID: commitID}
		if current != nil {
			next = *current
		}
		if containsReference(next.References, ref) {
			return &next, nil
		}
		next.References = normaliseReferences(append(next.References, ref))
		value, err := json.Marshal(next)
		if err != nil {
			return nil, fmt.Errorf("datasets: encode pin record %s/%s: %w", repository, commitID, err)
		}
		err = s.store.SetIf(ctx, []byte(PartitionKey), key, value, predicate)
		if errors.Is(err, kv.ErrPredicateFailed) {
			continue // another writer changed the pin; re-read and retry
		}
		if err != nil {
			return nil, err
		}
		return &next, nil
	}
	return nil, kv.ErrPredicateFailed
}

// RemovePinReference removes ref from the pin of (repository, commit). When that
// leaves the set empty, the pin becomes an orphan: the record is deleted (§5.3: "the
// branch is deleted when the set becomes empty") and deleted reports true, so the
// caller also removes the hidden pin branch. Removing a reference that is not there,
// or from a pin that does not exist, is a no-op and reports false.
//
// The empty set is written with a compare-and-swap before the record is deleted, and the
// delete itself is conditioned on that empty-set value: a concurrent AddPinReference that
// lands in between replaces the value and keeps its pin (BUG-5), and the removal is retried
// instead of dropping the new reference. If the pin is gone on the retry, the removal has
// already happened and the branch is kept.
func (s *Store) RemovePinReference(ctx context.Context, repository, commitID, ref string) (deleted bool, err error) {
	if err := checkSegment("pin reference", ref); err != nil {
		return false, err
	}
	key, err := pinKey(repository, commitID)
	if err != nil {
		return false, err
	}
	for attempt := 0; attempt < pinCASRetries; attempt++ {
		current, predicate, err := s.readPin(ctx, key)
		if err != nil {
			return false, err
		}
		if current == nil || !containsReference(current.References, ref) {
			return false, nil
		}
		remaining := make([]string, 0, len(current.References))
		for _, existing := range current.References {
			if existing != ref {
				remaining = append(remaining, existing)
			}
		}
		next := *current
		next.References = remaining
		value, err := json.Marshal(next)
		if err != nil {
			return false, fmt.Errorf("datasets: encode pin record %s/%s: %w", repository, commitID, err)
		}
		err = s.store.SetIf(ctx, []byte(PartitionKey), key, value, predicate)
		if errors.Is(err, kv.ErrPredicateFailed) {
			continue // another writer changed the pin; re-read and retry
		}
		if err != nil {
			return false, err
		}
		if len(remaining) > 0 {
			return false, nil
		}
		err = s.deleteIfUnchanged(ctx, key, value, canonicalValue[PinRecord])
		if errors.Is(err, kv.ErrPredicateFailed) {
			continue // a reference landed on the empty set; re-read and retry
		}
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, kv.ErrPredicateFailed
}

// ListPins returns every pin of repository in commit order. An unknown repository
// yields an empty slice and no error.
func (s *Store) ListPins(ctx context.Context, repository string) ([]*PinRecord, error) {
	prefix, err := pinsPrefix(repository)
	if err != nil {
		return nil, err
	}
	pins := make([]*PinRecord, 0)
	err = s.scanPrefix(ctx, prefix, func(key, value []byte) error {
		rec, err := decodeRecord[PinRecord]("pin record", key, value)
		if err != nil {
			return err
		}
		pins = append(pins, rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return pins, nil
}

// readPin reads a pin record together with the predicate that a conditional write
// needs. A missing pin yields (nil, nil): the caller is about to create it.
func (s *Store) readPin(ctx context.Context, key []byte) (*PinRecord, kv.Predicate, error) {
	res, err := s.store.Get(ctx, []byte(PartitionKey), key)
	if errors.Is(err, kv.ErrNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	rec, err := decodeRecord[PinRecord]("pin record", key, res.Value)
	if err != nil {
		return nil, nil, err
	}
	return rec, res.Predicate, nil
}

// containsReference reports whether refs contains ref.
func containsReference(refs []string, ref string) bool {
	for _, existing := range refs {
		if existing == ref {
			return true
		}
	}
	return false
}

// normaliseReferences returns refs sorted and duplicate free, the PinRecord set
// invariant (§5.3).
func normaliseReferences(refs []string) []string {
	if len(refs) == 0 {
		return nil
	}
	unique := make(map[string]struct{}, len(refs))
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		if _, ok := unique[ref]; ok {
			continue
		}
		unique[ref] = struct{}{}
		out = append(out, ref)
	}
	sort.Strings(out)
	return out
}

// ----------------------------------------------------------------------------
// Access grants (§5.3, §9.2)
// ----------------------------------------------------------------------------

// GetGrant reads one grant, returning kv.ErrNotFound unchanged when it does not
// exist.
func (s *Store) GetGrant(ctx context.Context, dataset, principalType, principalID string) (*GrantRecord, error) {
	key, err := grantKey(dataset, principalType, principalID)
	if err != nil {
		return nil, err
	}
	value, err := s.get(ctx, key)
	if err != nil {
		return nil, err
	}
	return decodeRecord[GrantRecord]("grant record", key, value)
}

// PutGrant writes a grant record. Granting access is idempotent (§9.2), so this is a
// plain Set.
func (s *Store) PutGrant(ctx context.Context, rec *GrantRecord) error {
	if rec == nil {
		return fmt.Errorf("datasets: %w", ErrNilRecord)
	}
	key, err := grantKey(rec.Dataset, rec.PrincipalType, rec.PrincipalID)
	if err != nil {
		return err
	}
	value, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("datasets: encode grant record %q: %w", key, err)
	}
	return s.put(ctx, key, value)
}

// DeleteGrant removes one grant record; deleting a missing grant is not an error.
func (s *Store) DeleteGrant(ctx context.Context, dataset, principalType, principalID string) error {
	key, err := grantKey(dataset, principalType, principalID)
	if err != nil {
		return err
	}
	return s.delete(ctx, key)
}

// ----------------------------------------------------------------------------
// Task records (§6.5, §10.1)
// ----------------------------------------------------------------------------

// GetTask reads one asynchronous task record, returning kv.ErrNotFound unchanged when
// it does not exist.
func (s *Store) GetTask(ctx context.Context, dataset, id string) (*TaskRecord, error) {
	key, err := taskKey(dataset, id)
	if err != nil {
		return nil, err
	}
	value, err := s.get(ctx, key)
	if err != nil {
		return nil, err
	}
	rec, err := decodeRecord[TaskRecord]("task record", key, value)
	if err != nil {
		return nil, err
	}
	if rec.Dataset != dataset || rec.ID != id {
		return nil, fmt.Errorf("%w: task key %q holds record %q/%q", ErrRecordMismatch, key, rec.Dataset, rec.ID)
	}
	return rec, nil
}

// PutTask writes a task record. Task status is a forward-only state machine owned by
// the single goroutine that runs the task (§6.5), so this is a plain Set.
func (s *Store) PutTask(ctx context.Context, rec *TaskRecord) error {
	if rec == nil {
		return fmt.Errorf("datasets: %w", ErrNilRecord)
	}
	key, err := taskKey(rec.Dataset, rec.ID)
	if err != nil {
		return err
	}
	value, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("datasets: encode task record %q: %w", rec.ID, err)
	}
	return s.put(ctx, key, value)
}

// ListTasks returns every task record of dataset, newest first. An unknown dataset yields
// an empty slice and no error. It is what finishDelete uses to remove the tasks of a
// deleted dataset and what repairInterrupted uses to settle the task of a publish that
// crashed (BUG-11).
func (s *Store) ListTasks(ctx context.Context, dataset string) ([]*TaskRecord, error) {
	prefix, err := tasksPrefix(dataset)
	if err != nil {
		return nil, err
	}
	tasks := make([]*TaskRecord, 0)
	err = s.scanPrefix(ctx, prefix, func(key, value []byte) error {
		rec, err := decodeRecord[TaskRecord]("task record", key, value)
		if err != nil {
			return err
		}
		tasks = append(tasks, rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.SliceStable(tasks, func(i, j int) bool {
		if !tasks[i].CreatedAt.Equal(tasks[j].CreatedAt) {
			return tasks[i].CreatedAt.After(tasks[j].CreatedAt)
		}
		return tasks[i].ID > tasks[j].ID // ULIDs: descending is newest first
	})
	return tasks, nil
}

// DeleteTask removes one task record; deleting a missing task is not an error.
func (s *Store) DeleteTask(ctx context.Context, dataset, id string) error {
	key, err := taskKey(dataset, id)
	if err != nil {
		return err
	}
	return s.delete(ctx, key)
}

// ListGrants returns every grant of dataset, ordered by principal type then
// principal id. An unknown dataset yields an empty slice and no error.
func (s *Store) ListGrants(ctx context.Context, dataset string) ([]*GrantRecord, error) {
	prefix, err := grantsPrefix(dataset)
	if err != nil {
		return nil, err
	}
	grants := make([]*GrantRecord, 0)
	err = s.scanPrefix(ctx, prefix, func(key, value []byte) error {
		rec, err := decodeRecord[GrantRecord]("grant record", key, value)
		if err != nil {
			return err
		}
		grants = append(grants, rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return grants, nil
}
