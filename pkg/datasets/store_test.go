package datasets

// Tests of the KV persistence layer (store.go, design §5.3, §5.4). They run against
// the in-memory kv.Store, which implements the same Store interface (including the
// SetIf predicate comparison) as postgres, DynamoDB, CosmosDB and local, and needs no
// external service.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	_ "github.com/treeverse/lakefs/pkg/kv/mem" // register the "mem" KV driver
)

// testTime is a fixed timestamp without a monotonic reading, so it survives a JSON
// round trip unchanged.
func testTime() time.Time {
	return time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
}

// newTestStore returns a Store over a fresh in-memory KV store, plus that KV store so
// a test can inspect raw values and scan the partition.
func newTestStore(t *testing.T) (*Store, kv.Store, context.Context) {
	t.Helper()
	ctx := t.Context()
	kvs, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	require.NoError(t, err, "open mem kv store")
	t.Cleanup(kvs.Close)
	return NewStore(kvs), kvs, ctx
}

// requireJSONEq asserts that two values encode to the same JSON document, which is
// the wire and KV representation of every record (§5.3).
func requireJSONEq(t *testing.T, want, got any) {
	t.Helper()
	wantJSON, err := json.Marshal(want)
	require.NoError(t, err)
	gotJSON, err := json.Marshal(got)
	require.NoError(t, err)
	require.JSONEq(t, string(wantJSON), string(gotJSON))
}

// partitionKeys returns every key stored in the datasets partition, in key order.
func partitionKeys(t *testing.T, kvs kv.Store, ctx context.Context) []string {
	t.Helper()
	it, err := kvs.Scan(ctx, []byte(PartitionKey), kv.ScanOptions{})
	require.NoError(t, err)
	defer it.Close()
	keys := make([]string, 0)
	for it.Next() {
		entry := it.Entry()
		require.NotNil(t, entry)
		keys = append(keys, string(entry.Key))
	}
	require.NoError(t, it.Err())
	return keys
}

func TestStoreKeyLayout(t *testing.T) {
	// The §5.3 table, key for key.
	tests := []struct {
		name string
		key  func() ([]byte, error)
		want string
	}{
		{
			name: "dataset",
			key:  func() ([]byte, error) { return datasetKey("customer-360") },
			want: "datasets/customer-360",
		},
		{
			name: "version",
			key:  func() ([]byte, error) { return versionKey("customer-360", 7) },
			want: "versions/customer-360/0000000007",
		},
		{
			name: "proposal",
			key:  func() ([]byte, error) { return proposalKey("customer-360", "01J9Z6Q0K8") },
			want: "proposals/customer-360/01J9Z6Q0K8",
		},
		{
			name: "metadata key",
			key:  func() ([]byte, error) { return metadataKeyKey("retention") },
			want: "metadata_keys/retention",
		},
		{
			name: "pin",
			key:  func() ([]byte, error) { return pinKey("lakehouse-repo", "30f9eb1c") },
			want: "pins/lakehouse-repo/30f9eb1c",
		},
		{
			name: "grant",
			key:  func() ([]byte, error) { return grantKey("customer-360", PrincipalTypeUser, "alice") },
			want: "grants/customer-360/user/alice",
		},
		{
			name: "group grant",
			key:  func() ([]byte, error) { return grantKey("customer-360", PrincipalTypeGroup, "data-consumers") },
			want: "grants/customer-360/group/data-consumers",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, err := tt.key()
			require.NoError(t, err)
			require.Equal(t, tt.want, string(key))
		})
	}
}

func TestStoreConstants(t *testing.T) {
	// The §5.3 layout constants that later phases depend on.
	require.Equal(t, "_datasets", PartitionKey)
	require.Equal(t, 256*1024, MaxInlineDefinitionBytes)
	require.Equal(t, "_dataset/definition.json", DefinitionPath)
}

func TestStoreVersionKeyOrdering(t *testing.T) { // Zero padding makes lexicographic key order equal numeric version order, which
	// is what makes ListVersions a plain prefix scan (§5.3).
	versions := []int{1000, 2, 1, 100, 10, 9, 3, 9999999999}
	keys := make([]string, 0, len(versions))
	for _, version := range versions {
		key, err := versionKey("customer-360", version)
		require.NoError(t, err)
		require.Len(t, string(key), len("versions/customer-360/")+versionKeyWidth)
		keys = append(keys, string(key))
	}
	sort.Strings(keys)

	sorted := append([]int(nil), versions...)
	sort.Ints(sorted)
	want := make([]string, 0, len(sorted))
	for _, version := range sorted {
		key, err := versionKey("customer-360", version)
		require.NoError(t, err)
		want = append(want, string(key))
	}
	require.Equal(t, want, keys)
}

func TestStoreKeyValidation(t *testing.T) {
	_, err := datasetKey("Customer-360")
	require.Error(t, err, "upper-case name is not a dataset name")
	require.ErrorIs(t, err, ErrInvalidRecordKey)
	var validationErr *ValidationError
	require.True(t, errors.As(err, &validationErr), "name problems are validation problems (§10.4)")

	_, err = datasetKey("")
	require.Error(t, err)

	_, err = versionKey("customer-360", 0)
	require.ErrorIs(t, err, ErrInvalidRecordKey, "versions are dense integers starting at 1 (§5.4)")

	_, err = proposalKey("customer-360", "")
	require.ErrorIs(t, err, ErrInvalidRecordKey)

	_, err = metadataKeyKey("Retention")
	require.ErrorIs(t, err, ErrInvalidRecordKey)

	_, err = pinKey("lakehouse-repo", "a/b")
	require.ErrorIs(t, err, ErrInvalidRecordKey)

	_, err = grantKey("customer-360", "team", "alice")
	require.ErrorIs(t, err, ErrInvalidRecordKey, "only user and group grants exist (§9.2)")
}

// newDatasetRecord returns a record of the §5.3 shape for round-trip and CAS tests.
func newDatasetRecord() *DatasetRecord {
	return &DatasetRecord{
		Name:          "customer-360",
		Description:   "Curated customer view for analytics",
		CreatedBy:     "alice",
		CreatedAt:     testTime(),
		LatestVersion: 2,
		State:         DatasetStateActive,
		StorageID:     "s3",
		StorageNS:     "s3://lakefs-datasets/customer-360-01J9Z6Q0K8",
		UpdatedAt:     testTime().Add(time.Minute),
	}
}

// newVersionRecord returns a small, inline-definition version record.
func newVersionRecord() VersionRecord {
	return VersionRecord{
		Dataset:   "customer-360",
		Version:   2,
		Message:   "Add geo reference data",
		Publisher: "alice",
		Definition: Definition{
			FormatVersion: FormatVersion,
			Description:   "Curated customer view for analytics",
			Metadata: map[string]any{
				"classification": "internal",
				"tier":           "gold",
			},
			Data: []DataItem{
				objectItem("reference/countries.csv", "geo/countries.csv"),
				prefixItem("customers", "data/customers/"),
			},
		},
		PublishedAt: testTime(),
		CommitID:    "30f9eb1c",
		MetaRangeID: "metarange-1",
		ProposalID:  "01J9Z6Q0K8",
		Stats: ItemStats{
			Objects: 42,
			Bytes:   4096,
			Items: []ItemStat{
				{Target: "customers", Objects: 41, Bytes: 4000},
				{Target: "reference/countries.csv", Objects: 1, Bytes: 96},
			},
		},
	}
}

func TestStoreRoundTrip(t *testing.T) {
	store, _, ctx := newTestStore(t)

	t.Run("dataset", func(t *testing.T) {
		want := newDatasetRecord()
		require.NoError(t, store.CreateDataset(ctx, want))
		got, err := store.GetDataset(ctx, want.Name)
		require.NoError(t, err)
		requireJSONEq(t, want, got)
	})

	t.Run("version", func(t *testing.T) {
		want := newVersionRecord()
		stored, err := store.PutVersion(ctx, want)
		require.NoError(t, err)
		require.Nil(t, stored.DefinitionRef, "the definition fits inline, so no ref is stored (§5.3)")
		requireJSONEq(t, want, stored)

		got, err := store.GetVersion(ctx, want.Dataset, want.Version)
		require.NoError(t, err)
		requireJSONEq(t, want, got)
		require.True(t, got.HasInlineDefinition())
	})

	t.Run("proposal", func(t *testing.T) {
		want := &ProposalRecord{
			ID:            "01J9Z6Q0K8",
			Dataset:       "customer-360",
			BaseVersion:   2,
			Definition:    newVersionRecord().Definition,
			Title:         "Add orders",
			Description:   "Include the orders prefix",
			Author:        "bob",
			Status:        ProposalStatusOpen,
			CreatedAt:     testTime(),
			PreviewBranch: "_proposal-01J9Z6Q0K8",
		}
		require.NoError(t, store.PutProposal(ctx, want))
		got, err := store.GetProposal(ctx, want.Dataset, want.ID)
		require.NoError(t, err)
		requireJSONEq(t, want, got)
	})

	t.Run("metadata key", func(t *testing.T) {
		want := MetadataKey{
			Name:          "tier",
			Type:          MetadataTypeEnum,
			Required:      true,
			AllowedValues: []string{"bronze", "silver", "gold"},
			Description:   "Data quality tier",
			CreatedAt:     testTime(),
			CreatedBy:     "alice",
		}
		require.NoError(t, store.PutMetadataKey(ctx, want))
		got, err := store.GetMetadataKey(ctx, want.Name)
		require.NoError(t, err)
		requireJSONEq(t, want, got)

		keys, err := store.ListMetadataKeys(ctx)
		require.NoError(t, err)
		require.Len(t, keys, 1)
		requireJSONEq(t, want, keys[0])
	})

	t.Run("pin", func(t *testing.T) {
		want := &PinRecord{
			Repository: "lakehouse-repo",
			CommitID:   "30f9eb1c",
			References: []string{
				PinReference("customer-360", 2),
				PinReference("customer-360", 1),
			},
		}
		require.NoError(t, store.PutPin(ctx, want))
		got, err := store.GetPin(ctx, want.Repository, want.CommitID)
		require.NoError(t, err)
		require.Equal(t, []string{
			"customer-360@v1",
			"customer-360@v2",
		}, got.References, "PutPin stores a sorted set")
	})

	t.Run("grant", func(t *testing.T) {
		want := &GrantRecord{
			Dataset:       "customer-360",
			PrincipalType: PrincipalTypeUser,
			PrincipalID:   "carol",
			PolicyName:    "dataset-customer-360-read",
			GrantedBy:     "alice",
			GrantedAt:     testTime(),
		}
		require.NoError(t, store.PutGrant(ctx, want))
		got, err := store.GetGrant(ctx, want.Dataset, want.PrincipalType, want.PrincipalID)
		require.NoError(t, err)
		requireJSONEq(t, want, got)
	})
}

func TestStoreListings(t *testing.T) {
	store, _, ctx := newTestStore(t)

	// Versions come back newest first (§10.1), whatever order they were written in.
	for _, version := range []int{2, 10, 1, 3} {
		rec := newVersionRecord()
		rec.Version = version
		_, err := store.PutVersion(ctx, rec)
		require.NoError(t, err)
	}
	versions, err := store.ListVersions(ctx, "customer-360")
	require.NoError(t, err)
	gotVersions := make([]int, len(versions))
	for i, rec := range versions {
		gotVersions[i] = rec.Version
	}
	require.Equal(t, []int{10, 3, 2, 1}, gotVersions)

	// Proposals come back newest first as well.
	for i := 0; i < 3; i++ {
		rec := &ProposalRecord{
			ID:        fmt.Sprintf("proposal-%d", i),
			Dataset:   "customer-360",
			Status:    ProposalStatusOpen,
			CreatedAt: testTime().Add(time.Duration(i) * time.Hour),
		}
		require.NoError(t, store.PutProposal(ctx, rec))
	}
	proposals, err := store.ListProposals(ctx, "customer-360")
	require.NoError(t, err)
	gotProposals := make([]string, len(proposals))
	for i, rec := range proposals {
		gotProposals[i] = rec.ID
	}
	require.Equal(t, []string{"proposal-2", "proposal-1", "proposal-0"}, gotProposals)

	// Grants come back in key order: principal type then principal id.
	for _, grant := range []struct{ principalType, principalID string }{
		{PrincipalTypeUser, "dave"},
		{PrincipalTypeGroup, "data-consumers"},
		{PrincipalTypeUser, "carol"},
	} {
		require.NoError(t, store.PutGrant(ctx, &GrantRecord{
			Dataset:       "customer-360",
			PrincipalType: grant.principalType,
			PrincipalID:   grant.principalID,
			GrantedAt:     testTime(),
		}))
	}
	grants, err := store.ListGrants(ctx, "customer-360")
	require.NoError(t, err)
	require.Len(t, grants, 3)
	require.Equal(t, "data-consumers", grants[0].PrincipalID)
	require.Equal(t, "carol", grants[1].PrincipalID)
	require.Equal(t, "dave", grants[2].PrincipalID)

	// An unknown dataset is not an error: it has no records.
	empty, err := store.ListVersions(ctx, "other-dataset")
	require.NoError(t, err)
	require.Empty(t, empty)
}

func TestStoreNotFound(t *testing.T) {
	store, _, ctx := newTestStore(t)

	_, err := store.GetDataset(ctx, "customer-360")
	require.ErrorIs(t, err, kv.ErrNotFound)

	_, err = store.GetVersion(ctx, "customer-360", 1)
	require.ErrorIs(t, err, kv.ErrNotFound)

	_, err = store.GetProposal(ctx, "customer-360", "01J9Z6Q0K8")
	require.ErrorIs(t, err, kv.ErrNotFound)

	_, err = store.GetMetadataKey(ctx, "tier")
	require.ErrorIs(t, err, kv.ErrNotFound)

	_, err = store.GetPin(ctx, "lakehouse-repo", "30f9eb1c")
	require.ErrorIs(t, err, kv.ErrNotFound)

	_, err = store.GetGrant(ctx, "customer-360", PrincipalTypeUser, "alice")
	require.ErrorIs(t, err, kv.ErrNotFound)

	// Deleting a missing record is not an error.
	require.NoError(t, store.DeleteDataset(ctx, "customer-360", nil))
	require.NoError(t, store.DeleteProposal(ctx, "customer-360", "01J9Z6Q0K8"))
	require.NoError(t, store.DeleteMetadataKey(ctx, "tier"))
	require.NoError(t, store.DeletePin(ctx, "lakehouse-repo", "30f9eb1c"))
	require.NoError(t, store.DeleteGrant(ctx, "customer-360", PrincipalTypeUser, "alice"))
}

func TestStoreDatasetCAS(t *testing.T) {
	store, _, ctx := newTestStore(t)

	// Create is a CAS with a nil predicate, so it fails when the record exists.
	record := newDatasetRecord()
	require.NoError(t, store.CreateDataset(ctx, record))
	err := store.CreateDataset(ctx, newDatasetRecord())
	require.ErrorIs(t, err, kv.ErrPredicateFailed, "two creates cannot both win")
	current, err := store.GetDataset(ctx, "customer-360")
	require.NoError(t, err)
	require.Equal(t, DatasetStateActive, current.State, "the losing create left the record untouched")

	// §5.4 step 3: CAS the record read above to the publishing state.
	publishing := *current
	publishing.State = DatasetStatePublishing
	publishing.PendingVersion = current.LatestVersion + 1
	require.NoError(t, store.PutDatasetIf(ctx, &publishing, current))

	// Re-using the stale record as the predicate fails and keeps the stored value.
	stale := *current
	stale.State = DatasetStatePublishing
	err = store.PutDatasetIf(ctx, &stale, current)
	require.ErrorIs(t, err, kv.ErrPredicateFailed)

	// The record written by the winning CAS is the predicate that works.
	active := publishing
	active.State = DatasetStateActive
	active.LatestVersion = publishing.PendingVersion
	active.PendingVersion = 0
	require.NoError(t, store.PutDatasetIf(ctx, &active, &publishing))

	got, err := store.GetDataset(ctx, "customer-360")
	require.NoError(t, err)
	require.Equal(t, DatasetStateActive, got.State)
	require.Equal(t, 3, got.LatestVersion)

	// The predicate must belong to the record being written: a previous record of a
	// different dataset is a programming error, not a lost race.
	other := newDatasetRecord()
	other.Name = "other-dataset"
	err = store.PutDatasetIf(ctx, other, current)
	require.ErrorIs(t, err, ErrRecordMismatch)

	// A conditional delete with a stale record fails and leaves the record.
	err = store.DeleteDataset(ctx, "customer-360", current)
	require.ErrorIs(t, err, kv.ErrPredicateFailed)
	_, err = store.GetDataset(ctx, "customer-360")
	require.NoError(t, err)

	// A conditional delete with the current record succeeds.
	require.NoError(t, store.DeleteDataset(ctx, "customer-360", got))
	_, err = store.GetDataset(ctx, "customer-360")
	require.ErrorIs(t, err, kv.ErrNotFound)
}

// TestStoreConcurrentPublishCAS puts §14 "two concurrent publishes on one dataset"
// under test: both publishers read the same record and then CAS it to publishing.
// Exactly one CAS may win, and the loser must see kv.ErrPredicateFailed - which is
// what the handler maps to 409 (§5.4 step 3).
func TestStoreConcurrentPublishCAS(t *testing.T) {
	store, _, ctx := newTestStore(t)
	require.NoError(t, store.CreateDataset(ctx, newDatasetRecord()))

	previous, err := store.GetDataset(ctx, "customer-360")
	require.NoError(t, err)

	const publishers = 8
	results := make([]error, publishers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < publishers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			next := *previous
			next.State = DatasetStatePublishing
			next.PendingVersion = previous.LatestVersion + 1
			<-start
			results[i] = store.PutDatasetIf(ctx, &next, previous)
		}(i)
	}
	close(start)
	wg.Wait()

	winners := 0
	for _, err := range results {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, kv.ErrPredicateFailed):
			// the losing publisher must retry with a fresh read
		default:
			require.NoError(t, err, "unexpected CAS error")
		}
	}
	require.Equal(t, 1, winners, "exactly one concurrent publish CAS may win")

	after, err := store.GetDataset(ctx, "customer-360")
	require.NoError(t, err)
	require.Equal(t, DatasetStatePublishing, after.State)
	require.Equal(t, previous.LatestVersion+1, after.PendingVersion)
}

func TestStoreVersionIsImmutable(t *testing.T) {
	store, _, ctx := newTestStore(t)

	rec := newVersionRecord()
	_, err := store.PutVersion(ctx, rec)
	require.NoError(t, err)

	rec.Message = "rewritten history"
	_, err = store.PutVersion(ctx, rec)
	require.ErrorIs(t, err, kv.ErrPredicateFailed, "version records are immutable after write (§5.3)")

	got, err := store.GetVersion(ctx, rec.Dataset, rec.Version)
	require.NoError(t, err)
	require.Equal(t, "Add geo reference data", got.Message)
}

// bigDefinition returns a definition with n items whose JSON is far above
// MaxInlineDefinitionBytes, the "1000 items is ~300 KB" case of §5.3.
func bigDefinition(n int) Definition {
	data := make([]DataItem, 0, n)
	for i := 0; i < n; i++ {
		data = append(data, DataItem{
			Target:     fmt.Sprintf("big/target-%04d", i),
			Repository: "lakehouse-repo",
			Ref:        SourceRef{Type: RefTypeCommit, ID: strings.Repeat("a", 40)},
			Type:       ItemTypeObject,
			Object:     &PathSource{Path: strings.Repeat("p", 260)},
		})
	}
	return Definition{FormatVersion: FormatVersion, Description: "big", Data: data}
}

func TestStoreVersionDefinitionRefSwitch(t *testing.T) {
	store, kvs, ctx := newTestStore(t)

	t.Run("large definition moves to definition_ref", func(t *testing.T) {
		rec := newVersionRecord()
		rec.Version = 1
		rec.CommitID = "c0ffee"
		rec.Definition = bigDefinition(1000)

		inline, err := json.Marshal(rec)
		require.NoError(t, err)
		require.Greater(t, len(inline), MaxInlineDefinitionBytes, "test fixture must exceed the threshold")

		stored, err := store.PutVersion(ctx, rec)
		require.NoError(t, err)
		require.NotNil(t, stored.DefinitionRef, "a definition over 256 KB is stored by reference (§5.3)")
		require.Equal(t, &DefinitionRef{CommitID: rec.CommitID, Path: DefinitionPath}, stored.DefinitionRef)
		require.True(t, definitionIsZero(stored.Definition), "the inline definition is dropped")
		require.False(t, stored.HasInlineDefinition())

		// The value that reached KV is small: no copy of the definition is left behind.
		key, err := versionKey(rec.Dataset, rec.Version)
		require.NoError(t, err)
		raw, err := kvs.Get(ctx, []byte(PartitionKey), key)
		require.NoError(t, err)
		require.LessOrEqual(t, len(raw.Value), MaxInlineDefinitionBytes)
		require.Contains(t, string(raw.Value), `"definition_ref"`)
		require.NotContains(t, string(raw.Value), strings.Repeat("p", 32), "definition data must not be stored inline")

		got, err := store.GetVersion(ctx, rec.Dataset, rec.Version)
		require.NoError(t, err)
		requireJSONEq(t, stored, got)

		// Everything except the definition is preserved.
		require.Equal(t, rec.Dataset, got.Dataset)
		require.Equal(t, rec.Message, got.Message)
		require.Equal(t, rec.Stats, got.Stats)
	})

	t.Run("small definition stays inline", func(t *testing.T) {
		rec := newVersionRecord()
		rec.Version = 2
		rec.Definition = Definition{FormatVersion: FormatVersion, Data: []DataItem{prefixItem("customers", "data/customers/")}}

		stored, err := store.PutVersion(ctx, rec)
		require.NoError(t, err)
		require.Nil(t, stored.DefinitionRef)
		require.True(t, stored.HasInlineDefinition())
		require.False(t, definitionIsZero(stored.Definition))

		key, err := versionKey(rec.Dataset, rec.Version)
		require.NoError(t, err)
		raw, err := kvs.Get(ctx, []byte(PartitionKey), key)
		require.NoError(t, err)
		require.Contains(t, string(raw.Value), `"definition"`)
		require.NotContains(t, string(raw.Value), `"definition_ref"`)
	})

	t.Run("explicit definition_ref wins", func(t *testing.T) {
		rec := newVersionRecord()
		rec.Version = 3
		rec.CommitID = "c0ffee"
		rec.Definition = bigDefinition(1000)
		rec.DefinitionRef = &DefinitionRef{CommitID: "explicit", Path: "_dataset/definition-3.json"}

		stored, err := store.PutVersion(ctx, rec)
		require.NoError(t, err)
		require.Equal(t, &DefinitionRef{CommitID: "explicit", Path: "_dataset/definition-3.json"}, stored.DefinitionRef)
		require.True(t, definitionIsZero(stored.Definition))
	})

	t.Run("no commit to point at", func(t *testing.T) {
		rec := newVersionRecord()
		rec.Version = 4
		rec.CommitID = ""
		rec.Definition = bigDefinition(1000)

		_, err := store.PutVersion(ctx, rec)
		require.ErrorIs(t, err, ErrDefinitionRefUnavailable)

		_, err = store.GetVersion(ctx, rec.Dataset, rec.Version)
		require.ErrorIs(t, err, kv.ErrNotFound, "a rejected record is not written")
	})

	t.Run("no definition at all", func(t *testing.T) {
		// A record without a definition cannot be shrunk, so it is stored as is
		// rather than rejected for the lack of a commit.
		rec := VersionRecord{Dataset: "customer-360", Version: 5}
		stored, err := store.PutVersion(ctx, rec)
		require.NoError(t, err)
		require.Nil(t, stored.DefinitionRef)
	})
}

func TestStorePins(t *testing.T) {
	store, _, ctx := newTestStore(t)
	const (
		repository = "lakehouse-repo"
		commitID   = "30f9eb1c"
	)
	ref := PinReference("customer-360", 1)
	otherRef := PinReference("customer-360", 2)

	// The first reference creates the pin.
	pin, err := store.AddPinReference(ctx, repository, commitID, ref)
	require.NoError(t, err)
	require.Equal(t, &PinRecord{Repository: repository, CommitID: commitID, References: []string{ref}}, pin)

	// Adding the same reference again is a no-op, not a duplicate.
	pin, err = store.AddPinReference(ctx, repository, commitID, ref)
	require.NoError(t, err)
	require.Equal(t, []string{ref}, pin.References)

	// A second reference is added to the same pin, keeping the set sorted.
	pin, err = store.AddPinReference(ctx, repository, commitID, otherRef)
	require.NoError(t, err)
	require.Equal(t, []string{ref, otherRef}, pin.References)

	stored, err := store.GetPin(ctx, repository, commitID)
	require.NoError(t, err)
	require.Equal(t, []string{ref, otherRef}, stored.References)

	// Removing one of two references keeps the pin.
	deleted, err := store.RemovePinReference(ctx, repository, commitID, ref)
	require.NoError(t, err)
	require.False(t, deleted)
	stored, err = store.GetPin(ctx, repository, commitID)
	require.NoError(t, err)
	require.Equal(t, []string{otherRef}, stored.References)

	// Removing a reference that is not there is a no-op.
	deleted, err = store.RemovePinReference(ctx, repository, commitID, ref)
	require.NoError(t, err)
	require.False(t, deleted)
	stored, err = store.GetPin(ctx, repository, commitID)
	require.NoError(t, err)
	require.Equal(t, []string{otherRef}, stored.References)

	// Removing from a pin that does not exist is a no-op too.
	deleted, err = store.RemovePinReference(ctx, repository, "deadbeef", ref)
	require.NoError(t, err)
	require.False(t, deleted)

	// Removing the last reference deletes the pin record (§5.3, §8.2).
	deleted, err = store.RemovePinReference(ctx, repository, commitID, otherRef)
	require.NoError(t, err)
	require.True(t, deleted, "the caller must be told to delete the hidden pin branch")
	_, err = store.GetPin(ctx, repository, commitID)
	require.ErrorIs(t, err, kv.ErrNotFound)

	// The pin can be recreated afterwards, e.g. by the reconciler (§12.3).
	pin, err = store.AddPinReference(ctx, repository, commitID, ref)
	require.NoError(t, err)
	require.Equal(t, []string{ref}, pin.References)

	pins, err := store.ListPins(ctx, repository)
	require.NoError(t, err)
	require.Len(t, pins, 1)
	require.Equal(t, commitID, pins[0].CommitID)

	require.NoError(t, store.DeletePin(ctx, repository, commitID))
	pins, err = store.ListPins(ctx, repository)
	require.NoError(t, err)
	require.Empty(t, pins)
}

// TestStorePinReferencesConcurrent adds references from several goroutines at once.
// A lost reference would let source GC collect objects a dataset version still
// serves (§8.2), so every add must survive.
func TestStorePinReferencesConcurrent(t *testing.T) {
	store, _, ctx := newTestStore(t)
	const (
		repository = "lakehouse-repo"
		commitID   = "30f9eb1c"
		writers    = 8
	)
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = store.AddPinReference(ctx, repository, commitID, PinReference(fmt.Sprintf("dataset-%d", i), 1))
		}(i)
	}
	close(start)
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}

	pin, err := store.GetPin(ctx, repository, commitID)
	require.NoError(t, err)
	require.Len(t, pin.References, writers, "no reference may be lost")
}

func TestStorePartitionLayout(t *testing.T) {
	store, kvs, ctx := newTestStore(t)

	require.NoError(t, store.CreateDataset(ctx, newDatasetRecord()))
	for _, version := range []int{1, 2} {
		rec := newVersionRecord()
		rec.Version = version
		_, err := store.PutVersion(ctx, rec)
		require.NoError(t, err)
	}
	require.NoError(t, store.PutProposal(ctx, &ProposalRecord{
		ID:        "01J9Z6Q0K8",
		Dataset:   "customer-360",
		Status:    ProposalStatusOpen,
		CreatedAt: testTime(),
	}))
	require.NoError(t, store.PutMetadataKey(ctx, MetadataKey{Name: "tier", Type: MetadataTypeString}))
	require.NoError(t, store.PutGrant(ctx, &GrantRecord{
		Dataset:       "customer-360",
		PrincipalType: PrincipalTypeUser,
		PrincipalID:   "carol",
		GrantedAt:     testTime(),
	}))
	_, err := store.AddPinReference(ctx, "lakehouse-repo", "30f9eb1c", PinReference("customer-360", 2))
	require.NoError(t, err)

	// Everything the control plane stores lives in the one dedicated partition, under
	// the §5.3 prefixes.
	require.Equal(t, []string{
		"datasets/customer-360",
		"grants/customer-360/user/carol",
		"metadata_keys/tier",
		"pins/lakehouse-repo/30f9eb1c",
		"proposals/customer-360/01J9Z6Q0K8",
		"versions/customer-360/0000000001",
		"versions/customer-360/0000000002",
	}, partitionKeys(t, kvs, ctx))

	// The partition is not the auth partition, so no auth key can be shadowed
	// (§5.3 "separate from auth/graveler partitions").
	require.Equal(t, "_datasets", PartitionKey)
	_, err = kvs.Get(ctx, []byte("auth"), []byte("datasets/customer-360"))
	require.ErrorIs(t, err, kv.ErrNotFound)
}
