package cleanup

// expire_test.go covers the retention decision, the table-property resolution and the
// expire phase end to end against the in-memory lakeFS of fake_test.go.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
)

// testNow is the instant every test uses, so a test never depends on the wall clock.
var testNow = time.Date(2024, 1, 10, 12, 0, 0, 0, time.UTC)

// retentionMetadata builds a v2 metadata document whose snapshots carry only a name of a
// manifest list: the retention decision never reads a manifest, so a placeholder is enough.
func retentionMetadata(t *testing.T, snapshots []testSnapshot, currentSnapshotID int64, properties map[string]string, refs map[string]any) table.Metadata {
	t.Helper()
	if properties == nil {
		properties = map[string]string{}
	}
	snapshotDocuments := make([]map[string]any, 0, len(snapshots))
	snapshotLog := make([]map[string]any, 0, len(snapshots))
	lastUpdated := testNow.UnixMilli()
	for index, snapshot := range snapshots {
		snapshotDocuments = append(snapshotDocuments, map[string]any{
			"snapshot-id":     snapshot.id,
			"timestamp-ms":    snapshot.timestampMs,
			"sequence-number": int64(index + 1),
			"summary":         map[string]any{"operation": "append"},
			"manifest-list":   fmt.Sprintf("s3://bucket/ns/_iceberg/t/metadata/snap-%d.avro", snapshot.id),
		})
		snapshotLog = append(snapshotLog, map[string]any{
			"snapshot-id":  snapshot.id,
			"timestamp-ms": snapshot.timestampMs,
		})
		if snapshot.timestampMs >= lastUpdated {
			lastUpdated = snapshot.timestampMs + 1
		}
	}
	sort.SliceStable(snapshotLog, func(i, j int) bool {
		return snapshotLog[i]["timestamp-ms"].(int64) < snapshotLog[j]["timestamp-ms"].(int64)
	})
	document := map[string]any{
		"format-version":        2,
		"table-uuid":            testTableUUIDText,
		"location":              "s3://bucket/ns/_iceberg/t",
		"last-sequence-number":  int64(len(snapshots)),
		"last-updated-ms":       lastUpdated,
		"last-column-id":        1,
		"current-schema-id":     0,
		"schemas":               []any{mustJSON(t, `{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}`)},
		"default-spec-id":       0,
		"partition-specs":       []any{map[string]any{"spec-id": 0, "fields": []any{}}},
		"last-partition-id":     999,
		"default-sort-order-id": 0,
		"sort-orders":           []any{map[string]any{"order-id": 0, "fields": []any{}}},
		"properties":            properties,
		"current-snapshot-id":   currentSnapshotID,
		"snapshots":             snapshotDocuments,
		"snapshot-log":          snapshotLog,
		"metadata-log":          []any{},
	}
	if refs != nil {
		document["refs"] = refs
	}
	data, err := json.Marshal(document)
	require.NoError(t, err)
	metadata, err := table.ParseMetadataBytes(data)
	require.NoError(t, err, "the fixture metadata must parse")
	return metadata
}

func TestSnapshotRetentionFor(t *testing.T) {
	day := 24 * time.Hour
	milliseconds := func(at time.Time) int64 { return at.UnixMilli() }
	snapshots := []testSnapshot{
		{id: 1, timestampMs: milliseconds(testNow.Add(-10 * day))},
		{id: 2, timestampMs: milliseconds(testNow.Add(-9 * day))},
		{id: 3, timestampMs: milliseconds(testNow.Add(-2 * day))},
		{id: 4, timestampMs: milliseconds(testNow.Add(-1 * time.Hour))},
	}
	retentionIDs := func(retention SnapshotRetention) (keep, expire []int64) {
		for _, snapshot := range retention.Keep {
			keep = append(keep, snapshot.SnapshotID)
		}
		for _, snapshot := range retention.Expire {
			expire = append(expire, snapshot.SnapshotID)
		}
		return keep, expire
	}

	t.Run("expires snapshots older than the window but keeps the newest two", func(t *testing.T) {
		metadata := retentionMetadata(t, snapshots, 4, nil, nil)
		retention := SnapshotRetentionFor(metadata, testNow, 5*day, 2)
		keep, expire := retentionIDs(retention)
		require.Equal(t, []int64{3, 4}, keep)
		require.Equal(t, []int64{1, 2}, expire)
	})

	t.Run("min-snapshots-to-keep protects old snapshots", func(t *testing.T) {
		metadata := retentionMetadata(t, snapshots, 4, nil, nil)
		retention := SnapshotRetentionFor(metadata, testNow, 5*day, 4)
		keep, expire := retentionIDs(retention)
		require.Equal(t, []int64{1, 2, 3, 4}, keep)
		require.Empty(t, expire)
	})

	t.Run("the current snapshot is kept even when it is old", func(t *testing.T) {
		old := []testSnapshot{
			{id: 1, timestampMs: milliseconds(testNow.Add(-30 * day))},
			{id: 2, timestampMs: milliseconds(testNow.Add(-25 * day))},
		}
		metadata := retentionMetadata(t, old, 1, nil, nil)
		retention := SnapshotRetentionFor(metadata, testNow, 5*day, 0)
		keep, expire := retentionIDs(retention)
		require.Contains(t, keep, int64(1))
		require.Equal(t, []int64{2}, expire)
	})

	t.Run("an iceberg ref protects its snapshot", func(t *testing.T) {
		metadata := retentionMetadata(t, snapshots, 4, nil, map[string]any{
			"release": map[string]any{"snapshot-id": 1, "type": "tag"},
		})
		retention := SnapshotRetentionFor(metadata, testNow, 5*day, 2)
		keep, expire := retentionIDs(retention)
		require.Contains(t, keep, int64(1))
		require.Equal(t, []int64{2}, expire)
	})

	t.Run("a snapshot exactly at the boundary is kept", func(t *testing.T) {
		boundary := []testSnapshot{
			{id: 1, timestampMs: milliseconds(testNow.Add(-5 * day))},
			{id: 2, timestampMs: milliseconds(testNow.Add(-5*day - time.Second))},
		}
		metadata := retentionMetadata(t, boundary, 1, nil, nil)
		retention := SnapshotRetentionFor(metadata, testNow, 5*day, 0)
		keep, expire := retentionIDs(retention)
		require.Contains(t, keep, int64(1))
		require.Equal(t, []int64{2}, expire)
	})

	t.Run("a snapshot with a future timestamp is kept", func(t *testing.T) {
		future := []testSnapshot{
			{id: 1, timestampMs: milliseconds(testNow.Add(24 * time.Hour))},
			{id: 2, timestampMs: milliseconds(testNow.Add(-30 * day))},
		}
		metadata := retentionMetadata(t, future, 1, nil, nil)
		retention := SnapshotRetentionFor(metadata, testNow, 5*day, 0)
		keep, expire := retentionIDs(retention)
		require.Equal(t, []int64{1}, keep)
		require.Equal(t, []int64{2}, expire)
	})

	t.Run("an empty table keeps nothing and expires nothing", func(t *testing.T) {
		metadata := retentionMetadata(t, nil, -1, nil, nil)
		retention := SnapshotRetentionFor(metadata, testNow, 5*day, 2)
		require.Empty(t, retention.Keep)
		require.Empty(t, retention.Expire)
	})
}

func TestSettingsFor(t *testing.T) {
	defaults := Config{MaxSnapshotAge: 5 * 24 * time.Hour, MinSnapshotsToKeep: 2}

	t.Run("defaults", func(t *testing.T) {
		metadata := retentionMetadata(t, nil, -1, nil, nil)
		settings, err := settingsFor(metadata, defaults)
		require.NoError(t, err)
		require.Equal(t, defaults.MaxSnapshotAge, settings.maxSnapshotAge)
		require.Equal(t, defaults.MinSnapshotsToKeep, settings.minSnapshotsToKeep)
		require.True(t, settings.gcEnabled)
	})

	t.Run("table properties override the defaults", func(t *testing.T) {
		metadata := retentionMetadata(t, nil, -1, map[string]string{
			PropertyMaxSnapshotAgeMs:   "3600000",
			PropertyMinSnapshotsToKeep: "7",
		}, nil)
		settings, err := settingsFor(metadata, defaults)
		require.NoError(t, err)
		require.Equal(t, time.Hour, settings.maxSnapshotAge)
		require.Equal(t, 7, settings.minSnapshotsToKeep)
	})

	t.Run("lakefs-gc.enabled=false disables the table", func(t *testing.T) {
		for _, value := range []string{"false", "False", " false "} {
			metadata := retentionMetadata(t, nil, -1, map[string]string{PropertyLakeFSGCEnabled: value}, nil)
			settings, err := settingsFor(metadata, defaults)
			require.NoError(t, err)
			require.False(t, settings.gcEnabled, "value %q", value)
		}
		metadata := retentionMetadata(t, nil, -1, map[string]string{PropertyLakeFSGCEnabled: "true"}, nil)
		settings, err := settingsFor(metadata, defaults)
		require.NoError(t, err)
		require.True(t, settings.gcEnabled)
	})

	t.Run("an unparsable property is an error", func(t *testing.T) {
		for _, properties := range []map[string]string{
			{PropertyMaxSnapshotAgeMs: "five days"},
			{PropertyMaxSnapshotAgeMs: "0"},
			{PropertyMinSnapshotsToKeep: "-1"},
			{PropertyMinSnapshotsToKeep: "many"},
		} {
			metadata := retentionMetadata(t, nil, -1, properties, nil)
			_, err := settingsFor(metadata, defaults)
			require.Error(t, err, "properties %v", properties)
		}
	})
}

// newExpireFixture returns a fake repository with one table whose four snapshots range from
// ten days old to one hour old.
func newExpireFixture(t *testing.T) (*fakeAPI, *testTable) {
	t.Helper()
	api := newFakeAPI()
	day := 24 * time.Hour
	fixture := newTestTable(t, api, fakeDefaultBranch, "sales", "orders", nil, []testSnapshot{
		{id: 101, timestampMs: testNow.Add(-10 * day).UnixMilli()},
		{id: 102, timestampMs: testNow.Add(-9 * day).UnixMilli()},
		{id: 103, timestampMs: testNow.Add(-2 * day).UnixMilli()},
		{id: 104, timestampMs: testNow.Add(-1 * time.Hour).UnixMilli()},
	})
	return api, fixture
}

func TestExpireRewritesAndStagesMetadata(t *testing.T) {
	api, fixture := newExpireFixture(t)
	runner := mustRunner(t, api, Config{Repository: fakeRepository, Now: func() time.Time { return testNow }, RunID: "expire-run"})

	result, err := runner.Run(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result.Expire)
	require.Equal(t, 1, result.Expire.Totals.Changed)
	require.Equal(t, 2, result.Expire.Totals.SnapshotsExpired)

	// The pointer was staged with a new metadata location, and that location is a metadata
	// file of the table, not of the old one.
	pointerEntry, ok := api.entry(fakeDefaultBranch, fixture.pointerPath)
	require.True(t, ok)
	pointer, err := icebergcatalog.DecodeTablePointer(pointerEntry.Metadata)
	require.NoError(t, err)
	require.NotEqual(t, fixture.metaFile, pointer.MetadataLocation)
	require.Contains(t, pointer.MetadataLocation, fixture.root+"/"+icebergcatalog.MetadataDirectory+"/")

	// The new metadata file exists, parses, and its snapshots are the retained ones.
	data, ok := api.objectAt(pointer.MetadataLocation)
	require.True(t, ok, "the new metadata object must exist at %q", pointer.MetadataLocation)
	metadata := parseMetadata(t, data)
	var snapshotIDs []int64
	for _, snapshot := range metadata.Snapshots() {
		snapshotIDs = append(snapshotIDs, snapshot.SnapshotID)
	}
	require.Equal(t, []int64{103, 104}, snapshotIDs)

	// The new metadata file is registered under the catalog's own file key, so the pointer
	// never references an unprotected object.
	entryPath, err := fileEntryPath(fixture.uuid, fixture.root, pointer.MetadataLocation)
	require.NoError(t, err)
	registration, ok := api.entry(fakeDefaultBranch, entryPath)
	require.True(t, ok, "the new metadata file must be registered at %q", entryPath)
	require.Equal(t, pointer.MetadataLocation, registration.PhysicalAddress)

	// The upload's own entry under _iceberg/... was removed again: the catalog's file
	// registration is the only one. (Uploads of reports are ordinary lakeFS objects and stay.)
	for _, upload := range api.uploads {
		if !strings.HasPrefix(upload, icebergcatalog.DataPrefix+"/") {
			continue
		}
		_, exists := api.entry(fakeDefaultBranch, upload)
		require.False(t, exists, "the temporary upload entry %q must be removed", upload)
	}

	// The report exists and is a mark of this schema version.
	reportData, ok := api.objectAt(fakeNamespace + "/" + reportPath(ExpireReportsPrefix, "expire-run", SummaryFileName))
	require.True(t, ok)
	var report ExpireReport
	require.NoError(t, json.Unmarshal(reportData, &report))
	require.Equal(t, ReportKindExpire, report.Kind)
	require.Equal(t, ReportVersion, report.Version)
	require.Equal(t, "expire-run", report.RunID)
	require.Equal(t, StatusExpired, report.Branches[0].Tables[0].Status)
	require.Equal(t, []int64{101, 102}, report.Branches[0].Tables[0].ExpiredIDs)

	// The combined run's sweep reclaims exactly the files the expire phase unreferenced -
	// the registrations of the two expired snapshots' manifest lists and manifests - and
	// nothing the retained snapshots still use.
	require.NotNil(t, result.Mark)
	require.NotNil(t, result.Sweep)
	require.Greater(t, result.Sweep.Totals.DeletedObjects, 0)
	for _, deleted := range api.deleted {
		for _, retained := range []string{"manifest-103", "manifest-104", "snap-103", "snap-104"} {
			require.NotContains(t, deleted.path, retained,
				"the sweep deleted the registration of a retained snapshot's file: %q", deleted.path)
		}
	}
}

func TestExpireDryRunWritesNothing(t *testing.T) {
	api, fixture := newExpireFixture(t)
	before := copyEntries(api.entries[fakeDefaultBranch])
	runner := mustRunner(t, api, Config{
		Repository: fakeRepository,
		DryRun:     true,
		Now:        func() time.Time { return testNow },
		RunID:      "dry-run",
	})

	result, err := runner.Run(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result.Expire)
	require.Equal(t, 1, result.Expire.Totals.Changed)
	require.Empty(t, api.uploads, "a dry run must upload nothing")
	require.Equal(t, before, api.entries[fakeDefaultBranch], "a dry run must stage nothing")

	// The mark phase still decided to sweep nothing (the retained files are protected), and
	// it wrote no report.
	require.NotNil(t, result.Mark)
	_, reportExists := api.objectAt(fakeNamespace + "/" + reportPath(SweepReportsPrefix, "dry-run", SummaryFileName))
	require.False(t, reportExists)
	_ = fixture
}

func TestExpireSkipsOptedOutTable(t *testing.T) {
	api := newFakeAPI()
	fixture := newTestTable(t, api, fakeDefaultBranch, "sales", "orders", map[string]string{
		PropertyLakeFSGCEnabled: "false",
	}, []testSnapshot{
		{id: 1, timestampMs: testNow.Add(-30 * 24 * time.Hour).UnixMilli()},
		{id: 2, timestampMs: testNow.Add(-time.Hour).UnixMilli()},
	})
	runner := mustRunner(t, api, Config{Repository: fakeRepository, Now: func() time.Time { return testNow }, RunID: "opt-out"})

	result, err := runner.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, result.Expire.Totals.Skipped)
	require.Zero(t, result.Expire.Totals.Changed)
	require.Equal(t, ReasonGCDisabled, result.Expire.Branches[0].Tables[0].Reason)
	requireNoTableFileUploads(t, api)
	require.Equal(t, fixture.metaFile, mustPointer(t, api, fixture.pointerPath).MetadataLocation)
}

func TestExpireSkipsExternalMetadata(t *testing.T) {
	api := newFakeAPI()
	fixture := newTestTable(t, api, fakeDefaultBranch, "sales", "orders", nil, []testSnapshot{
		{id: 1, timestampMs: testNow.Add(-30 * 24 * time.Hour).UnixMilli()},
		{id: 2, timestampMs: testNow.Add(-time.Hour).UnixMilli()},
	})
	// Point the table at a metadata file outside the repository's storage namespace: a
	// table registered from another catalog. The expire phase must not touch it.
	external := "s3://other-bucket/warehouse/orders/metadata/00000.metadata.json"
	pointerMetadata, err := icebergcatalog.EncodeTablePointer(icebergcatalog.TablePointer{
		MetadataLocation: external,
		TableUUID:        fixture.uuid.String(),
		FormatVersion:    2,
	})
	require.NoError(t, err)
	api.stageEntry(fakeDefaultBranch, ObjectInfo{
		Path:            fixture.pointerPath,
		PhysicalAddress: external,
		Checksum:        "etag",
		SizeBytes:       1,
		Mtime:           testNow,
		Metadata:        pointerMetadata,
	})
	runner := mustRunner(t, api, Config{Repository: fakeRepository, Now: func() time.Time { return testNow }, RunID: "external"})

	result, err := runner.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, result.Expire.Totals.Skipped)
	require.Equal(t, ReasonExternalMetadata, result.Expire.Branches[0].Tables[0].Reason)
	requireNoTableFileUploads(t, api)
}

func TestExpireAbortsOnV1EmbeddedManifests(t *testing.T) {
	api := newFakeAPI()
	fixture := newTestTable(t, api, fakeDefaultBranch, "sales", "orders", nil, []testSnapshot{
		{id: 1, timestampMs: testNow.Add(-30 * 24 * time.Hour).UnixMilli()},
	})
	manifestLocation := fixture.root + "/metadata/legacy-manifest.avro"
	api.putObject(manifestLocation, []byte("avro"))
	document := metadataV1Document(t, fixture, []string{manifestLocation})
	metadataLocation := fixture.root + "/metadata/00001-legacy.metadata.json"
	api.putObject(metadataLocation, document)
	entryPath, err := fileEntryPath(fixture.uuid, fixture.root, metadataLocation)
	require.NoError(t, err)
	api.stageEntry(fakeDefaultBranch, ObjectInfo{
		Path:            entryPath,
		PhysicalAddress: metadataLocation,
		Checksum:        "etag",
		SizeBytes:       int64(len(document)),
		Mtime:           testNow,
	})
	pointerMetadata, err := icebergcatalog.EncodeTablePointer(icebergcatalog.TablePointer{
		MetadataLocation: metadataLocation,
		TableUUID:        fixture.uuid.String(),
		FormatVersion:    1,
	})
	require.NoError(t, err)
	api.stageEntry(fakeDefaultBranch, ObjectInfo{
		Path:            fixture.pointerPath,
		PhysicalAddress: metadataLocation,
		Checksum:        "etag",
		SizeBytes:       int64(len(document)),
		Mtime:           testNow,
		Metadata:        pointerMetadata,
	})

	runner := mustRunner(t, api, Config{Repository: fakeRepository, Now: func() time.Time { return testNow }, RunID: "v1"})
	result, err := runner.Run(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEmbeddedManifests)
	require.NotNil(t, result.Expire)
	requireNoTableFileUploads(t, api)
	require.Empty(t, api.uploads, "the abort must happen before the first write")
	require.Empty(t, api.deleted)
}

func TestExpireLeavesConcurrentlyChangedTableAlone(t *testing.T) {
	api, fixture := newExpireFixture(t)
	runner := mustRunner(t, api, Config{Repository: fakeRepository, Now: func() time.Time { return testNow }, RunID: "race"})
	ctx := context.Background()
	require.NoError(t, runner.init(ctx))

	// Plan the table, then move its pointer the way a concurrent catalog commit would, and
	// check that applying the plan is refused instead of overwriting the new state.
	pointers, err := runner.listTablePointers(ctx, fakeDefaultBranch)
	require.NoError(t, err)
	require.Len(t, pointers, 1)
	action, tableReport, err := runner.planExpire(ctx, fakeDefaultBranch, pointers[0])
	require.NoError(t, err)
	require.NotNil(t, action)
	require.Equal(t, StatusExpired, tableReport.Status)

	concurrent := fixture.root + "/metadata/concurrent.metadata.json"
	pointerMetadata, err := icebergcatalog.EncodeTablePointer(icebergcatalog.TablePointer{
		MetadataLocation: concurrent,
		TableUUID:        fixture.uuid.String(),
		FormatVersion:    2,
	})
	require.NoError(t, err)
	api.stageEntry(fakeDefaultBranch, ObjectInfo{
		Path:            fixture.pointerPath,
		PhysicalAddress: concurrent,
		Checksum:        "etag",
		SizeBytes:       1,
		Mtime:           testNow,
		Metadata:        pointerMetadata,
	})

	err = runner.applyExpire(ctx, action)
	require.ErrorIs(t, err, errChangedConcurrently)
	require.Equal(t, concurrent, mustPointer(t, api, fixture.pointerPath).MetadataLocation)
	require.Empty(t, api.uploads, "a refused plan must not upload anything")
}

func TestRunnerValidation(t *testing.T) {
	api := newFakeAPI()
	for _, testCase := range []struct {
		name   string
		config Config
	}{
		{name: "no repository", config: Config{}},
		{name: "safety age too small", config: Config{Repository: fakeRepository, SafetyAge: time.Hour}},
		{name: "negative max age", config: Config{Repository: fakeRepository, MaxSnapshotAge: -time.Hour}},
		{name: "negative min keep", config: Config{Repository: fakeRepository, MinSnapshotsToKeep: -1}},
		{name: "mark and sweep only", config: Config{Repository: fakeRepository, MarkOnly: true, SweepOnly: true}},
		{name: "sweep only without mark id", config: Config{Repository: fakeRepository, SweepOnly: true}},
		{name: "mark id with mark only", config: Config{Repository: fakeRepository, MarkOnly: true, MarkID: "x"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := New(api, testCase.config)
			require.Error(t, err)
		})
	}

	t.Run("accepts the minimum safety age", func(t *testing.T) {
		_, err := New(api, Config{Repository: fakeRepository, SafetyAge: MinSafetyAge})
		require.NoError(t, err)
	})
}

func TestRunnerReportsMissingDefaultBranch(t *testing.T) {
	api := newFakeAPI()
	api.repo.DefaultBranch = ""
	runner := mustRunner(t, api, Config{Repository: fakeRepository, Now: func() time.Time { return testNow }})
	_, err := runner.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "default branch")
}

// requireNoTableFileUploads asserts that the run uploaded no table file (it may upload
// reports, which are ordinary lakeFS objects under _lakefs/retention/).
func requireNoTableFileUploads(t *testing.T, api *fakeAPI) {
	t.Helper()
	for _, upload := range api.uploads {
		require.False(t, strings.HasPrefix(upload, icebergcatalog.DataPrefix+"/"),
			"unexpected table file upload %q", upload)
	}
}

func mustRunner(t *testing.T, api *fakeAPI, config Config) *Runner {
	t.Helper()
	runner, err := New(api, config)
	require.NoError(t, err)
	return runner
}

func mustPointer(t *testing.T, api *fakeAPI, pointerPath string) icebergcatalog.TablePointer {
	t.Helper()
	entry, ok := api.entry(fakeDefaultBranch, pointerPath)
	require.True(t, ok, "pointer %q", pointerPath)
	pointer, err := icebergcatalog.DecodeTablePointer(entry.Metadata)
	require.NoError(t, err)
	return pointer
}

// TestExpireReportsAConcurrentChangeOnTheRightTable is the regression test for reporting by
// index instead of by pointer: the second table's plan must update the second table's report
// entry, not a stale copy of it.
func TestExpireReportsAConcurrentChangeOnTheRightTable(t *testing.T) {
	api := newFakeAPI()
	snapshots := []testSnapshot{
		{id: 1, timestampMs: testNow.Add(-30 * day).UnixMilli()},
		{id: 2, timestampMs: testNow.Add(-time.Hour).UnixMilli()},
	}
	orders := newTestTable(t, api, fakeDefaultBranch, "sales", "orders", nil, snapshots)
	returns := newTestTableWithUUID(t, api, fakeDefaultBranch, "sales", "returns",
		uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"), nil, snapshots)

	// Move the *first* table's pointer. Its plan is the one a by-pointer report would lose:
	// appending the second table's report reallocates the slice the first pointer refers to,
	// so an update through that pointer would go to a copy that never reaches the report.
	moved := false
	api.beforeStat = func(ref, path string) {
		if moved || path != orders.pointerPath {
			return
		}
		moved = true
		concurrent := orders.root + "/metadata/concurrent.metadata.json"
		pointerMetadata, err := icebergcatalog.EncodeTablePointer(icebergcatalog.TablePointer{
			MetadataLocation: concurrent,
			TableUUID:        orders.uuid.String(),
			FormatVersion:    2,
		})
		require.NoError(t, err)
		api.stageEntry(fakeDefaultBranch, ObjectInfo{
			Path:            orders.pointerPath,
			PhysicalAddress: concurrent,
			Checksum:        "etag",
			SizeBytes:       1,
			Mtime:           testNow,
			Metadata:        pointerMetadata,
		})
	}

	runner := mustRunner(t, api, Config{
		Repository:         fakeRepository,
		MinSnapshotsToKeep: 1,
		Now:                func() time.Time { return testNow },
		RunID:              "two-tables",
	})
	require.NoError(t, runner.init(context.Background()))
	report, err := runner.expire(context.Background())
	require.NoError(t, err)
	require.Len(t, report.Branches, 1)
	require.Len(t, report.Branches[0].Tables, 2)
	statuses := map[string]string{}
	reasons := map[string]string{}
	for _, table := range report.Branches[0].Tables {
		statuses[table.PointerPath] = table.Status
		reasons[table.PointerPath] = table.Reason
	}
	require.Equal(t, StatusSkipped, statuses[orders.pointerPath],
		"the first table's report entry must be updated, not a stale copy of it")
	require.Equal(t, ReasonChangedConcurrent, reasons[orders.pointerPath])
	require.Equal(t, StatusExpired, statuses[returns.pointerPath])
}
