package cleanup

// sweep_test.go covers the mark and sweep phases: which references protect a file, which
// candidates are marked, that the sweep deletes only registrations (never objects), and that
// a sweep re-derives the references instead of trusting its mark.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
)

const (
	day = 24 * time.Hour
)

// markFixture is a repository with one carrier table and one registration for every kind of
// protection the sweep must honor.
type markFixture struct {
	api           *fakeAPI
	table         *testTable
	gone          string // registration of a file no live reference protects: the only mark
	goneEntry     string
	young         string
	youngEntry    string
	stagedEntry   string
	taggedEntry   string
	retainedEntry string
	unknownEntry  string
}

// newMarkFixture builds the repository described in markFixture.
func newMarkFixture(t *testing.T) *markFixture {
	t.Helper()
	api := newFakeAPI()
	// A table with an old and a current snapshot: both data files are referenced by the
	// branch's current metadata, so neither may ever be marked.
	table := newTestTable(t, api, fakeDefaultBranch, "sales", "orders", nil, []testSnapshot{
		{id: 1, timestampMs: testNow.Add(-10 * day).UnixMilli()},
		{id: 2, timestampMs: testNow.Add(-1 * time.Hour).UnixMilli()},
	})
	require.Len(t, table.dataFiles, 2)
	for index, dataFile := range table.dataFiles {
		table.addRegistration(t, fakeDefaultBranch, "data/live-"+string(rune('a'+index))+".parquet", dataFile, testNow.Add(-30*day))
	}

	// A file no metadata references any more: the one file mark must decide to sweep.
	gone := table.root + "/data/gone.parquet"
	api.putObject(gone, []byte("data"))
	goneEntry := table.addRegistration(t, fakeDefaultBranch, "data/gone.parquet", gone, testNow.Add(-40*day))

	// A file younger than the safety age: registered but never swept.
	young := table.root + "/data/young.parquet"
	api.putObject(young, []byte("data"))
	youngEntry := table.addRegistration(t, fakeDefaultBranch, "data/young.parquet", young, testNow.Add(-time.Hour))

	// A staged registration: an uncommitted change the catalog already protects.
	stagedAddress := table.root + "/data/staged.parquet"
	api.putObject(stagedAddress, []byte("data"))
	stagedEntry := table.addRegistration(t, fakeDefaultBranch, "data/staged.parquet", stagedAddress, testNow.Add(-40*day))
	api.stageDiff(fakeDefaultBranch, stagedEntry, "added")

	// A tag on a commit outside the branch's chain whose table state references a file
	// registered on main. Only the tag protects it.
	api.setChain("tag-only", copyEntries(api.entries[fakeDefaultBranch]), CommitInfo{
		ID: "tag-only", CreationDate: testNow.Add(-20 * day),
	})
	taggedAddress := table.root + "/data/tagged.parquet"
	api.putObject(taggedAddress, []byte("data"))
	table.writeVersion(t, "tag-only", 2, 201, testNow.Add(-20*day).UnixMilli(), taggedAddress)
	taggedEntry := table.addRegistration(t, fakeDefaultBranch, "data/tagged.parquet", taggedAddress, testNow.Add(-40*day))
	api.addTag("release", "tag-only")

	// A GC-retained commit on main's first-parent chain whose table state references a file
	// registered on main. The commit is 2 days old, inside the 14-day retention period.
	mainEntries := copyEntries(api.entries[fakeDefaultBranch])
	api.setChain(fakeDefaultBranch, mainEntries,
		CommitInfo{ID: "head", CreationDate: testNow.Add(-time.Hour)},
		CommitInfo{ID: "retained", CreationDate: testNow.Add(-2 * day), Parents: []string{"old"}},
		CommitInfo{ID: "old", CreationDate: testNow.Add(-40 * day), Parents: []string{"older"}},
		CommitInfo{ID: "older", CreationDate: testNow.Add(-80 * day)},
	)
	retainedAddress := table.root + "/data/retained.parquet"
	api.putObject(retainedAddress, []byte("data"))
	table.writeVersion(t, "retained", 3, 301, testNow.Add(-2*day).UnixMilli(), retainedAddress)
	retainedEntry := table.addRegistration(t, fakeDefaultBranch, "data/retained.parquet", retainedAddress, testNow.Add(-40*day))

	// A table whose metadata cannot be read: its pointer names a metadata file that has no
	// catalog registration. Every registration of that table is protected.
	unknownUUID := uuid.MustParse("99999999-8888-7777-6666-555555555555")
	unknownRoot := api.repo.StorageNamespace + "/" + icebergcatalog.DataPrefix + "/" + unknownUUID.String()
	unknownEntry := addTableFileEntry(t, api, fakeDefaultBranch, unknownUUID, "data/x.parquet",
		unknownRoot+"/data/x.parquet", testNow.Add(-40*day))
	pointerMetadata, err := icebergcatalog.EncodeTablePointer(icebergcatalog.TablePointer{
		MetadataLocation: unknownRoot + "/metadata/missing.metadata.json",
		TableUUID:        unknownUUID.String(),
		FormatVersion:    2,
	})
	require.NoError(t, err)
	api.stageEntry(fakeDefaultBranch, ObjectInfo{
		Path:            pointerPathOf("sales", "broken"),
		PhysicalAddress: unknownRoot + "/metadata/missing.metadata.json",
		Checksum:        "etag",
		SizeBytes:       1,
		Mtime:           testNow,
		Metadata:        pointerMetadata,
	})

	return &markFixture{
		api:           api,
		table:         table,
		gone:          gone,
		goneEntry:     goneEntry,
		young:         young,
		youngEntry:    youngEntry,
		stagedEntry:   stagedEntry,
		taggedEntry:   taggedEntry,
		retainedEntry: retainedEntry,
		unknownEntry:  unknownEntry,
	}
}

func TestMarkProtectsEveryKindOfReference(t *testing.T) {
	fixture := newMarkFixture(t)
	runner := mustRunner(t, fixture.api, Config{
		Repository: fakeRepository,
		Now:        func() time.Time { return testNow },
		RunID:      "mark-1",
		MarkOnly:   true,
	})
	require.NoError(t, runner.init(context.Background()))

	report, marked, err := runner.mark(context.Background())
	require.NoError(t, err)
	require.Equal(t, ReportKindSweepMark, report.Kind)
	require.Equal(t, "mark-1", report.MarkID)
	require.Equal(t, 1, report.Totals.Marked, "only the unreferenced file may be marked")
	require.Len(t, marked, 1)
	require.Equal(t, normalizeAddress(fixture.gone), normalizeAddress(marked[0].Address))
	require.Equal(t, fixture.table.uuid.String(), marked[0].TableUUID)
	require.Len(t, marked[0].Registrations, 1)
	require.Equal(t, fakeDefaultBranch, marked[0].Registrations[0].Branch)
	require.Equal(t, fixture.goneEntry, marked[0].Registrations[0].Path)

	require.GreaterOrEqual(t, report.Totals.Referenced, 6, "branch, tag, retained, staged and live files are protected")
	require.Equal(t, 1, report.Totals.YoungerThanSafety)
	require.Equal(t, 1, report.Totals.UnknownTable)
	require.Equal(t, 1, report.Refs.UnknownTables)
	require.Equal(t, 1, report.Refs.Branches)
	require.Equal(t, 1, report.Refs.Tags)
	require.Equal(t, 1, report.Refs.StagedBranches)

	// The report and the object list are staged on the default branch.
	summaryData, ok := fixture.api.objectAt(fakeNamespace + "/" + reportPath(SweepReportsPrefix, "mark-1", SummaryFileName))
	require.True(t, ok)
	objectsData, ok := fixture.api.objectAt(fakeNamespace + "/" + reportPath(SweepReportsPrefix, "mark-1", ObjectsFileName))
	require.True(t, ok)
	objects, err := unmarshalMarkObjects(objectsData)
	require.NoError(t, err)
	require.Len(t, objects, 1)
	require.Equal(t, marked[0].Address, objects[0].Address)
	_ = summaryData

	// Mark deletes nothing.
	require.Empty(t, fixture.api.deleted)
}

func TestSweepDeletesOnlyMarkedRegistrations(t *testing.T) {
	fixture := newMarkFixture(t)
	markRunner := mustRunner(t, fixture.api, Config{
		Repository: fakeRepository,
		Now:        func() time.Time { return testNow },
		RunID:      "mark-1",
		MarkOnly:   true,
	})
	require.NoError(t, markRunner.init(context.Background()))
	_, _, err := markRunner.mark(context.Background())
	require.NoError(t, err)

	sweepRunner := mustRunner(t, fixture.api, Config{
		Repository: fakeRepository,
		Now:        func() time.Time { return testNow },
		RunID:      "sweep-1",
		SweepOnly:  true,
		MarkID:     "mark-1",
	})
	report, err := sweepRunner.Run(context.Background())
	require.NoError(t, err)
	require.NotNil(t, report.Sweep)
	require.Equal(t, "mark-1", report.Sweep.MarkID)
	require.Equal(t, 1, report.Sweep.Totals.Marked)
	require.Equal(t, 1, report.Sweep.Totals.DeletedObjects)
	require.Equal(t, 1, report.Sweep.Totals.DeletedEntries)
	require.Zero(t, report.Sweep.Totals.Failed)

	require.Len(t, fixture.api.deleted, 1)
	require.Equal(t, deletedEntry{branch: fakeDefaultBranch, path: fixture.goneEntry}, fixture.api.deleted[0])
	for _, path := range []string{
		fixture.youngEntry, fixture.stagedEntry, fixture.taggedEntry,
		fixture.retainedEntry, fixture.unknownEntry,
	} {
		_, exists := fixture.api.entry(fakeDefaultBranch, path)
		require.True(t, exists, "the registration %q must be kept", path)
	}
	// The physical object stays: the sweep removes registrations, lakeFS GC reclaims bytes.
	_, objectExists := fixture.api.objectAt(fixture.gone)
	require.True(t, objectExists)
}

func TestSweepRechecksReferencesInsteadOfTrustingTheMark(t *testing.T) {
	fixture := newMarkFixture(t)
	markRunner := mustRunner(t, fixture.api, Config{
		Repository: fakeRepository,
		Now:        func() time.Time { return testNow },
		RunID:      "mark-1",
		MarkOnly:   true,
	})
	require.NoError(t, markRunner.init(context.Background()))
	_, marked, err := markRunner.mark(context.Background())
	require.NoError(t, err)
	require.Len(t, marked, 1)

	// The table now references the marked file: write a new table state on main whose
	// snapshot contains it. A sweep that trusted its mark would delete a live file.
	fixture.table.writeVersion(t, fakeDefaultBranch, 4, 401, testNow.Add(-time.Minute).UnixMilli(), fixture.gone)

	sweepRunner := mustRunner(t, fixture.api, Config{
		Repository: fakeRepository,
		Now:        func() time.Time { return testNow },
		RunID:      "sweep-1",
		SweepOnly:  true,
		MarkID:     "mark-1",
	})
	report, err := sweepRunner.Run(context.Background())
	require.NoError(t, err)
	require.Zero(t, report.Sweep.Totals.DeletedObjects)
	require.Equal(t, 1, report.Sweep.Totals.Referenced)
	require.NotEmpty(t, report.Sweep.Skipped)
	require.Empty(t, fixture.api.deleted)

	entry, exists := fixture.api.entry(fakeDefaultBranch, fixture.goneEntry)
	require.True(t, exists)
	require.Equal(t, fixture.gone, entry.PhysicalAddress)
}

func TestSweepAppliesTheSafetyAgeItWasGiven(t *testing.T) {
	fixture := newMarkFixture(t)
	// A mark with the minimum safety age marks the 40-day-old file.
	markRunner := mustRunner(t, fixture.api, Config{
		Repository: fakeRepository,
		Now:        func() time.Time { return testNow },
		RunID:      "mark-1",
		MarkOnly:   true,
		SafetyAge:  MinSafetyAge,
	})
	require.NoError(t, markRunner.init(context.Background()))
	_, marked, err := markRunner.mark(context.Background())
	require.NoError(t, err)
	require.Len(t, marked, 1)

	// The sweep is configured with a longer safety age; the file is old enough for both,
	// so it is still swept. Then a sweep with a safety age longer than the file's age must
	// refuse it even though the mark named it.
	young := fixture.table.root + "/data/elder.parquet"
	fixture.table.addRegistration(t, fakeDefaultBranch, "data/elder.parquet", young, testNow.Add(-13*time.Hour))
	elderMarkRunner := mustRunner(t, fixture.api, Config{
		Repository: fakeRepository,
		Now:        func() time.Time { return testNow },
		RunID:      "mark-elder",
		MarkOnly:   true,
		SafetyAge:  MinSafetyAge,
	})
	require.NoError(t, elderMarkRunner.init(context.Background()))
	_, elderMarked, err := elderMarkRunner.mark(context.Background())
	require.NoError(t, err)
	require.Len(t, elderMarked, 2)

	sweepRunner := mustRunner(t, fixture.api, Config{
		Repository: fakeRepository,
		Now:        func() time.Time { return testNow },
		RunID:      "sweep-1",
		SweepOnly:  true,
		MarkID:     "mark-elder",
		SafetyAge:  72 * time.Hour,
	})
	report, err := sweepRunner.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, report.Sweep.Totals.DeletedObjects)
	require.Equal(t, 1, report.Sweep.Totals.YoungerThanSafety)
	require.Len(t, fixture.api.deleted, 1)
	_, exists := fixture.api.entry(fakeDefaultBranch, fixture.goneEntry)
	require.False(t, exists)
	elderEntry, err := icebergcatalog.TableFilePath(fixture.table.uuid, "data/elder.parquet")
	require.NoError(t, err)
	_, exists = fixture.api.entry(fakeDefaultBranch, elderEntry)
	require.True(t, exists)
}

func TestSweepRefusesAnUnusableMark(t *testing.T) {
	fixture := newMarkFixture(t)
	runner := mustRunner(t, fixture.api, Config{
		Repository: fakeRepository,
		Now:        func() time.Time { return testNow },
		RunID:      "sweep-1",
		SweepOnly:  true,
		MarkID:     "does-not-exist",
	})
	_, err := runner.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "mark")

	// A summary that belongs to another mark is refused too.
	other := MarkReport{
		Version: ReportVersion,
		Kind:    ReportKindSweepMark,
		MarkID:  "different",
	}
	data, err := marshalJSON(other)
	require.NoError(t, err)
	_, err = fixture.api.UploadObject(context.Background(), fakeRepository, fakeDefaultBranch,
		reportPath(SweepReportsPrefix, "does-not-exist", SummaryFileName), data)
	require.NoError(t, err)
	_, err = runner.Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "belongs to mark")
}

func TestMarkDryRunWritesNothing(t *testing.T) {
	fixture := newMarkFixture(t)
	runner := mustRunner(t, fixture.api, Config{
		Repository: fakeRepository,
		Now:        func() time.Time { return testNow },
		RunID:      "dry",
		DryRun:     true,
	})
	require.NoError(t, runner.init(context.Background()))
	report, marked, err := runner.mark(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, report.Totals.Marked)
	require.Len(t, marked, 1)
	require.Empty(t, fixture.api.uploads)
	for _, upload := range fixture.api.uploads {
		require.False(t, strings.HasPrefix(upload, "_lakefs/"), "dry run wrote %q", upload)
	}
}

// TestMarkProtectsWhenRetentionCannotBeDetermined covers the no-GC-rules fallback: without
// rules lakeFS never removes a commit, so every reachable commit is retained. A file that
// only a very old commit references is therefore protected, while a file no reference names
// is still marked.
func TestMarkProtectsWhenRetentionCannotBeDetermined(t *testing.T) {
	// With GC rules, a commit below the retention window is not retained, so a file only it
	// references is sweepable.
	newAncientFixture := func(t *testing.T) (*markFixture, string) {
		t.Helper()
		fixture := newMarkFixture(t)
		ancient := fixture.table.root + "/data/ancient.parquet"
		fixture.api.putObject(ancient, []byte("data"))
		fixture.table.writeVersion(t, "older", 5, 501, testNow.Add(-80*day).UnixMilli(), ancient)
		fixture.table.addRegistration(t, fakeDefaultBranch, "data/ancient.parquet", ancient, testNow.Add(-80*day))
		return fixture, ancient
	}

	t.Run("without rules every commit is retained", func(t *testing.T) {
		fixture, ancient := newAncientFixture(t)
		fixture.api.rulesErr = ErrGCRulesNotFound
		runner := mustRunner(t, fixture.api, Config{
			Repository: fakeRepository,
			Now:        func() time.Time { return testNow },
			RunID:      "mark-all",
			MarkOnly:   true,
		})
		require.NoError(t, runner.init(context.Background()))
		report, marked, err := runner.mark(context.Background())
		require.NoError(t, err)
		require.True(t, report.Refs.AllHistory)
		for _, object := range marked {
			require.NotEqual(t, normalizeAddress(ancient), normalizeAddress(object.Address),
				"a file of a reachable commit must be protected without GC rules")
		}
		require.Len(t, marked, 1, "only the file no reference names is marked")
	})

	t.Run("with rules a non-retained commit does not protect", func(t *testing.T) {
		fixture, ancient := newAncientFixture(t)
		runner := mustRunner(t, fixture.api, Config{
			Repository: fakeRepository,
			Now:        func() time.Time { return testNow },
			RunID:      "mark-rules",
			MarkOnly:   true,
		})
		require.NoError(t, runner.init(context.Background()))
		report, marked, err := runner.mark(context.Background())
		require.NoError(t, err)
		require.False(t, report.Refs.AllHistory)
		var addresses []string
		for _, object := range marked {
			addresses = append(addresses, normalizeAddress(object.Address))
		}
		require.Contains(t, addresses, normalizeAddress(ancient))
	})
}

// TestMarkProtectsAMergeSideBranch covers the last kind of commit lakeFS GC retains: a commit
// that is nobody's first parent (the tip of a merge side branch) is a GC starting point of its
// own, so a table state it carries is retained even though it is not on the branch's own
// first-parent chain.
func TestMarkProtectsAMergeSideBranch(t *testing.T) {
	api := newFakeAPI()
	table := newTestTable(t, api, fakeDefaultBranch, "sales", "orders", nil, []testSnapshot{
		{id: 1, timestampMs: testNow.Add(-1 * time.Hour).UnixMilli()},
	})

	// The side branch tip lives 10 days back (inside the 14-day default retention) and holds
	// a table state that references a file registered on main.
	api.setChain("side-tip", copyEntries(api.entries[fakeDefaultBranch]), CommitInfo{
		ID: "side-tip", CreationDate: testNow.Add(-10 * day),
	})
	sideFile := table.root + "/data/side.parquet"
	api.putObject(sideFile, []byte("data"))
	table.writeVersion(t, "side-tip", 2, 211, testNow.Add(-10*day).UnixMilli(), sideFile)
	table.addRegistration(t, fakeDefaultBranch, "data/side.parquet", sideFile, testNow.Add(-40*day))

	// main's head is a merge whose second parent is the side branch tip.
	api.setChain(fakeDefaultBranch, copyEntries(api.entries[fakeDefaultBranch]),
		CommitInfo{ID: "merge-head", CreationDate: testNow.Add(-time.Hour), Parents: []string{"main-parent", "side-tip"}},
		CommitInfo{ID: "main-parent", CreationDate: testNow.Add(-2 * day)},
	)

	runner := mustRunner(t, api, Config{
		Repository: fakeRepository,
		Now:        func() time.Time { return testNow },
		RunID:      "mark-merge",
		MarkOnly:   true,
	})
	require.NoError(t, runner.init(context.Background()))
	report, marked, err := runner.mark(context.Background())
	require.NoError(t, err)
	for _, object := range marked {
		require.NotEqual(t, normalizeAddress(sideFile), normalizeAddress(object.Address),
			"a merge side branch's table state must be protected")
	}
	require.Empty(t, marked)
	// The side branch tip was walked as its own GC starting point.
	require.GreaterOrEqual(t, report.Refs.RetainedCommits, 3)
}
