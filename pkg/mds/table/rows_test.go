package table

// The unit tests of the compaction read path: the merge of the per-file sorted streams.
//
// The end-to-end proofs - a compaction of a real table, of a table with a column this writer does
// not know, and of a file written before commit 2daeca114 - are in writer_test.go, against the
// in-process lakeFS server. These tests pin the two invariants of the merge itself, which a real
// table is too coarse to isolate: every output column has as many rows as the batch claims (R2T-1,
// the panic arrow's record builder raised), and the number of rows the merge holds does not follow
// the number of rows of the table (R2T-2).

import (
	"errors"
	"fmt"
	"iter"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/hadoop"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// extraColumn is a column a later schema version appended with a new ID, as design §3.1 allows.
const extraColumn = "extra"

// schemaWithExtraColumn is the ten columns of Schema plus one optional string column, with the
// Arrow schema iceberg-go derives from it. Its ID follows creation_date's, exactly what a later
// MDS schema version would append.
func schemaWithExtraColumn(t *testing.T) (*iceberg.Schema, *arrow.Schema) {
	t.Helper()
	fields := append(Schema().Fields(), iceberg.NestedField{
		ID: FieldCreationDate + 1, Name: extraColumn, Type: iceberg.StringType{}, Required: false,
	})
	schema := iceberg.NewSchemaWithIdentifiers(0, []int{FieldPath}, fields...)
	arrowSchema, err := ArrowSchema(schema)
	require.NoError(t, err)
	return schema, arrowSchema
}

// batchWithExtra builds one record batch of the schema of schemaWithExtraColumn: the ten known
// columns of rows and, for each row, the value of the extra column.
func batchWithExtra(t *testing.T, arrowSchema *arrow.Schema, rows []Row, extras []string) arrow.RecordBatch {
	t.Helper()
	require.Len(t, extras, len(rows))
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	extra := builder.Field(arrowSchema.NumFields() - 1).(*array.StringBuilder)
	for i, row := range rows {
		appendRow(builder, row)
		extra.Append(extras[i])
	}
	return builder.NewRecordBatch()
}

// testRow is a row of a test batch: the path and the value of the extra column.
type testRow struct {
	path  string
	extra string
}

func testRows(t *testing.T, arrowSchema *arrow.Schema, rows []testRow) arrow.RecordBatch {
	t.Helper()
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	values := make([]Row, 0, len(rows))
	extras := make([]string, 0, len(rows))
	for _, row := range rows {
		values = append(values, Row{
			Repository: "data-repo", Path: row.path, CommitID: "c1", SizeBytes: 1,
			LastModified: when, ETag: "etag", UserMetadata: map[string]string{},
		})
		extras = append(extras, row.extra)
	}
	return batchWithExtra(t, arrowSchema, values, extras)
}

// TestBuildMergedBatchKeepsColumnsTheWriterDoesNotKnow is the unit test of R2T-1's fix: a batch of
// the merge is built from the scan's own columns, so a column this writer does not know keeps its
// values, and every column has exactly the number of rows the batch claims. The old compaction
// rebuilt rows field by field and left the unknown column empty; arrow's record builder then
// panicked with "some fields have excessive number of rows".
func TestBuildMergedBatchKeepsColumnsTheWriterDoesNotKnow(t *testing.T) {
	_, arrowSchema := schemaWithExtraColumn(t)
	first := testRows(t, arrowSchema, []testRow{{"a", "a-extra"}, {"c", "c-extra"}})
	defer first.Release()
	second := testRows(t, arrowSchema, []testRow{{"b", "b-extra"}, {"d", "d-extra"}})
	defer second.Release()

	// The merge order of the interleaved files: a and c from the first batch, b and d from the
	// second, i.e. no group of consecutive rows comes from one batch only.
	rows := []rowLoc{
		{batch: first, row: 0},
		{batch: second, row: 0},
		{batch: first, row: 1},
		{batch: second, row: 1},
	}
	batch, err := buildMergedBatch(arrowSchema, rows)
	require.NoError(t, err)
	defer batch.Release()

	require.EqualValues(t, len(rows), batch.NumRows())
	require.Equal(t, arrowSchema.NumFields(), int(batch.NumCols()), "the output carries every scan column")
	for c := range arrowSchema.NumFields() {
		require.EqualValues(t, len(rows), batch.Column(c).Len(),
			"column %q has a value per row", arrowSchema.Field(c).Name)
	}

	require.Equal(t, []string{"a", "b", "c", "d"}, columnStrings(t, batch, ColumnPath))
	require.Equal(t, []string{"a-extra", "b-extra", "c-extra", "d-extra"},
		columnStrings(t, batch, extraColumn), "the column the writer does not know keeps its values")
}

// TestBuildMergedBatchSlicesOneRun pins the zero-copy arm: rows that are consecutive in one source
// batch become a slice of it, with the same columns.
func TestBuildMergedBatchSlicesOneRun(t *testing.T) {
	_, arrowSchema := schemaWithExtraColumn(t)
	source := testRows(t, arrowSchema, []testRow{{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}})
	defer source.Release()

	batch, err := buildMergedBatch(arrowSchema, []rowLoc{
		{batch: source, row: 1}, {batch: source, row: 2},
	})
	require.NoError(t, err)
	defer batch.Release()

	require.EqualValues(t, 2, batch.NumRows())
	require.Equal(t, []string{"b", "c"}, columnStrings(t, batch, ColumnPath))
	require.Equal(t, []string{"2", "3"}, columnStrings(t, batch, extraColumn))
}

// columnStrings reads a string column of a batch as a []string.
func columnStrings(t *testing.T, batch arrow.RecordBatch, name string) []string {
	t.Helper()
	indices := batch.Schema().FieldIndices(name)
	require.NotEmpty(t, indices, "the batch has a %q column", name)
	column := batch.Column(indices[0]).(*array.String)
	values := make([]string, 0, column.Len())
	for i := range column.Len() {
		values = append(values, column.Value(i))
	}
	return values
}

// TestRecordCursorWalksEveryRow pins the cursor under the merge: every call to advance moves to the
// next row, and a batch boundary moves to the next batch. A cursor that did not step would spin
// forever on the first row, which is not a failure the merge can report.
func TestRecordCursorWalksEveryRow(t *testing.T) {
	_, arrowSchema := schemaWithExtraColumn(t)
	batches := []arrow.RecordBatch{
		testRows(t, arrowSchema, []testRow{{"a", "1"}, {"b", "2"}}),
		testRows(t, arrowSchema, []testRow{{"c", "3"}}),
	}
	cursor := newRecordCursor(iterateBatches(arrowSchema, batches))
	t.Cleanup(cursor.release)
	cursor.schema = arrowSchema

	var got []string
	for cursor.advance() {
		path, err := stringAt(cursor.batch.Column(pathColumnOf(t, arrowSchema)), cursor.row)
		require.NoError(t, err)
		got = append(got, path)
	}
	require.NoError(t, cursor.err())
	require.Equal(t, []string{"a", "b", "c"}, got, "the cursor visits every row of every batch once")
}

// iterateBatches is a batch source over a slice, the shape a scan's ReadTasks returns.
func iterateBatches(schema *arrow.Schema, batches []arrow.RecordBatch) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		for _, batch := range batches {
			if !yield(batch, nil) {
				return
			}
		}
	}
}

// pathColumnOf returns the position of the path column of an Arrow schema.
func pathColumnOf(t *testing.T, schema *arrow.Schema) int {
	t.Helper()
	column, err := pathColumn(schema)
	require.NoError(t, err)
	return column
}

// numberedStream is a sorted rowStream over a synthetic table of rows in batches of batchSize. It
// carries the schema of the merge and counts the rows of the batch it is holding, which is what the
// memory bound is about.
type numberedStream struct {
	idx         int
	arrowSchema *arrow.Schema
	batch       *recordCursor
	column      int
	path        string
	row         rowLoc
}

func newNumberedStream(t *testing.T, idx, rows, batchSize int) *numberedStream {
	t.Helper()
	arrowSchema, err := ArrowSchema(Schema())
	require.NoError(t, err)
	source := func(yield func(Row, error) bool) {
		when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
		for i := range rows {
			if !yield(Row{
				Repository: "data-repo", Path: fmt.Sprintf("path-%08d", i), CommitID: "c1",
				SizeBytes: 1, LastModified: when, ETag: "etag", UserMetadata: map[string]string{},
			}, nil) {
				return
			}
		}
	}
	batches := rowBatches(arrowSchema, source, batchSize)
	cursor := newRecordCursor(batches)
	return &numberedStream{
		idx: idx, arrowSchema: arrowSchema, batch: cursor,
		column: pathColumnOf(t, arrowSchema),
	}
}

func (s *numberedStream) index() int            { return s.idx }
func (s *numberedStream) schema() *arrow.Schema { return s.arrowSchema }
func (s *numberedStream) currentPath() string   { return s.path }
func (s *numberedStream) currentRow() rowLoc    { return s.row }
func (s *numberedStream) release()              { s.batch.release() }
func (s *numberedStream) held() int {
	if s.batch.batch == nil {
		return 0
	}
	return int(s.batch.batch.NumRows()) - s.batch.row
}

func (s *numberedStream) advance() (bool, error) {
	if !s.batch.advance() {
		return false, s.batch.err()
	}
	path, err := stringAt(s.batch.batch.Column(s.column), s.batch.row)
	if err != nil {
		return false, err
	}
	s.path = path
	s.row = rowLoc{batch: s.batch.batch, row: s.batch.row}
	return true, nil
}

// TestMergeHoldsABoundedNumberOfRows is the unit half of R2T-2: the merge holds one record batch
// per file and one batch of output, so the number of rows it holds is independent of the number of
// rows of the table. The compaction used to collect every row of the table as a Row (about 340 B
// each, and 1.5 KiB/row for the whole path) and hand them back one batch at a time.
func TestMergeHoldsABoundedNumberOfRows(t *testing.T) {
	const (
		batchSize = 100
		output    = 64
	)
	peak := func(rows int) int {
		arrowSchema, err := ArrowSchema(Schema())
		require.NoError(t, err)
		streams := []rowStream{
			newNumberedStream(t, 0, rows, batchSize),
			newNumberedStream(t, 1, rows, batchSize),
		}
		defer func() {
			for _, stream := range streams {
				stream.release()
			}
		}()
		var highest atomic.Int64
		restore := MergeRowsHeld(func(held int) {
			for {
				was := highest.Load()
				if int64(held) <= was || highest.CompareAndSwap(was, int64(held)) {
					return
				}
			}
		})
		defer restore()

		var count int
		for batch, err := range mergedBatches(arrowSchema, streams, output) {
			require.NoError(t, err)
			count += int(batch.NumRows())
			batch.Release()
		}
		require.Equal(t, 2*rows, count, "the merge yields every row")
		return int(highest.Load())
	}

	small := peak(1_000)
	large := peak(20_000)
	require.Equal(t, small, large,
		"the merge holds the same number of rows for 1 000 and 20 000 rows: %d", small)
	// One batch per file plus the pending output batch, and no more.
	require.LessOrEqual(t, large, 2*batchSize+output, "the merge holds a bounded number of rows")
}

// TestCompactFileBatchRows is the unit test of the merge's memory bound that MDS_REVIEW_3.md R3T-4
// asks for: the per-file read batch is the merge's row budget divided by the number of files, with a
// floor that keeps a very wide table decoding parquet in efficient batches. The floor is what makes
// the "rows follow the number of files" claim stop holding above
// compactMergeRowBudget/compactMergeMinFileRows files; the design's memory bound has to say so
// (R3T-5, and the commit message of this fix).
func TestCompactFileBatchRows(t *testing.T) {
	// The production budget: 2 × 64 Ki rows.
	require.Equal(t, 2*DefaultRecordBatchSize, compactMergeRowBudget)
	require.Equal(t, 131_072, compactFileBatchRows(0), "an empty plan has no division to do")
	require.Equal(t, 131_072, compactFileBatchRows(1), "one file may read the whole budget")
	require.Equal(t, 65_536, compactFileBatchRows(2))
	require.Equal(t, 256, compactFileBatchRows(512), "131072/512 is exactly the floor")
	require.Equal(t, 256, compactFileBatchRows(513), "131072/513 rounds down below the floor")
	require.Equal(t, 256, compactFileBatchRows(10_000), "the floor, not 13 rows")

	// A budget that divides evenly and one that does not, both above the floor.
	restore := CompactMergeRowBudget(3_000)
	defer restore()
	require.Equal(t, 3_000, compactFileBatchRows(1))
	require.Equal(t, 1_000, compactFileBatchRows(3))
	require.Equal(t, 750, compactFileBatchRows(4), "3000/4 is above the floor, so the budget decides")
	require.Equal(t, 333, compactFileBatchRows(9), "3000/9 rounds down and stays above the floor")
	require.Equal(t, 256, compactFileBatchRows(12), "250 is below the floor, so the floor decides")
}

// panickingStream is a rowStream whose advance panics: an unexpected failure inside the merge,
// which runs on iceberg-go's writer goroutine, where a panic would kill the indexer process.
type panickingStream struct{ idx int }

func (s *panickingStream) index() int             { return s.idx }
func (s *panickingStream) schema() *arrow.Schema  { return nil }
func (s *panickingStream) currentPath() string    { return "" }
func (s *panickingStream) currentRow() rowLoc     { return rowLoc{} }
func (s *panickingStream) held() int              { return 0 }
func (s *panickingStream) release()               {}
func (s *panickingStream) advance() (bool, error) { panic("a stream blew up") }

// TestMergeRowsConvertsAPanicToErrMergeRows is MDS_REVIEW_3.md R3T-6: the merge runs inside
// iceberg-go's writer goroutine, as the argument of iter.Pull2, where a panic cannot be recovered by
// the indexer and would kill the process. mergedBatches therefore recovers and reports ErrMergeRows
// as the iterator's error. Nothing tested that recover, that yield can be called from a deferred
// function during unwinding, or that Next returns false and Err carries the sentinel afterwards.
func TestMergeRowsConvertsAPanicToErrMergeRows(t *testing.T) {
	_, arrowSchema := schemaWithExtraColumn(t)
	reader := newMergeReader(arrowSchema, []rowStream{&panickingStream{idx: 0}})
	defer reader.Release()

	require.False(t, reader.Next(), "a panicking merge yields no batch")
	require.ErrorIs(t, reader.Err(), ErrMergeRows,
		"the panic is reported as the commit's error, not propagated")
	require.ErrorContains(t, reader.Err(), "a stream blew up", "the panic value is kept for the log")
	require.False(t, reader.Next(), "the reader stays finished after the error")
}

// heldBatchStream is a rowStream that holds one record batch, allocated from a given allocator, and
// releases it in release. It yields no row when it is pulled: it exists so that a test can see with
// a memory.CheckedAllocator whether the stream was freed when the reader was released without a
// pull.
type heldBatchStream struct {
	idx         int
	arrowSchema *arrow.Schema
	batch       arrow.RecordBatch
}

func newHeldBatchStream(t *testing.T, alloc memory.Allocator, schema *arrow.Schema, idx int) *heldBatchStream {
	t.Helper()
	builder := array.NewRecordBuilder(alloc, schema)
	appendRow(builder, Row{
		Repository: "data-repo", Path: fmt.Sprintf("path-%d", idx), UserMetadata: map[string]string{},
	})
	batch := builder.NewRecordBatch()
	builder.Release()
	return &heldBatchStream{idx: idx, arrowSchema: schema, batch: batch}
}

func (s *heldBatchStream) index() int             { return s.idx }
func (s *heldBatchStream) schema() *arrow.Schema  { return s.arrowSchema }
func (s *heldBatchStream) currentPath() string    { return "" }
func (s *heldBatchStream) currentRow() rowLoc     { return rowLoc{} }
func (s *heldBatchStream) held() int              { return 0 }
func (s *heldBatchStream) advance() (bool, error) { return false, nil }
func (s *heldBatchStream) release() {
	if s.batch != nil {
		s.batch.Release()
		s.batch = nil
	}
}

// TestCompactionReaderReleaseFreesStreamsWithoutAPull is R3T-7 of MDS_REVIEW_3.md: the streams a
// compaction opens used to be freed only by the merge's own defer, and iter.Pull2 does not run the
// body of an iterator that was stopped before its first next() (go's src/iter/iter.go: the coroutine
// returns at once when pull.done is set). Compact opens the reader and then runs ReplaceFiles and
// Append, so an error from either leaves the reader released-before-first-pull - and every scan, and
// for a legacy file every batch, still open. The reader owns the streams now, so its Release frees
// them whether the merge ran or not.
func TestCompactionReaderReleaseFreesStreamsWithoutAPull(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	arrowSchema, err := ArrowSchema(Schema())
	require.NoError(t, err)
	streams := []rowStream{
		newHeldBatchStream(t, alloc, arrowSchema, 0),
		newHeldBatchStream(t, alloc, arrowSchema, 1),
	}
	reader := newMergeReader(arrowSchema, streams)
	require.Positive(t, alloc.CurrentAlloc(),
		"the streams hold a batch each before the merge reads anything")

	// What Compact's deferred release does when ReplaceFiles or Append fails before a batch is pulled.
	reader.Release()

	alloc.AssertSize(t, 0)
}

// ---------------------------------------------------------------------------------------------
// The row source → batch adapter of Rebuild (rowBatches, NewRowsRecordReader):
// MDS_REVIEW_4.md R4T-2 (the builder's Release) and R4T-4 (a panic in the source).
// ---------------------------------------------------------------------------------------------

// localTableRepository is the data repository the rows of a localTable belong to.
const localTableRepository = "data-repo"

// localTable creates an empty system.object_metadata table in a temporary directory, through
// iceberg-go's hadoop catalog. A hadoop catalog is a filesystem warehouse with no server and no
// KV store, so a test of this package's internals can create, write and compact a real table
// without the in-process lakeFS fixture - which lives in the package's external test files
// (server_test.go) and cannot be reached from here.
//
// The table is created by the package's own CreateTable, so it carries the schema, the sort order
// and the properties of design §3.1; extra is merged over them the way the indexer's
// configuration would.
func localTable(t *testing.T, extra iceberg.Properties) *icebergtable.Table {
	t.Helper()
	cat, err := hadoop.NewCatalog("mds-test", t.TempDir(), iceberg.Properties{})
	require.NoError(t, err)
	require.NoError(t, cat.CreateNamespace(t.Context(), icebergtable.Identifier{Namespace}, nil),
		"create the catalog namespace")
	tbl, err := CreateTable(t.Context(), cat, localTableRepository, extra)
	require.NoError(t, err, "create the table")
	return tbl
}

// localRow returns one row of the localTableRepository for a path. The other columns are fixed so
// that a test compares paths and not values.
func localRow(path string) Row {
	return Row{
		Repository: localTableRepository, Path: path, CommitID: "c1", SizeBytes: 1,
		LastModified: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), ETag: "etag",
		UserMetadata: map[string]string{},
	}
}

// localRows returns one row per path.
func localRows(paths ...string) []Row {
	rows := make([]Row, 0, len(paths))
	for _, path := range paths {
		rows = append(rows, localRow(path))
	}
	return rows
}

// localTablePaths returns every path of the table at its current snapshot, in scan order.
func localTablePaths(t *testing.T, tbl *icebergtable.Table) []string {
	t.Helper()
	scan := tbl.Scan()
	defer func() { _ = scan.Close() }()
	_, batches, err := scan.ToArrowRecords(t.Context())
	require.NoError(t, err, "scan the table")
	paths := make([]string, 0)
	for batch, err := range batches {
		require.NoError(t, err)
		column := batch.Column(pathColumnOf(t, batch.Schema()))
		for i := range int(batch.NumRows()) {
			value, err := stringAt(column, i)
			require.NoError(t, err)
			paths = append(paths, value)
		}
		batch.Release()
	}
	return paths
}

// TestRowBatchesReleasesTheBuilderItHolds is MDS_REVIEW_4.md R4T-2. rowBatches installed
// `defer builder.Release()` on the record builder that existed when the body started and replaced
// the builder in flush after every batch. A deferred method value evaluates its receiver at the
// defer statement, so the first builder was released twice - once by flush and once by the defer -
// and the builder allocated after the last flush was never released at all. Rebuild takes this
// path on every row source, and so does the compaction's own unit test streams
// (newNumberedStream).
//
// The double release is invisible in the default build: array.RecordBuilder.Release only asserts,
// in arrow's assert build, that the refcount is positive (arrow/internal/debug/assert_on.go), and
// the default build compiles the assertion out. **Run this test with the assertion tag to see it
// catch the bug:**
//
//	go test -tags assert -count=1 -run TestRowBatchesReleasesTheBuilderItHolds ./pkg/mds/table/
//
// The tag cannot be used for the whole package: the existing table tests fail under it with
// "len for zstd CompressBound should be > 0", an unrelated upstream assertion in the Parquet
// writer, which is why the R4T-2 probe drove rowBatches directly and this test does the same.
//
// The memory.CheckedAllocator half guards the other direction - a builder that is never released
// while it holds values - on every way out of the body: a drained source, a source whose last batch
// ends exactly at the boundary, a consumer that stops after one batch, and a source that fails.
// It cannot see the double release, and on the first three cases it would have passed before the
// fix for the wrong reason: the builder the old code leaked there was the empty one allocated after
// the last flush, which holds no Arrow buffer. The failing-source case is the one where the leaked
// builder still holds a row, so that subtest fails in the default build too - with a leak of 2 304
// bytes in the checked allocator ("invalid memory size exp=0, got=2304").
func TestRowBatchesReleasesTheBuilderItHolds(t *testing.T) {
	arrowSchema, err := ArrowSchema(Schema())
	require.NoError(t, err)

	run := func(t *testing.T, rows int, fail bool, stopAfter, wantBatches int) {
		t.Helper()
		alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
		previous := rowBatchesAllocator
		rowBatchesAllocator = alloc
		t.Cleanup(func() { rowBatchesAllocator = previous })

		source := func(yield func(Row, error) bool) {
			for i := range rows {
				if !yield(localRow(fmt.Sprintf("path-%02d", i)), nil) {
					return
				}
			}
			if fail {
				yield(Row{}, errors.New("the row source failed"))
			}
		}

		var batches int
		// Batches of two rows, so a source of five rows flushes twice and ends with a partial
		// builder - exactly the shape the old defer got wrong.
		for batch, err := range rowBatches(arrowSchema, source, 2) {
			if err != nil {
				require.ErrorContains(t, err, "the row source failed")
				break
			}
			batches++
			if stopAfter > 0 && batches == stopAfter {
				// An iterator releases a batch whose yield it could not complete: the break makes
				// flush's yield return false, and flush frees the batch itself.
				break
			}
			batch.Release()
		}
		require.Equal(t, wantBatches, batches, "every batch the source completes reaches the consumer")
		alloc.AssertSize(t, 0)
	}

	t.Run("drained source", func(t *testing.T) { run(t, 5, false, 0, 3) })
	t.Run("last batch at the boundary", func(t *testing.T) { run(t, 4, false, 0, 2) })
	// The first two rows fill a batch and reach the consumer; the third row is still in the builder
	// when the source fails, so it is dropped by the error path - which is where the builder the old
	// defer never released had values in it.
	t.Run("source fails", func(t *testing.T) { run(t, 3, true, 0, 1) })
	t.Run("consumer stops after one batch", func(t *testing.T) { run(t, 5, false, 1, 1) })
}

// panickingRows returns a row source that yields one row per path and then panics - the failure of
// the R4T-4 probe (a write to a nil map), made explicit. panicked is set before the panic so a test
// can tell that the source was reached.
func panickingRows(panicked *atomic.Bool, paths ...string) Rows {
	return func(yield func(Row, error) bool) {
		for _, path := range paths {
			if !yield(localRow(path), nil) {
				return
			}
		}
		panicked.Store(true)
		panic("the row source blew up")
	}
}

// TestRowsRecordReaderConvertsAPanicOfTheSource is MDS_REVIEW_4.md R4T-4: the row source of a
// rebuild is pulled inside iceberg-go's writer goroutine (Append ranges over the reader in the
// goroutine of unpartitionedWrite, table/arrow_utils.go:2131-2141), so a panic there has no recover
// in this package or in the indexer above it and kills the process - unlike the compaction's merge,
// which converted its panics to ErrMergeRows (MDS_REVIEW_3.md R3T-6). rowBatches now uses the same
// shape and reports ErrRowsPanic as the reader's error.
//
// The assertion is on the reader and not on a pull loop of this test, because the reader's error is
// what Transaction.Append returns and therefore what Rebuild returns; pushing the reader here means
// a regression takes the test binary down, as it did for the probe.
func TestRowsRecordReaderConvertsAPanicOfTheSource(t *testing.T) {
	var panicked atomic.Bool
	// Two rows and a batch size of two: the panic is raised after a whole batch was handed out, so
	// the error has to arrive in the middle of the stream, not instead of it.
	reader, err := NewRowsRecordReader(Schema(), panickingRows(&panicked, "path-00", "path-01"), 2)
	require.NoError(t, err)
	defer reader.Release()

	require.True(t, reader.Next(), "the batch before the panic is readable")
	require.Equal(t, []string{"path-00", "path-01"},
		columnStrings(t, reader.RecordBatch(), ColumnPath))
	require.False(t, reader.Next(), "the source's panic yields no further batch")
	require.True(t, panicked.Load(), "the row source was reached")
	require.ErrorIs(t, reader.Err(), ErrRowsPanic,
		"the panic is reported as the write's error, not propagated")
	require.ErrorContains(t, reader.Err(), "the row source blew up", "the panic value is kept for the log")
	require.False(t, reader.Next(), "the reader stays finished after the error")
}

// TestRebuildConvertsAPanicOfTheSource is the end-to-end half of R4T-4: a real rebuild of a real
// table with a panicking source returns the error and the test binary survives, instead of dying
// inside iceberg-go's writer goroutine the way the probe's process did.
func TestRebuildConvertsAPanicOfTheSource(t *testing.T) {
	tbl := localTable(t, nil)

	var panicked atomic.Bool
	rebuilt, err := Rebuild(t.Context(), tbl, panickingRows(&panicked, "path-00"), "c1")
	require.True(t, panicked.Load(), "the row source was reached")
	require.Error(t, err, "Rebuild reports the panic instead of taking the process down")
	require.ErrorIs(t, err, ErrRowsPanic, "the reader's error is the commit's error")
	require.ErrorContains(t, err, "the row source blew up")
	require.Nil(t, rebuilt, "a failed rebuild returns no table")
}

// ---------------------------------------------------------------------------------------------
// The compaction's per-file read batch (MDS_REVIEW_4.md R4T-6).
// ---------------------------------------------------------------------------------------------

// localDataFile returns a data file of the localTable schema for a path; only the path is used by
// the scan-plan arithmetic these tests pin.
func localDataFile(t *testing.T, path string) iceberg.DataFile {
	t.Helper()
	file, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		path, iceberg.ParquetFile, nil, nil, nil, 1, 1)
	require.NoError(t, err, "build data file %q", path)
	return file.Build()
}

// TestCompactionFileBatchRowsCountsFiles is MDS_REVIEW_4.md R4T-6: design §4.9 says the merge
// holds "one read batch per data file, each max(256, 131072/files)", but the code divided the
// budget by the number of scan tasks - and iceberg-go plans several tasks for one Parquet file
// above read.split.target-size (table/scan_splits.go:55-60). A split file therefore shrank every
// file's batch below the documented size, by the number of splits, down to the 256-row floor.
//
// The tasks here have the shape the scan plans for a split file: one File, several Start/Length
// ranges. With the production budget, one file gets 131072 rows whatever the number of tasks, and
// two files get 65536.
func TestCompactionFileBatchRowsCountsFiles(t *testing.T) {
	splitFile := localDataFile(t, "local:///warehouse/system/object_metadata/data/00000-0.parquet")
	split := []icebergtable.FileScanTask{
		{File: splitFile, Start: 0, Length: 10},
		{File: splitFile, Start: 10, Length: 10},
		{File: splitFile, Start: 20, Length: 10},
		{File: splitFile, Start: 30, Length: 10},
	}
	require.Equal(t, 1, compactionFileCount(split), "the four tasks carry one data file")
	require.Equal(t, max(compactMergeMinFileRows, compactMergeRowBudget/1),
		compactionFileBatchRows(split))
	require.Equal(t, 131_072, compactionFileBatchRows(split),
		"§4.9: a file of a one-file table reads max(256, 131072/1) rows, split or not")

	otherFile := localDataFile(t, "local:///warehouse/system/object_metadata/data/00001-0.parquet")
	twoFiles := append(split, icebergtable.FileScanTask{File: otherFile, Start: 0, Length: 10})
	require.Equal(t, 2, compactionFileCount(twoFiles))
	require.Equal(t, 65_536, compactionFileBatchRows(twoFiles), "max(256, 131072/2)")

	require.Zero(t, compactionFileCount(nil))
	require.Equal(t, compactMergeRowBudget, compactionFileBatchRows(nil),
		"an empty plan has no division to do")
}

// TestCompactSizesTheReadBatchByFilesOnASplitTable is the end-to-end half of R4T-6, in the shape of
// TestCompactReplacesASplitFileOnce (writer_test.go, MDS_REVIEW_3.md R3T-1): a split target of one
// byte and a row group limit of 1 200 rows make the writer produce one data file of two row groups,
// which the scan plans as two tasks that carry the same file.
//
// The merge's budget is set to 1 000 rows, so the per-file read batch is comfortably inside a task
// (1 200 rows) and the batch the stream pulls is the batch the budget asks for. Before the fix the
// budget was divided by the two tasks (500 rows) and the streams pulled 500-row batches; the
// documented arithmetic - max(256, 131072/files) - divides by the one file and reads 1 000.
//
// Like every test that writes a data file, this one cannot run under -tags assert: an unrelated
// upstream assertion in the Parquet writer fails the write with "len for zstd CompressBound should
// be > 0" (see TestRowBatchesReleasesTheBuilderItHolds). The batches it counts are read back, so
// the reading half carries no assertion of its own.
func TestCompactSizesTheReadBatchByFilesOnASplitTable(t *testing.T) {
	const (
		files       = 1
		rows        = 2 * 1_200
		mergeBudget = 1_000
		// max(256, 131072/files), with the budget a test can reach.
		wantBatchRows = mergeBudget / files
	)
	restoreBudget := CompactMergeRowBudget(mergeBudget)
	t.Cleanup(restoreBudget)

	tbl := localTable(t, iceberg.Properties{
		icebergtable.ReadSplitTargetSizeKey:  "1",
		icebergtable.ParquetRowGroupLimitKey: "1200",
	})
	paths := make([]string, 0, rows)
	for i := range rows {
		paths = append(paths, fmt.Sprintf("path-%04d", i))
	}
	seeded, err := ApplyDelta(t.Context(), tbl, localRows(paths...), nil, "c1")
	require.NoError(t, err)

	scan := seeded.Scan()
	tasks, err := scan.PlanFiles(t.Context())
	require.NoError(t, err)
	require.NoError(t, scan.Close())
	dataFiles := map[string]struct{}{}
	for _, task := range tasks {
		dataFiles[task.File.FilePath()] = struct{}{}
	}
	require.Len(t, dataFiles, files, "the delta wrote one data file")
	require.Greater(t, len(tasks), len(dataFiles),
		"the scan plans more tasks than data files, so the file is split into ranges")

	var highest atomic.Int64
	unobserve := StreamBatchRows(func(observed int) {
		for {
			was := highest.Load()
			if int64(observed) <= was || highest.CompareAndSwap(was, int64(observed)) {
				return
			}
		}
	})
	defer unobserve()

	compacted, err := Compact(t.Context(), seeded, "c1")
	require.NoError(t, err)
	require.EqualValues(t, max(compactMergeMinFileRows, wantBatchRows), highest.Load(),
		"one read batch per data file, max(256, budget/files) rows, not budget/tasks")
	require.Equal(t, paths, localTablePaths(t, compacted), "the compaction keeps every row")
}
