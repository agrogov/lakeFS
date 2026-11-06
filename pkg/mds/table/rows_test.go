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
	"fmt"
	"iter"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
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
