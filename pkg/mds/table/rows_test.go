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
