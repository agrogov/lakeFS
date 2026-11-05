package table

import (
	"fmt"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

// TestChunks: the IN lists of one delta are at most DefaultDeleteChunkSize paths each, and more
// than ten thousand paths split instead of becoming one predicate (design §4.3 step 3).
func TestChunks(t *testing.T) {
	paths := make([]string, 0, DefaultDeleteChunkSize+1)
	for i := range DefaultDeleteChunkSize + 1 {
		paths = append(paths, fmt.Sprintf("path-%06d", i))
	}

	got := chunks(paths, DefaultDeleteChunkSize)
	require.Len(t, got, 2)
	require.Len(t, got[0], DefaultDeleteChunkSize)
	require.Len(t, got[1], 1)
	require.Equal(t, paths, slices.Concat(got...), "chunking must not drop or reorder a path")

	require.Empty(t, chunks(nil, DefaultDeleteChunkSize))
	require.Equal(t, [][]string{{"a"}}, chunks([]string{"a"}, DefaultDeleteChunkSize))
}

// TestPathFilter: the copy-on-write fallback names the same paths as the merge-on-read chunks, one
// IN list per chunk joined with OR, and an empty path set is AlwaysFalse - never AlwaysTrue, which
// would replace the whole table.
func TestPathFilter(t *testing.T) {
	require.IsType(t, iceberg.AlwaysFalse{}, pathFilter(nil, DefaultDeleteChunkSize))

	// iceberg.IsIn reduces a one-value list to EQUAL, which names the same row.
	single := pathFilter([]string{"a"}, DefaultDeleteChunkSize)
	require.NotNil(t, single)
	require.Contains(t, single.String(), "path")
	require.NotContains(t, single.String(), "OR")

	joined := pathFilter([]string{"a", "b", "c", "d", "e"}, 2)
	require.NotNil(t, joined)
	require.Contains(t, joined.String(), "Or(")
	for _, path := range []string{"a", "b", "c", "d", "e"} {
		require.Contains(t, joined.String(), path)
	}
}

// TestChangedPaths: one predicate covers the upserts and the deletes, sorted and deduplicated, so
// a path that is both replaced and deleted appears once and a retried commit sends the same bytes.
func TestChangedPaths(t *testing.T) {
	rows := []Row{{Path: "b"}, {Path: "a"}, {Path: "b"}}
	require.Equal(t, []string{"a", "b", "c"}, changedPaths(rows, []string{"c", "a"}))
	require.Empty(t, changedPaths(nil, nil))
}

// CompactRecordBatchSize sets how many rows one record batch of a compaction carries and returns
// the function that restores the previous value.
//
// It is the test half of the multi-batch write path of Compact (MDS_REVIEW.md TB-2): iceberg-go
// sorts every record batch it is handed on its own, so a compaction that writes several batches is
// only in the table's declared path order if the writer sorted the rows before batching them. The
// alternative to this seam is a test table of 65 537 rows, because Compact takes no options and
// therefore no batch size (TB-4). It exists only in the test build of the package.
func CompactRecordBatchSize(size int) (restore func()) {
	previous := compactRecordBatchSize
	compactRecordBatchSize = size
	return func() { compactRecordBatchSize = previous }
}

// MergeRowsHeld installs an observer of the number of rows the compaction's merge holds and
// returns the function that removes it. The observer runs on the goroutine that feeds the
// transaction, before every record batch the merge emits.
//
// It is the test half of R2T-2's memory bound: writer_test.go compacts two tables of very different
// sizes and requires the observed number of held rows not to follow the row count.
func MergeRowsHeld(observe func(held int)) (restore func()) {
	mergeRowsHeld.Store(&observe)
	return func() { mergeRowsHeld.Store(nil) }
}

// CompactMergeRowBudget sets the number of rows the compaction's merge may hold across the files of
// a snapshot and returns the function that restores the previous value. It is the test half of
// R2T-2's memory bound: writer_test.go sets it so that a table of a few thousand rows already
// exceeds it, and requires the rows the merge holds not to follow the row count.
func CompactMergeRowBudget(rows int) (restore func()) {
	previous := compactMergeRowBudget
	compactMergeRowBudget = rows
	return func() { compactMergeRowBudget = previous }
}

// ExtraColumn is the name of the optional column SchemaWithExtraColumn appends.
const ExtraColumn = extraColumn

// SchemaWithExtraColumn returns Schema plus one optional string column with a new ID at the end -
// the state design §3.1 allows a later schema version to create, and the state iceberg-go's record
// builder used to panic on when a compaction rebuilt rows field by field (R2T-1).
func SchemaWithExtraColumn() *iceberg.Schema {
	fields := append(Schema().Fields(), iceberg.NestedField{
		ID: FieldCreationDate + 1, Name: ExtraColumn, Type: iceberg.StringType{}, Required: false,
	})
	return iceberg.NewSchemaWithIdentifiers(0, []int{FieldPath}, fields...)
}

// NewExtraColumnRecordReader returns a reader over rows with extra as the value of the optional
// column SchemaWithExtraColumn appends, i.e. a row written by a writer that knows the column this
// writer does not. The caller owns the reader and must Release it.
func NewExtraColumnRecordReader(sc *iceberg.Schema, rows []Row, extra []string) (array.RecordReader, error) {
	arrowSchema, err := ArrowSchema(sc)
	if err != nil {
		return nil, err
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	values := builder.Field(arrowSchema.NumFields() - 1).(*array.StringBuilder)
	for i, row := range rows {
		appendRow(builder, row)
		values.Append(extra[i])
	}
	batch := builder.NewRecordBatch()
	reader, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{batch})
	batch.Release()
	return reader, err
}
