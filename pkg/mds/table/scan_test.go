package table_test

// Reading a table back row by row.
//
// The tests decode the scan's record batches into the same Row values the writer was given, so an
// assertion is a comparison of Go values: a timestamp or a user-metadata map that a reader would
// interpret differently fails as a value difference rather than as an Arrow type difference. The
// decoder also pins the two representation invariants of the schema: user_metadata is an empty map
// and never null, and the optional columns are null exactly when the writer was given no value.

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

// scanRows returns every row of the table at the branch head, ordered by path.
func scanRows(t *testing.T, tbl *icebergtable.Table) []table.Row {
	t.Helper()
	scan := tbl.Scan()
	defer func() { _ = scan.Close() }()
	_, batches, err := scan.ToArrowRecords(t.Context())
	require.NoError(t, err, "scan the table")

	var rows []table.Row
	for batch, err := range batches {
		require.NoError(t, err)
		for i := range int(batch.NumRows()) {
			rows = append(rows, decodeRow(batch, i))
		}
		batch.Release()
	}
	slices.SortFunc(rows, func(a, b table.Row) int { return strings.Compare(a.Path, b.Path) })
	return rows
}

// requireScanRows asserts that the table holds exactly want, ordered by path.
func requireScanRows(t *testing.T, tbl *icebergtable.Table, want []table.Row) {
	t.Helper()
	slices.SortFunc(want, func(a, b table.Row) int { return strings.Compare(a.Path, b.Path) })
	require.Equal(t, want, scanRows(t, tbl))
}

// decodeRow reads row i of one record batch. The column positions are those of the fixed schema
// (design §3.1); the scan returns the table's columns in schema order.
func decodeRow(batch arrow.RecordBatch, i int) table.Row {
	return table.Row{
		Repository:   batch.Column(0).(*array.String).Value(i),
		Path:         batch.Column(1).(*array.String).Value(i),
		CommitID:     batch.Column(2).(*array.String).Value(i),
		SizeBytes:    batch.Column(3).(*array.Int64).Value(i),
		LastModified: timestampValue(batch.Column(4).(*array.Timestamp), i),
		ETag:         batch.Column(5).(*array.String).Value(i),
		UserMetadata: userMetadataValue(batch.Column(6).(*array.Map), i),
		Committer:    optionalString(batch.Column(7).(*array.String), i),
		ContentType:  optionalString(batch.Column(8).(*array.String), i),
		CreationDate: optionalTimestamp(batch.Column(9).(*array.Timestamp), i),
	}
}

func timestampValue(values *array.Timestamp, i int) time.Time {
	return time.UnixMicro(int64(values.Value(i))).UTC()
}

func optionalTimestamp(values *array.Timestamp, i int) *time.Time {
	if values.IsNull(i) {
		return nil
	}
	value := timestampValue(values, i)
	return &value
}

func optionalString(values *array.String, i int) *string {
	if values.IsNull(i) {
		return nil
	}
	value := values.Value(i)
	return &value
}

func userMetadataValue(values *array.Map, i int) map[string]string {
	if values.IsNull(i) {
		return nil
	}
	start, end := values.ValueOffsets(i)
	keys := values.Keys().(*array.String)
	items := values.Items().(*array.String)
	metadata := make(map[string]string, end-start)
	for j := start; j < end; j++ {
		metadata[keys.Value(int(j))] = items.Value(int(j))
	}
	return metadata
}
