package table

import (
	"cmp"
	"container/heap"
	"context"
	"fmt"
	"iter"
	"slices"
	"sort"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"
)

// The positions of the columns in the fixed schema of Schema, and therefore in every record batch
// this package builds. They are named because a record builder addresses columns by position, and
// a bare number in appendRow would be a second, silent copy of the schema's order.
const (
	columnRepository = iota
	columnPath
	columnCommitID
	columnSizeBytes
	columnLastModified
	columnETag
	columnUserMetadata
	columnCommitter
	columnContentType
	columnCreationDate
)

// DefaultRecordBatchSize is how many rows one Arrow record batch of this package carries. A
// rebuild of a large branch (design §4.3 step 2) is bounded by it: rows are built in batches
// instead of all at once, so memory follows the batch size and not the size of the change set.
const DefaultRecordBatchSize = 64 * 1024

// Rows is a row source: it yields the rows of a table in the order they should be written, and a
// non-nil error as its last value when the source fails.
//
// The convention for the error is the one iter.Seq2 needs: a value is a row when the error is nil,
// and a non-nil error is terminal. A source that pages the lakeFS API (design §4.3 step 2) yields
// one row at a time and stops on the first error instead of returning a partial success.
type Rows = iter.Seq2[Row, error]

// Row is one row of system.object_metadata (§3.1): one object on one branch of one data
// repository, as of the commit the table state describes.
//
// The optional fields are pointers because the table's columns are optional and "unset" is a
// different value from "the empty string": a commit with no committer (or an object with no
// content type) is stored as null, not as "". UserMetadata is required in the schema and is never
// null: an object without user metadata gets an empty map (§3.1).
type Row struct {
	// Repository is the data repository name: the same value for every row of a table (§3.1).
	Repository string
	// Path is the object key, and the table's identifier field.
	Path string
	// CommitID is the full ID of the commit of the data repository that last added or changed the
	// object on this branch (§3.2).
	CommitID string
	// SizeBytes is the object size.
	SizeBytes int64
	// LastModified is the object's modification time, in the commit being applied.
	LastModified time.Time
	// ETag is the object checksum, of the content only.
	ETag string
	// UserMetadata is the object's user metadata as lakeFS stores it; nil is stored as an empty
	// map (§3.1).
	UserMetadata map[string]string
	// Committer is the committer of CommitID; nil is stored as null.
	Committer *string
	// ContentType is the object's content type; nil is stored as null.
	ContentType *string
	// CreationDate is the creation time of the commit CommitID - not of the object (§3.1); nil is
	// stored as null.
	CreationDate *time.Time
}

// ArrowSchema returns the Arrow schema of Row for an Iceberg table schema, with field IDs so
// that iceberg-go matches columns by ID rather than by position (design §4.3 step 3:
// table.SchemaToArrowSchema(schema, nil, true, false)).
//
// The schema is the *table's* schema (Table.Schema()), not the constant this package creates the
// table with: take field IDs from the loaded table, never from the constants. The constants are the
// created table's IDs (see Schema), so the two agree at creation; the rule is what keeps a reader
// right if the catalog's own numbering, or a future schema change, ever differs.
func ArrowSchema(sc *iceberg.Schema) (*arrow.Schema, error) {
	schema, err := icebergtable.SchemaToArrowSchema(sc, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("convert the object_metadata schema to arrow: %w", err)
	}
	return schema, nil
}

// NewRecordReader returns a reader over rows, in batches of at most batchSize rows (a
// non-positive batchSize means DefaultRecordBatchSize).
//
// The rows are written in the order given: the caller that writes a table is responsible for
// sorting them by path, which is the table's sort order (§3.1). The caller owns the returned
// reader and must Release it.
func NewRecordReader(sc *iceberg.Schema, rows []Row, batchSize int) (array.RecordReader, error) {
	if batchSize <= 0 {
		batchSize = DefaultRecordBatchSize
	}
	if err := checkKnownSchema(sc); err != nil {
		return nil, err
	}
	schema, err := ArrowSchema(sc)
	if err != nil {
		return nil, err
	}

	batches := make([]arrow.RecordBatch, 0, (len(rows)+batchSize-1)/batchSize)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	for i, row := range rows {
		appendRow(builder, row)
		if (i+1)%batchSize == 0 {
			batches = append(batches, builder.NewRecordBatch())
			builder.Release()
			builder = array.NewRecordBuilder(memory.DefaultAllocator, schema)
		}
	}
	if len(rows)%batchSize != 0 {
		batches = append(batches, builder.NewRecordBatch())
	}
	builder.Release()

	reader, err := array.NewRecordReader(schema, batches)
	if err != nil {
		for _, batch := range batches {
			batch.Release()
		}
		return nil, fmt.Errorf("build a record reader: %w", err)
	}
	return reader, nil
}

// NewRowsRecordReader returns a streaming reader over a row source, without materialising the
// source: it pulls rows from rows and builds one record batch at a time.
//
// This is what makes Rebuild bounded by the batch size rather than by the size of the branch
// (design §4.3 step 2). The caller owns the returned reader and must Release it: the source runs
// in a goroutine (iter.Pull2) until the reader is released or exhausted.
func NewRowsRecordReader(sc *iceberg.Schema, rows Rows, batchSize int) (array.RecordReader, error) {
	if batchSize <= 0 {
		batchSize = DefaultRecordBatchSize
	}
	if err := checkKnownSchema(sc); err != nil {
		return nil, err
	}
	schema, err := ArrowSchema(sc)
	if err != nil {
		return nil, err
	}
	return newPullRecordReader(schema, rowBatches(schema, rows, batchSize)), nil
}

// NewBatchesRecordReader returns a streaming reader over an existing batch source: it is how a
// caller that already holds record batches - a scan, or a test that needs to write a specific order
// - hands them to a transaction without collecting them again.
//
// The batches must carry schema, which becomes the reader's schema. The reader takes ownership of
// every batch the source yields: it releases a batch when it moves to the next one and the batch it
// holds when it is released. The caller owns the reader and must Release it.
func NewBatchesRecordReader(schema *arrow.Schema, batches iter.Seq2[arrow.RecordBatch, error]) array.RecordReader {
	return newPullRecordReader(schema, batches)
}

// rowBatches converts a row source into a batch source of at most batchSize rows per batch.
func rowBatches(schema *arrow.Schema, rows Rows, batchSize int) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer builder.Release()
		count := 0
		stopped := false
		flush := func() bool {
			if count == 0 {
				return true
			}
			batch := builder.NewRecordBatch()
			builder.Release()
			builder = array.NewRecordBuilder(memory.DefaultAllocator, schema)
			count = 0
			if !yield(batch, nil) {
				batch.Release()
				stopped = true
				return false
			}
			return true
		}

		rows(func(row Row, err error) bool {
			if err != nil {
				stopped = true
				yield(nil, err)
				return false
			}
			appendRow(builder, row)
			count++
			if count == batchSize {
				return flush()
			}
			return true
		})
		if !stopped {
			flush()
		}
	}
}

// pullRecordReader is a record reader over a push iterator, pulled one batch at a time
// (iter.Pull2). It owns the current batch: moving to the next one and Release release it.
type pullRecordReader struct {
	schema *arrow.Schema
	next   func() (arrow.RecordBatch, error, bool)
	stop   func()
	// onRelease frees what the reader owns beyond the batches it yields - the streams a compaction
	// opened - and runs after the iterator has been stopped, so that no goroutine can touch a freed
	// stream (MDS_REVIEW_3.md R3T-7).
	onRelease func()
	current   arrow.RecordBatch
	err       error
	released  bool
}

func newPullRecordReader(schema *arrow.Schema, batches iter.Seq2[arrow.RecordBatch, error]) *pullRecordReader {
	next, stop := iter.Pull2(batches)
	return &pullRecordReader{schema: schema, next: next, stop: stop}
}

func (r *pullRecordReader) Schema() *arrow.Schema { return r.schema }

func (r *pullRecordReader) Next() bool {
	if r.released || r.err != nil {
		return false
	}
	batch, err, ok := r.next()
	if !ok {
		return false
	}
	if err != nil {
		r.err = err
		if batch != nil {
			batch.Release()
		}
		return false
	}
	if r.current != nil {
		r.current.Release()
	}
	r.current = batch
	return true
}

func (r *pullRecordReader) RecordBatch() arrow.RecordBatch { return r.current }

// Record is the deprecated name of RecordBatch (arrow.RecordReader).
func (r *pullRecordReader) Record() arrow.RecordBatch { return r.current }

func (r *pullRecordReader) Err() error { return r.err }

// Retain is a no-op: the reader is not reference counted, and every owner that shares it is the
// same owner. array.IterFromReader retains and releases around one iteration only.
func (r *pullRecordReader) Retain() {}

func (r *pullRecordReader) Release() {
	if r.released {
		return
	}
	r.released = true
	r.stop()
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.onRelease != nil {
		r.onRelease()
		r.onRelease = nil
	}
}

// appendRow appends one row to builder. The columns are addressed by position in the fixed schema
// of Schema, which is also the order of the fields of Row.
func appendRow(builder *array.RecordBuilder, row Row) {
	builder.Field(columnRepository).(*array.StringBuilder).Append(row.Repository)
	builder.Field(columnPath).(*array.StringBuilder).Append(row.Path)
	builder.Field(columnCommitID).(*array.StringBuilder).Append(row.CommitID)
	builder.Field(columnSizeBytes).(*array.Int64Builder).Append(row.SizeBytes)
	builder.Field(columnLastModified).(*array.TimestampBuilder).Append(arrow.Timestamp(row.LastModified.UnixMicro()))
	builder.Field(columnETag).(*array.StringBuilder).Append(row.ETag)

	appendUserMetadata(builder.Field(columnUserMetadata).(*array.MapBuilder), row.UserMetadata)

	appendOptionalString(builder.Field(columnCommitter).(*array.StringBuilder), row.Committer)
	appendOptionalString(builder.Field(columnContentType).(*array.StringBuilder), row.ContentType)
	if row.CreationDate == nil {
		builder.Field(columnCreationDate).(*array.TimestampBuilder).AppendNull()
	} else {
		builder.Field(columnCreationDate).(*array.TimestampBuilder).Append(arrow.Timestamp(row.CreationDate.UnixMicro()))
	}
}

// appendUserMetadata appends the map of one row. A nil map is an empty map, never a null: the
// column is required and a reader iterating it must not have to handle null (§3.1).
//
// The keys are written in sorted order so that the same row produces the same bytes, which makes
// two runs of the indexer over the same commit comparable.
func appendUserMetadata(builder *array.MapBuilder, metadata map[string]string) {
	builder.Append(true)
	if len(metadata) == 0 {
		return
	}
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, cmp.Compare)

	keyBuilder := builder.KeyBuilder().(*array.StringBuilder)
	valueBuilder := builder.ItemBuilder().(*array.StringBuilder)
	for _, key := range keys {
		keyBuilder.Append(key)
		valueBuilder.Append(metadata[key])
	}
}

// appendOptionalString appends a nullable string. The schema of the optional columns is a plain
// string builder (Arrow does not carry Iceberg's requiredness in the data), so null is AppendNull.
func appendOptionalString(builder *array.StringBuilder, value *string) {
	if value == nil {
		builder.AppendNull()
		return
	}
	builder.Append(*value)
}

// compactMergeRowBudget is the number of rows the compaction's merge may hold across every data
// file of the snapshot: the rows of one read batch per file. A file's read batch is
// compactMergeRowBudget / files, so a table of many files does not multiply the bound, and
// compactMergeMinFileRows keeps a batch large enough to decode parquet efficiently when a table has
// very many files (R2T-2).
//
// compactMergeRowBudget is a variable, not a constant, only so that a test can set it to a size a
// test table reaches (CompactMergeRowBudget in writer_internal_test.go). Production never changes
// it.
var compactMergeRowBudget = 2 * DefaultRecordBatchSize

// compactMergeMinFileRows is the floor of one file's read batch (see compactMergeRowBudget).
const compactMergeMinFileRows = 256

// compactFileBatchRows returns the number of rows one read batch of a compaction's file may carry,
// for a snapshot of files data files.
func compactFileBatchRows(files int) int {
	if files <= 0 {
		return compactMergeRowBudget
	}
	return max(compactMergeMinFileRows, compactMergeRowBudget/files)
}

// ---------------------------------------------------------------------------------------------
// The compaction read path
//
// A compaction keeps the table's declared path ASC order (§3.1, §4.9) without collecting the
// table: it merges the rows of the table's data files, one sorted stream per file, so the live set
// of the merge is one record batch per file and one record batch of output instead of every row of
// the table as a Row (R2T-2).
//
// The merge never decodes a row into the ten fields of Row and never builds a row field by field.
// It keeps the scan's own Arrow columns and emits slices of them, so a column this version of the
// writer does not know - an optional column a later schema version appended with a new ID, which
// design §3.1 allows - keeps its value through a compaction instead of panicking arrow's record
// builder with a column that has no values (R2T-1).
// ---------------------------------------------------------------------------------------------

// rowLoc is the position of one row: the record batch of a file that holds it and the row index
// inside that batch.
type rowLoc struct {
	batch arrow.RecordBatch
	row   int
}

// rowStream is one sorted stream of rows of a compaction: the live rows of one data file, in the
// table's declared path order.
type rowStream interface {
	// index is the stream's position in the compaction's file list. It breaks a tie between two
	// streams that carry the same path, so the merged order is deterministic.
	index() int
	// schema is the Arrow schema the stream's batches carry.
	schema() *arrow.Schema
	// currentPath is the path of the row the stream is positioned on.
	currentPath() string
	// currentRow is the location of that row.
	currentRow() rowLoc
	// held is how many rows of the stream the merge is holding: the rows of the record batch the
	// stream is positioned on that are still to be merged. It is what mergeRowsHeld reports.
	held() int
	// advance moves to the next row and reports whether there is one.
	advance() (bool, error)
	// release frees the stream: its file, its scan, its batches and its goroutine.
	release()
}

// compactReader returns a reader over the rows of tasks in the table's declared path order. It is
// what Compact appends, and it is a pull reader: the merge runs as the transaction pulls batches.
//
// Every file the writer produces is an ascending contiguous slice of the declared order - a delta
// sorts its upserts before writing them, a rebuild streams a sorted source and refuses an unsorted
// one, and a compaction since commit 2daeca114 merges sorted files - so a file can be streamed as
// one run of a k-way merge. The merge holds one read batch per file and one batch of output, and
// the per-file read batch is compactMergeRowBudget / files, so the rows held follow the number of
// files and not the number of rows of the table. A file written by a compaction before that commit
// may be [b d a c] across its batches instead: reading only the path column detects that, and such a
// file is read whole, sorted on its own and merged as a materialised run. The fallback costs the
// memory of every legacy file at once, not of one file: a k-way merge cannot stream a run whose rows
// are not already in order, so each legacy file stays resident until the merge is done. It is the
// memory of the legacy files only and it disappears once the table has been compacted once.
//
// The detection is a second read of every file, of its path column only, before the merge reads the
// rows. That is the price of knowing a file is sorted without collecting it; a table whose files are
// all sorted pays it too, and it is bounded by the same per-file read batch as the merge.
//
// The caller owns the reader and must Release it, whether or not a batch was pulled.
func compactReader(ctx context.Context, tbl *icebergtable.Table, tasks []icebergtable.FileScanTask) (*pullRecordReader, error) {
	streams, schema, err := openCompactionStreams(ctx, tbl, tasks)
	if err != nil {
		return nil, err
	}
	return newMergeReader(schema, streams), nil
}

// newMergeReader returns a pull reader over the merge of streams, which it owns: releasing the
// reader releases the streams, whether or not the merge ran. The merge's own defer cannot be the
// only place that frees them, because iter.Pull2 stops a body that was never started without running
// it - and Compact opens the reader before it calls ReplaceFiles and Append, either of which can
// fail before the first batch is pulled (MDS_REVIEW_3.md R3T-7).
func newMergeReader(schema *arrow.Schema, streams []rowStream) *pullRecordReader {
	reader := newPullRecordReader(schema, mergedBatches(schema, streams, compactRecordBatchSize))
	reader.onRelease = func() {
		for _, stream := range streams {
			stream.release()
		}
	}
	return reader
}

// openCompactionStreams opens one run per scan task and returns the Arrow schema they carry. A
// file whose live rows are in path order is streamed; any other file is materialised and sorted.
func openCompactionStreams(ctx context.Context, tbl *icebergtable.Table, tasks []icebergtable.FileScanTask) ([]rowStream, *arrow.Schema, error) {
	streams := make([]rowStream, 0, len(tasks))
	release := func() {
		for _, stream := range streams {
			stream.release()
		}
	}
	batchRows := compactFileBatchRows(len(tasks))
	var schema *arrow.Schema
	for i, task := range tasks {
		ordered, err := taskRowsInPathOrder(ctx, tbl, task, batchRows)
		if err != nil {
			release()
			return nil, nil, err
		}
		var stream rowStream
		if ordered {
			stream, err = newStreamedRun(ctx, tbl, task, i, batchRows)
		} else {
			stream, err = newMaterializedRun(ctx, tbl, task, i, batchRows)
		}
		if err != nil {
			release()
			return nil, nil, err
		}
		streams = append(streams, stream)
		schema = stream.schema()
	}
	if schema == nil {
		// No task, so no row: the caller appends nothing, and the schema of the reader is the
		// table's own projection. An empty plan is the only way to reach this branch now that a
		// streamed run stores the schema of its scan at open (R3T-8): a reader with rows always
		// declares the schema its batches carry. It is still needed by a caller that asks for the
		// schema of a reader over an empty table.
		scan := tbl.Scan()
		defer func() { _ = scan.Close() }()
		projection, err := scan.Projection()
		if err != nil {
			return nil, nil, fmt.Errorf("project the columns of the compaction: %w", err)
		}
		schema, err = ArrowSchema(projection)
		if err != nil {
			return nil, nil, err
		}
	}
	return streams, schema, nil
}

// taskRowsInPathOrder reports whether the live rows of one scan task ascend by path, reading only
// the path column. It is the detection of design §4.9's legacy file: a file this writer produced
// after commit 2daeca114 always answers true, and a file that answers false is materialised instead
// of merged as a stream (see compactReader).
//
// Equal paths are in order: path is the identifier field, so a table that carries a duplicate is
// invalid, but a compaction of it can still keep the table's declared order instead of refusing
// the table forever.
func taskRowsInPathOrder(ctx context.Context, tbl *icebergtable.Table, task icebergtable.FileScanTask, batchRows int) (bool, error) {
	scan := tbl.Scan(icebergtable.WithSelectedFields(ColumnPath), icebergtable.WithArrowBatchSize(batchRows))
	defer func() { _ = scan.Close() }()
	schema, batches, err := scan.ReadTasks(ctx, []icebergtable.FileScanTask{task})
	if err != nil {
		return false, fmt.Errorf("read the path column of %s: %w", task.File.FilePath(), err)
	}
	column, err := pathColumn(schema)
	if err != nil {
		return false, err
	}
	last := ""
	seen := false
	total := int64(0)
	ordered := true
	for batch, err := range batches {
		if err != nil {
			if batch != nil {
				batch.Release()
			}
			return false, fmt.Errorf("read the path column of %s: %w", task.File.FilePath(), err)
		}
		values := batch.Column(column)
		for i := range int(batch.NumRows()) {
			path, err := stringAt(values, i)
			if err != nil {
				batch.Release()
				return false, fmt.Errorf("read row %d of %s: %w", total+int64(i), task.File.FilePath(), err)
			}
			if seen && path < last {
				ordered = false
			}
			seen, last = true, path
		}
		total += batch.NumRows()
		batch.Release()
		if !ordered {
			// The answer cannot change back, and the rest of the file is not needed.
			break
		}
	}
	return ordered, nil
}

// streamedRun is one data file whose live rows ascend by path, read lazily: it holds the file's
// current record batch, not its rows.
type streamedRun struct {
	idx    int
	file   string
	scan   *icebergtable.Scan
	batch  *recordCursor
	column int
	path   string
	row    rowLoc
}

func newStreamedRun(ctx context.Context, tbl *icebergtable.Table, task icebergtable.FileScanTask, idx int, batchRows int) (*streamedRun, error) {
	scan := tbl.Scan(icebergtable.WithArrowBatchSize(batchRows))
	schema, batches, err := scan.ReadTasks(ctx, []icebergtable.FileScanTask{task})
	if err != nil {
		_ = scan.Close()
		return nil, fmt.Errorf("read %s: %w", task.File.FilePath(), err)
	}
	column, err := pathColumn(schema)
	if err != nil {
		_ = scan.Close()
		return nil, err
	}
	cursor := newRecordCursor(batches)
	// Store the scan's schema at open instead of waiting for the first batch: a streamed run is the
	// common case, so leaving it nil made the reader declare another schema - the projection, whose
	// fields carry the table's field IDs, while the scan's batches carry none - on every compaction
	// whose last file happened to be a streamed one (MDS_REVIEW_3.md R3T-8).
	cursor.schema = schema
	return &streamedRun{
		idx:    idx,
		file:   task.File.FilePath(),
		scan:   scan,
		batch:  cursor,
		column: column,
	}, nil
}

func (r *streamedRun) index() int            { return r.idx }
func (r *streamedRun) schema() *arrow.Schema { return r.batch.schema }
func (r *streamedRun) currentPath() string   { return r.path }
func (r *streamedRun) currentRow() rowLoc    { return r.row }
func (r *streamedRun) release()              { r.batch.release(); _ = r.scan.Close() }
func (r *streamedRun) held() int {
	if r.batch.batch == nil {
		return 0
	}
	return int(r.batch.batch.NumRows()) - r.batch.row
}

func (r *streamedRun) advance() (bool, error) {
	if !r.batch.advance() {
		if err := r.batch.err(); err != nil {
			return false, fmt.Errorf("read %s: %w", r.file, err)
		}
		return false, nil
	}
	path, err := stringAt(r.batch.batch.Column(r.column), r.batch.row)
	if err != nil {
		return false, fmt.Errorf("read row %d of %s: %w", r.batch.row, r.file, err)
	}
	r.path = path
	r.row = rowLoc{batch: r.batch.batch, row: r.batch.row}
	return true, nil
}

// materializedRun is one data file whose live rows do not ascend by path: the whole file is read,
// its rows are sorted by path, and the sorted rows are merged like any other run. Only a file
// written before commit 2daeca114 can need this.
type materializedRun struct {
	idx         int
	rows        []locatedRow
	batches     []arrow.RecordBatch
	next        int
	arrowSchema *arrow.Schema
}

// locatedRow is one row of a materialised run together with the path it was sorted by.
type locatedRow struct {
	path string
	row  rowLoc
}

func newMaterializedRun(ctx context.Context, tbl *icebergtable.Table, task icebergtable.FileScanTask, idx int, batchRows int) (*materializedRun, error) {
	scan := tbl.Scan(icebergtable.WithArrowBatchSize(batchRows))
	schema, batches, err := scan.ReadTasks(ctx, []icebergtable.FileScanTask{task})
	if err != nil {
		_ = scan.Close()
		return nil, fmt.Errorf("read %s: %w", task.File.FilePath(), err)
	}
	column, err := pathColumn(schema)
	if err != nil {
		_ = scan.Close()
		return nil, err
	}
	run := &materializedRun{idx: idx, arrowSchema: schema}
	readErr := func() error {
		row := 0
		for batch, err := range batches {
			if err != nil {
				if batch != nil {
					batch.Release()
				}
				return err
			}
			run.batches = append(run.batches, batch)
			values := batch.Column(column)
			for i := range int(batch.NumRows()) {
				path, err := stringAt(values, i)
				if err != nil {
					return fmt.Errorf("read row %d of %s: %w", row+i, task.File.FilePath(), err)
				}
				run.rows = append(run.rows, locatedRow{path: path, row: rowLoc{batch: batch, row: i}})
			}
			row += int(batch.NumRows())
		}
		return nil
	}()
	if readErr != nil {
		for _, batch := range run.batches {
			batch.Release()
		}
		_ = scan.Close()
		return nil, fmt.Errorf("read %s: %w", task.File.FilePath(), readErr)
	}
	if err := scan.Close(); err != nil {
		run.release()
		return nil, fmt.Errorf("close %s: %w", task.File.FilePath(), err)
	}
	sort.SliceStable(run.rows, func(i, j int) bool { return run.rows[i].path < run.rows[j].path })
	return run, nil
}

func (r *materializedRun) index() int            { return r.idx }
func (r *materializedRun) schema() *arrow.Schema { return r.arrowSchema }
func (r *materializedRun) currentPath() string   { return r.rows[r.next-1].path }
func (r *materializedRun) currentRow() rowLoc    { return r.rows[r.next-1].row }
func (r *materializedRun) held() int             { return len(r.rows) - r.next }
func (r *materializedRun) release() {
	for _, batch := range r.batches {
		batch.Release()
	}
	r.batches = nil
	r.rows = nil
}

func (r *materializedRun) advance() (bool, error) {
	if r.next >= len(r.rows) {
		return false, nil
	}
	r.next++
	return true, nil
}

// recordCursor walks the record batches of one stream, holding one batch at a time. It is the
// streaming half of a rowStream and the only place that owns a file's batches while they are read.
type recordCursor struct {
	schema *arrow.Schema
	next   func() (arrow.RecordBatch, error, bool)
	stop   func()
	batch  arrow.RecordBatch
	row    int
	fail   error
	done   bool
	freed  bool
}

func newRecordCursor(batches iter.Seq2[arrow.RecordBatch, error]) *recordCursor {
	next, stop := iter.Pull2(batches)
	return &recordCursor{next: next, stop: stop}
}

// advance positions the cursor on the next row, pulling the next batch when the current one is
// exhausted, and reports whether there is one.
func (c *recordCursor) advance() bool {
	if c.done || c.freed {
		return false
	}
	if c.batch != nil {
		// Step past the row the caller was on; row numbers are 1-based between calls.
		c.row++
	}
	for {
		if c.batch != nil && c.row < int(c.batch.NumRows()) {
			return true
		}
		if c.batch != nil {
			c.batch.Release()
			c.batch = nil
		}
		batch, err, ok := c.next()
		if err != nil {
			c.fail = err
			c.done = true
			return false
		}
		if !ok {
			c.done = true
			return false
		}
		if batch == nil {
			continue
		}
		c.schema = batch.Schema()
		if observe := streamBatchRows.Load(); observe != nil {
			(*observe)(int(batch.NumRows()))
		}
		c.batch, c.row = batch, 0
	}
}

func (c *recordCursor) err() error { return c.fail }

func (c *recordCursor) release() {
	if c.freed {
		return
	}
	c.freed = true
	if c.stop != nil {
		c.stop()
		c.stop = nil
	}
	if c.batch != nil {
		c.batch.Release()
		c.batch = nil
	}
}

// mergeRowsHeld is the test seam of the merge's memory bound: when it is not nil, mergedBatches
// reports, before every batch it emits, how many rows it is holding - the rows waiting to be
// written plus the rows of each stream's current record batch. Production leaves it nil, and the
// check costs one atomic load per batch (TestCompactHoldsABoundedNumberOfRows).
var mergeRowsHeld atomic.Pointer[func(held int)]

// streamBatchRows is the test seam of one stream's read batch: when it is not nil, every record
// batch a stream pulls from its file is reported with its row count, so a test can require the
// batch rows a file's stream actually sees instead of the sum the merge holds
// (TestCompactDividesTheMergeBudgetAcrossFiles, MDS_REVIEW_3.md R3T-4). Production leaves it nil,
// and the check costs one atomic load per pulled batch.
var streamBatchRows atomic.Pointer[func(rows int)]

// compactionAllocator allocates the record batches the merge builds by concatenating the scan's
// columns. It is a variable, not memory.DefaultAllocator, only so that a test can route it through a
// memory.CheckedAllocator and require a compaction to free every batch it built (CompactionAllocator
// in writer_internal_test.go, MDS_REVIEW_3.md R3T-7). Production never changes it.
var compactionAllocator memory.Allocator = memory.DefaultAllocator

// mergedBatches merges sorted runs into the table's declared path order, one record batch of at
// most batchSize rows at a time. The k-way merge holds one record batch per file (K is the number
// of files of the snapshot, not the number of rows) and one batch of output, so its memory does not
// follow the size of the table (R2T-2).
//
// The streams belong to the caller, not to the iterator: the body runs only if a batch is pulled, so
// it cannot be the only place that frees them (newMergeReader, R3T-7). The batches it retains for
// its own output it does release, in its own defer, on every path out of the body.
func mergedBatches(schema *arrow.Schema, streams []rowStream, batchSize int) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		// The writer this merge feeds runs on its own goroutine (iceberg-go pulls the reader inside
		// the goroutine of unpartitionedWrite, where a panic cannot be recovered by the indexer).
		// A merge must therefore never panic: anything unexpected is reported as the commit's error
		// (R2T-1), and the caller's transaction is aborted.
		defer func() {
			if recovered := recover(); recovered != nil {
				yield(nil, fmt.Errorf("%w: %v", ErrMergeRows, recovered))
			}
		}()
		if batchSize <= 0 {
			batchSize = DefaultRecordBatchSize
		}

		pending := make([]rowLoc, 0, batchSize)
		releasePending := func() {
			for _, loc := range pending {
				loc.batch.Release()
			}
			pending = pending[:0]
		}
		defer releasePending()
		flush := func() bool {
			if observe := mergeRowsHeld.Load(); observe != nil {
				held := len(pending)
				for _, stream := range streams {
					held += stream.held()
				}
				(*observe)(held)
			}
			batch, err := buildMergedBatch(schema, pending)
			releasePending()
			if err != nil {
				yield(nil, err)
				return false
			}
			if !yield(batch, nil) {
				batch.Release()
				return false
			}
			return true
		}

		ordered := make(streamHeap, 0, len(streams))
		for _, stream := range streams {
			ok, err := stream.advance()
			if err != nil {
				yield(nil, err)
				return
			}
			if ok {
				heap.Push(&ordered, stream)
			}
		}

		last := ""
		seen := false
		for ordered.Len() > 0 {
			stream := heap.Pop(&ordered).(rowStream)
			path := stream.currentPath()
			if seen && path < last {
				// Every run is sorted, so this cannot happen: report it instead of writing files
				// that contradict the table's declared order (§3.1).
				yield(nil, fmt.Errorf("%w: %q comes before %q", ErrRowsOutOfOrder, path, last))
				return
			}
			seen, last = true, path
			loc := stream.currentRow()
			loc.batch.Retain()
			pending = append(pending, loc)
			if len(pending) >= batchSize && !flush() {
				return
			}
			ok, err := stream.advance()
			if err != nil {
				yield(nil, err)
				return
			}
			if ok {
				heap.Push(&ordered, stream)
			}
		}
		if len(pending) > 0 {
			flush()
		}
	}
}

// streamHeap orders the runs of a merge by the path of the row each is positioned on. Two runs that
// carry the same path (a table with a duplicate path, which the identifier forbids) are ordered by
// the position of their file, so the output is deterministic.
type streamHeap []rowStream

func (h streamHeap) Len() int { return len(h) }
func (h streamHeap) Less(i, j int) bool {
	left, right := h[i].currentPath(), h[j].currentPath()
	if left != right {
		return left < right
	}
	return h[i].index() < h[j].index()
}
func (h streamHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *streamHeap) Push(x any)   { *h = append(*h, x.(rowStream)) }
func (h *streamHeap) Pop() any {
	old := *h
	n := len(old)
	last := old[n-1]
	*h = old[:n-1]
	return last
}

// buildMergedBatch builds one record batch of the merge out of the scan's own columns: a single run
// of rows of one source batch is a zero-copy slice, and several runs are concatenated column by
// column. Nothing is decoded field by field, so a column this writer does not know keeps its value
// (R2T-1), and every column of the output has exactly len(rows) rows, which is the invariant arrow's
// record builder used to panic on.
func buildMergedBatch(schema *arrow.Schema, rows []rowLoc) (arrow.RecordBatch, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("%w: a batch with no rows", ErrMergeRows)
	}
	// Group consecutive rows that come from the same source batch: one slice per group.
	type group struct {
		batch      arrow.RecordBatch
		start, end int
	}
	groups := make([]group, 0, len(rows))
	for _, loc := range rows {
		if n := len(groups); n > 0 && groups[n-1].batch == loc.batch && groups[n-1].end == loc.row {
			groups[n-1].end = loc.row + 1
			continue
		}
		groups = append(groups, group{batch: loc.batch, start: loc.row, end: loc.row + 1})
	}
	if len(groups) == 1 {
		return groups[0].batch.NewSlice(int64(groups[0].start), int64(groups[0].end)), nil
	}

	columns := make([]arrow.Array, schema.NumFields())
	for c := range columns {
		parts := make([]arrow.Array, 0, len(groups))
		for _, group := range groups {
			parts = append(parts, array.NewSlice(group.batch.Column(c), int64(group.start), int64(group.end)))
		}
		merged, err := array.Concatenate(parts, compactionAllocator)
		for _, part := range parts {
			part.Release()
		}
		if err != nil {
			for _, column := range columns[:c] {
				column.Release()
			}
			return nil, fmt.Errorf("concatenate column %q of the compaction: %w", schema.Field(c).Name, err)
		}
		columns[c] = merged
	}
	batch := array.NewRecordBatch(schema, columns, int64(len(rows)))
	for _, column := range columns {
		column.Release()
	}
	return batch, nil
}

// pathColumn returns the position of the path column in an Arrow schema. The column is found by
// name, not by position: a later schema version may append columns, and a table this writer does
// not know how to build rows for is still one a compaction can read.
func pathColumn(schema *arrow.Schema) (int, error) {
	indices := schema.FieldIndices(ColumnPath)
	if len(indices) == 0 {
		return 0, fmt.Errorf("%w: no %q column", ErrUnknownSchema, ColumnPath)
	}
	return indices[0], nil
}

// stringAt reads one cell of a string column. A null path is an error: path is the identifier field
// and the sort key, so a compaction cannot place a row that has none.
func stringAt(values arrow.Array, i int) (string, error) {
	if values.IsNull(i) {
		return "", fmt.Errorf("%w: row %d has no path", ErrMergeRows, i)
	}
	switch column := values.(type) {
	case *array.String:
		return column.Value(i), nil
	case *array.LargeString:
		return column.Value(i), nil
	case *array.StringView:
		return column.Value(i), nil
	case *array.Dictionary:
		return column.Dictionary().ValueStr(column.GetValueIndex(i)), nil
	default:
		return "", fmt.Errorf("%w: the path column is %T, not a string column", ErrMergeRows, values)
	}
}
