package table

import (
	"cmp"
	"fmt"
	"iter"
	"slices"
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
	schema, err := ArrowSchema(sc)
	if err != nil {
		return nil, err
	}
	return newPullRecordReader(schema, rowBatches(schema, rows, batchSize)), nil
}

// NewBatchesRecordReader returns a streaming reader over an existing batch source. It is how a
// scan's record batches are handed to a transaction without collecting them first (Compact).
//
// The caller owns the reader and must Release it.
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
	schema   *arrow.Schema
	next     func() (arrow.RecordBatch, error, bool)
	stop     func()
	current  arrow.RecordBatch
	err      error
	released bool
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
