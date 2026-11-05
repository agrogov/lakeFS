package table

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sort"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
)

const (
	// DefaultDeleteChunkSize is the largest number of paths one delete predicate names
	// (design §4.3 step 3: "The IN list is chunked (≤ 10 000 paths per Delete call) inside the
	// same transaction"). A change set larger than the rebuild threshold is a rebuild instead of
	// a delta (§4.3 step 2), so this bounds one predicate and not the work of one commit.
	DefaultDeleteChunkSize = 10_000
)

// compactRecordBatchSize is how many rows one record batch of a compaction carries. It is a
// variable, and not DefaultRecordBatchSize, only so that the package's tests can exercise the
// multi-batch write path of Compact without building 65 536 rows: CompactRecordBatchSize in
// writer_internal_test.go sets it for one test. Production never changes it.
var compactRecordBatchSize = DefaultRecordBatchSize

var (
	// ErrNoRepository is returned when the table carries no lakefs.mds.repository property: the
	// indexer cannot tell which data repository the rows belong to, and a snapshot without
	// lakefs.mds.source-repository would lose the provenance design §4.3 requires.
	ErrNoRepository = errors.New("the table has no lakefs.mds.repository property")

	// ErrNoSourceCommit is returned when a delta, a rebuild or a compaction is applied without a
	// source commit. The table property and every snapshot summary carry the commit (§4.3), and an
	// empty value would silently keep the indexer at an unknown position (§3.3).
	ErrNoSourceCommit = errors.New("no source commit")

	// ErrUnknownDeleteMode is returned for a write.delete.mode the writer does not implement.
	ErrUnknownDeleteMode = errors.New("unknown write.delete.mode")

	// ErrRowsOutOfOrder is returned when a row source of a rebuild yields a path that does not
	// come after the previous one. The table declares sort order path ASC (§3.1), and a rebuild
	// streams its source instead of collecting it (design §4.3 step 2), so the order of the files
	// it writes is the order of the source: rather than writing files that contradict the table's
	// declared order, a source that breaks it is refused (MDS_REVIEW.md TB-2).
	ErrRowsOutOfOrder = errors.New("rows are not in path order")

	// ErrNoTable is returned when a write is applied without a loaded table. The caller loads the
	// table from the catalog (design §4.2 step 2 and §4.4), so a nil table is a programming error
	// and not a state a write can report to the catalog.
	ErrNoTable = errors.New("no table")
)

// Option configures one write of this package. The zero set of options is the table's own
// behaviour: deletes follow the table's write.delete.mode, the IN lists are chunked at
// DefaultDeleteChunkSize and the rows are batched at DefaultRecordBatchSize.
type Option func(*writeOptions)

type writeOptions struct {
	deleteMode      string
	deleteChunkSize int
	recordBatchSize int
}

// WithDeleteMode overrides the manner in which deleted paths are removed for this write:
// WriteModeMergeOnRead writes position delete files (design §4.3 step 3, §10 decision 2) and
// WriteModeCopyOnWrite rewrites the data files that hold a deleted path (the fallback of §13 and
// the plan's risk list). The default is the table's own write.delete.mode property.
func WithDeleteMode(mode string) Option {
	return func(o *writeOptions) { o.deleteMode = mode }
}

// WithDeleteChunkSize overrides how many paths one delete predicate may name. Design §4.3 fixes
// the default at 10 000; the option exists so that a test can prove the chunking without building
// ten thousand rows.
func WithDeleteChunkSize(size int) Option {
	return func(o *writeOptions) { o.deleteChunkSize = size }
}

// WithRecordBatchSize overrides how many rows one Arrow record batch carries.
func WithRecordBatchSize(size int) Option {
	return func(o *writeOptions) { o.recordBatchSize = size }
}

// resolve returns the effective options of one write against tbl.
func resolve(tbl *icebergtable.Table, opts []Option) (writeOptions, error) {
	o := writeOptions{
		deleteMode:      tbl.Properties().Get(PropertyWriteDeleteMode, WriteModeCopyOnWrite),
		deleteChunkSize: DefaultDeleteChunkSize,
		recordBatchSize: DefaultRecordBatchSize,
	}
	for _, apply := range opts {
		apply(&o)
	}
	if o.deleteChunkSize <= 0 {
		o.deleteChunkSize = DefaultDeleteChunkSize
	}
	if o.recordBatchSize <= 0 {
		o.recordBatchSize = DefaultRecordBatchSize
	}
	switch o.deleteMode {
	case WriteModeMergeOnRead, WriteModeCopyOnWrite:
		return o, nil
	default:
		return writeOptions{}, fmt.Errorf("%w: %q", ErrUnknownDeleteMode, o.deleteMode)
	}
}

// repositoryOf returns the data repository a table belongs to: the lakefs.mds.repository property
// the table is created with (§3.1).
func repositoryOf(tbl *icebergtable.Table) (string, error) {
	repository := tbl.Properties().Get(PropertyRepository, "")
	if repository == "" {
		return "", ErrNoRepository
	}
	return repository, nil
}

// ApplyDelta applies one commit of a data repository to the table (design §4.3 step 3): the
// upserts replace the rows of their paths, the deletes remove theirs, and the table property
// lakefs.mds.source-commit records the commit that was applied.
//
// Everything happens in one Iceberg transaction and one commit:
//
//	tx.Delete(ctx, In("path", upserts ∪ deletes))   (chunked, merge-on-read)
//	tx.Append(ctx, upserts)
//	tx.SetProperties({"lakefs.mds.source-commit": sourceCommit})
//	tx.Commit(ctx)
//
// so a reader of the branch head sees the whole commit or none of it (§4.3: "Step 3 is the
// atomicity boundary for readers on the branch head"). Every snapshot the transaction adds
// carries lakefs.mds.source-commit and lakefs.mds.source-repository in its summary, and the
// table property carries the commit even when the change set is empty.
//
// An empty delta is not a no-op: the property still moves, which is what makes a commit that
// changed no object a recoverable indexing position (§4.3 step 3, §4.4).
//
// The deletes are applied in merge-on-read mode by default, which writes position delete files;
// WithDeleteMode(WriteModeCopyOnWrite) selects the fallback that rewrites the affected data files
// instead (design §13 risk "Delete files and catalog staging").
func ApplyDelta(ctx context.Context, tbl *icebergtable.Table, upserts []Row, deletes []string, sourceCommit string, opts ...Option) (*icebergtable.Table, error) {
	if tbl == nil {
		return nil, ErrNoTable
	}
	repository, err := repositoryOf(tbl)
	if err != nil {
		return nil, err
	}
	if sourceCommit == "" {
		return nil, ErrNoSourceCommit
	}
	o, err := resolve(tbl, opts)
	if err != nil {
		return nil, err
	}
	snapshotProps := SnapshotProperties(repository, sourceCommit)

	rows := slices.Clone(upserts)
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Path < rows[j].Path })
	paths := changedPaths(rows, deletes)

	txn := tbl.NewTransaction()
	switch o.deleteMode {
	case WriteModeMergeOnRead:
		for _, chunk := range chunks(paths, o.deleteChunkSize) {
			if err := txn.Delete(ctx, iceberg.IsIn(iceberg.Reference(ColumnPath), chunk...), snapshotProps); err != nil {
				return nil, fmt.Errorf("delete %d paths: %w", len(chunk), err)
			}
		}
		if len(rows) > 0 {
			reader, err := NewRecordReader(tbl.Schema(), rows, o.recordBatchSize)
			if err != nil {
				return nil, err
			}
			appendErr := txn.Append(ctx, reader, snapshotProps)
			reader.Release()
			if appendErr != nil {
				return nil, fmt.Errorf("append %d rows: %w", len(rows), appendErr)
			}
		}
	case WriteModeCopyOnWrite:
		// One Overwrite both removes the old rows and writes the new ones, so the upserts must not
		// be appended in a second step: that would duplicate them. With no paths to replace, an
		// append is enough - and it is not an overwrite, which without a filter would replace the
		// whole table.
		switch {
		case len(paths) > 0:
			reader, err := NewRecordReader(tbl.Schema(), rows, o.recordBatchSize)
			if err != nil {
				return nil, err
			}
			overwriteErr := txn.Overwrite(ctx, reader, snapshotProps,
				icebergtable.WithOverwriteFilter(pathFilter(paths, o.deleteChunkSize)))
			reader.Release()
			if overwriteErr != nil {
				return nil, fmt.Errorf("overwrite %d paths: %w", len(paths), overwriteErr)
			}
		case len(rows) > 0:
			reader, err := NewRecordReader(tbl.Schema(), rows, o.recordBatchSize)
			if err != nil {
				return nil, err
			}
			appendErr := txn.Append(ctx, reader, snapshotProps)
			reader.Release()
			if appendErr != nil {
				return nil, fmt.Errorf("append %d rows: %w", len(rows), appendErr)
			}
		}
	}

	if err := txn.SetProperties(iceberg.Properties{PropertySourceCommit: sourceCommit}); err != nil {
		return nil, fmt.Errorf("record the source commit: %w", err)
	}
	committed, err := txn.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("commit the delta at %s: %w", sourceCommit, err)
	}
	return committed, nil
}

// Rebuild replaces the whole table with rows at sourceCommit (design §4.3 step 2): the rows of a
// full listing of the data repository at the commit, written with one transaction that removes
// every file of the current snapshot and appends them.
//
// It is the path taken when a change set is larger than rebuild_threshold (the caller decides
// that; this function only writes), when a branch is first indexed (§4.5 step 2) and when a
// history rewrite forces a fresh listing (§4.7). The rows are streamed: the source runs as the
// batches are pulled, so memory is bounded by the batch size rather than by the branch.
//
// The removal is ReplaceFiles over every live data file and every live delete file of the current
// snapshot, followed by an Append. A plain Overwrite with no filter is not enough: iceberg-go
// drives its copy-on-write deletion from the filter, so it deletes and rewrites the data files
// and leaves their position delete files in the snapshot. Those files would then be referenced by
// the live snapshot, so no cleanup could reclaim them, and the snapshot summary's
// total-delete-files - the counter of §4.9's compaction trigger - would never fall back to zero
// (TB-1). The rows written are the same either way; a whole-table rewrite replaces the rows those
// delete files applied to.
//
// The rows are written in the order the source yields them, which is the table's declared sort
// order (§3.1: path ASC): the source is streamed, so the writer cannot sort it without collecting
// the branch in memory, and a source that breaks the order is refused with ErrRowsOutOfOrder
// rather than written into files that contradict the declared order (MDS_REVIEW.md TB-2).
func Rebuild(ctx context.Context, tbl *icebergtable.Table, rows Rows, sourceCommit string, opts ...Option) (*icebergtable.Table, error) {
	if tbl == nil {
		return nil, ErrNoTable
	}
	repository, err := repositoryOf(tbl)
	if err != nil {
		return nil, err
	}
	if sourceCommit == "" {
		return nil, ErrNoSourceCommit
	}
	o, err := resolve(tbl, opts)
	if err != nil {
		return nil, err
	}
	snapshotProps := SnapshotProperties(repository, sourceCommit)

	reader, err := NewRowsRecordReader(tbl.Schema(), orderedRows(rows), o.recordBatchSize)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	txn := tbl.NewTransaction()
	if err := replaceSnapshotFiles(ctx, txn, tbl, snapshotProps); err != nil {
		return nil, fmt.Errorf("replace the files of the table at %s: %w", sourceCommit, err)
	}
	if err := txn.Append(ctx, reader, snapshotProps); err != nil {
		return nil, fmt.Errorf("append the rows of the rebuild at %s: %w", sourceCommit, err)
	}
	if err := txn.SetProperties(iceberg.Properties{PropertySourceCommit: sourceCommit}); err != nil {
		return nil, fmt.Errorf("record the source commit: %w", err)
	}
	committed, err := txn.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("commit the rebuild at %s: %w", sourceCommit, err)
	}
	return committed, nil
}

// Compact rewrites the table at its current snapshot with the rows it already holds (design §4.9):
// the table keeps its logical contents under the same lakefs.mds.source-commit, and the position
// delete files the delta path accumulated are removed.
//
// The transaction has two updates. ReplaceFiles removes every data file of the scan plan and every
// delete file of the current snapshot without adding files - it is the compaction primitive of
// iceberg-go's transaction, and the only one that removes delete files - and Append writes the
// scanned rows back. A plain Overwrite (design §4.9's wording) rewrites the data files but leaves
// the delete files in place: iceberg-go's overwrite producer removes only the files it matches, so
// the delete files would keep accumulating and keep being read. The rows are identical either way,
// which is what §4.9 asks the compaction to preserve; deleting the files is what makes it a
// compaction.
//
// The delete files are enumerated from the snapshot's manifest list, not from the scan tasks: a
// scan task carries only the delete files that apply to the data file it reads, so a delete file
// whose data files were replaced by a Rebuild (§4.3 step 2, §4.5, §4.7) is carried by no task. It
// would then stay in the manifest list forever - referenced by the live snapshot, so no file
// cleanup could reclaim it - and the snapshot summary's total-delete-files, which is exactly what
// §4.9's delete-file trigger compares, would never fall back to zero (TB-1).
//
// The rows are read through a scan and re-sorted by path before they are written back: the scan
// plans the table's data files in the order they were committed, not in the table's declared sort
// order, and iceberg-go sorts each record batch it is handed on its own ("Sort each batch
// independently before writing. This is per-batch, not per-file", table/rolling_data_writer.go),
// so streaming the scan's batches straight back into Append writes files of [b d a c] for the data
// files [a c] and [b d] - a table whose files no longer carry the `path ASC` order the table
// metadata advertises (§3.1; the iceberg spec's sort order is what writers honour, MDS_REVIEW.md
// TB-2). Sorting the rows first and batching them after makes every batch a contiguous slice of
// the sorted table, so the batches the writer sorts are already in the declared order. Two commits
// of interleaved paths compact into one file of [a b c d], and the per-file lower/upper bounds on
// path stay narrow, which is what the next delta's IN (path ...) scan prunes with. Sorting needs
// the rows, not the batches: they are collected as rows and handed to the writer one batch at a
// time (rowBatches), so the peak memory is the table's rows plus one record batch. The caller
// commits R-metadata afterwards with the "mds: compact" message and without a new tag (§4.9, M4's
// maintenance task).
//
// Compact takes no Option: it removes and re-adds whole files and writes no delete predicate, so
// the delete mode and the delete chunk size of a delta do not apply, and the batch size is the
// one rowBatches uses (MDS_REVIEW.md TB-4).
func Compact(ctx context.Context, tbl *icebergtable.Table, sourceCommit string) (*icebergtable.Table, error) {
	if tbl == nil {
		return nil, ErrNoTable
	}
	repository, err := repositoryOf(tbl)
	if err != nil {
		return nil, err
	}
	if sourceCommit == "" {
		return nil, ErrNoSourceCommit
	}
	snapshotProps := SnapshotProperties(repository, sourceCommit)

	scan := tbl.Scan()
	defer func() { _ = scan.Close() }()
	tasks, err := scan.PlanFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("plan the compaction of the table at %s: %w", sourceCommit, err)
	}
	dataFiles := dataFilesOf(tasks)
	deleteFiles, err := snapshotFilesOf(ctx, tbl, iceberg.ManifestContentDeletes)
	if err != nil {
		return nil, fmt.Errorf("list the delete files of the table at %s: %w", sourceCommit, err)
	}
	schema, batches, err := scan.ReadTasks(ctx, tasks)
	if err != nil {
		return nil, fmt.Errorf("scan the table at %s: %w", sourceCommit, err)
	}
	rows, err := rowsOfBatches(batches)
	if err != nil {
		return nil, fmt.Errorf("read the rows of the table at %s: %w", sourceCommit, err)
	}
	slices.SortFunc(rows, func(a, b Row) int { return cmp.Compare(a.Path, b.Path) })
	reader := NewBatchesRecordReader(schema, rowBatches(schema, rowSource(rows), compactRecordBatchSize))
	defer reader.Release()

	txn := tbl.NewTransaction()
	if len(dataFiles) > 0 || len(deleteFiles) > 0 {
		if err := txn.ReplaceFiles(ctx, dataFiles, nil, deleteFiles, snapshotProps); err != nil {
			return nil, fmt.Errorf("replace the files of the table at %s: %w", sourceCommit, err)
		}
	}
	if len(tasks) > 0 {
		if err := txn.Append(ctx, reader, snapshotProps); err != nil {
			return nil, fmt.Errorf("rewrite the table at %s: %w", sourceCommit, err)
		}
	}
	if err := txn.SetProperties(iceberg.Properties{PropertySourceCommit: sourceCommit}); err != nil {
		return nil, fmt.Errorf("record the source commit: %w", err)
	}
	committed, err := txn.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("commit the compaction at %s: %w", sourceCommit, err)
	}
	return committed, nil
}

// replaceSnapshotFiles removes every live data file and every live delete file of the current
// snapshot without adding files: the first update of a whole-table rewrite, which leaves the
// transaction empty so that the caller appends the new rows into the same commit.
func replaceSnapshotFiles(ctx context.Context, txn *icebergtable.Transaction, tbl *icebergtable.Table, snapshotProps iceberg.Properties) error {
	dataFiles, err := snapshotFilesOf(ctx, tbl, iceberg.ManifestContentData)
	if err != nil {
		return err
	}
	deleteFiles, err := snapshotFilesOf(ctx, tbl, iceberg.ManifestContentDeletes)
	if err != nil {
		return err
	}
	if len(dataFiles) == 0 && len(deleteFiles) == 0 {
		return nil
	}
	return txn.ReplaceFiles(ctx, dataFiles, nil, deleteFiles, snapshotProps)
}

// snapshotFilesOf returns the live files of one content type in the table's current snapshot,
// read from the snapshot's manifest list and the manifests it references. Deleted manifest
// entries are skipped and a path is returned once.
//
// A scan plan is not a substitute: it carries a delete file only through the data file it applies
// to, so a delete file whose data files are gone (orphaned by a rebuild, or by a copy-on-write
// rewrite that kept it) is invisible to it, while it is still referenced by the snapshot.
func snapshotFilesOf(ctx context.Context, tbl *icebergtable.Table, content iceberg.ManifestContent) ([]iceberg.DataFile, error) {
	snapshot := tbl.CurrentSnapshot()
	if snapshot == nil {
		return nil, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("get the file IO of the table: %w", err)
	}
	manifests, err := snapshot.Manifests(fs)
	if err != nil {
		return nil, fmt.Errorf("read the manifest list of snapshot %d: %w", snapshot.SnapshotID, err)
	}
	files := make([]iceberg.DataFile, 0)
	seen := make(map[string]struct{})
	for _, manifest := range manifests {
		if manifest.ManifestContent() != content {
			continue
		}
		entries, err := manifestEntriesOf(fs, manifest)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			file := entry.DataFile()
			if _, ok := seen[file.FilePath()]; ok {
				continue
			}
			seen[file.FilePath()] = struct{}{}
			files = append(files, file)
		}
	}
	return files, nil
}

// manifestEntriesOf returns the live entries of one manifest: ReadManifest skips the entries whose
// status is DELETED, which are files the snapshot no longer holds.
func manifestEntriesOf(fs icebergio.IO, manifest iceberg.ManifestFile) ([]iceberg.ManifestEntry, error) {
	file, err := fs.Open(manifest.FilePath())
	if err != nil {
		return nil, fmt.Errorf("open manifest %s: %w", manifest.FilePath(), err)
	}
	entries, readErr := iceberg.ReadManifest(manifest, file, true)
	closeErr := file.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read manifest %s: %w", manifest.FilePath(), readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close manifest %s: %w", manifest.FilePath(), closeErr)
	}
	return entries, nil
}

// dataFilesOf returns the data file of every scan task: a scan plans one task per live data file
// of the snapshot, so this is the whole data side of a compaction.
func dataFilesOf(tasks []icebergtable.FileScanTask) []iceberg.DataFile {
	dataFiles := make([]iceberg.DataFile, 0, len(tasks))
	for _, task := range tasks {
		dataFiles = append(dataFiles, task.File)
	}
	return dataFiles
}

// changedPaths returns the paths one delta must replace or remove: the paths of the upserts and
// the deleted paths, deduplicated and sorted. Sorting makes the predicate and the snapshot
// deterministic, which is what a retried commit (same request) and a test both rely on.
func changedPaths(upserts []Row, deletes []string) []string {
	paths := make([]string, 0, len(upserts)+len(deletes))
	for _, row := range upserts {
		paths = append(paths, row.Path)
	}
	paths = append(paths, deletes...)
	slices.SortFunc(paths, cmp.Compare)
	return slices.Compact(paths)
}

// chunks splits paths into slices of at most size elements (size > 0).
func chunks(paths []string, size int) [][]string {
	out := make([][]string, 0, (len(paths)+size-1)/size)
	for len(paths) > 0 {
		n := min(size, len(paths))
		out = append(out, paths[:n])
		paths = paths[n:]
	}
	return out
}

// pathFilter returns the predicate of a copy-on-write replace: an IN list per chunk, joined with
// OR. It names the same paths a merge-on-read write deletes in several chunks, so the two modes
// always touch the same rows.
func pathFilter(paths []string, chunkSize int) iceberg.BooleanExpression {
	parts := chunks(paths, chunkSize)
	filters := make([]iceberg.BooleanExpression, 0, len(parts))
	for _, chunk := range parts {
		filters = append(filters, iceberg.IsIn(iceberg.Reference(ColumnPath), chunk...))
	}
	switch len(filters) {
	case 0:
		return iceberg.AlwaysFalse{}
	case 1:
		return filters[0]
	default:
		return iceberg.NewOr(filters[0], filters[1], filters[2:]...)
	}
}

// orderedRows wraps a row source and enforces the table's declared sort order: every row must
// name a path that comes after the previous one (§3.1: path ASC, and the identifier field, so a
// path appears once). It is how a rebuild keeps streaming its source - memory bounded by the
// record batch rather than by the branch (design §4.3 step 2) - and still writes only files whose
// rows follow the order their table metadata declares (MDS_REVIEW.md TB-2).
func orderedRows(rows Rows) Rows {
	return func(yield func(Row, error) bool) {
		last := ""
		seen := false
		rows(func(row Row, err error) bool {
			if err != nil {
				return yield(Row{}, err)
			}
			if seen && row.Path <= last {
				return yield(Row{}, fmt.Errorf("%w: %q is not after %q", ErrRowsOutOfOrder, row.Path, last))
			}
			seen, last = true, row.Path
			return yield(row, nil)
		})
	}
}

// rowSource turns rows into a row source, which is what rowBatches reads. Compact materialises
// the table's rows to sort them and hands them back one batch at a time, so that the batch being
// appended is not a second copy of the whole table.
func rowSource(rows []Row) Rows {
	return func(yield func(Row, error) bool) {
		for _, row := range rows {
			if !yield(row, nil) {
				return
			}
		}
	}
}

// rowsOfBatches decodes a scan's record batches into rows, in the order the batches and their rows
// carry them. Compact uses it to re-sort the table before it writes it back.
//
// Every batch is released as soon as its rows are decoded: the sorted rows are the only copy
// Compact keeps.
func rowsOfBatches(batches iter.Seq2[arrow.RecordBatch, error]) ([]Row, error) {
	var rows []Row
	for batch, err := range batches {
		if err != nil {
			if batch != nil {
				batch.Release()
			}
			return nil, err
		}
		for i := range int(batch.NumRows()) {
			rows = append(rows, rowOfBatch(batch, i))
		}
		batch.Release()
	}
	return rows, nil
}

// rowOfBatch decodes row i of one record batch. The columns are addressed by the positions of the
// fixed schema, the same ones appendRow writes (rows.go): the scan returns the table's columns in
// schema order, and schema changes only append optional columns with new IDs (§3.1), so the first
// ten positions stay the ones of that fixed schema.
func rowOfBatch(batch arrow.RecordBatch, i int) Row {
	return Row{
		Repository:   batch.Column(columnRepository).(*array.String).Value(i),
		Path:         batch.Column(columnPath).(*array.String).Value(i),
		CommitID:     batch.Column(columnCommitID).(*array.String).Value(i),
		SizeBytes:    batch.Column(columnSizeBytes).(*array.Int64).Value(i),
		LastModified: timestampOf(batch.Column(columnLastModified).(*array.Timestamp), i),
		ETag:         batch.Column(columnETag).(*array.String).Value(i),
		UserMetadata: userMetadataOf(batch.Column(columnUserMetadata).(*array.Map), i),
		Committer:    optionalStringOf(batch.Column(columnCommitter).(*array.String), i),
		ContentType:  optionalStringOf(batch.Column(columnContentType).(*array.String), i),
		CreationDate: optionalTimestampOf(batch.Column(columnCreationDate).(*array.Timestamp), i),
	}
}

// timestampOf reads one timestamp cell as a time. appendRow writes it as Unix microseconds in UTC,
// so the round trip through a scan gives the same value back.
func timestampOf(values *array.Timestamp, i int) time.Time {
	return time.UnixMicro(int64(values.Value(i))).UTC()
}

// userMetadataOf reads one user_metadata map cell. A null map becomes a nil map, which appendRow
// writes back as the empty map the schema requires (§3.1).
func userMetadataOf(values *array.Map, i int) map[string]string {
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

// optionalStringOf reads one cell of an optional string column: null is the unset value.
func optionalStringOf(values *array.String, i int) *string {
	if values.IsNull(i) {
		return nil
	}
	value := values.Value(i)
	return &value
}

// optionalTimestampOf reads one cell of an optional timestamp column: null is the unset value.
func optionalTimestampOf(values *array.Timestamp, i int) *time.Time {
	if values.IsNull(i) {
		return nil
	}
	value := timestampOf(values, i)
	return &value
}
