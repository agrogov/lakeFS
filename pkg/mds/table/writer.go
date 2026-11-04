package table

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"
)

const (
	// DefaultDeleteChunkSize is the largest number of paths one delete predicate names
	// (design §4.3 step 3: "The IN list is chunked (≤ 10 000 paths per Delete call) inside the
	// same transaction"). A change set larger than the rebuild threshold is a rebuild instead of
	// a delta (§4.3 step 2), so this bounds one predicate and not the work of one commit.
	DefaultDeleteChunkSize = 10_000
)

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
// full listing of the data repository at the commit, written with one Overwrite and no filter.
//
// It is the path taken when a change set is larger than rebuild_threshold (the caller decides
// that; this function only writes), when a branch is first indexed (§4.5 step 2) and when a
// history rewrite forces a fresh listing (§4.7). The rows are streamed: the source runs as the
// batches are pulled, so memory is bounded by the batch size rather than by the branch.
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

	reader, err := NewRowsRecordReader(tbl.Schema(), rows, o.recordBatchSize)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	txn := tbl.NewTransaction()
	if err := txn.Overwrite(ctx, reader, snapshotProps); err != nil {
		return nil, fmt.Errorf("overwrite the table at %s: %w", sourceCommit, err)
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
// The transaction has two updates. ReplaceFiles removes every data file and every delete file of
// the current snapshot without adding files - it is the compaction primitive of iceberg-go's
// transaction, and the only one that removes delete files - and Append writes the scanned rows
// back. A plain Overwrite (design §4.9's wording) rewrites the data files but leaves the delete
// files in place: iceberg-go's overwrite producer removes only the files it matches, so the delete
// files would keep accumulating and keep being read. The rows are identical either way, which is
// what §4.9 asks the compaction to preserve; deleting the files is what makes it a compaction.
//
// The rows are read through a scan and streamed back through the same transaction, so compaction
// does not collect the table in memory. The caller commits R-metadata afterwards with the
// "mds: compact" message and without a new tag (§4.9, M4's maintenance task).
func Compact(ctx context.Context, tbl *icebergtable.Table, sourceCommit string, opts ...Option) (*icebergtable.Table, error) {
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
	if _, err := resolve(tbl, opts); err != nil {
		return nil, err
	}
	snapshotProps := SnapshotProperties(repository, sourceCommit)

	scan := tbl.Scan()
	defer func() { _ = scan.Close() }()
	tasks, err := scan.PlanFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("plan the compaction of the table at %s: %w", sourceCommit, err)
	}
	dataFiles, deleteFiles := filesOf(tasks)
	schema, batches, err := scan.ReadTasks(ctx, tasks)
	if err != nil {
		return nil, fmt.Errorf("scan the table at %s: %w", sourceCommit, err)
	}
	reader := NewBatchesRecordReader(schema, batches)
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

// filesOf returns the data files and the delete files a set of scan tasks reads: the data file of
// every task, and the position and equality delete files applied to them, each once.
func filesOf(tasks []icebergtable.FileScanTask) (dataFiles, deleteFiles []iceberg.DataFile) {
	seen := make(map[string]struct{})
	dataFiles = make([]iceberg.DataFile, 0, len(tasks))
	for _, task := range tasks {
		dataFiles = append(dataFiles, task.File)
		for _, group := range [][]iceberg.DataFile{task.DeleteFiles, task.EqualityDeleteFiles} {
			for _, file := range group {
				path := file.FilePath()
				if _, ok := seen[path]; ok {
					continue
				}
				seen[path] = struct{}{}
				deleteFiles = append(deleteFiles, file)
			}
		}
	}
	return dataFiles, deleteFiles
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
