package table

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"

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

	// ErrMergeRows is returned when a compaction cannot merge the rows of the table: a scan row has
	// no path (the identifier and the sort key), a scan column the path is read from is not a string
	// column, or the merge itself panicked. The merge runs inside iceberg-go's writer goroutine,
	// where a panic would kill the process and no recover in the indexer could reach it, so it is
	// reported as a commit error instead (R2T-1).
	ErrMergeRows = errors.New("cannot merge the rows of the compaction")

	// ErrRowsOutOfOrder is returned when a row source of a rebuild yields a path that does not
	// come after the previous one, or when a delta names the same path in two upserts. The table
	// declares sort order path ASC and path is the identifier field, so a path appears once
	// (§3.1); a rebuild streams its source instead of collecting it (design §4.3 step 2), so the
	// order of the files it writes is the order of the source: rather than writing files that
	// contradict the table's declared order or two rows for one identifier, such a source is
	// refused (MDS_REVIEW.md TB-2, MDS_REVIEW_4.md R4T-5).
	ErrRowsOutOfOrder = errors.New("rows are not in path order")

	// ErrNoTable is returned when a write is applied without a loaded table. The caller loads the
	// table from the catalog (design §4.2 step 2 and §4.4), so a nil table is a programming error
	// and not a state a write can report to the catalog.
	ErrNoTable = errors.New("no table")

	// ErrStaleTable is returned when the table handle a write started from no longer names the
	// branch's snapshot, and the writer cannot rebase the work it did against that snapshot.
	//
	// A write reads the snapshot (a rebuild's rows, a compaction's files and rows) and then commits
	// removals relative to it, so replaying its transaction against a newer snapshot would keep the
	// concurrent writer's file while re-adding the rows the rewrite read - a duplicate path (R2T-3).
	// Every write pins commit.retry.num-retries to 0 so such a commit is refused, and the one
	// commit that runs before the pin (the repair of a table created before the property existed)
	// reports this error when it was itself replayed and moved the branch. The caller reloads the
	// table and retries the tick (§4.4), which is the recovery the design documents.
	ErrStaleTable = errors.New("the table moved since it was read")
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
// the plan's risk list). The default is the table's own write.delete.mode property, and
// iceberg-go's copy-on-write when the table does not carry it (see resolve).
//
// WriteModeCopyOnWrite needs no property: Overwrite always rewrites the files it matches.
// WriteModeMergeOnRead is applied by iceberg-go's Delete, which takes the mode from the table's
// write.delete.mode (v0.7.0 table/transaction.go:2603-2619), so it is what a table that carries the
// property writes - as every table this package creates does (§3.1).
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
//
// An absent write.delete.mode keeps iceberg-go's default, copy-on-write (iceberg-go v0.7.0
// table/properties.go:101-102), and not merge-on-read, the mode this package creates its tables
// with (§3.1). The reason is that the delete mode of a transaction is not this package's to choose
// for a table that does not carry the property: iceberg-go reads write.delete.mode from the
// metadata the transaction is building and runs either Delete or Overwrite from it (v0.7.0
// table/transaction.go:2603-2619), so a resolve that answered merge-on-read for an absent property
// would name a mode the transaction does not run unless this writer wrote the property into the
// table on every delta - a per-table setting §3.1 makes at creation, and one that would outlive the
// write and change what other writers of the same table do. Reporting the mode that actually runs
// is also the lesson of R4T-1: claiming one delete mode and applying another is how a row came
// back. Copy-on-write is a correct answer on such a table now that that arm folds the live delete
// files into its data files first (compactDeleteFiles), at the fallback's documented cost (§13);
// merge-on-read remains the mode of every table this package creates.
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

// writeProperties returns the table properties every write of this package pins in its own
// transaction: the source commit of the change set (§4.3 step 3) and commit.retry.num-retries = 0.
//
// A table created by this package already carries the retry property (Properties, §3.1), and
// ensureNoReplay migrates a table created before the property existed. Re-asserting it on every
// write keeps a concurrent or later writer of the same table - the catalog's metadata is what
// iceberg-go reads its retry budget from - from making a whole-table rewrite replayable (R2T-3).
func writeProperties(sourceCommit string) iceberg.Properties {
	return iceberg.Properties{
		PropertySourceCommit:  sourceCommit,
		PropertyCommitRetries: CommitRetriesNone,
	}
}

// ensureNoReplay returns a handle of tbl whose commit.retry.num-retries property is 0, so that the
// commit the caller is about to make is never replayed against a newer snapshot.
//
// iceberg-go decides replayability from the properties of the metadata the transaction was started
// on, not from the properties the transaction stages (table.doCommit reads t.metadata.Properties()),
// so setting the property in the write's own transaction would protect the *next* write and not
// this one. A table that already carries 0 - every table this package creates, and every table after
// its first write - is returned as it is. A table created before the property existed, or one an
// operator raised, is repaired with one metadata-only commit (no snapshot) and the returned handle
// is used for the write.
//
// The repair's own commit still runs with the old replay budget, so it can be replayed onto a newer
// parent. That is harmless for the repair itself (it only sets a property), but a replay means the
// branch moved while this writer's rows were being read, and the write that follows must not
// overwrite a commit it never read: it returns ErrStaleTable instead, and the caller reloads and
// retries the tick (§4.4).
func ensureNoReplay(ctx context.Context, tbl *icebergtable.Table) (*icebergtable.Table, error) {
	if tbl.Properties().GetUInt64(PropertyCommitRetries, icebergtable.CommitNumRetriesDefault) == 0 {
		return tbl, nil
	}
	txn := tbl.NewTransaction()
	if err := txn.SetProperties(iceberg.Properties{PropertyCommitRetries: CommitRetriesNone}); err != nil {
		return nil, fmt.Errorf("pin %s: %w", PropertyCommitRetries, err)
	}
	before := snapshotIDOf(tbl)
	migrated, err := txn.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("pin %s: %w", PropertyCommitRetries, err)
	}
	// The repair's own commit is the one commit of this package that runs while the replay budget of
	// the loaded table is still non-zero, so it can be replayed onto a newer parent. The repair
	// itself is idempotent and survives that, but the caller's work was read from the older
	// snapshot: continuing would let a stale rewrite replace files it never read. Report the move
	// instead, so the caller reloads and retries the tick (§4.4).
	if after := snapshotIDOf(migrated); after != before {
		return nil, fmt.Errorf("%w: snapshot %d is now %d", ErrStaleTable, before, after)
	}
	return migrated, nil
}

// snapshotIDOf returns the ID of the table's current snapshot, or 0 when the table has none.
func snapshotIDOf(tbl *icebergtable.Table) int64 {
	if snapshot := tbl.CurrentSnapshot(); snapshot != nil {
		return snapshot.SnapshotID
	}
	return 0
}

// ensureOptionalUserMetadata returns a handle of tbl whose schema declares user_metadata optional,
// and applies that one schema update to a table whose schema still declares it required.
//
// The current guide makes the column optional - "Required: no. If none exists, an empty map is
// stored" - and rows keep writing an empty map, so the update changes only what the schema allows,
// never a row (G13, METADATA_SEARCH_REALIGN.md RA7). Iceberg allows a required field to become
// optional and refuses the reverse, so there is exactly one direction to evolve, and a table this
// package creates already carries it (Schema). Only a table created before the change carries the
// required field, and it is evolved here on the next write.
//
// It is applied once per table, not once per write: the guard is the loaded schema itself, so a
// table that already carries the optional field - including the one the previous write returned -
// costs no request and no commit. iceberg-go's UpdateSchema would also stage nothing for a schema
// that already matches (BuildUpdates finds the current schema ID and stages no update), so a second
// attempt could not add a commit even if the guard were missing; the guard is what makes "once" a
// property of the code and not of a library detail.
//
// The update keeps the field ID (7) and the map's key and value IDs (8, 9): it changes the
// Required flag of an existing field, and never rebuilds the column. Every ID a data file already
// carries therefore stays the ID this schema gives the column.
//
// The commit is metadata-only: it adds no snapshot, so the row set a caller read is the row set the
// write that follows commits. Writing the schema and the rows into one transaction is not an
// option - the reader the write appends was built from the schema of one handle, and a table's
// transaction commits the metadata it was started on - so the two commits are ordered: this one
// first, then the write's own.
//
// Every entry point calls it after ensureNoReplay, so this commit is made with
// commit.retry.num-retries already pinned to 0 and is never replayed: a lost optimistic-concurrency
// check here (a concurrent writer evolving the same table) is a conflict the indexer retries, which
// is the recovery of §4.4.
func ensureOptionalUserMetadata(ctx context.Context, tbl *icebergtable.Table) (*icebergtable.Table, error) {
	field, ok := tbl.Schema().FindFieldByName(ColumnUserMetadata)
	if !ok || !field.Required {
		// A table without the column is not one this writer evolves: checkKnownSchema refuses it
		// on the row-building paths with ErrUnknownSchema, which is the honest answer.
		return tbl, nil
	}
	txn := tbl.NewTransaction()
	if err := txn.UpdateSchema(true, false).
		UpdateColumn([]string{ColumnUserMetadata}, icebergtable.ColumnUpdate{
			Required: iceberg.Optional[bool]{Valid: true, Val: false},
		}).
		Commit(); err != nil {
		return nil, fmt.Errorf("make %s optional: %w", ColumnUserMetadata, err)
	}
	updated, err := txn.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("make %s optional: %w", ColumnUserMetadata, err)
	}
	return updated, nil
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
// The one exception is a copy-on-write delta on a snapshot that still carries live delete files:
// it commits one compaction before it commits the delta (compactDeleteFiles), so the branch head
// moves twice, inside the same lakeFS commit of §4.3 step 4. The intermediate snapshot holds the
// rows of the snapshot before it - a compaction rewrites files and removes delete files, it does
// not change the row set (§4.9) - so a reader between the two commits sees the delta not applied
// yet, never a half-applied one.
//
// An empty delta is not a no-op: the property still moves, which is what makes a commit that
// changed no object a recoverable indexing position (§4.3 step 3, §4.4).
//
// The deletes are applied in merge-on-read mode by default, which writes position delete files;
// WithDeleteMode(WriteModeCopyOnWrite) selects the fallback that rewrites the affected data files
// instead (design §13 risk "Delete files and catalog staging"). A copy-on-write write cannot read a
// data file that still has a delete file through it, so the arm folds the snapshot's delete files
// into its data files first, with one compaction (compactDeleteFiles, R4T-1).
//
// A delta with upserts refuses a table whose schema carries a column this writer does not know
// (ErrUnknownSchema): the upserts are built from Row's ten fields, and a table with an optional
// column a later schema version appended (§3.1) cannot be written that way (R2T-1). A delete-only
// delta writes no row and is still applied. The upserts name a path at most once (ErrRowsOutOfOrder
// otherwise, as a rebuild's source does), because path is the identifier field (§3.1, R4T-5). The
// write is never replayed: every write pins commit.retry.num-retries to 0 and migrates a table
// created before the property existed (R2T-3). A table whose schema still declares user_metadata
// required - one created before G13 - is evolved to the optional column once, before the write,
// with a metadata-only commit of its own (ensureOptionalUserMetadata).
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
	if len(upserts) > 0 {
		// The upserts are rebuilt from Row's ten fields; a table with a column this writer does
		// not know cannot be written that way, so it is refused instead of panicked on (R2T-1).
		// A delete-only delta is still allowed: it writes no row.
		if err := checkKnownSchema(tbl.Schema()); err != nil {
			return nil, err
		}
	}
	sorted := slices.Clone(upserts)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })
	// Path is the identifier field and the table's sort key (§3.1: "One row per (branch, path)"),
	// so an upsert may name a path once. Writing the same path twice would put two rows under one
	// identifier, which nothing in Iceberg refuses on append and a later IN (path ...) delete then
	// removes together; Rebuild refuses the identical input with ErrRowsOutOfOrder (orderedRows), so
	// the two write paths keep one input contract (R4T-5). Sorting first is what makes a duplicate
	// adjacent, and it happens before any commit, so a refused delta changes nothing.
	for i := 1; i < len(sorted); i++ {
		if sorted[i].Path == sorted[i-1].Path {
			return nil, fmt.Errorf("%w: %q appears twice in the upserts", ErrRowsOutOfOrder, sorted[i].Path)
		}
	}
	paths := changedPaths(sorted, deletes)

	tbl, err = ensureNoReplay(ctx, tbl)
	if err != nil {
		return nil, fmt.Errorf("make the delta at %s non-replayable: %w", sourceCommit, err)
	}
	tbl, err = ensureOptionalUserMetadata(ctx, tbl)
	if err != nil {
		return nil, fmt.Errorf("evolve the schema for the delta at %s: %w", sourceCommit, err)
	}
	snapshotProps := SnapshotProperties(repository, sourceCommit)

	// A copy-on-write delta rewrites the data files its filter matches by reading them alone
	// (iceberg-go's rewriteSingleFile builds a scan task with no delete file), so on a snapshot that
	// still carries delete files it reads the rows they removed back as live (R4T-1). Folding them
	// into the data files first is what makes the arm correct; the compaction is paid only while
	// such a snapshot is met, because a copy-on-write delta writes no delete file of its own.
	if o.deleteMode == WriteModeCopyOnWrite && len(paths) > 0 {
		tbl, err = compactDeleteFiles(ctx, tbl)
		if err != nil {
			return nil, fmt.Errorf("compact the delete files before the copy-on-write delta at %s: %w", sourceCommit, err)
		}
	}

	txn := tbl.NewTransaction()
	switch o.deleteMode {
	case WriteModeMergeOnRead:
		for _, chunk := range chunks(paths, o.deleteChunkSize) {
			if err := txn.Delete(ctx, iceberg.IsIn(iceberg.Reference(ColumnPath), chunk...), snapshotProps); err != nil {
				return nil, fmt.Errorf("delete %d paths: %w", len(chunk), err)
			}
		}
		if len(sorted) > 0 {
			reader, err := NewRecordReader(tbl.Schema(), sorted, o.recordBatchSize)
			if err != nil {
				return nil, err
			}
			appendErr := txn.Append(ctx, reader, snapshotProps)
			reader.Release()
			if appendErr != nil {
				return nil, fmt.Errorf("append %d rows: %w", len(sorted), appendErr)
			}
		}
	case WriteModeCopyOnWrite:
		// One Overwrite both removes the old rows and writes the new ones, so the upserts must not
		// be appended in a second step: that would duplicate them. With no paths to replace, an
		// append is enough - and it is not an overwrite, which without a filter would replace the
		// whole table.
		switch {
		case len(paths) > 0:
			reader, err := deltaUpsertReader(tbl.Schema(), sorted, o.recordBatchSize)
			if err != nil {
				return nil, err
			}
			overwriteErr := txn.Overwrite(ctx, reader, snapshotProps,
				icebergtable.WithOverwriteFilter(pathFilter(paths, o.deleteChunkSize)))
			reader.Release()
			if overwriteErr != nil {
				return nil, fmt.Errorf("overwrite %d paths: %w", len(paths), overwriteErr)
			}
		case len(sorted) > 0:
			reader, err := NewRecordReader(tbl.Schema(), sorted, o.recordBatchSize)
			if err != nil {
				return nil, err
			}
			appendErr := txn.Append(ctx, reader, snapshotProps)
			reader.Release()
			if appendErr != nil {
				return nil, fmt.Errorf("append %d rows: %w", len(sorted), appendErr)
			}
		}
	}

	if err := txn.SetProperties(writeProperties(sourceCommit)); err != nil {
		return nil, fmt.Errorf("record the source commit: %w", err)
	}
	committed, err := txn.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("commit the delta at %s: %w", sourceCommit, err)
	}
	return committed, nil
}

// deltaUpsertReader returns the reader a copy-on-write delta writes: the upserts, or - for a
// delete-only delta - an empty reader over the table's own Arrow schema.
//
// The empty reader is what lets a delete-only delta still apply to a table with a column this
// writer does not know, which §3.1 promises: "A delete-only delta still applies, because it writes
// no row". NewRecordReader checks the table's schema (checkKnownSchema), so building one for a row
// set of length zero refused the delta with ErrUnknownSchema and every delete of a branch failed,
// even though there is no row to fill (R4T-3). Nothing is written for zero rows, and the Overwrite
// itself re-emits the columns of the files it rewrites, so the column the writer does not know
// keeps its value.
func deltaUpsertReader(sc *iceberg.Schema, rows []Row, batchSize int) (array.RecordReader, error) {
	if len(rows) > 0 {
		return NewRecordReader(sc, rows, batchSize)
	}
	schema, err := ArrowSchema(sc)
	if err != nil {
		return nil, err
	}
	return NewBatchesRecordReader(schema, func(_ func(arrow.RecordBatch, error) bool) {}), nil
}

// compactDeleteFiles folds the live delete files of the table's snapshot into its data files, and
// returns tbl unchanged when the snapshot carries none.
//
// It is what makes a copy-on-write delta correct after a merge-on-read one. iceberg-go's
// copy-on-write rewrite reads the data file it rewrites and nothing else: the FileScanTask that
// rewriteSingleFile builds carries no DeleteFiles (iceberg-go v0.7.0 table/transaction.go:2889-2899,
// while table/scanner.go:2110-2115 shows the field the scan would apply). A row an earlier delete
// file removed is therefore read back as live and written into the rewritten file, and the old
// delete file - whose data file is no longer in the snapshot - can neither remove it nor be applied
// to the new file: the row comes back with the values it had before it was deleted, silently, in a
// mode the design documents as a fallback (R4T-1).
//
// The compaction applies the delete files and removes them, so the Overwrite that follows reads
// data files only. It costs one rewrite of the table, paid only while a copy-on-write delta runs on
// a snapshot that still carries delete files; a copy-on-write delta writes none, so a table written
// that way pays it at most once after the last merge-on-read delete. The delete files are listed
// from the snapshot's manifest list, not from a scan plan, for the reason Compact documents: a scan
// task carries only the delete files that apply to the data file it reads.
//
// The fold records the table's *current* position as its source commit, not the commit the delta is
// applying, and it is a commit of its own: iceberg-go stages several snapshots inside one commit
// but two commits cannot be one, and the indexer's recovery finishes a staged write whose property
// is the next commit C (§4.4). A fold that recorded C would therefore let a delta that failed after
// it be committed as if C had been applied, with C's change set missing - the position would move
// over rows that never changed. Recorded at the current position, whose rows the compaction still
// holds (§4.9: a compaction does not change the row set), the staging a failed delta leaves behind
// is not a commit the recovery recognises, so it is discarded and the tick re-indexes C (resetStaging
// in pkg/mds/indexer).
func compactDeleteFiles(ctx context.Context, tbl *icebergtable.Table) (*icebergtable.Table, error) {
	deleteFiles, err := snapshotFilesOf(ctx, tbl, iceberg.ManifestContentDeletes)
	if err != nil {
		return nil, fmt.Errorf("list the delete files of the table: %w", err)
	}
	if len(deleteFiles) == 0 {
		return tbl, nil
	}
	position := tbl.Properties().Get(PropertySourceCommit, "")
	if position == "" {
		// A table with delete files and no recorded position was not written by this package (its
		// every write records one), so there is no commit the compaction could keep the table at.
		// Refusing is honest; the caller's full-listing path (Rebuild) needs no position and fixes
		// the table, delete files included.
		return nil, fmt.Errorf("%w: the table carries delete files but no %s to compact at",
			ErrNoSourceCommit, PropertySourceCommit)
	}
	return Compact(ctx, tbl, position)
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
//
// The write is never replayed. Every write of this package pins commit.retry.num-retries to 0
// (writeProperties), and ensureNoReplay migrates a table that was created before the property
// existed, so a lost optimistic-concurrency check is a conflict the indexer retries rather than a
// replayed rewrite that duplicates rows (R2T-3). A table whose schema carries a column this writer
// does not know is refused with ErrUnknownSchema: a rebuild cannot fill a column it was not given
// (R2T-1). A table whose user_metadata is still required is evolved once, before the rows are
// written (ensureOptionalUserMetadata, G13).
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
	// A rebuild writes rows it was given as Row's ten fields, and it replaces the whole table: a
	// table with a column this writer does not know is refused rather than having that column
	// emptied by a rebuild it cannot fill (R2T-1).
	if err := checkKnownSchema(tbl.Schema()); err != nil {
		return nil, err
	}
	tbl, err = ensureNoReplay(ctx, tbl)
	if err != nil {
		return nil, fmt.Errorf("make the rebuild at %s non-replayable: %w", sourceCommit, err)
	}
	tbl, err = ensureOptionalUserMetadata(ctx, tbl)
	if err != nil {
		return nil, fmt.Errorf("evolve the schema for the rebuild at %s: %w", sourceCommit, err)
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
	if err := txn.SetProperties(writeProperties(sourceCommit)); err != nil {
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
// The removal list is one entry per data file, not one per scan task: iceberg-go splits a file above
// read.split.target-size into several tasks, and ReplaceFiles refuses a path that appears twice
// (dataFilesOf, R3T-1). The read side is unaffected - each split range is a disjoint set of row
// groups - so a split file is read once and removed once.
//
// The delete files are enumerated from the snapshot's manifest list, not from the scan tasks: a
// scan task carries only the delete files that apply to the data file it reads, so a delete file
// whose data files were replaced by a Rebuild (§4.3 step 2, §4.5, §4.7) is carried by no task. It
// would then stay in the manifest list forever - referenced by the live snapshot, so no file
// cleanup could reclaim it - and the snapshot summary's total-delete-files, which is exactly what
// §4.9's delete-file trigger compares, would never fall back to zero (TB-1).
//
// The reader is owned by this function and released by Release, which frees the streams it opened,
// so an error from ReplaceFiles or from Append before the first batch is pulled still closes every
// file (R3T-7).
//
// The rows come back out of a scan in the table's declared path order: the scan plans the table's
// data files in the order they were committed, not in the table's declared sort order, and
// iceberg-go sorts each record batch it is handed on its own ("Sort each batch independently
// before writing. This is per-batch, not per-file", table/rolling_data_writer.go), so streaming
// the scan's batches straight back into Append writes files of [b d a c] for the data files [a c]
// and [b d] - a table whose files no longer carry the `path ASC` order the table metadata
// advertises (§3.1; the iceberg spec's sort order is what writers honour, MDS_REVIEW.md TB-2).
// The reader of compactReader merges the per-file sorted streams instead (a k-way merge, one
// record batch per file and one of output), so every batch the writer sorts is already a
// contiguous slice of the declared order, and two commits of interleaved paths compact into one
// file of [a b c d] with narrow path bounds for the next delta's IN (path ...) scan to prune with
// (R2T-2). A file written by a compaction before commit 2daeca114 may not be sorted: reading only
// its path column detects that, and the file is then read whole and sorted on its own before it is
// merged. The memory of the fallback is therefore the memory of every legacy file at once, not of
// the table: a k-way merge cannot stream a run whose rows are not already in order, so each legacy
// file stays resident until the merge is done (R3T-3). It is bounded by the legacy files only, and
// the first compaction after the fix removes the reason for it.
//
// The rows are the scan's own Arrow columns, sliced and concatenated, never rebuilt from the ten
// fields of Row: a column this writer does not know - an optional column a later schema version
// appended with a new ID, which §3.1 allows - keeps its values through the rewrite (R2T-1).
//
// The compaction also never replays. Its own transaction pins commit.retry.num-retries to 0
// (writeProperties) and ensureNoReplay migrates a table created before the property existed, so a
// lost CAS is a conflict the indexer retries instead of a replay of the removals against a newer
// snapshot - which would keep a concurrent writer's data file while re-adding the rows the
// compaction read, and duplicate a path (R2T-3).
//
// The caller commits R-metadata afterwards with the "mds: compact" message and without a new tag
// (§4.9, M4's maintenance task). A table whose user_metadata is still required is evolved once,
// before the scan that feeds the rewrite (ensureOptionalUserMetadata, G13), so the batches the
// merge hands to Append carry the schema the transaction commits.
//
// Compact takes no Option: it removes and re-adds whole files and writes no delete predicate, so
// the delete mode and the delete chunk size of a delta do not apply, and the batch size is the
// one compactRecordBatchSize fixes (MDS_REVIEW.md TB-4).
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

	tbl, err = ensureNoReplay(ctx, tbl)
	if err != nil {
		return nil, fmt.Errorf("make the compaction at %s non-replayable: %w", sourceCommit, err)
	}
	tbl, err = ensureOptionalUserMetadata(ctx, tbl)
	if err != nil {
		return nil, fmt.Errorf("evolve the schema for the compaction at %s: %w", sourceCommit, err)
	}
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
	reader, err := compactReader(ctx, tbl, tasks)
	if err != nil {
		return nil, fmt.Errorf("read the table at %s: %w", sourceCommit, err)
	}
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
	if err := txn.SetProperties(writeProperties(sourceCommit)); err != nil {
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

// dataFilesOf returns the data files of a scan plan, one entry per file: a scan plans one task per
// live data file of the snapshot, except that iceberg-go splits a Parquet file larger than
// read.split.target-size into several tasks that all carry the same File, each with its own Start
// and Length (iceberg-go v0.7.0 table/scan_splits.go:55-60, planned by table/scanner.go:1820).
//
// ReplaceFiles refuses a delete list in which a path appears twice - "delete data file paths must be
// unique for ReplaceFiles" (table/transaction.go:1843) or, when the snapshot has no delete file,
// "delete data file paths must be unique for ReplaceDataFilesWithDataFiles" (table/transaction.go:1682)
// - so listing a split file once per task made the first compaction of a table holding such a file
// fail, and the trigger, read from the snapshot the failed compaction left alone, failed again on
// every tick (MDS_REVIEW_3.md R3T-1). One entry per path removes exactly the file: the split ranges
// are disjoint row groups, so the read side still reads all of it.
func dataFilesOf(tasks []icebergtable.FileScanTask) []iceberg.DataFile {
	dataFiles := make([]iceberg.DataFile, 0, len(tasks))
	seen := make(map[string]struct{}, len(tasks))
	for _, task := range tasks {
		path := task.File.FilePath()
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
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
