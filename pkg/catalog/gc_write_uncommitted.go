package catalog

import (
	"context"
	"errors"
	"strings"
	stdatomic "sync/atomic"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// UncommittedGCProtector decides whether a staged object the uncommitted-GC prepare step has just
// listed must be left out of the report, i.e. must not be offered to the GC job for deletion.
//
// key is the object key relative to the repository's storage namespace - the form the report
// stores as physical_address - lastModified is the staged entry's recorded modification time and
// now is the clock the decision is taken at.
//
// The uncommitted GC has no other extension point: it walks branches, takes each branch's
// uncommitted diff and writes one row per staged entry. Installations that need to protect part of
// the namespace from that report (the Iceberg catalog does, see ICEBERG_CATALOG_PLAN.md §6.3 and
// pkg/icebergcatalog/gc.go) install a protector at startup with SetUncommittedGCProtector; with no
// protector installed every staged entry is reported, which is the upstream behaviour.
type UncommittedGCProtector func(key string, lastModified, now time.Time) bool

// uncommittedGCProtector holds the protector installed by SetUncommittedGCProtector. It is read
// on every staged entry of every prepare run and written once at startup, so it is an atomic
// pointer rather than a plain variable.
var uncommittedGCProtector stdatomic.Pointer[UncommittedGCProtector]

// SetUncommittedGCProtector installs protector, or clears it when protector is nil. It is called
// once at startup by the wrapper binary that owns the protected area; a nil protector keeps the
// default behaviour of reporting every staged object.
func SetUncommittedGCProtector(protector UncommittedGCProtector) {
	if protector == nil {
		uncommittedGCProtector.Store(nil)
		return
	}
	uncommittedGCProtector.Store(&protector)
}

// uncommittedGCSkip applies the installed protector to one staged entry. An entry with no
// modification time is handed over as the zero time, which a protector reads as "age unknown".
func uncommittedGCSkip(key string, lastModified *timestamppb.Timestamp) bool {
	protector := uncommittedGCProtector.Load()
	if protector == nil {
		return false
	}
	var modified time.Time
	if lastModified != nil {
		modified = lastModified.AsTime()
	}
	return (*protector)(key, modified, time.Now())
}

func gcWriteUncommitted(ctx context.Context, store Store, repository *graveler.RepositoryRecord, w *UncommittedWriter, mark *GCUncommittedMark, runID string, maxFileSize int64, prepareDuration time.Duration) (*GCUncommittedMark, bool, error) {
	pw, err := writer.NewParquetWriterFromWriter(w, new(UncommittedParquetObject), gcParquetParallelNum)
	if err != nil {
		return nil, false, err
	}
	pw.CompressionType = parquet.CompressionCodec_GZIP

	branchIterator, err := store.ListBranches(ctx, repository, graveler.WithShowHidden(true))
	if err != nil {
		return nil, false, err
	}
	defer branchIterator.Close()

	normalizedStorageNamespace := normalizeStorageNamespace(string(repository.StorageNamespace))

	if mark != nil {
		branchIterator.SeekGE(mark.BranchID)
	}

	count := 0
	var nextMark *GCUncommittedMark
	hasData := false
	startTime := time.Now()
	for branchIterator.Next() {
		nextMark, count, err = processBranch(ctx, store, repository, branchIterator.Value().BranchID, runID, pw, normalizedStorageNamespace, maxFileSize, prepareDuration, w, count, mark, startTime)
		if err != nil && !errors.Is(err, graveler.ErrBranchNotFound) {
			// return on error, unless its a branch not found error.
			// on branch not found, fall through to the next branch (next mark nil on error) to keep duration check.
			return nil, false, err
		}
		if nextMark != nil {
			break
		}

		// check if we need to stop - based on prepare duration only.
		if prepareDuration > 0 && time.Since(startTime) > prepareDuration {
			if !branchIterator.Next() {
				// last branch processed, no next mark
				break
			}
			// set next mark to the next branch
			nextBranchID := branchIterator.Value().BranchID
			nextMark = &GCUncommittedMark{
				RunID:    runID,
				BranchID: nextBranchID,
			}
			break
		}
	}
	if branchIterator.Err() != nil {
		return nil, false, branchIterator.Err()
	}

	if err := pw.WriteStop(); err != nil {
		return nil, false, err
	}

	if count > 0 {
		hasData = true
	}
	return nextMark, hasData, err
}

func normalizeStorageNamespace(namespace string) string {
	if !strings.HasSuffix(namespace, DefaultPathDelimiter) {
		namespace += DefaultPathDelimiter
	}
	return namespace
}

func processBranch(ctx context.Context, store Store, repository *graveler.RepositoryRecord, branchID graveler.BranchID, runID string, parquetWriter *writer.ParquetWriter, normalizedStorageNamespace string, maxFileSize int64, prepareDuration time.Duration, writer *UncommittedWriter, count int, mark *GCUncommittedMark, startTime time.Time) (*GCUncommittedMark, int, error) {
	diffIterator, err := store.DiffUncommitted(ctx, repository, branchID)
	if err != nil {
		return nil, 0, err
	}
	defer diffIterator.Close()

	var nextMark *GCUncommittedMark

	if mark != nil && mark.BranchID == branchID && mark.Path != "" {
		diffIterator.SeekGE(graveler.Key(mark.Path))
	}

	for diffIterator.Next() {
		diff := diffIterator.Value()

		// Skip tombstones
		if diff.Type == graveler.DiffTypeRemoved {
			continue
		}

		entry, err := ValueToEntry(diff.Value)
		if err != nil {
			return nil, 0, err
		}

		// Skip non-relative addresses outside the storage namespace
		entryAddress := entry.Address
		if entry.AddressType != Entry_RELATIVE {
			if !strings.HasPrefix(entry.Address, normalizedStorageNamespace) {
				continue
			}
			entryAddress = entryAddress[len(normalizedStorageNamespace):]
		}

		// The key is now relative to the storage namespace, which is what an installed protector
		// expects; a protected entry is left out of the report entirely, so it is neither counted
		// towards this page nor written to the parquet file the GC job reads.
		if uncommittedGCSkip(entryAddress, entry.LastModified) {
			continue
		}

		count++
		if count%gcPeriodicCheckSize == 0 {
			if err := parquetWriter.Flush(true); err != nil {
				return nil, 0, err
			}
		}

		// check if we need to stop - based on max file size or prepare duration.
		// prepare duration is optional, if 0 it will be ignored.
		// prepare duration is used to stop the process in cases we scan a lot of data, and we want to stop
		// so the api call will not time out.
		if writer.Size() > maxFileSize || (prepareDuration > 0 && time.Since(startTime) > prepareDuration) {
			nextMark = &GCUncommittedMark{
				RunID:    runID,
				BranchID: branchID,
				Path:     Path(diff.Key.String()),
			}
			break
		}

		err = parquetWriter.Write(UncommittedParquetObject{
			PhysicalAddress: entryAddress,
			CreationDate:    entry.LastModified.AsTime().Unix(),
		})
		if err != nil {
			return nil, 0, err
		}
	}

	return nextMark, count, diffIterator.Err()
}
