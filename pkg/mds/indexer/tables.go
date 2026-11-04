package indexer

// tables.go declares the seam between the indexer and the Iceberg side of the metadata table
// (design §4.3 step 3, §4.9). pkg/mds/table (M3) knows how to write the table; this package knows
// *when* to write it. The seam is two small interfaces so that every crash window and every
// recovery path of §4.4 can be exercised with a fake table and without a lakeFS server.

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/pkg/mds/table"
)

// Table is the system.object_metadata table of one branch of R-metadata. A handle reflects the
// branch it was loaded from: the catalog reads the staged pointer of a branch, so SourceCommit
// reports a staged position when the branch has uncommitted changes (§3.3, §4.3 step 3).
//
// Handles are not safe for concurrent use; one branch tick owns one handle.
type Table interface {
	// SourceCommit returns the lakefs.mds.source-commit table property, or the empty string when
	// the table has none (a table that was created but never written).
	SourceCommit() string

	// SnapshotCount returns the number of snapshots the table metadata carries. §4.9's
	// every_commits trigger counts them.
	SnapshotCount() int

	// DeleteFiles returns the number of delete files the current snapshot reads. §4.9's
	// max_delete_files trigger counts them.
	DeleteFiles() int

	// ApplyDelta applies one commit of the data repository: the upserts replace the rows of their
	// paths, the deletes remove theirs, and the table property lakefs.mds.source-commit moves to
	// sourceCommit (§4.3 step 3). It returns ErrConflict when another writer moved the table.
	ApplyDelta(ctx context.Context, upserts []table.Row, deletes []string, sourceCommit string) error

	// Rebuild replaces the whole table with rows at sourceCommit (§4.3 step 2).
	Rebuild(ctx context.Context, rows table.Rows, sourceCommit string) error

	// Compact rewrites the table at its current rows under sourceCommit (§4.9).
	Compact(ctx context.Context, sourceCommit string) error

	// Close releases the resources of the handle.
	Close() error
}

// TableStore reads and writes the tables of the companion repositories of the data repositories
// the indexer is configured with. A companion repository is named after its data repository (§3.3),
// so every call names the data repository it indexes into.
type TableStore interface {
	// LoadTable returns the table of one branch of the data repository's companion repository, or
	// ErrTableNotFound when that branch holds no table yet (§4.5). The caller owns the handle and
	// must Close it.
	LoadTable(ctx context.Context, dataRepository, branch string) (Table, error)

	// CreateTable creates the namespace system and the table system.object_metadata on the
	// default branch of the companion repository and returns the handle, with the creation staged
	// (§4.8). It reports ErrTableExists when the table is already there.
	CreateTable(ctx context.Context, dataRepository, branch string) (Table, error)
}

// ErrTableExists is reported by TableStore.CreateTable when the table already exists: an
// initialisation that was interrupted after the table was created but before its commit is
// completed by the next tick (§4.8).
var ErrTableExists = errors.New("the metadata table already exists")
