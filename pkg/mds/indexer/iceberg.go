package indexer

// iceberg.go is the production half of the TableStore seam: it reads and writes the
// system.object_metadata table of one branch of R-metadata through the lakeFS Iceberg REST
// catalog, using the writer of pkg/mds/table (design §4.3 step 3, §4.6, §4.9).
//
// Each branch of R-metadata has its own catalog URL - <endpoint>/iceberg/relative_to/
// <R-metadata>.<branch>/api - so a handle is bound to one branch and carries the catalog it was
// loaded from: the transaction of a write commits through that catalog. A handle is therefore
// closed when the branch is done with (Table.Close), not earlier.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

// snapshotPropertyDeleteFiles is the snapshot summary key iceberg-go maintains for the number of
// delete files the table carries. It is what the compaction trigger of §4.9 counts.
const snapshotPropertyDeleteFiles = "total-delete-files"

// IcebergStore is the production TableStore: the metadata tables of the companion repositories,
// read and written through the lakeFS Iceberg REST catalog. It is stateless: every call addresses
// the companion repository R-metadata of the data repository it is given (§3.3).
type IcebergStore struct {
	// Endpoint is the lakeFS base URL. The catalog lives under it (§4.3 step 3).
	Endpoint string

	// AccessKeyID and SecretAccessKey are the credentials of the indexer principal (§8), sent as
	// HTTP basic authentication on every catalog request.
	AccessKeyID     string
	SecretAccessKey string

	// Storage holds the iceberg-go IO properties used when the catalog does not vend write
	// credentials (§4.6).
	Storage iceberg.Properties

	// RequestTimeout bounds every HTTP request this store's catalog client sends (§4.1, §7). It is
	// the same bound the lakeFS API client of the indexer uses; zero means the table package's
	// default. Iceberg-go writes data, manifest and metadata files through its own IO, not
	// through this client, so a long write does not race this timeout.
	RequestTimeout time.Duration
}

var _ TableStore = (*IcebergStore)(nil)

// LoadTable implements TableStore: the table of one branch of R-metadata, with the staged writes
// of that branch if it has any (§4.3 step 3).
func (s *IcebergStore) LoadTable(ctx context.Context, dataRepository, branch string) (Table, error) {
	cat, err := s.catalog(ctx, dataRepository, branch)
	if err != nil {
		return nil, err
	}
	tbl, err := cat.LoadTable(ctx, table.Identifier())
	if err != nil {
		_ = cat.Close()
		if errors.Is(err, catalog.ErrNoSuchTable) {
			return nil, ErrTableNotFound
		}
		return nil, fmt.Errorf("load %s.%s of %s@%s: %w",
			table.Namespace, table.Name, metadataRepository(dataRepository), branch, err)
	}
	return &icebergTable{catalog: cat, table: tbl}, nil
}

// CreateTable implements TableStore: it creates the namespace system and the table
// system.object_metadata on the default branch of R-metadata (§3.3, §4.8). Both stay staged until
// the indexer commits the branch, which is what makes the initialisation commit atomic.
func (s *IcebergStore) CreateTable(ctx context.Context, dataRepository, branch string) (Table, error) {
	cat, err := s.catalog(ctx, dataRepository, branch)
	if err != nil {
		return nil, err
	}
	if err := cat.CreateNamespace(ctx, icebergtable.Identifier{table.Namespace}, nil); err != nil &&
		!errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
		_ = cat.Close()
		return nil, fmt.Errorf("create namespace %q of %s@%s: %w",
			table.Namespace, metadataRepository(dataRepository), branch, err)
	}
	tbl, err := table.CreateTable(ctx, cat, dataRepository, nil)
	if err != nil {
		_ = cat.Close()
		if errors.Is(err, catalog.ErrTableAlreadyExists) {
			return nil, ErrTableExists
		}
		return nil, fmt.Errorf("create %s.%s of %s@%s: %w",
			table.Namespace, table.Name, metadataRepository(dataRepository), branch, err)
	}
	return &icebergTable{catalog: cat, table: tbl}, nil
}

// catalog builds the REST catalog of one branch of R-metadata (§4.3 step 3).
func (s *IcebergStore) catalog(ctx context.Context, dataRepository, branch string) (*rest.Catalog, error) {
	return table.NewCatalog(ctx, table.CatalogConfig{
		Endpoint:        s.Endpoint,
		Repository:      metadataRepository(dataRepository),
		Branch:          branch,
		AccessKeyID:     s.AccessKeyID,
		SecretAccessKey: s.SecretAccessKey,
		Storage:         s.Storage,
		RequestTimeout:  s.RequestTimeout,
	})
}

// icebergTable is one Table of one branch of R-metadata: the loaded table and the catalog it was
// loaded from.
type icebergTable struct {
	catalog *rest.Catalog
	table   *icebergtable.Table
}

var _ Table = (*icebergTable)(nil)

// SourceCommit implements Table: the lakefs.mds.source-commit property of the table, which on a
// branch is the staged value when the branch has uncommitted changes and the committed value
// otherwise (§3.3).
func (t *icebergTable) SourceCommit() string {
	return t.table.Properties()[table.PropertySourceCommit]
}

// SnapshotCount implements Table.
func (t *icebergTable) SnapshotCount() int {
	return len(t.table.Metadata().Snapshots())
}

// Refresh implements Table: it reloads the table through the catalog the handle was loaded from.
// A fresh TableStore.LoadTable would build a new REST catalog, whose construction eagerly fetches
// /v1/config and whose Close drops the connection pool, once per applied commit (IX-3).
func (t *icebergTable) Refresh(ctx context.Context) error {
	return t.table.Refresh(ctx)
}

// DeleteFiles implements Table: the number of delete files of the current snapshot, read from the
// snapshot summary instead of the manifest list so that a tick does not pay an extra read for a
// maintenance decision (§4.9).
func (t *icebergTable) DeleteFiles() int {
	snapshot := t.table.CurrentSnapshot()
	if snapshot == nil || snapshot.Summary == nil {
		return 0
	}
	files, err := strconv.Atoi(snapshot.Summary.Properties[snapshotPropertyDeleteFiles])
	if err != nil {
		return 0
	}
	return files
}

// ApplyDelta implements Table (§4.3 step 3).
func (t *icebergTable) ApplyDelta(ctx context.Context, upserts []table.Row, deletes []string, sourceCommit string) error {
	updated, err := table.ApplyDelta(ctx, t.table, upserts, deletes, sourceCommit)
	if err != nil {
		return tableError(err)
	}
	t.table = updated
	return nil
}

// Rebuild implements Table (§4.3 step 2).
func (t *icebergTable) Rebuild(ctx context.Context, rows table.Rows, sourceCommit string) error {
	updated, err := table.Rebuild(ctx, t.table, rows, sourceCommit)
	if err != nil {
		return tableError(err)
	}
	t.table = updated
	return nil
}

// Compact implements Table (§4.9).
func (t *icebergTable) Compact(ctx context.Context, sourceCommit string) error {
	updated, err := table.Compact(ctx, t.table, sourceCommit)
	if err != nil {
		return tableError(err)
	}
	t.table = updated
	return nil
}

// Close implements Table: it releases the catalog of the branch.
func (t *icebergTable) Close() error {
	if t.catalog == nil {
		return nil
	}
	err := t.catalog.Close()
	t.catalog = nil
	return err
}

// tableError maps the writer's errors onto the indexer's: a refused commit is ErrConflict, which
// §4.4 answers by reloading the table and re-running recovery.
//
// table.ErrStaleTable is the writer's other "the table moved since it was read" answer (the repair
// commit of ensureNoReplay observed the move, pkg/mds/table/writer.go), and the writer documents it
// as the caller's retry ("the caller reloads and retries the tick (§4.4)"). Mapping only
// icebergtable.ErrCommitFailed made a tick that met it fail the branch for the whole period instead
// of reloading and retrying like the 409 path (R3I-3).
func tableError(err error) error {
	if errors.Is(err, icebergtable.ErrCommitFailed) || errors.Is(err, table.ErrStaleTable) {
		return fmt.Errorf("%w: %w", ErrConflict, err)
	}
	return err
}
