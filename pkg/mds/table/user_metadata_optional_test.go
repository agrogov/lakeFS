package table_test

// G13 of METADATA_SEARCH_REALIGN.md / RA7: user_metadata is optional.
//
// The current guide declares the column "Required: no" and says that "If none exists, an empty map
// is stored". The schema of this package therefore declares the field optional, keeping its ID
// (7) and the IDs of the map's key and value (8, 9), and the rows it writes still carry an empty
// map - never a null - so nothing a reader sees changes.
//
// Iceberg allows a required field to become optional and refuses the reverse, so a table created
// with the earlier required field is evolved in place, once, by the next write: the tests below
// pin the created table, the legacy table's evolution for every write path, the single update, and
// that the create-path field IDs do not move (TestCreateTableFieldIDs).

import (
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

// legacyUserMetadataSchema is the schema of a table created before G13: the fixed schema of §3.1
// with user_metadata still required. Every ID is the one Schema gives it, because the change is a
// requiredness flag and not a new column.
func legacyUserMetadataSchema(t *testing.T) *iceberg.Schema {
	t.Helper()
	fields := table.Schema().Fields()
	require.Equal(t, table.ColumnUserMetadata, fields[6].Name, "user_metadata is the seventh column")
	require.Equal(t, table.FieldUserMetadata, fields[6].ID)
	fields[6].Required = true
	return iceberg.NewSchemaWithIdentifiers(0, []int{table.FieldPath}, fields...)
}

// appendThroughIceberg commits rows with iceberg-go alone, without this package's writer, so that
// a test can seed a table and still hand the writer a schema that carries the required field.
func appendThroughIceberg(t *testing.T, env *testEnv, tbl *icebergtable.Table, rows []table.Row, sourceCommit string) *icebergtable.Table {
	t.Helper()
	reader, err := table.NewRecordReader(tbl.Schema(), rows, 0)
	require.NoError(t, err)
	defer reader.Release()
	txn := tbl.NewTransaction()
	require.NoError(t, txn.Append(t.Context(), reader, table.SnapshotProperties(env.dataRepository(), sourceCommit)))
	require.NoError(t, txn.SetProperties(iceberg.Properties{table.PropertySourceCommit: sourceCommit}))
	appended, err := txn.Commit(t.Context())
	require.NoError(t, err, "append the seed rows")
	return appended
}

// requireOptionalUserMetadata asserts that sc declares user_metadata optional with the IDs of §3.1.
func requireOptionalUserMetadata(t *testing.T, sc *iceberg.Schema) {
	t.Helper()
	field, ok := sc.FindFieldByName(table.ColumnUserMetadata)
	require.True(t, ok, "user_metadata is a column")
	require.False(t, field.Required, "user_metadata is optional (G13)")
	require.Equal(t, table.FieldUserMetadata, field.ID, "the optional field keeps its ID")
	mapType, ok := field.Type.(*iceberg.MapType)
	require.True(t, ok, "user_metadata is a map")
	require.Equal(t, table.FieldUserMetadataKey, mapType.KeyID, "the map key keeps its ID")
	require.Equal(t, table.FieldUserMetadataValue, mapType.ValueID, "the map value keeps its ID")
	require.False(t, mapType.ValueRequired, "the map value stays optional (§3.1)")
}

// TestSchemaUserMetadataIsOptional pins the create request itself, before any catalog is involved:
// the field keeps ID 7 and the map keeps key 8 and value 9.
func TestSchemaUserMetadataIsOptional(t *testing.T) {
	requireOptionalUserMetadata(t, table.Schema())
	// Nothing else moved: the columns before user_metadata are still required, and the trailing
	// ones are still optional.
	for _, name := range []string{table.ColumnRepository, table.ColumnPath, table.ColumnCommitID,
		table.ColumnSizeBytes, table.ColumnLastModified, table.ColumnETag} {
		field, ok := table.Schema().FindFieldByName(name)
		require.True(t, ok, "column %s", name)
		require.True(t, field.Required, "column %s stays required", name)
	}
	for _, name := range []string{table.ColumnCommitter, table.ColumnContentType, table.ColumnCreationDate} {
		field, ok := table.Schema().FindFieldByName(name)
		require.True(t, ok, "column %s", name)
		require.False(t, field.Required, "column %s stays optional", name)
	}
}

// TestCreatedTableReadsBackOptionalUserMetadata is the first case of the card: a table created with
// the new schema reads back - through the catalog, as a reader loads it - with the optional field,
// and a row with no user metadata is stored as an empty map rather than a null.
func TestCreatedTableReadsBackOptionalUserMetadata(t *testing.T) {
	env := newTestEnv(t)
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	created := env.createTable(t)
	requireOptionalUserMetadata(t, created.Schema())

	_, err := table.ApplyDelta(t.Context(), created, []table.Row{
		row(t, "a", "c1", 1, when, nil),
		row(t, "b", "c1", 2, when, map[string]string{"k": "v"}),
	}, nil, "c1")
	require.NoError(t, err)

	reloaded := env.loadTable(t)
	requireOptionalUserMetadata(t, reloaded.Schema())
	rows := scanRows(t, reloaded)
	require.Equal(t, []string{"a", "b"}, pathsOf(rows))
	require.NotNil(t, rows[0].UserMetadata, "an object without user metadata gets an empty map, not a null")
	require.Empty(t, rows[0].UserMetadata)
	require.Equal(t, map[string]string{"k": "v"}, rows[1].UserMetadata)
}

// TestWriterEvolvesLegacyUserMetadataOnce covers the second case of the card for every write path:
// a table created with the required field accepts the update, and a row with no user_metadata is
// then written and read back.
//
// Two things make the update "once" observable. The table carries exactly two schemas after the
// write and still two after a second write of the same kind, so the update was added once and not
// re-applied; and the second write adds as many snapshots as the first one did, so the update
// itself - which happened only before the first - added no snapshot: it is metadata-only, and the
// row set a caller read is the row set the write committed.
func TestWriterEvolvesLegacyUserMetadataOnce(t *testing.T) {
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	writes := []struct {
		name string
		// write runs one write of the package against a legacy table and returns its result.
		write func(t *testing.T, tbl *icebergtable.Table) *icebergtable.Table
		// paths are the paths the table holds afterwards.
		paths []string
	}{
		{
			name: "delta",
			write: func(t *testing.T, tbl *icebergtable.Table) *icebergtable.Table {
				updated, err := table.ApplyDelta(t.Context(), tbl, []table.Row{
					row(t, "c", "c2", 3, when, nil),
				}, nil, "c2")
				require.NoError(t, err)
				return updated
			},
			paths: []string{"a", "c"},
		},
		{
			name: "rebuild",
			write: func(t *testing.T, tbl *icebergtable.Table) *icebergtable.Table {
				rebuilt, err := table.Rebuild(t.Context(), tbl, rowSeq([]table.Row{
					row(t, "a", "c2", 1, when, nil),
					row(t, "c", "c2", 3, when, nil),
				}), "c2")
				require.NoError(t, err)
				return rebuilt
			},
			paths: []string{"a", "c"},
		},
		{
			name: "compaction",
			write: func(t *testing.T, tbl *icebergtable.Table) *icebergtable.Table {
				compacted, err := table.Compact(t.Context(), tbl, "c2")
				require.NoError(t, err)
				return compacted
			},
			paths: []string{"a"},
		},
	}
	for _, tc := range writes {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestEnv(t)
			legacy := env.createTableWith(t, legacyUserMetadataSchema(t), nil)
			field, ok := legacy.Schema().FindFieldByName(table.ColumnUserMetadata)
			require.True(t, ok)
			require.True(t, field.Required, "the fixture starts where a table created before G13 starts")

			seeded := appendThroughIceberg(t, env, legacy, []table.Row{
				row(t, "a", "c1", 1, when, nil),
			}, "c1")
			before := snapshotCount(seeded)

			updated := tc.write(t, seeded)
			reloaded := env.loadTable(t)
			requireOptionalUserMetadata(t, reloaded.Schema())
			require.Len(t, reloaded.Metadata().Schemas(), 2,
				"one schema was added, so the update ran once")
			firstWrite := snapshotCount(reloaded) - before
			require.Positive(t, firstWrite)

			// The same write again: the table it starts from already carries the optional field, so
			// the schema update cannot run, and the write's own snapshot count is the measure of what
			// the first write's delta was.
			second := tc.write(t, updated)
			require.Equal(t, firstWrite, snapshotCount(second)-snapshotCount(reloaded),
				"the schema update is metadata-only: it added no snapshot")
			require.Len(t, second.Metadata().Schemas(), 2,
				"the second write re-applies no schema update")

			requireOptionalUserMetadata(t, env.loadTable(t).Schema())
			rows := scanRows(t, second)
			require.Equal(t, tc.paths, pathsOf(rows))
			for _, r := range rows {
				require.NotNil(t, r.UserMetadata, "%s: an object without user metadata gets an empty map", r.Path)
				require.Empty(t, r.UserMetadata)
			}
		})
	}
}

// commitRecorder counts the commits a client sends: a POST of an update to the table. It is how a
// test sees that the schema update is a commit of its own, and that it is not re-sent.
type commitRecorder struct {
	mu       sync.Mutex
	requests []string
}

// wrap installs the recorder over the catalog's HTTP handler.
func (r *commitRecorder) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/namespaces/"+table.Namespace+"/tables/"+table.Name) {
			r.mu.Lock()
			r.requests = append(r.requests, req.URL.Path)
			r.mu.Unlock()
		}
		next.ServeHTTP(w, req)
	})
}

// commits returns how many commits the client sent so far.
func (r *commitRecorder) commits() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.requests)
}

// TestWriterAppliesUserMetadataUpdateOnce is the third case of the card: the update lands with the
// first write of a legacy table - one commit before the write's own - and the second write sends
// its own commit alone, because the table it loads already carries the optional field.
func TestWriterAppliesUserMetadataUpdateOnce(t *testing.T) {
	recorder := &commitRecorder{}
	env := newTestEnvWith(t, testEnvOptions{wrapHandler: recorder.wrap})
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	legacy := env.createTableWith(t, legacyUserMetadataSchema(t), nil)

	_, err := table.ApplyDelta(t.Context(), legacy, []table.Row{
		row(t, "a", "c1", 1, when, nil),
	}, nil, "c1")
	require.NoError(t, err)
	require.Equal(t, 2, recorder.commits(), "the first write evolves the schema and then commits the delta")

	reloaded := env.loadTable(t)
	requireOptionalUserMetadata(t, reloaded.Schema())

	_, err = table.ApplyDelta(t.Context(), reloaded, []table.Row{
		row(t, "b", "c2", 2, when, nil),
	}, nil, "c2")
	require.NoError(t, err)
	require.Equal(t, 3, recorder.commits(), "the second write sends one commit: the update is not re-applied")

	after := env.loadTable(t)
	requireOptionalUserMetadata(t, after.Schema())
	require.Len(t, after.Metadata().Schemas(), 2, "still one schema update, not two")
	require.Equal(t, []string{"a", "b"}, pathsOf(scanRows(t, after)))
}
