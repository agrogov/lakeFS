package icebergcatalog

// Tests for the read half of the catalog store (ICEBERG_CATALOG_PLAN.md §6.1, §12 "unit tests:
// identifier parsing" and "CAS race" neighbours). The fixture is a real catalog store (graveler +
// committed ranges) on an in-memory KV store and an in-memory block adapter, like the datasets
// fixture, so "staged wins over committed" and "a tag/commit is readable" are facts of the same
// graveler code the server runs, not assumptions of a mock.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	_ "github.com/treeverse/lakefs/pkg/kv/mem" // register the "mem" KV driver
	"github.com/treeverse/lakefs/pkg/upload"
)

const (
	storeTestRepo = "repo"
	storeTestUUID = "9c12d441-03fe-4693-9a96-a0705ddf69c1"
	storeAltUUID  = "11111111-1111-1111-1111-111111111111"
)

var (
	storeTestConfigOnce sync.Once
	storeTestConfigBase config.ConfigImpl
	storeTestConfigErr  error
)

// storeTestConfig builds the process configuration the fixture's catalog is built from.
// config.NewConfig writes process globals, so it runs once (like the datasets fixture) rather than
// per fixture.
func storeTestConfig(t *testing.T) *config.ConfigImpl {
	t.Helper()
	storeTestConfigOnce.Do(func() {
		// The catalog builds its block adapter from the process configuration; force mem.
		viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
		_, storeTestConfigErr = config.NewConfig("", &storeTestConfigBase)
	})
	require.NoError(t, storeTestConfigErr, "build test config")
	cfg := storeTestConfigBase // nested sections are values, so this copy belongs to the fixture
	return &cfg
}

type storeFixture struct {
	ctx     context.Context
	catalog *catalog.Catalog
	store   *Store
}

func newStoreFixture(t *testing.T) *storeFixture {
	t.Helper()
	ctx := t.Context()

	kvStore, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	require.NoError(t, err, "open mem KV store")
	t.Cleanup(func() { kvStore.Close() })

	cat, err := catalog.New(ctx, catalog.Config{
		Config:       storeTestConfig(t),
		KVStore:      kvStore,
		PathProvider: upload.DefaultPathProvider,
	})
	require.NoError(t, err, "build catalog")
	t.Cleanup(func() { _ = cat.Close() })

	_, err = cat.CreateRepository(ctx, storeTestRepo, "", "mem://"+storeTestRepo+"-ns", "main", false)
	require.NoError(t, err, "create repository")

	return &storeFixture{ctx: ctx, catalog: cat, store: NewStore(cat, cat.BlockAdapter)}
}

// stageEntry stages one ordinary entry on main, exactly how a catalog write will create a marker
// or a pointer (I2-1 adds the conditional variant).
func (f *storeFixture) stageEntry(t *testing.T, entryPath string, metadata map[string]string) {
	t.Helper()
	err := f.catalog.CreateEntry(f.ctx, storeTestRepo, "main", catalog.DBEntry{
		Path:            entryPath,
		PhysicalAddress: entryPath,
		AddressType:     catalog.AddressTypeRelative,
		CreationDate:    time.Now(),
		Metadata:        metadata,
	})
	require.NoError(t, err, "stage entry %s", entryPath)
}

func (f *storeFixture) commit(t *testing.T, message string) string {
	t.Helper()
	log, err := f.catalog.Commit(f.ctx, storeTestRepo, "main", message, "store-test", nil, nil, nil, false)
	require.NoError(t, err, "commit %s", message)
	return log.Reference
}

func (f *storeFixture) createTag(t *testing.T, tagID, ref string) {
	t.Helper()
	_, err := f.catalog.CreateTag(f.ctx, storeTestRepo, tagID, ref)
	require.NoError(t, err, "create tag %s at %s", tagID, ref)
}

// putMetadata writes content at a fully qualified URI under the repository storage namespace and
// returns that URI, the way a client (or the commit path of I2-1) puts a metadata.json.
func (f *storeFixture) putMetadata(t *testing.T, key, content string) string {
	t.Helper()
	repo, err := f.catalog.GetRepository(f.ctx, storeTestRepo)
	require.NoError(t, err, "get repository")
	location := strings.TrimRight(repo.StorageNamespace, "/") + "/" + key
	_, err = f.catalog.BlockAdapter.Put(f.ctx, block.ObjectPointer{
		StorageID:        repo.StorageID,
		StorageNamespace: repo.StorageNamespace,
		Identifier:       location,
		IdentifierType:   block.IdentifierTypeFull,
	}, int64(len(content)), strings.NewReader(content), block.PutOpts{})
	require.NoError(t, err, "put metadata at %s", location)
	return location
}

func pointerMetadata(location, tableUUID string, formatVersion int) map[string]string {
	return map[string]string{
		MetadataLocationKey: location,
		TableUUIDKey:        tableUUID,
		FormatVersionKey:    strconv.Itoa(formatVersion),
	}
}

// storeTestMetadataTemplate is the v2 example metadata of the Iceberg table spec. Only the
// table-uuid and the table location vary between fixtures; the snapshots point at manifest lists
// this test never reads.
const storeTestMetadataTemplate = `{
  "format-version": 2,
  "table-uuid": %q,
  "location": %q,
  "last-sequence-number": 34,
  "last-updated-ms": 1602638573590,
  "last-column-id": 3,
  "current-schema-id": 1,
  "schemas": [
    {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]},
    {
      "type": "struct",
      "schema-id": 1,
      "identifier-field-ids": [1, 2],
      "fields": [
        {"id": 1, "name": "x", "required": true, "type": "long"},
        {"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"},
        {"id": 3, "name": "z", "required": true, "type": "long"}
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
  "last-partition-id": 1000,
  "default-sort-order-id": 3,
  "sort-orders": [
    {
      "order-id": 3,
      "fields": [
        {"transform": "identity", "source-id": 2, "direction": "asc", "null-order": "nulls-first"},
        {"transform": "bucket[4]", "source-id": 3, "direction": "desc", "null-order": "nulls-last"}
      ]
    }
  ],
  "properties": {"read.split.target.size": "134217728"},
  "current-snapshot-id": 3055729675574597004,
  "snapshots": [
    {
      "snapshot-id": 3051729675574597004,
      "timestamp-ms": 1515100955770,
      "sequence-number": 0,
      "summary": {"operation": "append"},
      "manifest-list": "s3://a/b/1.avro"
    },
    {
      "snapshot-id": 3055729675574597004,
      "parent-snapshot-id": 3051729675574597004,
      "timestamp-ms": 1555100955770,
      "sequence-number": 1,
      "summary": {"operation": "append"},
      "manifest-list": "s3://a/b/2.avro",
      "schema-id": 1
    }
  ],
  "snapshot-log": [
    {"snapshot-id": 3051729675574597004, "timestamp-ms": 1515100955770},
    {"snapshot-id": 3055729675574597004, "timestamp-ms": 1555100955770}
  ],
  "metadata-log": [{"metadata-file": "s3://bucket/.../v1.json", "timestamp-ms": 1515100}],
  "refs": {"test": {"snapshot-id": 3051729675574597004, "type": "tag", "max-ref-age-ms": 10000000}}
}`

func storeTestMetadata(t *testing.T, tableUUID, tableLocation string) string {
	t.Helper()
	document := fmt.Sprintf(storeTestMetadataTemplate, tableUUID, tableLocation)
	require.Contains(t, document, tableUUID)
	return document
}

func TestStoreRead(t *testing.T) {
	namespace := Namespace{"inventory", "books"}

	t.Run("namespace marker", func(t *testing.T) {
		f := newStoreFixture(t)
		markerPath, err := namespaceMarkerPath(namespace)
		require.NoError(t, err)
		require.Equal(t, "_lakefs_iceberg/ns/inventory/books/.namespace", markerPath,
			"the layout is the one ICEBERG_CATALOG_PLAN.md section 6.1 documents")

		properties := map[string]string{"owner": "data-team", "description": "book inventory"}
		f.stageEntry(t, markerPath, properties)

		got, err := f.store.GetNamespace(f.ctx, Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace})
		require.NoError(t, err)
		require.Equal(t, properties, got.Properties)
	})

	t.Run("table pointer and metadata on a branch", func(t *testing.T) {
		f := newStoreFixture(t)
		tableLocation := "mem://" + storeTestRepo + "-ns/_iceberg/" + storeTestUUID
		metadataLocation := f.putMetadata(t,
			"_iceberg/"+storeTestUUID+"/metadata/00001-"+storeTestUUID+".metadata.json",
			storeTestMetadata(t, storeTestUUID, tableLocation))

		pointerPath, err := tablePointerPath(Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace, Table: "orders"})
		require.NoError(t, err)
		require.Equal(t, "_lakefs_iceberg/tables/inventory/books/orders/.pointer", pointerPath)
		f.stageEntry(t, pointerPath, pointerMetadata(metadataLocation, storeTestUUID, 2))

		got, err := f.store.GetTable(f.ctx, Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace, Table: "orders"})
		require.NoError(t, err)
		require.Equal(t, TablePointer{
			MetadataLocation: metadataLocation,
			TableUUID:        storeTestUUID,
			FormatVersion:    2,
		}, got.Pointer)
		require.Equal(t, storeTestUUID, got.Metadata.TableUUID().String())
		require.Equal(t, tableLocation, got.Metadata.Location())
		require.Equal(t, 2, got.Metadata.Version())
		require.NotEmpty(t, got.MetadataJSON, "the raw metadata.json is kept for cheap pass-through")
		require.Equal(t, "orders", got.Address.Table)
	})

	t.Run("staged pointer wins, commit and tag refs stay read-only", func(t *testing.T) {
		f := newStoreFixture(t)
		mainAddr := Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace, Table: "orders"}
		pointerPath, err := tablePointerPath(mainAddr)
		require.NoError(t, err)

		oldLocation := f.putMetadata(t,
			"_iceberg/"+storeTestUUID+"/metadata/00001.metadata.json",
			storeTestMetadata(t, storeTestUUID, "mem://"+storeTestRepo+"-ns/_iceberg/"+storeTestUUID))
		f.stageEntry(t, pointerPath, pointerMetadata(oldLocation, storeTestUUID, 2))
		oldCommit := f.commit(t, "create the table")

		newTableUUID := storeAltUUID
		newLocation := f.putMetadata(t,
			"_iceberg/"+newTableUUID+"/metadata/00002.metadata.json",
			storeTestMetadata(t, newTableUUID, "mem://"+storeTestRepo+"-ns/_iceberg/"+newTableUUID))
		f.stageEntry(t, pointerPath, pointerMetadata(newLocation, newTableUUID, 2))

		// The branch answers with the staged pointer before its lakeFS commit.
		staged, err := f.store.GetTable(f.ctx, mainAddr)
		require.NoError(t, err)
		require.Equal(t, newTableUUID, staged.Metadata.TableUUID().String())

		// A commit ref is immutable: it still answers with the metadata it was created with.
		commitAddr := mainAddr
		commitAddr.Ref = oldCommit
		atCommit, err := f.store.GetTable(f.ctx, commitAddr)
		require.NoError(t, err)
		require.Equal(t, storeTestUUID, atCommit.Metadata.TableUUID().String())
		require.Equal(t, oldLocation, atCommit.Pointer.MetadataLocation)

		// A tag is just another ref: the same read path, the same answer.
		f.createTag(t, "v1", oldCommit)
		tagAddr := mainAddr
		tagAddr.Ref = "v1"
		atTag, err := f.store.GetTable(f.ctx, tagAddr)
		require.NoError(t, err)
		require.Equal(t, storeTestUUID, atTag.Metadata.TableUUID().String())
		require.Equal(t, oldLocation, atTag.Pointer.MetadataLocation)
	})

	t.Run("missing marker is NoSuchNamespaceException", func(t *testing.T) {
		f := newStoreFixture(t)
		_, err := f.store.GetNamespace(f.ctx, Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace})
		require.Error(t, err)
		var missing *NoSuchNamespaceException
		require.True(t, errors.As(err, &missing), "got %T: %v", err, err)
		require.Equal(t, namespace, missing.Namespace)
		require.Equal(t, "main", missing.Ref)
	})

	t.Run("missing pointer is NoSuchTableException", func(t *testing.T) {
		f := newStoreFixture(t)
		_, err := f.store.GetTable(f.ctx, Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace, Table: "orders"})
		require.Error(t, err)
		var missing *NoSuchTableException
		require.True(t, errors.As(err, &missing), "got %T: %v", err, err)
		require.Equal(t, "orders", missing.Table)
		require.Equal(t, namespace, missing.Namespace)
	})

	t.Run("pointer fields are validated", func(t *testing.T) {
		f := newStoreFixture(t)
		addr := Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace, Table: "orders"}
		pointerPath, err := tablePointerPath(addr)
		require.NoError(t, err)

		// No metadata-location: the pointer is corrupt, not missing.
		f.stageEntry(t, pointerPath, map[string]string{
			TableUUIDKey:     storeTestUUID,
			FormatVersionKey: "2",
		})
		_, err = f.store.GetTable(f.ctx, addr)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidPointer)

		var missing *NoSuchTableException
		require.False(t, errors.As(err, &missing), "an existing but invalid pointer is not a missing table")

		tests := []struct {
			name     string
			metadata map[string]string
			want     string
		}{
			{
				name: "missing table uuid",
				metadata: map[string]string{
					MetadataLocationKey: "mem://repo-ns/metadata.json",
					FormatVersionKey:    "2",
				},
				want: TableUUIDKey,
			},
			{
				name: "missing format version",
				metadata: map[string]string{
					MetadataLocationKey: "mem://repo-ns/metadata.json",
					TableUUIDKey:        storeTestUUID,
				},
				want: FormatVersionKey,
			},
			{
				name: "non-integer format version",
				metadata: map[string]string{
					MetadataLocationKey: "mem://repo-ns/metadata.json",
					TableUUIDKey:        storeTestUUID,
					FormatVersionKey:    "two",
				},
				want: FormatVersionKey,
			},
			{
				name: "unsupported format version",
				metadata: map[string]string{
					MetadataLocationKey: "mem://repo-ns/metadata.json",
					TableUUIDKey:        storeTestUUID,
					FormatVersionKey:    "3",
				},
				want: FormatVersionKey,
			},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				f := newStoreFixture(t)
				f.stageEntry(t, pointerPath, tc.metadata)
				_, err := f.store.GetTable(f.ctx, addr)
				require.Error(t, err)
				require.ErrorIs(t, err, ErrInvalidPointer)
				require.Contains(t, err.Error(), tc.want)
			})
		}
	})

	t.Run("pointer to a missing metadata file is an error, not a missing table", func(t *testing.T) {
		f := newStoreFixture(t)
		addr := Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace, Table: "orders"}
		pointerPath, err := tablePointerPath(addr)
		require.NoError(t, err)
		f.stageEntry(t, pointerPath, pointerMetadata("mem://"+storeTestRepo+"-ns/_iceberg/gone/metadata.json", storeTestUUID, 2))

		_, err = f.store.GetTable(f.ctx, addr)
		require.Error(t, err)
		require.Contains(t, err.Error(), "read iceberg metadata")

		var missing *NoSuchTableException
		require.False(t, errors.As(err, &missing), "the pointer exists; the metadata file is what is absent")
	})
}
