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
	"github.com/treeverse/lakefs/pkg/graveler"
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
	return newStoreFixtureReadOnly(t, false)
}

// newStoreFixtureReadOnly builds the same fixture on a repository created read-only, i.e. an
// immutable (datasets) repository. A catalog write against it must be rejected without Force.
func newStoreFixtureReadOnly(t *testing.T, readOnly bool) *storeFixture {
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

	_, err = cat.CreateRepository(ctx, storeTestRepo, "", "mem://"+storeTestRepo+"-ns", "main", readOnly)
	require.NoError(t, err, "create repository")

	return &storeFixture{ctx: ctx, catalog: cat, store: NewStore(cat, cat.BlockAdapter)}
}

// stageEntry stages one ordinary entry on main, unconditionally: the write of a fixture that is
// not testing the condition itself.

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

// headRef is the commit main points at, i.e. what an explicit lakeFS commit last wrote.
func (f *storeFixture) headRef(t *testing.T) string {
	t.Helper()
	ref, err := f.catalog.GetBranchReference(f.ctx, storeTestRepo, "main")
	require.NoError(t, err, "get the main branch reference")
	return ref
}

// entryAt reads the raw entry at path on ref, through the same staged-then-committed resolution a
// reader gets.
func (f *storeFixture) entryAt(t *testing.T, ref, entryPath string) (*catalog.DBEntry, error) {
	t.Helper()
	return f.catalog.GetEntry(f.ctx, storeTestRepo, ref, entryPath, catalog.GetEntryParams{})
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

// TestStoreWrite covers the write half: markers and pointers are staged on the branch through the
// same conditional path (create on an absent value, replace on a matching condition), never
// auto-committed, and never with Force.
func TestStoreWrite(t *testing.T) {
	namespace := Namespace{"inventory", "books"}
	properties := map[string]string{"owner": "data-team", "description": "book inventory"}

	t.Run("create namespace stages a marker and does not commit it", func(t *testing.T) {
		f := newStoreFixture(t)
		addr := Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace}
		headBefore := f.headRef(t)

		require.NoError(t, f.store.CreateNamespace(f.ctx, addr, properties))

		markerPath, err := namespaceMarkerPath(namespace)
		require.NoError(t, err)

		// The write is in the branch staging area, with the properties as its user metadata.
		staged, err := f.entryAt(t, "main", markerPath)
		require.NoError(t, err)
		require.Equal(t, properties, map[string]string(staged.Metadata))
		require.Equal(t, catalog.AddressTypeRelative, staged.AddressType)
		require.Equal(t, markerPath, staged.PhysicalAddress, "a marker is a metadata-only entry")

		// No auto-commit: main still points at the commit it pointed at before the write, and that
		// commit knows nothing about the marker.
		require.Equal(t, headBefore, f.headRef(t), "a catalog write must not commit")
		_, err = f.entryAt(t, headBefore, markerPath)
		require.ErrorIs(t, err, graveler.ErrNotFound, "the marker is staged, not committed")

		// The staged marker is already readable through the catalog.
		got, err := f.store.GetNamespace(f.ctx, addr)
		require.NoError(t, err)
		require.Equal(t, properties, got.Properties)

		// An explicit lakeFS commit is what makes it part of the repository.
		commit := f.commit(t, "create the namespace")
		committed, err := f.entryAt(t, commit, markerPath)
		require.NoError(t, err)
		require.Equal(t, properties, map[string]string(committed.Metadata))
	})

	t.Run("create namespace is a create on an absent value", func(t *testing.T) {
		f := newStoreFixture(t)
		addr := Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace}

		require.NoError(t, f.store.CreateNamespace(f.ctx, addr, properties))

		// Staged conflict: the marker in the staging area already counts.
		err := f.store.CreateNamespace(f.ctx, addr, map[string]string{"owner": "someone-else"})
		require.ErrorIs(t, err, ErrAlreadyExists)
		got, err := f.store.GetNamespace(f.ctx, addr)
		require.NoError(t, err)
		require.Equal(t, properties, got.Properties, "the losing create must not overwrite the winner")

		// Committed conflict: a create that a previous commit landed also counts.
		f.commit(t, "create the namespace")
		err = f.store.CreateNamespace(f.ctx, addr, map[string]string{"owner": "someone-else"})
		require.ErrorIs(t, err, ErrAlreadyExists)
	})

	t.Run("update namespace requires the marker", func(t *testing.T) {
		f := newStoreFixture(t)
		addr := Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace}

		err := f.store.UpdateNamespace(f.ctx, addr, properties)
		require.Error(t, err)
		var missing *NoSuchNamespaceException
		require.True(t, errors.As(err, &missing), "an update of an absent namespace is a 404, got %v", err)

		require.NoError(t, f.store.CreateNamespace(f.ctx, addr, map[string]string{"owner": "data-team", "keep": "no"}))
		next := map[string]string{"owner": "platform", "description": "book inventory"}
		require.NoError(t, f.store.UpdateNamespace(f.ctx, addr, next))

		got, err := f.store.GetNamespace(f.ctx, addr)
		require.NoError(t, err)
		require.Equal(t, next, got.Properties, "the properties are replaced as a whole set; merging is the caller's")

		// The update is staged, and survives a commit.
		f.commit(t, "update the namespace")
		got, err = f.store.GetNamespace(f.ctx, addr)
		require.NoError(t, err)
		require.Equal(t, next, got.Properties)
	})

	t.Run("update namespace of a namespace created in the same staging area", func(t *testing.T) {
		f := newStoreFixture(t)
		addr := Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace}

		require.NoError(t, f.store.CreateNamespace(f.ctx, addr, nil), "a namespace with no properties is createable")
		require.NoError(t, f.store.UpdateNamespace(f.ctx, addr, properties))

		got, err := f.store.GetNamespace(f.ctx, addr)
		require.NoError(t, err)
		require.Equal(t, properties, got.Properties)
	})

	t.Run("create table stages a pointer", func(t *testing.T) {
		f := newStoreFixture(t)
		addr := Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace, Table: "orders"}
		tableLocation := "mem://" + storeTestRepo + "-ns/_iceberg/" + storeTestUUID
		metadataLocation := f.putMetadata(t,
			"_iceberg/"+storeTestUUID+"/metadata/00001.metadata.json",
			storeTestMetadata(t, storeTestUUID, tableLocation))
		pointer := TablePointer{MetadataLocation: metadataLocation, TableUUID: storeTestUUID, FormatVersion: 2}

		headBefore := f.headRef(t)
		require.NoError(t, f.store.CreateTable(f.ctx, addr, pointer))
		require.Equal(t, headBefore, f.headRef(t), "a catalog write must not commit")

		pointerPath, err := tablePointerPath(addr)
		require.NoError(t, err)
		staged, err := f.entryAt(t, "main", pointerPath)
		require.NoError(t, err)
		require.Equal(t, pointerMetadata(metadataLocation, storeTestUUID, 2), map[string]string(staged.Metadata),
			"the pointer's user metadata is the wire layout of section 6.1")

		// What was written reads back through the read half, unchanged.
		got, err := f.store.GetTable(f.ctx, addr)
		require.NoError(t, err)
		require.Equal(t, pointer, got.Pointer)
		require.Equal(t, tableLocation, got.Metadata.Location())

		// A commit keeps it, and a tag on that commit reads it too.
		commit := f.commit(t, "create the table")
		f.createTag(t, "v1", commit)
		atTag := addr
		atTag.Ref = "v1"
		got, err = f.store.GetTable(f.ctx, atTag)
		require.NoError(t, err)
		require.Equal(t, pointer, got.Pointer)
	})

	t.Run("create table is a create on an absent value", func(t *testing.T) {
		f := newStoreFixture(t)
		addr := Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace, Table: "orders"}
		pointer := TablePointer{
			MetadataLocation: f.putMetadata(t,
				"_iceberg/"+storeTestUUID+"/metadata/00001.metadata.json",
				storeTestMetadata(t, storeTestUUID, "mem://"+storeTestRepo+"-ns/_iceberg/"+storeTestUUID)),
			TableUUID:     storeTestUUID,
			FormatVersion: 2,
		}
		require.NoError(t, f.store.CreateTable(f.ctx, addr, pointer))

		// A second create with a different pointer must not replace the first one's pointer.
		other := TablePointer{
			MetadataLocation: f.putMetadata(t,
				"_iceberg/"+storeAltUUID+"/metadata/00001.metadata.json",
				storeTestMetadata(t, storeAltUUID, "mem://"+storeTestRepo+"-ns/_iceberg/"+storeAltUUID)),
			TableUUID:     storeAltUUID,
			FormatVersion: 2,
		}
		err := f.store.CreateTable(f.ctx, addr, other)
		require.ErrorIs(t, err, ErrAlreadyExists)

		got, err := f.store.GetTable(f.ctx, addr)
		require.NoError(t, err)
		require.Equal(t, pointer, got.Pointer, "the losing create must not orphan the existing metadata.json")

		// The same holds once the first create is committed.
		f.commit(t, "create the table")
		err = f.store.CreateTable(f.ctx, addr, other)
		require.ErrorIs(t, err, ErrAlreadyExists)
	})

	t.Run("pointer swap conditions on metadata-location", func(t *testing.T) {
		f := newStoreFixture(t)
		addr := Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace, Table: "orders"}

		putPointer := func(tableUUID string, seq int) TablePointer {
			pointer := TablePointer{
				MetadataLocation: f.putMetadata(t,
					"_iceberg/"+tableUUID+"/metadata/"+strconv.Itoa(seq)+".metadata.json",
					storeTestMetadata(t, tableUUID, "mem://"+storeTestRepo+"-ns/_iceberg/"+tableUUID)),
				TableUUID:     tableUUID,
				FormatVersion: 2,
			}
			return pointer
		}

		base := putPointer(storeTestUUID, 1)
		require.NoError(t, f.store.CreateTable(f.ctx, addr, base))

		next := putPointer(storeTestUUID, 2)
		require.NoError(t, f.store.UpdateTablePointer(f.ctx, addr, base, next))

		got, err := f.store.GetTable(f.ctx, addr)
		require.NoError(t, err)
		require.Equal(t, next, got.Pointer)
		require.Equal(t, next.TableUUID, got.Metadata.TableUUID().String(),
			"the metadata.json the pointer names is the one GetTable loaded")

		// The base pointer is now stale: a second commit built on it loses the compare-and-swap
		// instead of moving the pointer back to a metadata.json that is no longer current.
		stale := putPointer(storeTestUUID, 3)
		err = f.store.UpdateTablePointer(f.ctx, addr, base, stale)
		require.ErrorIs(t, err, graveler.ErrPreconditionFailed)

		got, err = f.store.GetTable(f.ctx, addr)
		require.NoError(t, err)
		require.Equal(t, next, got.Pointer, "a lost swap leaves the winner's pointer in place")

		// A swap on a pointer that does not exist is a precondition failure too, not a create:
		// UpdateTablePointer must never be the way an absent pointer gets created (§6.2 step 6).
		missingAddr := addr
		missingAddr.Table = "invoices"
		err = f.store.UpdateTablePointer(f.ctx, missingAddr, base, next)
		require.ErrorIs(t, err, graveler.ErrPreconditionFailed)
		_, err = f.store.GetTable(f.ctx, missingAddr)
		var missing *NoSuchTableException
		require.True(t, errors.As(err, &missing), "the failed swap must not create a pointer, got %v", err)
	})

	t.Run("a protected branch rejects a catalog write", func(t *testing.T) {
		f := newStoreFixture(t)
		require.NoError(t, f.catalog.SetBranchProtectionRules(f.ctx, storeTestRepo, &graveler.BranchProtectionRules{
			BranchPatternToBlockedActions: map[string]*graveler.BranchProtectionBlockedActions{
				"main": {Value: []graveler.BranchProtectionBlockedAction{
					graveler.BranchProtectionBlockedAction_STAGING_WRITE,
				}},
			},
		}, nil))

		addr := Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace}
		require.ErrorIs(t, f.store.CreateNamespace(f.ctx, addr, properties), graveler.ErrWriteToProtectedBranch,
			"writes go through without Force, so branch protection applies")
	})

	t.Run("an immutable repository rejects a catalog write", func(t *testing.T) {
		f := newStoreFixtureReadOnly(t, true)
		addr := Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace}
		require.ErrorIs(t, f.store.CreateNamespace(f.ctx, addr, properties), graveler.ErrReadOnlyRepository,
			"writes go through without Force, so a read-only repository applies")
	})

	t.Run("the write half rejects what the read half would refuse", func(t *testing.T) {
		f := newStoreFixture(t)
		addr := Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace, Table: "orders"}
		location := "mem://" + storeTestRepo + "-ns/_iceberg/" + storeTestUUID + "/metadata/00001.metadata.json"

		for _, tc := range []struct {
			name    string
			pointer TablePointer
		}{
			{name: "no metadata location", pointer: TablePointer{TableUUID: storeTestUUID, FormatVersion: 2}},
			{name: "no table uuid", pointer: TablePointer{MetadataLocation: location, FormatVersion: 2}},
			{name: "format version 0", pointer: TablePointer{MetadataLocation: location, TableUUID: storeTestUUID}},
			{name: "format version 3", pointer: TablePointer{MetadataLocation: location, TableUUID: storeTestUUID, FormatVersion: 3}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				require.ErrorIs(t, f.store.CreateTable(f.ctx, addr, tc.pointer), ErrInvalidPointer)
				// Nothing was written: the read half still answers "no such table".
				_, err := f.store.GetTable(f.ctx, addr)
				var missing *NoSuchTableException
				require.True(t, errors.As(err, &missing), "got %v", err)
			})
		}

		require.ErrorIs(t,
			f.store.UpdateTablePointer(f.ctx, addr, TablePointer{TableUUID: storeTestUUID, FormatVersion: 2},
				TablePointer{MetadataLocation: location, TableUUID: storeTestUUID, FormatVersion: 2}),
			ErrInvalidPointer, "a swap with no base metadata-location has no compare-and-swap target")

		_, err := EncodeNamespaceProperties(map[string]string{"": "nameless"})
		require.ErrorIs(t, err, ErrInvalidNamespaceProperties)

		metadata, err := EncodeNamespaceProperties(nil)
		require.NoError(t, err)
		require.NotNil(t, metadata, "a marker with no properties still carries metadata, not nil")
		require.Empty(t, metadata)

		addr.Ref = ""
		require.ErrorIs(t, f.store.CreateNamespace(f.ctx, addr, properties), ErrInvalidIdentifier)
		addr.Ref = "main"
		addr.Namespace = Namespace{"inventory/../etc"}
		require.ErrorIs(t, f.store.CreateNamespace(f.ctx, addr, properties), ErrInvalidIdentifier,
			"a namespace level cannot escape the reserved prefix")
	})
}

// TestPointerCAS is the §12 "CAS race" unit test at pointer granularity: two clients built their
// commit on the same base pointer and both swap. Exactly one wins; the other sees
// graveler.ErrPreconditionFailed, which the commit endpoint answers with 409 (I2-3). The rounds
// repeat so both orderings of the two goroutines are exercised.
func TestPointerCAS(t *testing.T) {
	f := newStoreFixture(t)
	namespace := Namespace{"inventory", "books"}
	addr := Address{Repo: storeTestRepo, Ref: "main", Namespace: namespace, Table: "orders"}

	putPointer := func(seq int) TablePointer {
		return TablePointer{
			MetadataLocation: f.putMetadata(t,
				"_iceberg/"+storeTestUUID+"/metadata/"+strconv.Itoa(seq)+".metadata.json",
				storeTestMetadata(t, storeTestUUID, "mem://"+storeTestRepo+"-ns/_iceberg/"+storeTestUUID)),
			TableUUID:     storeTestUUID,
			FormatVersion: 2,
		}
	}

	base := putPointer(1)
	require.NoError(t, f.store.CreateTable(f.ctx, addr, base))

	const rounds = 8
	for round := 1; round <= rounds; round++ {
		current, err := f.store.GetTable(f.ctx, addr)
		require.NoError(t, err)
		base = current.Pointer

		// Both clients read the same base pointer and each writes its own metadata.json, the way
		// two commits build on the same snapshot (metadata files are never overwritten).
		next := []TablePointer{putPointer(1000 + round*2), putPointer(1001 + round*2)}

		errs := make([]error, len(next))
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range next {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start // both goroutines are running before either swaps
				errs[i] = f.store.UpdateTablePointer(f.ctx, addr, base, next[i])
			}(i)
		}
		close(start)
		wg.Wait()

		winners := 0
		for i, err := range errs {
			switch {
			case err == nil:
				winners++
			case errors.Is(err, graveler.ErrPreconditionFailed):
				// The loser must be told the pointer moved, so its caller answers 409 and retries
				// against the winner's metadata.json rather than overwriting it.
			default:
				require.NoError(t, err, "round %d: goroutine %d failed with an unexpected error", round, i)
			}
		}
		require.Equal(t, 1, winners, "round %d: exactly one goroutine must win, errors: %v", round, errs)

		// The surviving pointer is the winner's, whole: the loser did not half-apply.
		got, err := f.store.GetTable(f.ctx, addr)
		require.NoError(t, err)
		require.Contains(t, []TablePointer{next[0], next[1]}, got.Pointer,
			"round %d: the branch must name one of the two candidates", round)
		for i, err := range errs {
			if err == nil {
				require.Equal(t, next[i], got.Pointer, "round %d: the winner's pointer is the one stored", round)
			}
		}
	}
}
