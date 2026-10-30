package datasets

// helpers_test.go extends the §15.1 fixture (kv/mem + block/mem + a real catalog) with
// the operations the resolve and publish tests need: repositories with real object
// bytes, commits built from entries with relative addresses, and reads of the resulting
// dataset repository.

import (
	"bytes"
	"crypto/md5" //nolint:gosec // lakeFS object ETags are MD5 hashes (see block/mem)
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
)

// datasetFixture is the spike fixture (a real catalog on mem KV and the mem block
// adapter) plus dataset-test helpers.
type datasetFixture struct {
	*spikeFixture

	// objectSeq numbers the physical objects the fixture writes, and addresses maps a
	// repository + logical key to the physical address of its latest upload.
	objectSeq int
	addresses map[string]string
}

func newDatasetFixture(t *testing.T) *datasetFixture {
	t.Helper()
	return &datasetFixture{spikeFixture: newSpikeFixture(t), addresses: make(map[string]string)}
}

// testObject is one source object: its repository key and its bytes.
type testObject struct {
	key     string
	content []byte
}

// etagOf mirrors the ETag the mem and local adapters compute for a single-part object,
// so tests can assert that materialisation preserved the source ETag.
func etagOf(content []byte) string {
	sum := md5.Sum(content) //nolint:gosec
	return hex.EncodeToString(sum[:])
}

// putObject writes content into a repository's storage namespace, the way an upload
// through the gateway would: relative to the repository namespace, with no metadata
// entry yet.
func (f *datasetFixture) putObject(t *testing.T, repo, key string, content []byte) {
	t.Helper()
	rec := f.repositoryRecord(t, repo)
	_, err := f.cat.BlockAdapter.Put(f.ctx, block.ObjectPointer{
		StorageID:        rec.StorageID.String(),
		StorageNamespace: rec.StorageNamespace.String(),
		Identifier:       key,
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(content)), bytes.NewReader(content), block.PutOpts{})
	require.NoError(t, err, "put %s/%s", repo, key)
}

// storeObject writes content under a fresh physical address and returns that address.
// lakeFS uploads never reuse an address (the path provider of pkg/upload gives every
// upload its own key), which is what lets two commits hold different bytes for the same
// logical path — and therefore what makes "v1 keeps reading v1 data" a meaningful check.
func (f *datasetFixture) storeObject(t *testing.T, repo, logicalKey string, content []byte) string {
	t.Helper()
	f.objectSeq++
	address := fmt.Sprintf("objects/%03d/%s", f.objectSeq, logicalKey)
	f.putObject(t, repo, address, content)
	f.addresses[repo+"\x00"+logicalKey] = address
	return address
}

// addressOf returns the physical address of the last upload of a repository key.
func (f *datasetFixture) addressOf(t *testing.T, repo, logicalKey string) string {
	t.Helper()
	address, ok := f.addresses[repo+"\x00"+logicalKey]
	require.True(t, ok, "no object was stored for %s/%s", repo, logicalKey)
	return address
}

// commitObjects writes objects to a repository namespace, stages an entry per object
// with a relative address (the common case §6.3 has to convert) and commits them,
// returning the new commit ID.
func (f *datasetFixture) commitObjects(t *testing.T, repo, branch string, objects ...testObject) graveler.CommitID {
	t.Helper()
	for _, obj := range objects {
		address := f.storeObject(t, repo, obj.key, obj.content)
		require.NoError(t, f.cat.CreateEntry(f.ctx, repo, branch, catalog.DBEntry{
			Path:            obj.key,
			PhysicalAddress: address,
			AddressType:     catalog.AddressTypeRelative,
			CreationDate:    time.Now(),
			Size:            int64(len(obj.content)),
			Checksum:        etagOf(obj.content),
			ContentType:     "application/octet-stream",
			Metadata:        catalog.Metadata{"origin": repo},
		}), "create entry %s/%s", repo, obj.key)
	}
	log, err := f.cat.Commit(f.ctx, repo, branch, "commit "+repo+"/"+branch, "tester", nil, nil, nil, false)
	require.NoError(t, err, "commit %s/%s", repo, branch)
	return graveler.CommitID(log.Reference)
}

// listPaths returns the repository keys of a ref in metarange order, which is sorted:
// the assertion that publish produced a key-sorted metarange (§6.4).
func (f *datasetFixture) listPaths(t *testing.T, repo, ref string) []string {
	t.Helper()
	entries, _, err := f.cat.ListEntries(f.ctx, repo, ref, "", "", "", catalog.ListEntriesLimitMax)
	require.NoError(t, err, "list entries of %s@%s", repo, ref)
	paths := make([]string, len(entries))
	for i, entry := range entries {
		paths[i] = entry.Path
	}
	return paths
}

// datasetEntry returns the dataset repository entry at path.
func (f *datasetFixture) datasetEntry(t *testing.T, dataset, ref, path string) *catalog.DBEntry {
	t.Helper()
	entry, err := f.cat.GetEntry(f.ctx, dataset, ref, path, catalog.GetEntryParams{})
	require.NoError(t, err, "get entry %s/%s/%s", dataset, ref, path)
	return entry
}

// readDatasetObject reads a dataset entry the way the S3 gateway does: through the
// block adapter, using the entry's own address and address type
// (gateway/operations/getobject.go). This is the zero-copy read of goal G3.
func (f *datasetFixture) readDatasetObject(t *testing.T, dataset, ref, path string) []byte {
	t.Helper()
	rec := f.repositoryRecord(t, dataset)
	entry := f.datasetEntry(t, dataset, ref, path)
	reader, err := f.cat.BlockAdapter.Get(f.ctx, block.ObjectPointer{
		StorageID:        rec.StorageID.String(),
		StorageNamespace: rec.StorageNamespace.String(),
		Identifier:       entry.PhysicalAddress,
		IdentifierType:   entry.AddressType.ToIdentifierType(),
	})
	require.NoError(t, err, "read %s/%s", dataset, path)
	defer func() { require.NoError(t, reader.Close()) }()
	data, err := io.ReadAll(reader)
	require.NoError(t, err, "read %s/%s", dataset, path)
	return data
}

// readDefinitionDocument fetches and decodes the synthetic _dataset/definition.json of a
// version (§6.4), asserting that it kept its relative address into the dataset namespace.
func (f *datasetFixture) readDefinitionDocument(t *testing.T, dataset, ref string) definitionDocument {
	t.Helper()
	entry := f.datasetEntry(t, dataset, ref, DefinitionPath)
	require.Equal(t, catalog.AddressTypeRelative, entry.AddressType,
		"the definition object lives in the dataset's own namespace, so its entry stays relative")
	require.Equal(t, DefinitionContentType, entry.ContentType)

	var doc definitionDocument
	require.NoError(t, json.Unmarshal(f.readDatasetObject(t, dataset, ref, DefinitionPath), &doc),
		"decode definition document")
	return doc
}

// totalBytes sums the source sizes of a test object list.
func totalBytes(objects []testObject) int64 {
	var total int64
	for _, obj := range objects {
		total += int64(len(obj.content))
	}
	return total
}

// testTime is a fixed creation time, so tests can assert that materialisation preserved
// an entry's LastModified without depending on the wall clock.
func testTime() time.Time {
	return time.Unix(1_700_000_000, 0).UTC()
}

// commitFullAddressEntry stages one entry whose physical address is already a full URI
// (the shape imports and older lakeFS versions produce, which §6.3 must pass through
// unchanged) and commits it.
func (f *datasetFixture) commitFullAddressEntry(t *testing.T, repo, branch, key string, content []byte) graveler.CommitID {
	t.Helper()
	f.putObject(t, repo, key, content)
	rec := f.repositoryRecord(t, repo)
	qk, err := f.cat.BlockAdapter.ResolveNamespace(rec.StorageID.String(), rec.StorageNamespace.String(), key, block.IdentifierTypeRelative)
	require.NoError(t, err, "resolve %s", key)
	require.NoError(t, f.cat.CreateEntry(f.ctx, repo, branch, catalog.DBEntry{
		Path:            key,
		PhysicalAddress: qk.Format(),
		AddressType:     catalog.AddressTypeFull,
		CreationDate:    testTime(),
		Size:            int64(len(content)),
		Checksum:        etagOf(content),
		ContentType:     "application/octet-stream",
	}), "create full-address entry %s", key)
	log, err := f.cat.Commit(f.ctx, repo, branch, "commit full address", "tester", nil, nil, nil, false)
	require.NoError(t, err, "commit %s/%s", repo, branch)
	return graveler.CommitID(log.Reference)
}
