package icebergcatalog

// Tests for the Azure location translation (METADATA_SEARCH_AZURE.md D1, D2, task AZ1).
//
// The mapping tests are table-driven over the D1 rows and over both values of
// iceberg.azure.location_host, because those are the values a client's FileIO sees: one wrong cell
// is a table no engine can open. The rest pin the two halves of D2 - the area checks that decide
// what may be translated, and the six call sites that hand a translated location to the block
// adapter - plus the explicit statement that the S3 and mem branches are byte-identical to what
// they were before this change.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
)

const (
	// azureTestNamespace is D1's first row: the public cloud, a container and a path.
	azureTestNamespace = "https://acct.blob.core.windows.net/c/p"

	// azureTestIcebergLocation is the Iceberg form of azureTestNamespace.
	azureTestIcebergLocation = "abfss://c@acct.dfs.core.windows.net/p"

	// azureTestLegacyArea is the table area in the block adapter's spelling, i.e. what a table
	// created before the translation has its files under (D2).
	azureTestLegacyArea = azureTestNamespace + "/_iceberg/"
)

// useAzureLocationHost sets iceberg.azure.location_host for one test and restores the previous value
// afterwards: the mapping is a package function (config.go records the setting once), so a test that
// exercises both spellings must say which one it is testing.
func useAzureLocationHost(t *testing.T, host string) {
	t.Helper()
	previous := azureLocationHost()
	setAzureLocationHost(host)
	t.Cleanup(func() { setAzureLocationHost(previous) })
}

// TestIcebergNamespaceLocation covers every row of the D1 table for both location_host values, the
// two schemes, and the container-root form. It also pins the refusals: a host that is not a
// supported Azure storage account domain of this installation must not be translated into a
// location no engine could open.
func TestIcebergNamespaceLocation(t *testing.T) {
	tests := []struct {
		name      string
		host      string
		namespace string
		want      string
	}{
		{
			name:      "public cloud, dfs",
			host:      AzureLocationHostDFS,
			namespace: "https://acct.blob.core.windows.net/c/p",
			want:      "abfss://c@acct.dfs.core.windows.net/p",
		},
		{
			name:      "china, dfs",
			host:      AzureLocationHostDFS,
			namespace: "https://acct.blob.core.chinacloudapi.cn/c/p",
			want:      "abfss://c@acct.dfs.core.chinacloudapi.cn/p",
		},
		{
			name:      "us government, dfs",
			host:      AzureLocationHostDFS,
			namespace: "https://acct.blob.core.usgovcloudapi.net/c/p",
			want:      "abfss://c@acct.dfs.core.usgovcloudapi.net/p",
		},
		{
			name:      "test domain, dfs",
			host:      AzureLocationHostDFS,
			namespace: "https://acct.azurite.test/c/p",
			want:      "abfss://c@acct.azurite.test/p",
		},
		{
			name:      "http namespace is abfs, not abfss",
			host:      AzureLocationHostDFS,
			namespace: "http://acct.blob.core.windows.net/c/p",
			want:      "abfs://c@acct.dfs.core.windows.net/p",
		},
		{
			name:      "public cloud, blob host",
			host:      AzureLocationHostBlob,
			namespace: "https://acct.blob.core.windows.net/c/p",
			want:      "abfss://c@acct.blob.core.windows.net/p",
		},
		{
			name:      "china, blob host",
			host:      AzureLocationHostBlob,
			namespace: "https://acct.blob.core.chinacloudapi.cn/c/p",
			want:      "abfss://c@acct.blob.core.chinacloudapi.cn/p",
		},
		{
			name:      "us government, blob host",
			host:      AzureLocationHostBlob,
			namespace: "https://acct.blob.core.usgovcloudapi.net/c/p",
			want:      "abfss://c@acct.blob.core.usgovcloudapi.net/p",
		},
		{
			name:      "test domain, blob host",
			host:      AzureLocationHostBlob,
			namespace: "https://acct.azurite.test/c/p",
			want:      "abfss://c@acct.azurite.test/p",
		},
		{
			name:      "http namespace, blob host",
			host:      AzureLocationHostBlob,
			namespace: "http://acct.blob.core.windows.net/c/p",
			want:      "abfs://c@acct.blob.core.windows.net/p",
		},
		{
			name:      "container root",
			host:      AzureLocationHostDFS,
			namespace: "https://acct.blob.core.windows.net/c",
			want:      "abfss://c@acct.dfs.core.windows.net",
		},
		{
			name:      "a trailing slash is not part of the location",
			host:      AzureLocationHostDFS,
			namespace: "https://acct.blob.core.windows.net/c/p/",
			want:      "abfss://c@acct.dfs.core.windows.net/p",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useAzureLocationHost(t, tt.host)
			got, err := IcebergNamespaceLocation(tt.namespace)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}

	refusals := []struct {
		name      string
		namespace string
	}{
		{name: "not an Azure host", namespace: "https://acct.example.com/c/p"},
		{name: "a domain that merely starts with a supported one", namespace: "https://acct.blob.core.windows.net.evil.com/c/p"},
		{name: "a cloud this adapter does not serve", namespace: "https://acct.blob.core.australiacloudapi.net/c/p"},
		{name: "an S3 namespace", namespace: "s3://bucket/prefix"},
		{name: "a mem namespace", namespace: "mem://repo-ns"},
		{name: "a local path", namespace: "/var/lib/lakefs/data"},
		{name: "empty", namespace: ""},
	}
	for _, tt := range refusals {
		t.Run("refuses "+tt.name, func(t *testing.T) {
			useAzureLocationHost(t, DefaultAzureLocationHost)
			location, err := IcebergNamespaceLocation(tt.namespace)
			require.ErrorIs(t, err, ErrInvalidAzureNamespace)
			require.Empty(t, location)
		})
	}
}

// TestTableAreaLocationAzure checks that the area every prefix check goes through is spelled in the
// Iceberg form for an Azure namespace, for both location_host values, and that DefaultTableLocation
// inherits it.
func TestTableAreaLocationAzure(t *testing.T) {
	for _, tt := range []struct {
		host string
		area string
	}{
		{host: AzureLocationHostDFS, area: azureTestIcebergLocation + "/_iceberg/"},
		{host: AzureLocationHostBlob, area: "abfss://c@acct.blob.core.windows.net/p/_iceberg/"},
	} {
		t.Run(tt.host, func(t *testing.T) {
			useAzureLocationHost(t, tt.host)
			require.Equal(t, tt.area, TableAreaLocation(azureTestNamespace))

			location, err := DefaultTableLocation(azureTestNamespace, storeTestUUID)
			require.NoError(t, err)
			require.Equal(t, tt.area+storeTestUUID, location)
		})
	}
}

// TestTableAreaLocationNonAzureUnchanged is section 6 of the design in test form: the S3 and mem
// branches of every touched function keep their previous output, byte for byte.
func TestTableAreaLocationNonAzureUnchanged(t *testing.T) {
	useAzureLocationHost(t, AzureLocationHostBlob) // the setting must not reach a non-Azure namespace
	for _, namespace := range []string{
		"s3://bucket/prefix",
		"gs://bucket/prefix",
		"mem://repo-ns",
		"/var/lib/lakefs/data",
	} {
		t.Run(namespace, func(t *testing.T) {
			area := strings.TrimRight(namespace, "/") + "/" + DataPrefix + "/"
			require.Equal(t, area, TableAreaLocation(namespace))

			root, err := TableRoot(namespace, uuid.MustParse(storeTestUUID))
			require.NoError(t, err)
			require.Equal(t, area+storeTestUUID, root)

			// blockIdentifier is the identity: the adapter reads this spelling already.
			require.Equal(t, area+"x/y", blockIdentifier(namespace, area+"x/y"))

			// normalizeTableLocation accepts exactly what it accepted before: a location under the
			// namespace-form area, and nothing in the legacy https spelling (the tolerance is Azure
			// only).
			location, err := normalizeTableLocation(namespace, area+storeTestUUID)
			require.NoError(t, err)
			require.Equal(t, area+storeTestUUID, location)

			_, err = normalizeTableLocation(namespace, "https://acct.blob.core.windows.net/c/p/_iceberg/"+storeTestUUID)
			require.ErrorIs(t, err, ErrInvalidTableLocation)
		})
	}
}

// TestBlockIdentifierRoundTrip is D2's round trip: a location the catalog produced for a table is
// translated back into the namespace form the block adapter reads, and the reverse direction (a
// legacy table's area) is the identity.
func TestBlockIdentifierRoundTrip(t *testing.T) {
	for _, host := range []string{AzureLocationHostDFS, AzureLocationHostBlob} {
		useAzureLocationHost(t, host)
		for _, namespace := range []string{
			azureTestNamespace,
			"http://acct.azurite.test/c/p",
			"https://acct.blob.core.chinacloudapi.cn/c",
		} {
			area := TableAreaLocation(namespace)
			require.Equal(t, namespace+"/_iceberg/x/y", blockIdentifier(namespace, area+"x/y"))
			// A legacy table's location is already in the adapter's spelling.
			require.Equal(t, namespace+"/_iceberg/x/y", blockIdentifier(namespace, namespace+"/_iceberg/x/y"))
			// Only a location that is under the area is translated; anything else is returned as it
			// is, so this never becomes a rewrite-anywhere primitive.
			require.Equal(t, "abfss://other@acct.dfs.core.windows.net/p/_iceberg/x/y",
				blockIdentifier(namespace, "abfss://other@acct.dfs.core.windows.net/p/_iceberg/x/y"))
			require.Equal(t, "https://acct.blob.core.windows.net/c/other/x",
				blockIdentifier(namespace, "https://acct.blob.core.windows.net/c/other/x"))
		}
	}
}

// TestNormalizeTableLocationAzure covers the Azure branch of the create/commit check: the canonical
// area is accepted, the legacy namespace spelling is accepted and returned in the canonical form
// (TableLocation never returns the legacy spelling), and every other spelling - another container,
// another account, wasbs (a non-goal, N4), the other scheme, dot and empty segments, the area itself
// - is refused.
func TestNormalizeTableLocationAzure(t *testing.T) {
	useAzureLocationHost(t, DefaultAzureLocationHost)
	area := TableAreaLocation(azureTestNamespace)
	require.Equal(t, azureTestIcebergLocation+"/_iceberg/", area)

	accepted := []struct {
		name      string
		requested string
		want      string
	}{
		{
			name:      "a canonical location",
			requested: area + storeTestUUID,
			want:      area + storeTestUUID,
		},
		{
			name:      "a nested canonical location",
			requested: area + "team/orders",
			want:      area + "team/orders",
		},
		{
			name:      "a legacy https location is normalized",
			requested: azureTestLegacyArea + storeTestUUID,
			want:      area + storeTestUUID,
		},
	}
	for _, tt := range accepted {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeTableLocation(azureTestNamespace, tt.requested)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
			require.NotEqual(t, azureTestLegacyArea+storeTestUUID, got,
				"the legacy spelling is tolerated on the way in and never returned")
		})
	}

	refused := []struct {
		name      string
		requested string
	}{
		{name: "another container", requested: "abfss://other@acct.dfs.core.windows.net/p/_iceberg/" + storeTestUUID},
		{name: "another account", requested: "abfss://c@other.dfs.core.windows.net/p/_iceberg/" + storeTestUUID},
		{name: "another path", requested: "abfss://c@acct.dfs.core.windows.net/other/_iceberg/" + storeTestUUID},
		{name: "wasbs is not served", requested: "wasbs://c@acct.blob.core.windows.net/p/_iceberg/" + storeTestUUID},
		{name: "abfs under an https namespace", requested: "abfs://c@acct.dfs.core.windows.net/p/_iceberg/" + storeTestUUID},
		{name: "the legacy spelling of another container", requested: "https://acct.blob.core.windows.net/other/_iceberg/" + storeTestUUID},
		{name: "a dot segment", requested: area + "team/../orders"},
		{name: "a dot segment in the legacy spelling", requested: azureTestLegacyArea + "team/../orders"},
		{name: "an empty segment", requested: area + "team//orders"},
		{name: "the area itself", requested: area},
		{name: "the legacy area itself", requested: azureTestLegacyArea},
	}
	for _, tt := range refused {
		t.Run("refuses "+tt.name, func(t *testing.T) {
			location, err := normalizeTableLocation(azureTestNamespace, tt.requested)
			require.ErrorIs(t, err, ErrInvalidTableLocation)
			require.Empty(t, location)
		})
	}
}

// TestTableRootForMetadataAzure covers the commit/register area rule (tableRootForMetadata) on an
// Azure namespace: the table's canonical location is its root, a location in the legacy namespace
// spelling is still accepted so an old table can be committed, and a location under another area is
// refused as the 400 of §13.
func TestTableRootForMetadataAzure(t *testing.T) {
	useAzureLocationHost(t, DefaultAzureLocationHost)
	canonicalRoot := azureTestIcebergLocation + "/_iceberg/" + storeTestUUID
	legacyRoot := azureTestNamespace + "/_iceberg/" + storeTestUUID

	root, err := tableRootForMetadata(azureTestNamespace, filesTestMetadata(t, filesTestMetadataDocument(canonicalRoot, nil, -1)))
	require.NoError(t, err)
	require.Equal(t, canonicalRoot, root)

	root, err = tableRootForMetadata(azureTestNamespace, filesTestMetadata(t, filesTestMetadataDocument(legacyRoot, nil, -1)))
	require.NoError(t, err, "a table created before the translation stays committable")
	require.Equal(t, legacyRoot, root, "its files are under the legacy spelling")

	_, err = tableRootForMetadata(azureTestNamespace, filesTestMetadata(t,
		filesTestMetadataDocument("abfss://other@acct.dfs.core.windows.net/p/_iceberg/"+storeTestUUID, nil, -1)))
	require.ErrorIs(t, err, ErrForeignTableFile)
	require.ErrorIs(t, err, ErrTableLocationOutsideArea)
}

// TestTableFileCollectorAzure covers the collector half of D2: a planned file's physical address is
// the block adapter's spelling of a location validated to be under the table root, and a manifest
// entry in another container or another account is refused before it is fetched.
func TestTableFileCollectorAzure(t *testing.T) {
	useAzureLocationHost(t, DefaultAzureLocationHost)
	tableUUID := uuid.MustParse(storeTestUUID)

	root, err := TableRoot(azureTestNamespace, tableUUID)
	require.NoError(t, err)
	require.Equal(t, azureTestIcebergLocation+"/_iceberg/"+storeTestUUID, root)

	collector := &tableFileCollector{
		tableUUID:        tableUUID,
		root:             root,
		storageNamespace: azureTestNamespace,
		maxFiles:         10,
		files:            make(map[string]TableFile),
	}
	modified := time.UnixMilli(filesTestSnapshotTimestampMs).UTC()

	manifestList := root + "/" + filesTestManifestListKey
	require.NoError(t, collector.add(TableFileKindManifestList, manifestList, 42, modified))

	files := collector.sorted()
	require.Len(t, files, 1)
	require.Equal(t, filesTestManifestListKey, strings.TrimPrefix(files[0].Path, "_lakefs_iceberg/files/"+storeTestUUID+"/"))
	require.Equal(t, azureTestNamespace+"/_iceberg/"+storeTestUUID+"/"+filesTestManifestListKey, files[0].PhysicalAddress,
		"the staged entry must name the object the azure adapter can read")

	foreign := []struct {
		name     string
		location string
	}{
		{
			name:     "another container",
			location: "abfss://other@acct.dfs.core.windows.net/p/_iceberg/" + storeTestUUID + "/metadata/x.avro",
		},
		{
			name:     "another account",
			location: "abfss://c@other.dfs.core.windows.net/p/_iceberg/" + storeTestUUID + "/metadata/x.avro",
		},
	}
	for _, tt := range foreign {
		t.Run("refuses a manifest list in "+tt.name, func(t *testing.T) {
			err := collector.add(TableFileKindManifestList, tt.location, 1, modified)
			var foreignErr *ForeignTableFileError
			require.ErrorAs(t, err, &foreignErr)
			require.ErrorIs(t, err, ErrForeignTableFile)
			require.Equal(t, tt.location, foreignErr.Location)
			require.Equal(t, root, foreignErr.Root)
			require.Len(t, collector.files, 1, "a refused file is not added to the plan")
		})
	}
}

// TestAzureStoreCallSites is the wiring half of D2: each of the functions that hands a table location
// to the block adapter, or stores it as a lakeFS physical address, uses the adapter's spelling. The
// catalog and the adapter are minimal fakes - only the calls these functions make are recorded - so
// the assertions are about the identifier the adapter would receive, which is the point of the
// change.
func TestAzureStoreCallSites(t *testing.T) {
	useAzureLocationHost(t, DefaultAzureLocationHost)

	repo := &catalog.Repository{Name: "repo", StorageNamespace: azureTestNamespace}
	cat := &azureTestCatalog{repo: repo}
	adapter := &azureTestBlock{objects: make(map[string][]byte)}
	store := NewStore(cat, adapter)
	tableUUID := uuid.MustParse(storeTestUUID)
	root := azureTestIcebergLocation + "/_iceberg/" + storeTestUUID
	adapterSpelling := func(relative string) string {
		return azureTestNamespace + "/_iceberg/" + storeTestUUID + "/" + relative
	}

	t.Run("TableLocation returns the canonical form", func(t *testing.T) {
		location, err := store.TableLocation(t.Context(), repo.Name, azureTestLegacyArea+storeTestUUID, storeTestUUID)
		require.NoError(t, err)
		require.Equal(t, root, location)

		location, err = store.TableLocation(t.Context(), repo.Name, "", storeTestUUID)
		require.NoError(t, err)
		require.Equal(t, root, location)
	})

	t.Run("writeDocument writes the adapter spelling", func(t *testing.T) {
		adapter.reset()
		location := root + "/metadata/00001.metadata.json"
		document := filesTestMetadataDocument(root, nil, -1)
		require.NoError(t, store.writeDocument(t.Context(), repo.Name, location, []byte(document)))

		require.Equal(t, []string{adapterSpelling("metadata/00001.metadata.json")}, adapter.putIdentifiers())
		require.Equal(t, document, string(adapter.objects[adapterSpelling("metadata/00001.metadata.json")]))
	})

	t.Run("loadMetadata reads the adapter spelling", func(t *testing.T) {
		adapter.reset()
		location := root + "/metadata/00001.metadata.json"
		document := filesTestMetadataDocument(root, nil, -1)
		adapter.objects[adapterSpelling("metadata/00001.metadata.json")] = []byte(document)

		metadata, raw, err := store.loadMetadata(t.Context(), repo.Name, location)
		require.NoError(t, err)
		require.Equal(t, document, string(raw))
		require.Equal(t, tableUUID, metadata.TableUUID())
		require.Equal(t, []string{adapterSpelling("metadata/00001.metadata.json")}, adapter.getIdentifiers())
	})

	t.Run("loadMetadata tolerates the legacy spelling", func(t *testing.T) {
		adapter.reset()
		location := azureTestLegacyArea + storeTestUUID + "/metadata/00001.metadata.json"
		document := filesTestMetadataDocument(azureTestLegacyArea+storeTestUUID, nil, -1)
		adapter.objects[location] = []byte(document)

		_, raw, err := store.loadMetadata(t.Context(), repo.Name, location)
		require.NoError(t, err, "a table created before the translation stays readable")
		require.Equal(t, document, string(raw))
		require.Equal(t, []string{location}, adapter.getIdentifiers(), "the legacy location is already the adapter's spelling")
	})

	t.Run("readTableObject reads the adapter spelling", func(t *testing.T) {
		adapter.reset()
		location := root + "/metadata/snap.avro"
		adapter.objects[adapterSpelling("metadata/snap.avro")] = []byte("avro")

		data, err := store.readTableObject(t.Context(), repo, location)
		require.NoError(t, err)
		require.Equal(t, "avro", string(data))
		require.Equal(t, []string{adapterSpelling("metadata/snap.avro")}, adapter.getIdentifiers())
	})

	t.Run("loadViewMetadata reads the adapter spelling", func(t *testing.T) {
		adapter.reset()
		location := root + "/metadata/00001.view.json"
		document := azureTestViewMetadata(t, root)
		adapter.objects[adapterSpelling("metadata/00001.view.json")] = document

		_, raw, err := store.loadViewMetadata(t.Context(), repo.Name, location)
		require.NoError(t, err)
		require.Equal(t, document, raw)
		require.Equal(t, []string{adapterSpelling("metadata/00001.view.json")}, adapter.getIdentifiers())
	})

	t.Run("StageViewMetadata stages the adapter spelling", func(t *testing.T) {
		cat.reset()
		location := root + "/metadata/00001.view.json"
		require.NoError(t, store.StageViewMetadata(t.Context(),
			Address{Repo: repo.Name, Ref: "main"}, tableUUID, root,
			[]TableMetadataFile{{Location: location, Size: 7}}))

		entries := cat.createdEntries()
		require.Len(t, entries, 1)
		require.Equal(t, adapterSpelling("metadata/00001.view.json"), entries[0].PhysicalAddress)
		require.Equal(t, catalog.AddressTypeFull, entries[0].AddressType)
	})

	t.Run("StageViewMetadata refuses a document under another root", func(t *testing.T) {
		cat.reset()
		err := store.StageViewMetadata(t.Context(),
			Address{Repo: repo.Name, Ref: "main"}, tableUUID, root,
			[]TableMetadataFile{{Location: "abfss://other@acct.dfs.core.windows.net/p/_iceberg/" + storeTestUUID + "/metadata/x.json", Size: 7}})
		require.ErrorIs(t, err, ErrForeignTableFile)
		require.Empty(t, cat.createdEntries(), "a refused document stages nothing")
	})
}

// azureTestViewMetadata builds a view metadata document at location, the way the view create path
// does, so loadViewMetadata has something to parse.
func azureTestViewMetadata(t *testing.T, location string) []byte {
	t.Helper()
	metadata, err := view.NewMetadataWithUUID(
		viewsTestVersion(t, view.InitialVersionID, "select 1", table.Identifier{"inventory"}),
		viewsTestSchema(), location, iceberg.Properties{}, uuid.MustParse(storeTestUUID))
	require.NoError(t, err, "build view metadata")
	document, err := json.Marshal(metadata)
	require.NoError(t, err, "marshal view metadata")
	return document
}

// azureTestCatalog answers one Azure repository and records the entries staged on it. The embedded
// Catalog interface leaves the rest of the surface unimplemented, so a test that reached another
// method would fail loudly instead of reading the wrong fixture.
type azureTestCatalog struct {
	Catalog
	repo *catalog.Repository

	mu      sync.Mutex
	created []catalog.DBEntry
}

func (c *azureTestCatalog) GetRepository(_ context.Context, _ string) (*catalog.Repository, error) {
	return c.repo, nil
}

func (c *azureTestCatalog) CreateEntry(_ context.Context, _ string, _ string, entry catalog.DBEntry, _ ...graveler.SetOptionsFunc) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.created = append(c.created, entry)
	return nil
}

func (c *azureTestCatalog) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.created = nil
}

func (c *azureTestCatalog) createdEntries() []catalog.DBEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]catalog.DBEntry(nil), c.created...)
}

// azureTestBlock records the object identifiers every read and write receives. It is how the tests
// observe the identifier the real azure adapter would be handed.
type azureTestBlock struct {
	block.Adapter

	mu      sync.Mutex
	gets    []block.ObjectPointer
	puts    []block.ObjectPointer
	objects map[string][]byte
}

func (b *azureTestBlock) Get(_ context.Context, obj block.ObjectPointer) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.gets = append(b.gets, obj)
	data, ok := b.objects[obj.Identifier]
	if !ok {
		return nil, block.ErrDataNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (b *azureTestBlock) Put(_ context.Context, obj block.ObjectPointer, _ int64, reader io.Reader, _ block.PutOpts) (*block.PutResponse, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.puts = append(b.puts, obj)
	b.objects[obj.Identifier] = data
	return &block.PutResponse{}, nil
}

func (b *azureTestBlock) reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.gets = nil
	b.puts = nil
}

func (b *azureTestBlock) getIdentifiers() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	identifiers := make([]string, 0, len(b.gets))
	for _, obj := range b.gets {
		identifiers = append(identifiers, obj.Identifier)
	}
	return identifiers
}

func (b *azureTestBlock) putIdentifiers() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	identifiers := make([]string, 0, len(b.puts))
	for _, obj := range b.puts {
		identifiers = append(identifiers, obj.Identifier)
	}
	return identifiers
}
