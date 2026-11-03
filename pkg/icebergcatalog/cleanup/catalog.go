package cleanup

// catalog.go answers the one question the sweep depends on: which physical files does a
// table's metadata reference? It reads a table pointer's metadata.json and then walks
// every snapshot, manifest list and manifest of that document.
//
// It is the read half of pkg/icebergcatalog's commit path (pkg/icebergcatalog/files.go
// "What is registered"), viewed from the other side: files.go registers the files a
// snapshot *added*, and this file collects the files a metadata document *references*, so
// that the sweep's live set is a superset of every registration the catalog ever staged
// for the table. The two use the same decoder (iceberg-go: table.ParseMetadataBytes,
// iceberg.ReadManifestList, iceberg.NewManifestReader), the same reserved key layout
// (layout.go) and the same block of the Iceberg table spec.
//
// # What counts as referenced
//
// Everything the metadata document names is protected, whether or not it is currently
// needed for a read:
//
//   - the metadata.json itself (the pointer's metadata-location);
//   - every metadata-log entry (previous metadata files). Iceberg treats the log as
//     history, but the file is a real object and deleting it while the current document
//     names it would leave a dangling reference for any tool that reads the log, so it is
//     kept;
//   - statistics and partition-statistics files (Puffin files);
//   - every snapshot's manifest list, and for a v1 snapshot that embeds manifests, every
//     embedded manifest;
//   - every manifest a manifest list names, and every file every manifest entry names -
//     including EXISTING and DELETED entries. EXISTING entries name files that earlier
//     snapshots added and that current snapshots still use; DELETED entries name files
//     the snapshot stopped using, and over-protecting them costs a file that another
//     branch may still reference (a shared data file is perfectly legal) while
//     under-protecting costs data.
//
// A file listed in a manifest but no longer present is added anyway: the entry is a name,
// not an existence check, and the sweep only ever deletes registrations of files in the
// candidate set.
//
// # Failure is protection
//
// Any failure to resolve a reference - a metadata file whose catalog entry is missing, a
// manifest outside the table root, an unreadable or unparsable manifest - makes the whole
// table "unreadable", and the sweep then protects *every* registration of that table. It
// deliberately does not fall back to "protect what was collected before the failure":
// a partially collected live set is indistinguishable from a smaller one, and the sweep's
// contract is that it never deletes a file that may still be live. This is also how
// external-metadata tables are handled: they have no catalog file entries, so their
// metadata cannot be read here, so none of their files is ever swept.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

// ErrForeignFile is wrapped when a metadata document references a file outside its own
// table root. It is a protection signal, not a client error: the sweep treats the table
// as unreadable rather than trusting a live set that skips the file.
var ErrForeignFile = errors.New("table file outside the table root")

// initialManifestEntries is the initial capacity of a parsed manifest's file list. A
// manifest usually holds many entries; 16 is only the growth hint.
const initialManifestEntries = 16

// maxManifestCacheBytes bounds the memory the per-run manifest cache may use. Metadata
// documents and manifest lists are small (kilobytes) and are always cached; manifests can
// be megabytes and are cached only while the budget lasts, because the same manifest is
// commonly referenced by many metadata versions and re-reading it for every version is
// both slow and expensive. The budget counts raw manifest bytes.
const maxManifestCacheBytes = 64 << 20

// catalogIndex reads and caches Iceberg catalog documents for one run. All caches are
// keyed by normalized physical location: a document is immutable at a location (the
// catalog never overwrites a metadata file, pkg/icebergcatalog/metadata.go), so reading
// it once is enough even when thousands of retained commits point at the same version.
type catalogIndex struct {
	api        API
	repository string

	metadata      map[string]table.Metadata
	manifestLists map[string][]iceberg.ManifestFile
	manifests     map[string][]dataFileRef
	manifestBytes int64
}

// dataFileRef is one file a manifest entry names. Path is kept verbatim (the manifest's
// spelling of it); SizeBytes is informational and is not used to decide anything.
type dataFileRef struct {
	Path      string
	SizeBytes int64
}

// newCatalogIndex returns an empty index.
func newCatalogIndex(api API, repository string) *catalogIndex {
	return &catalogIndex{
		api:           api,
		repository:    repository,
		metadata:      make(map[string]table.Metadata),
		manifestLists: make(map[string][]iceberg.ManifestFile),
		manifests:     make(map[string][]dataFileRef),
	}
}

// readMetadata reads table metadata.json through the catalog's file registration. ref is
// the branch, tag or commit the read happens on, so a metadata file that is only staged
// is found on the staging branch and a metadata file in an old commit is found at that
// commit.
//
// tableRoot is the table's physical root and is used only to build the entry key; the
// authoritative root of the parsed document is Metadata.Location(). A document that
// declares a different root than the one used to find it is accepted - a registered table
// may live anywhere under the table area - and the caller must prefer
// Metadata.Location() afterwards.
func (c *catalogIndex) readMetadata(ctx context.Context, ref string, tableUUID uuid.UUID, tableRoot, location string) (table.Metadata, error) {
	key := normalizeAddress(location)
	if metadata, ok := c.metadata[key]; ok {
		return metadata, nil
	}
	if key == "" {
		return nil, fmt.Errorf("%w: the table has no metadata location", ErrInvalidCatalogState)
	}
	entryPath, err := fileEntryPath(tableUUID, tableRoot, location)
	if err != nil {
		return nil, err
	}
	data, err := c.api.GetObject(ctx, c.repository, ref, entryPath)
	if err != nil {
		return nil, fmt.Errorf("read table metadata %q through %q: %w", location, entryPath, err)
	}
	metadata, err := table.ParseMetadataBytes(data)
	if err != nil {
		return nil, fmt.Errorf("parse table metadata %q: %w", location, err)
	}
	c.metadata[key] = metadata
	return metadata, nil
}

// collect adds every file the metadata document references to add. metadataLocation is
// the location the pointer names, which is the document's own file (the parsed document
// does not carry its own location). tableRoot is Metadata.Location().
func (c *catalogIndex) collect(ctx context.Context, ref string, tableUUID uuid.UUID, tableRoot, metadataLocation string, metadata table.Metadata, add func(address string)) error {
	root := strings.TrimRight(tableRoot, "/")
	if root == "" {
		return fmt.Errorf("%w: the table metadata declares no location", ErrInvalidCatalogState)
	}
	add(metadataLocation)

	for previous := range metadata.PreviousFiles() {
		if previous.MetadataFile != "" {
			add(previous.MetadataFile)
		}
	}
	for statistics := range metadata.Statistics() {
		if statistics.StatisticsPath != "" {
			add(statistics.StatisticsPath)
		}
	}
	for statistics := range metadata.PartitionStatistics() {
		if statistics.StatisticsPath != "" {
			add(statistics.StatisticsPath)
		}
	}

	for _, snapshot := range metadata.Snapshots() {
		// The manifest list is a file of the snapshot in its own right, and it is the one
		// a v1 snapshot may leave out (embedded manifests instead). Add it before it is
		// read, so a manifest list that fails to parse is still protected by the caller's
		// unreadable-table rule rather than silently dropped from the live set.
		if snapshot.ManifestList != "" {
			add(snapshot.ManifestList)
		}
		for _, manifestLocation := range snapshot.ManifestLocations {
			add(manifestLocation)
		}
		manifests, err := c.snapshotManifests(ctx, ref, tableUUID, root, metadata, snapshot)
		if err != nil {
			return err
		}
		for _, manifest := range manifests {
			manifestPath := manifest.FilePath()
			add(manifestPath)
			files, err := c.manifestFiles(ctx, ref, tableUUID, root, manifest)
			if err != nil {
				return err
			}
			for _, file := range files {
				if file.Path != "" {
					add(file.Path)
				}
			}
		}
	}
	return nil
}

// snapshotManifests resolves the manifests of one snapshot: the manifest list, or the
// embedded manifest locations of a v1 snapshot (the deprecated "manifests" field).
//
// The v1 form is read, not refused, because the *mark* phase must protect such a table
// even though the *expire* phase refuses to rewrite it (planExpire checks that case and
// aborts the run). Refusing here would make a repository that only ever runs
// --sweep-only sweep the files of a v1 table it cannot read.
func (c *catalogIndex) snapshotManifests(ctx context.Context, ref string, tableUUID uuid.UUID, root string, metadata table.Metadata, snapshot table.Snapshot) ([]iceberg.ManifestFile, error) {
	if snapshot.ManifestList != "" {
		return c.manifestList(ctx, ref, tableUUID, root, snapshot.ManifestList)
	}
	specID := int32(metadata.DefaultPartitionSpec()) //nolint:gosec // an Iceberg spec id is an int32 by spec
	manifests := make([]iceberg.ManifestFile, 0, len(snapshot.ManifestLocations))
	for _, location := range snapshot.ManifestLocations {
		manifests = append(manifests, iceberg.NewManifestFile(
			1, location, 0, specID, snapshot.SnapshotID).Build())
	}
	return manifests, nil
}

// manifestList reads and parses one manifest list, caching the result by location.
func (c *catalogIndex) manifestList(ctx context.Context, ref string, tableUUID uuid.UUID, root, location string) ([]iceberg.ManifestFile, error) {
	key := normalizeAddress(location)
	if manifests, ok := c.manifestLists[key]; ok {
		return manifests, nil
	}
	entryPath, err := fileEntryPath(tableUUID, root, location)
	if err != nil {
		return nil, fmt.Errorf("%w: manifest list %q under %q: %w", ErrForeignFile, location, root, err)
	}
	data, err := c.api.GetObject(ctx, c.repository, ref, entryPath)
	if err != nil {
		return nil, fmt.Errorf("read manifest list %q through %q: %w", location, entryPath, err)
	}
	manifests, err := iceberg.ReadManifestList(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("parse manifest list %q: %w", location, err)
	}
	c.manifestLists[key] = manifests
	return manifests, nil
}

// manifestFiles reads one manifest and returns the files its entries name, caching the
// result by location while the manifest cache budget allows it.
func (c *catalogIndex) manifestFiles(ctx context.Context, ref string, tableUUID uuid.UUID, root string, manifest iceberg.ManifestFile) ([]dataFileRef, error) {
	location := manifest.FilePath()
	key := normalizeAddress(location)
	if files, ok := c.manifests[key]; ok {
		return files, nil
	}
	entryPath, err := fileEntryPath(tableUUID, root, location)
	if err != nil {
		return nil, fmt.Errorf("%w: manifest %q under %q: %w", ErrForeignFile, location, root, err)
	}
	data, err := c.api.GetObject(ctx, c.repository, ref, entryPath)
	if err != nil {
		return nil, fmt.Errorf("read manifest %q through %q: %w", location, entryPath, err)
	}
	reader, err := iceberg.NewManifestReader(manifest, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("read manifest %q: %w", location, err)
	}
	defer func() { _ = reader.Close() }()

	files := make([]dataFileRef, 0, initialManifestEntries)
	for {
		entry, err := reader.ReadEntry()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read an entry of manifest %q: %w", location, err)
		}
		dataFile := entry.DataFile()
		files = append(files, dataFileRef{Path: dataFile.FilePath(), SizeBytes: dataFile.FileSizeBytes()})
	}
	if c.manifestBytes+int64(len(data)) <= maxManifestCacheBytes {
		c.manifests[key] = files
		c.manifestBytes += int64(len(data))
	}
	return files, nil
}

// hasEmbeddedManifests reports whether a v1 table uses the deprecated inline "manifests"
// field on any snapshot. Expire refuses such a table (planExpire): the rewrite would have
// to decide how every reader resolves the embedded manifest locations, and the task is to
// abort rather than guess.
func hasEmbeddedManifests(metadata table.Metadata) bool {
	if metadata.Version() != 1 {
		return false
	}
	for _, snapshot := range metadata.Snapshots() {
		if snapshot.ManifestList == "" && len(snapshot.ManifestLocations) > 0 {
			return true
		}
	}
	return false
}
