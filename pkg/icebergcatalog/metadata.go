package icebergcatalog

// Writing table metadata (ICEBERG_CATALOG_PLAN.md §6.2 steps 3-4).
//
// A commit never edits metadata.json in place. It writes a new file and then swaps the table
// pointer to it (commit.go), so a reader that resolved the old pointer keeps a complete, valid
// document and a lost compare-and-swap leaves only an unreachable file behind. That is why the
// name of a metadata file must be unique and must not be reused:
//
//	<storage_namespace>/_iceberg/<table-uuid>/metadata/<seq>-<uuid>.metadata.json
//
// <seq> is a per-table version counter derived from the name of the file it replaces (falling
// back to the length of metadata-log for a table whose current file was written by another
// catalog), zero-padded so that a lexical listing is also a chronological one. The trailing
// <uuid> is a fresh random UUID per write: two commits racing on the same base therefore write
// two different files even though both compute the same <seq>, which is what makes "never
// overwrite a metadata file" a property of the name rather than of the object store's
// conditional-write support (the block adapter has none).
//
// The location is fully qualified - the same form the pointer stores in
// iceberg.metadata-location (§6.1) - and new files always land under the table's location from
// metadata.json, not next to whichever file happens to be current. A table registered from
// another catalog may therefore keep its old metadata files where they are while every file this
// catalog writes lands in <table-location>/metadata/.
//
// Licensing: this file is a clean-room implementation from the Apache Iceberg table spec
// (Apache-2.0), https://iceberg.apache.org/spec/#table-metadata, and the Iceberg REST OpenAPI
// spec (Apache-2.0),
// https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml (task I2-3).
// No code is taken from lakeFS Enterprise or from upstream lakeFS >= 1.87 (BUSL 1.1).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/block"
)

const (
	// MetadataDirectory is the subdirectory of a table's location that holds its metadata files.
	// The client-facing convention (every Iceberg writer puts metadata.json under
	// <location>/metadata/) and the layout ICEBERG_CATALOG_PLAN.md §6.1 shows.
	MetadataDirectory = "metadata"

	// metadataFileSuffix is the Iceberg convention for a table metadata file.
	metadataFileSuffix = ".metadata.json"

	// metadataSequenceWidth is the zero padding of <seq>. Five digits keeps a listing sorted for
	// the first 99 999 versions of a table, which is far past the metadata-log limit
	// (write.metadata.previous-versions-max, 100 by default) and purely cosmetic: the sequence is
	// parsed back as an integer, never compared as a string.
	metadataSequenceWidth = 5
)

// NewMetadataFileLocation returns the fully qualified location of the next metadata.json of a
// table (ICEBERG_CATALOG_PLAN.md §6.2 step 4).
//
// tableLocation is the table's location as metadata.json carries it; baseLocation is the location
// of the metadata file being replaced, which is how the sequence number is continued. meta is the
// new document: only its metadata-log is used, and only when baseLocation was not written by this
// catalog and does not carry a sequence number (a registered table).
//
// The result is never equal to baseLocation: the name carries a fresh random UUID, so a commit
// cannot overwrite a metadata file even if it races another commit that computed the same
// sequence number.
func NewMetadataFileLocation(tableLocation, baseLocation string, meta table.Metadata) (string, error) {
	if tableLocation == "" {
		return "", fmt.Errorf("%w: the table metadata has no location to write a metadata file under",
			ErrInvalidPointer)
	}
	sequence := nextMetadataSequence(baseLocation, meta)
	fileUUID, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("generate a metadata file name: %w", err)
	}
	return strings.TrimRight(tableLocation, "/") + "/" + MetadataDirectory + "/" +
		metadataFileName(sequence, fileUUID), nil
}

// metadataFileName formats the file name of one metadata version: <seq>-<uuid>.metadata.json.
func metadataFileName(sequence int, fileUUID uuid.UUID) string {
	return fmt.Sprintf("%0*d-%s%s", metadataSequenceWidth, sequence, fileUUID, metadataFileSuffix)
}

// nextMetadataSequence returns the sequence number of the metadata file that replaces
// baseLocation. A base file this catalog wrote continues its sequence; anything else (a file
// written by another catalog, or no base at all for the initial metadata of a new table) falls
// back to the number of metadata-log entries, which is the number of versions the document itself
// remembers.
func nextMetadataSequence(baseLocation string, meta table.Metadata) int {
	if sequence, ok := metadataFileSequence(baseLocation); ok {
		return sequence + 1
	}
	return metadataLogLength(meta) + 1
}

// metadataFileSequence parses the <seq> of a base file name. It accepts both the name this catalog
// writes ("00007-<uuid>.metadata.json") and a bare sequence number ("7.metadata.json", the form
// other catalogs and the test fixtures use), so a table registered with either continues counting.
func metadataFileSequence(location string) (int, bool) {
	name := strings.TrimSuffix(path.Base(location), metadataFileSuffix)
	if name == path.Base(location) {
		return 0, false // the file is not named *.metadata.json
	}
	if dash := strings.IndexByte(name, '-'); dash >= 0 {
		name = name[:dash]
	}
	sequence, err := strconv.Atoi(name)
	if err != nil || sequence < 0 {
		return 0, false
	}
	return sequence, true
}

// metadataLogLength counts the metadata-log entries of meta.
func metadataLogLength(meta table.Metadata) int {
	count := 0
	for range meta.PreviousFiles() {
		count++
	}
	return count
}

// WriteMetadata writes one metadata.json version to the object store at location and returns the
// exact bytes written, so the caller can hand them to a client without re-marshalling the parsed
// model (§6.2 step 7).
//
// The object store has no compare-and-put, so this call cannot itself refuse to overwrite: the
// caller must pass a fresh NewMetadataFileLocation. A failed write leaves no pointer behind and
// nothing is swapped, so it is a plain failure of the request (§6.2 step 4 fails before step 6).
func (s *Store) WriteMetadata(ctx context.Context, repositoryID, location string, meta table.Metadata) ([]byte, error) {
	data, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshal iceberg metadata for %q: %w", location, err)
	}
	if err := s.writeDocument(ctx, repositoryID, location, data); err != nil {
		return nil, err
	}
	return data, nil
}

// writeDocument puts one already-serialized metadata document at location through the block
// adapter. It is the write half WriteMetadata and WriteViewMetadata (views.go) share: both write
// <location>/metadata/<seq>-<uuid>.metadata.json into the repository's storage namespace with the
// server's credentials, and neither may exist twice under one name.
func (s *Store) writeDocument(ctx context.Context, repositoryID, location string, data []byte) error {
	repo, err := s.catalog.GetRepository(ctx, repositoryID)
	if err != nil {
		return fmt.Errorf("resolve repository %q: %w", repositoryID, err)
	}
	_, err = s.block.Put(ctx, block.ObjectPointer{
		StorageID:        repo.StorageID,
		StorageNamespace: repo.StorageNamespace,
		Identifier:       location,
		IdentifierType:   block.IdentifierTypeFull,
	}, int64(len(data)), bytes.NewReader(data), block.PutOpts{})
	if err != nil {
		return fmt.Errorf("write iceberg metadata %q: %w", location, err)
	}
	return nil
}
