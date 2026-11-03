package importer

// The table metadata document: parsing, the fields the importer reads, and the copy-mode rewrite.
//
// The document is decoded into map[string]any with json.Decoder.UseNumber rather than into a
// struct. Two reasons, both load-bearing:
//
//   - Unknown fields must survive. The Iceberg table spec adds fields between format versions and
//     clients add properties, and a copy must not silently drop a part of the table it does not
//     know about.
//   - Numbers must survive exactly. Snapshot and sequence numbers exceed 2^53 (the Iceberg spec
//     says so explicitly), so decoding them into float64 and re-encoding would corrupt them.
//     json.Number round-trips the original text.
//
// Only the fields the import acts on are rewritten: `location`, `table-uuid`, every snapshot's
// `manifest-list` and v1 inline `manifests`, and the location-valued table properties. The
// document's `metadata-log`, `statistics` and `partition-statistics` are removed in copy mode
// because they name files register does not stage; see doc.go.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// metadataLocationProperties are the table properties whose values are physical locations and
// which therefore have to follow the table to its new root. They are the write-default properties
// of the Iceberg table spec.
var metadataLocationProperties = []string{
	"write.data.path",
	"write.metadata.path",
	"write.metadata.statistics.path",
}

// Metadata is a parsed table metadata document. The raw document is kept for the register call
// when no rewrite is needed.
type Metadata struct {
	document map[string]any
	original []byte
}

// ParseMetadata parses a table metadata document from the JSON a catalog returned.
func ParseMetadata(data []byte) (*Metadata, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var document map[string]any
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("%w: decode metadata document: %w", ErrInvalidMetadata, err)
	}
	if decoder.More() {
		return nil, fmt.Errorf("%w: metadata document has trailing JSON values", ErrInvalidMetadata)
	}
	if document == nil {
		return nil, fmt.Errorf("%w: metadata document is not a JSON object", ErrInvalidMetadata)
	}
	return &Metadata{document: document, original: append([]byte(nil), data...)}, nil
}

// Original returns the document exactly as it was read. It is what the default mode registers:
// adopting a document must not re-encode it.
func (m *Metadata) Original() []byte { return m.original }

// FormatVersion returns the document's format-version. A document without one, or with a value
// that is not an integer, is invalid.
func (m *Metadata) FormatVersion() (int, error) {
	value, ok := m.document["format-version"]
	if !ok {
		return 0, fmt.Errorf("%w: the document carries no format-version", ErrInvalidMetadata)
	}
	version, err := numberAsInt(value)
	if err != nil {
		return 0, fmt.Errorf("%w: format-version: %w", ErrInvalidMetadata, err)
	}
	return version, nil
}

// Location returns the table's base location, normalized (no trailing slash). Metadata without a
// location cannot be copied, and register needs it to decide the table root.
func (m *Metadata) Location() string {
	value, _ := m.document["location"].(string)
	return normalizeLocation(value)
}

// TableUUID returns the document's table-uuid, or "" when it carries none (the spec makes it
// optional in a v1 document).
func (m *Metadata) TableUUID() string {
	value, _ := m.document["table-uuid"].(string)
	return strings.TrimSpace(value)
}

// ManifestListLocations returns every snapshot's manifest-list location, in document order and
// without duplicates.
func (m *Metadata) ManifestListLocations() []string {
	var locations []string
	seen := map[string]bool{}
	for _, snapshot := range m.snapshots() {
		location, ok := snapshot["manifest-list"].(string)
		if !ok || location == "" || seen[location] {
			continue
		}
		seen[location] = true
		locations = append(locations, location)
	}
	return locations
}

// InlineManifestLocations returns the locations in every snapshot's v1 `manifests` array, in
// document order and without duplicates. A v1 table written before manifest lists existed carries
// its manifests there instead of in a manifest-list file.
func (m *Metadata) InlineManifestLocations() []string {
	var locations []string
	seen := map[string]bool{}
	for _, snapshot := range m.snapshots() {
		manifests, _ := snapshot["manifests"].([]any)
		for _, entry := range manifests {
			location, ok := entry.(string)
			if !ok || location == "" || seen[location] {
				continue
			}
			seen[location] = true
			locations = append(locations, location)
		}
	}
	return locations
}

// snapshots returns the document's snapshot objects, ignoring anything of another shape.
func (m *Metadata) snapshots() []map[string]any {
	entries, _ := m.document["snapshots"].([]any)
	snapshots := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		if snapshot, ok := entry.(map[string]any); ok {
			snapshots = append(snapshots, snapshot)
		}
	}
	return snapshots
}

// RewriteForCopy turns the document into the metadata of the copied table:
//
//   - `location` becomes targetRoot and `table-uuid` becomes tableUUID;
//   - every snapshot's `manifest-list` and v1 inline `manifests` are passed through rewrite, which
//     maps a source location to its target location and fails on a location outside the source
//     table root;
//   - the location-valued write properties are moved to the new root when they were under the old
//     one;
//   - `metadata-log`, `statistics` and `partition-statistics` are dropped (see doc.go).
//
// rewrite is called once per distinct location in the order the document carries them.
func (m *Metadata) RewriteForCopy(sourceRoot, targetRoot, tableUUID string, rewrite func(string) (string, error)) error {
	m.document["location"] = targetRoot
	m.document["table-uuid"] = tableUUID

	for _, snapshot := range m.snapshots() {
		if location, ok := snapshot["manifest-list"].(string); ok && location != "" {
			rewritten, err := rewrite(location)
			if err != nil {
				return err
			}
			snapshot["manifest-list"] = rewritten
		}
		manifests, ok := snapshot["manifests"].([]any)
		if !ok {
			continue
		}
		rewrittenManifests := make([]any, len(manifests))
		for i, entry := range manifests {
			location, ok := entry.(string)
			if !ok || location == "" {
				rewrittenManifests[i] = entry
				continue
			}
			rewritten, err := rewrite(location)
			if err != nil {
				return err
			}
			rewrittenManifests[i] = rewritten
		}
		snapshot["manifests"] = rewrittenManifests
	}

	for _, key := range metadataLocationProperties {
		value, ok := m.document["properties"].(map[string]any)
		if !ok {
			break
		}
		property, ok := value[key].(string)
		if !ok || property == "" {
			continue
		}
		// A property under the old root follows the table. One that names another location is
		// left alone: it is a client's explicit choice, not a file of this table.
		if inside(sourceRoot, property) {
			relative, err := relativeTo(sourceRoot, property)
			if err != nil {
				return err
			}
			value[key] = joinLocation(targetRoot, relative)
		}
	}

	delete(m.document, "metadata-log")
	delete(m.document, "statistics")
	delete(m.document, "partition-statistics")
	return nil
}

// JSON re-encodes the document. Only a rewritten document is marshalled; the default mode uses
// Original.
func (m *Metadata) JSON() ([]byte, error) {
	encoded, err := json.Marshal(m.document)
	if err != nil {
		return nil, fmt.Errorf("%w: encode metadata document: %w", ErrInvalidMetadata, err)
	}
	return encoded, nil
}

// numberAsInt converts a decoded JSON number to an int. A JSON number that is not an integer (or
// does not fit an int) is refused rather than rounded.
func numberAsInt(value any) (int, error) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, fmt.Errorf("%w: %v is not a number", ErrInvalidMetadata, value)
	}
	parsed, err := number.Int64()
	if err != nil {
		return 0, fmt.Errorf("%w: %w", ErrInvalidMetadata, err)
	}
	return int(parsed), nil
}
