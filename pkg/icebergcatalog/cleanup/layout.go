package cleanup

// layout.go holds the path vocabulary the two phases share: the reserved catalog area on
// a branch, the physical table area inside the storage namespace, the mapping from a
// physical file address to the lakeFS entry that registers it, and the reverse mapping
// from an entry path back to the table UUID it belongs to.
//
// It is a separate file because both phases depend on it and both must agree on it: if
// expire staged a metadata file under one key and mark looked for it under another, the
// file would look unreferenced and the sweep would delete its registration. Every key is
// therefore derived from the constants pkg/icebergcatalog exports (ReservedPrefix,
// FilesDirectory, TablesDirectory, MetadataDirectory, DataPrefix, TableFilePath), never
// spelled out a second time.

import (
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
)

const (
	// ReportsPrefix is where every report of this job lands: the reserved
	// _lakefs/retention/ prefix, next to the lakeFS GC's own reports under
	// _lakefs/retention/gc/ (design/accepted/gc-two-steps-separation.md).
	ReportsPrefix = "_lakefs/retention/iceberg"

	// ExpireReportsPrefix holds one directory per expire run.
	ExpireReportsPrefix = ReportsPrefix + "/expire"

	// SweepReportsPrefix holds one directory per mark or sweep run. A mark run's
	// directory is named after its mark ID so a later --sweep-only run can find it.
	SweepReportsPrefix = ReportsPrefix + "/sweep"

	// SummaryFileName is the report every phase writes (and --sweep-only reads for the
	// mark it resumes).
	SummaryFileName = "summary.json"

	// ObjectsFileName is the mark report's candidate list, one JSON object per line.
	// It is a separate file from summary.json because a repository can mark hundreds of
	// thousands of files while its summary stays a few kilobytes, and because a line
	// format can be streamed and grepped.
	ObjectsFileName = "objects.jsonl"

	// filesDirectory is the reserved branch area that holds one entry per table file
	// (ICEBERG_CATALOG_PLAN.md §6.3), aliased from pkg/icebergcatalog so the two never
	// drift.
	filesDirectory = icebergcatalog.FilesDirectory

	// tablesDirectory is the reserved branch area that holds one .pointer per table.
	tablesDirectory = icebergcatalog.TablesDirectory
)

// reservedPrefix is the branch prefix that holds all catalog state
// (ICEBERG_CATALOG_PLAN.md §6.1), aliased from pkg/icebergcatalog.
const reservedPrefix = icebergcatalog.ReservedPrefix

// filesPrefix is the reserved entry prefix of every table-file registration:
// _lakefs_iceberg/files/.
var filesPrefix = reservedPrefix + "/" + filesDirectory + "/"

// tablesPrefix is the reserved entry prefix of every table pointer:
// _lakefs_iceberg/tables/.
var tablesPrefix = reservedPrefix + "/" + tablesDirectory + "/"

// pointerName is the leaf name of a table pointer entry.
const pointerName = icebergcatalog.TablePointerName

// normalizeAddress returns a comparable form of a physical file address. Two strings that
// name the same object must compare equal, because the sweep decides "is this file
// referenced?" by a map lookup: a metadata document writes
// "s3://bucket/ns/_iceberg/x/data/f.parquet" and a lakeFS entry reports the same address,
// but a table registered by another catalog may carry a "./" or a doubled slash in its
// paths. The function cleans the path component only; the scheme, host and bucket stay
// exactly as written, so it can never merge two different buckets.
//
// An address that does not parse as a URL is returned trimmed of its trailing slash. That
// fallback keeps a relative/odd address usable as a key; it does not make two different
// addresses equal.
func normalizeAddress(address string) string {
	address = strings.TrimSpace(address)
	if address == "" {
		return ""
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme == "" {
		return strings.TrimRight(address, "/")
	}
	parsed.Fragment = ""
	parsed.RawQuery = ""
	if parsed.Path != "" {
		parsed.Path = path.Clean(parsed.Path)
	}
	return parsed.String()
}

// underPrefix reports whether p is under prefix, with prefix being a directory path
// (a trailing slash is required at the boundary). It is a plain string check on purpose:
// a path such as "_lakefs_iceberg/filesX/..." must not be treated as being inside
// "_lakefs_iceberg/files/".
func underPrefix(p, prefix string) bool {
	prefix = strings.TrimRight(prefix, "/") + "/"
	return strings.HasPrefix(p, prefix)
}

// relativeTo returns p without the directory prefix. The caller has already established
// underPrefix; the special case of equality returns "".
func relativeTo(p, prefix string) string {
	prefix = strings.TrimRight(prefix, "/")
	if p == prefix {
		return ""
	}
	return strings.TrimPrefix(p, prefix+"/")
}

// storageKey returns the lakeFS entry path of a physical location: the location relative
// to the repository's storage namespace. It is how the expire phase turns the new
// metadata file's fully qualified location into the path UploadObject writes: uploading
// "_iceberg/<uuid>/metadata/<name>" makes lakeFS write the object at
// "<storage-namespace>/_iceberg/<uuid>/metadata/<name>", which is exactly where the
// catalog and every Iceberg client expect it.
func storageKey(namespace, location string) (string, error) {
	namespace = strings.TrimRight(namespace, "/")
	if namespace == "" {
		return "", fmt.Errorf("%w: the repository has no storage namespace", ErrInvalidLayout)
	}
	if !underPrefix(location, namespace) {
		return "", fmt.Errorf("%w: %q is outside the storage namespace %q", ErrInvalidLayout, location, namespace)
	}
	key := relativeTo(location, namespace)
	if key == "" {
		return "", fmt.Errorf("%w: %q is the storage namespace itself", ErrInvalidLayout, location)
	}
	return key, nil
}

// inTableArea reports whether a table location lives in the catalog's physical table
// area, <storage-namespace>/_iceberg/ (ICEBERG_CATALOG_PLAN.md §6.3, pkg/icebergcatalog
// DataPrefix). A table whose location is elsewhere - one registered from another catalog
// with an external warehouse - is an external-metadata table: it can be read through the
// catalog but this job must not rewrite or sweep it.
func inTableArea(namespace, location string) bool {
	namespace = strings.TrimRight(namespace, "/")
	if namespace == "" {
		return false
	}
	area := namespace + "/" + icebergcatalog.DataPrefix
	return location == area || underPrefix(location, area)
}

// fileEntryPath returns the reserved entry path that registers a physical file of a
// table, given the table's UUID and the table's physical root:
//
//	_lakefs_iceberg/files/<table-uuid>/<path relative to the table root>
//
// It delegates to pkg/icebergcatalog.TableFilePath, which validates the relative path, so
// a path from a client-written manifest cannot climb out of the table's directory in the
// reserved area (files.go "The foreign-location check is a security control").
func fileEntryPath(tableUUID uuid.UUID, tableRoot, location string) (string, error) {
	root := strings.TrimRight(tableRoot, "/")
	if root == "" {
		return "", fmt.Errorf("%w: the table has no physical root", ErrInvalidLayout)
	}
	if !underPrefix(location, root) {
		return "", fmt.Errorf("%w: %q is outside the table root %q", ErrInvalidLayout, location, root)
	}
	return icebergcatalog.TableFilePath(tableUUID, relativeTo(location, root))
}

// tableFileEntryUUID parses the table UUID out of a registration entry path:
// _lakefs_iceberg/files/<uuid>/<...>. It returns false for a path that is not one of the
// catalog's file registrations, so a stray entry under the reserved prefix cannot make
// the sweep attribute a file to a table it does not belong to.
func tableFileEntryUUID(entryPath string) (uuid.UUID, bool) {
	if !underPrefix(entryPath, filesPrefix) {
		return uuid.Nil, false
	}
	rest := relativeTo(entryPath, filesPrefix)
	segment, _, ok := strings.Cut(rest, "/")
	if !ok || segment == "" {
		return uuid.Nil, false
	}
	parsed, err := uuid.Parse(segment)
	if err != nil {
		return uuid.Nil, false
	}
	return parsed, true
}

// isPointerPath reports whether an entry path is a table pointer:
// _lakefs_iceberg/tables/<ns...>/<table>/.pointer.
func isPointerPath(entryPath string) bool {
	return underPrefix(entryPath, tablesPrefix) && path.Base(entryPath) == pointerName
}
