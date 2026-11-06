package icebergcatalog

// Where a table's files live (ICEBERG_CATALOG_PLAN.md §6.1, §6.2, §14 Q-I5, task I2-4).
//
// An Iceberg table has a *base location*: the directory its metadata, manifest and data files are
// written under. The client writes those files itself (unless it delegates), and metadata.json
// carries the location as its `location` field, so the location is part of the table's identity
// rather than a property of one request.
//
// lakeFS places a managed table here (§14 Q-I5):
//
//	<storage_namespace>/_iceberg/<table-uuid>/{metadata,data}/…
//
// with `<storage_namespace>` the repository's storage namespace. Three properties of that form
// matter, and each of them is a decision rather than a coincidence:
//
//   - **It contains no ref.** The location is the same on every branch, tag and commit that holds
//     the table, so a metadata.json written on one branch stays valid after a merge, after the
//     branch is deleted, and when a tag is read years later. Deriving the location from the branch
//     name instead - the `s3://<repo>/<branch>/…` gateway form - is the alternative §6.3 rejects:
//     absolute paths would then embed the branch, and after a merge `main` would point into
//     `feature/…` (the flaw of the deprecated HadoopCatalog
//     https://github.com/treeverse/lakeFS-iceberg).
//   - **It is under DataPrefix, not ReservedPrefix.** `_iceberg/` in the storage namespace holds
//     the objects the clients and the catalog write, which are shared by every branch and are
//     never copied (gc.go, §6.3). `_lakefs_iceberg/` holds the catalog's own entries *on the
//     branch*, which diff, merge and revert version. Keeping the two apart is what lets the
//     uncommitted GC tell a table file from a catalog entry, and it is why both names exist.
//   - **It carries the table UUID, not the table name.** A UUID survives a drop-and-recreate of
//     the same name (the new table gets a new location, so the old files are collectable), and it
//     is the identity the spec requires metadata.json to carry as `table-uuid`.
//
// A client that sends its own location keeps it, provided it is inside the catalog's table area
// `<storage_namespace>/_iceberg/`. That is the warehouse check this catalog can perform - lakeFS
// has no warehouse property beyond the repository's storage namespace - and it is also a security
// boundary: the server writes the initial metadata.json at the location with its own storage
// credentials, so an unrestricted client-supplied location would be a write-anywhere primitive.
// A location outside the area is refused with 400; the same rule is applied by register (I2-6)
// and is the "client writes outside the table location" guard of §13.
//
// Clean-room note (ICEBERG_CATALOG_PLAN.md §2, DATASETS_DESIGN.md §2): written from
// ICEBERG_CATALOG_PLAN.md §6.1/§6.2/§14 Q-I5, the Apache Iceberg table spec (Apache-2.0)
// https://iceberg.apache.org/spec/#table-metadata and the public lakeFS documentation only. No
// code was taken from lakeFS Enterprise or from upstream lakeFS >= 1.87 (BSL 1.1).

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// ErrInvalidTableLocation is wrapped when a table location cannot be used: it is not inside the
// repository's table area (DataPrefix), or it is the area itself, or a path segment of it is empty
// or dot-only. The HTTP layer maps it to 400 BadRequestException: a location the catalog cannot
// write under is a malformed create request, not a server failure.
var ErrInvalidTableLocation = errors.New("invalid iceberg table location")

// TableAreaLocation is the directory that holds every managed table of a repository, relative to
// the storage namespace (gc.go DataPrefix, §6.1/§6.3):
//
//	<storage_namespace>/_iceberg/
//
// On an Azure blockstore the namespace is spelled the way the catalog hands it to clients
// (azure_location.go D1), so the area is the one every Iceberg FileIO opens:
//
//	abfss://<container>@<account>.<dfs domain>/<path>/_iceberg/
//
// Every other blockstore keeps its own spelling, so the S3, GCS, local and mem branches of the
// catalog see exactly the strings they saw before Azure translation existed.
//
// The trailing slash is part of the value: it is what keeps the area from matching a sibling key
// that merely starts with the same characters ("_icebergx/…").
func TableAreaLocation(storageNamespace string) string {
	return strings.TrimRight(tableAreaNamespace(storageNamespace), "/") + "/" + DataPrefix + "/"
}

// tableAreaNamespace returns the storage namespace in the form the catalog hands out: the Iceberg
// location form for an Azure namespace (azure_location.go D1), and the namespace itself everywhere
// else. An Azure-shaped namespace whose domain this installation cannot translate is left alone,
// which is the pre-Azure behaviour and the legacy spelling the area checks tolerate (D2).
func tableAreaNamespace(storageNamespace string) string {
	if location, ok := icebergNamespaceLocation(storageNamespace); ok {
		return location
	}
	return storageNamespace
}

// legacyTableAreaLocation returns the table area in the block adapter's own spelling for an Azure
// namespace, and the empty string for every other blockstore:
//
//	https://<account>.<blob domain>/<container>/<path>/_iceberg/
//
// It is what D2's read-path tolerance accepts as a second spelling: a table created before the
// location translation has its files under this area, so reading, committing and registering it
// must keep working. Nothing new is ever placed there - TableLocation returns the Iceberg form
// (location.go normalizeTableLocation) - so this is a tolerance, not a second canonical form.
func legacyTableAreaLocation(storageNamespace string) string {
	if _, ok := icebergNamespaceLocation(storageNamespace); !ok {
		return ""
	}
	return strings.TrimRight(storageNamespace, "/") + "/" + DataPrefix + "/"
}

// blockIdentifier returns a table location in the block adapter's own spelling, i.e. the form
// lakeFS entries, the committed and uncommitted GC, stat and the S3 gateway read through the same
// adapter (D2). On an Azure blockstore an Iceberg location is abfs[s]://, which the adapter cannot
// resolve, so the area prefix is swapped for the namespace prefix:
//
//	abfss://<container>@<account>.<dfs domain>/<path>/_iceberg/<uuid>/metadata/x
//	-> https://<account>.<blob domain>/<container>/<path>/_iceberg/<uuid>/metadata/x
//
// It converts only a location that is already under the area, and only by replacing that prefix:
// a location the caller did not validate - a foreign bucket, another container, a stray string -
// is returned unchanged, so this cannot become a write-anywhere primitive (§13). A location in the
// legacy namespace spelling is already in the adapter's form and is returned as it is.
func blockIdentifier(storageNamespace, location string) string {
	if legacyTableAreaLocation(storageNamespace) == "" {
		return location
	}
	area := TableAreaLocation(storageNamespace)
	relative, ok := strings.CutPrefix(location, area)
	if !ok {
		return location
	}
	return strings.TrimRight(storageNamespace, "/") + "/" + DataPrefix + "/" + relative
}

// DefaultTableLocation is the managed location of a table (§14 Q-I5):
//
//	<storage_namespace>/_iceberg/<table-uuid>
//
// with <storage_namespace> in the form the catalog hands out - the Iceberg abfs[s] form on an
// Azure blockstore (azure_location.go D1), the namespace itself everywhere else.
//
// It is a pure function so that the layout is one expression instead of a concatenation repeated
// by the create path and its tests, and so that the location of a table can be computed without a
// catalog: both inputs are already known (the repository's storage namespace and the UUID the
// caller generated).
//
// A missing storage namespace and a table UUID that is not a UUID are errors rather than an
// empty segment: either would produce a location that no client can resolve, and the pointer that
// stored it would then be unreadable.
func DefaultTableLocation(storageNamespace, tableUUID string) (string, error) {
	if strings.TrimSpace(storageNamespace) == "" {
		return "", fmt.Errorf("%w: the repository has no storage namespace to place the table in",
			ErrInvalidTableLocation)
	}
	if _, err := uuid.Parse(tableUUID); err != nil {
		return "", fmt.Errorf("%w: %q is not a table uuid: %w", ErrInvalidTableLocation, tableUUID, err)
	}
	return TableAreaLocation(storageNamespace) + tableUUID, nil
}

// TableLocation resolves the base location of a new table: the client's `location` when the
// create request carried one, and DefaultTableLocation(tableUUID) otherwise (§14 Q-I5).
//
// A requested location is accepted only when it is inside the repository's table area; see
// normalizeTableLocation for why. The returned location never has a trailing slash, so
// <location>/metadata/<file> joins cleanly, and it is always in the canonical spelling: on an
// Azure blockstore the Iceberg abfs[s] form, never the legacy https form (D2).
func (s *Store) TableLocation(ctx context.Context, repositoryID, requested, tableUUID string) (string, error) {
	repository, err := s.catalog.GetRepository(ctx, repositoryID)
	if err != nil {
		return "", fmt.Errorf("resolve repository %q: %w", repositoryID, err)
	}
	if strings.TrimSpace(requested) == "" {
		return DefaultTableLocation(repository.StorageNamespace, tableUUID)
	}
	return normalizeTableLocation(repository.StorageNamespace, requested)
}

// normalizeTableLocation validates the location a client asked for and returns it in the form the
// catalog stores: no surrounding whitespace, no trailing slash.
//
// The location must be inside TableAreaLocation(storageNamespace). Two things follow from that
// check, and both are deliberate:
//
//   - the catalog only ever writes - and therefore only ever authorises itself to write - under
//     its own table area. The initial metadata.json goes there with the server's credentials, so
//     honouring an arbitrary location would let a client choose where the server writes.
//   - a table's files stay inside the storage namespace whose GC, grace window (§6.3) and
//     staging rules lakeFS controls. A table whose files live in some other bucket would be
//     outside every one of them.
//
// The remainder is then checked segment by segment: an empty segment ("a//b") or a dot segment
// ("a/../b") would make the location name a different directory than the string suggests, and the
// metadata file the catalog writes would land outside the area it validated.
//
// On an Azure blockstore the legacy namespace spelling of the area is accepted as a second spelling
// (D2): a client that names an https:// area location - a table, or a create request, written
// before the location translation - is not refused, and the returned location is the canonical
// Iceberg form, because TableLocation never returns the legacy spelling.
func normalizeTableLocation(storageNamespace, requested string) (string, error) {
	location := strings.TrimRight(strings.TrimSpace(requested), "/")
	area := TableAreaLocation(storageNamespace)
	relative, ok := strings.CutPrefix(location, area)
	normalized := location
	if !ok {
		legacyArea := legacyTableAreaLocation(storageNamespace)
		if legacyArea == "" {
			return "", fmt.Errorf("%w: the table location %q is not inside the catalog's table area %q",
				ErrInvalidTableLocation, location, area)
		}
		relative, ok = strings.CutPrefix(location, legacyArea)
		if !ok {
			return "", fmt.Errorf("%w: the table location %q is not inside the catalog's table area %q",
				ErrInvalidTableLocation, location, area)
		}
		// The legacy spelling names the same directory, so the location is the canonical one: the
		// catalog hands out one form, and a new table always gets it.
		normalized = area + relative
	}
	if relative == "" {
		return "", fmt.Errorf("%w: %q is the table area itself, not a table location",
			ErrInvalidTableLocation, location)
	}
	for _, segment := range strings.Split(relative, "/") {
		if segment == "" {
			return "", fmt.Errorf("%w: the table location %q has an empty path segment",
				ErrInvalidTableLocation, location)
		}
		if segment == "." || segment == ".." {
			return "", fmt.Errorf("%w: the table location %q has the reserved path segment %q",
				ErrInvalidTableLocation, location, segment)
		}
	}
	return normalized, nil
}
