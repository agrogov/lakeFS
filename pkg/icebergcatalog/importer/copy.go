package importer

// Copying a table into the lakeFS storage namespace.
//
// A copy is planned completely before the first byte is written: the metadata document is parsed,
// every manifest list and manifest is read, its paths are mapped to the new root, and the set of
// files to write is known. Only then are the files uploaded, manifests and manifest lists first,
// the metadata document last. That order matters: a metadata document that appears before the
// files it names would be a table a client can load and then fail to read, while an uploaded file
// that no metadata names yet is invisible and will simply be overwritten by the next attempt.
//
// Files are written through the lakeFS object API (the Uploader interface), at the key
// `_iceberg/<new-table-uuid>/<path relative to the source table root>`. That key is chosen because
// the object API resolves it to <storage_namespace>/_iceberg/<uuid>/... - exactly the managed
// table location the catalog validates register against (pkg/icebergcatalog/location.go,
// files.go), so the rewritten document's locations are the physical locations lakeFS just wrote.
//
// The table gets a new UUID, derived deterministically from the source metadata location and the
// target identifier. A fresh UUID per target table is not cosmetic: the catalog stages a table's
// files under _lakefs_iceberg/files/<table-uuid>/ (files.go §6.3), so two target tables sharing a
// UUID on one branch would share entries and a drop of one would release the other's. Deriving it
// instead of randomizing it keeps a resumed import on the same location.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

const (
	// defaultCopyConcurrency is the number of files copied at once. It is a small number on
	// purpose: the bottleneck is the lakeFS object API and, for S3 sources, one client's
	// connection pool, and a whole-table import should not open hundreds of connections.
	defaultCopyConcurrency = 8

	// metadataFilePrefix is the directory of the copied table's metadata document. The single
	// leading sequence number follows the Iceberg convention and keeps the file name sortable
	// next to the documents a later commit writes.
	metadataFilePrefix = "metadata/00000-"

	// metadataFileSuffix is the extension of a table metadata document.
	metadataFileSuffix = ".metadata.json"
)

// importUUIDNamespace is the fixed namespace of the name-based UUID a copied table gets. It is a
// constant of the importer, not a random value: the same source document imported to the same
// target must always land on the same location, so a retry after a failure overwrites its own
// files instead of orphaning them.
var importUUIDNamespace = uuid.MustParse("6f6f1a52-4a1c-4f2e-9c5a-2f3b1d0c7e11")

// Uploader writes one object through the lakeFS object API at a branch-relative key.
type Uploader interface {
	Upload(ctx context.Context, repository, branch, key string, size int64, body io.Reader) error
}

// copiedFile is one file of a copy plan. content is set for a file the importer re-encoded
// (metadata, manifest lists and manifests); a data file has no content and is streamed from its
// source location.
type copiedFile struct {
	source  string
	target  string
	content []byte
}

// copyPlan is the complete set of files a copy writes, and the metadata location of the new table.
type copyPlan struct {
	metadataLocation string
	root             string
	files            []copiedFile
}

// copyTableUUID derives the target table'uuid from the source metadata location and the target
// identifier (see the file comment).
func copyTableUUID(sourceMetadataLocation string, targetNamespace []string, table string) string {
	key := sourceMetadataLocation + sourceNamespaceSeparator +
		joinLevels(targetNamespace) + sourceNamespaceSeparator + table
	return uuid.NewSHA1(importUUIDNamespace, []byte(key)).String()
}

// joinLevels joins namespace levels with the catalog's separator for hashing.
func joinLevels(levels []string) string {
	return strings.Join(levels, sourceNamespaceSeparator)
}

// buildCopyPlan reads the source table's manifests and manifest lists and returns every file to
// copy, with the rewritten bytes of the files that contain locations.
//
// sourceRoot is the table's `location` in the source document: every file the copy knows about has
// to be below it, because that is what makes a file's relative path - and therefore its target
// location - well defined. A table whose data lives elsewhere is reported as failed with that file
// named.
func buildCopyPlan(ctx context.Context, reader FileReader, metadata *Metadata, sourceRoot, targetRoot, tableUUID string) (*copyPlan, error) {
	rewrite := func(location string) (string, error) {
		relative, err := relativeTo(sourceRoot, location)
		if err != nil {
			return "", fmt.Errorf("%w: file %q is not under the source table location %q",
				ErrTableCopy, location, sourceRoot)
		}
		return joinLocation(targetRoot, relative), nil
	}

	plan := &copyPlan{root: targetRoot}

	// 1. The manifest lists named by the snapshots, and the manifests a v1 document carries inline.
	manifestListLocations := metadata.ManifestListLocations()
	manifestListBytes := make(map[string][]byte, len(manifestListLocations))
	manifestSet := map[string]bool{}
	for _, location := range metadata.InlineManifestLocations() {
		manifestSet[location] = true
	}
	for _, location := range manifestListLocations {
		data, err := readSmallFile(ctx, reader, location)
		if err != nil {
			return nil, err
		}
		manifestListBytes[location] = data
		locations, err := scanManifestList(data)
		if err != nil {
			return nil, fmt.Errorf("%w: manifest list %q: %w", ErrTableCopy, location, err)
		}
		for _, manifest := range locations {
			manifestSet[manifest] = true
		}
	}

	manifestLocations := make([]string, 0, len(manifestSet))
	for location := range manifestSet {
		manifestLocations = append(manifestLocations, location)
	}
	sort.Strings(manifestLocations)

	// 2. Every manifest: rewrite its data-file paths and remember the re-encoded length its
	// manifest-list record must carry.
	manifestLengths := make(map[string]int64, len(manifestLocations))
	dataFiles := map[string]int{}
	for _, location := range manifestLocations {
		data, err := readSmallFile(ctx, reader, location)
		if err != nil {
			return nil, err
		}
		rewritten, refs, err := rewriteManifest(data, rewrite)
		if err != nil {
			return nil, fmt.Errorf("%w: manifest %q: %w", ErrTableCopy, location, err)
		}
		manifestLengths[location] = int64(len(rewritten))
		target, err := rewrite(location)
		if err != nil {
			return nil, err
		}
		plan.files = append(plan.files, copiedFile{source: location, target: target, content: rewritten})
		for _, ref := range refs {
			if ref.Status == manifestEntryStatusDeleted {
				continue
			}
			if previous, ok := dataFiles[ref.Location]; !ok || previous == manifestEntryStatusDeleted {
				dataFiles[ref.Location] = ref.Status
			}
		}
	}

	// 3. The manifest lists, now that the new manifest lengths are known.
	for _, location := range manifestListLocations {
		rewritten, err := rewriteManifestList(manifestListBytes[location], rewrite, func(manifest string) (int64, error) {
			length, ok := manifestLengths[manifest]
			if !ok {
				return 0, fmt.Errorf("%w: manifest list %q names manifest %q, which is not part of the table",
					ErrTableCopy, location, manifest)
			}
			return length, nil
		})
		if err != nil {
			return nil, fmt.Errorf("%w: manifest list %q: %w", ErrTableCopy, location, err)
		}
		target, err := rewrite(location)
		if err != nil {
			return nil, err
		}
		plan.files = append(plan.files, copiedFile{source: location, target: target, content: rewritten})
	}

	// 4. The data files, streamed rather than buffered.
	dataLocations := make([]string, 0, len(dataFiles))
	for location := range dataFiles {
		dataLocations = append(dataLocations, location)
	}
	sort.Strings(dataLocations)
	for _, location := range dataLocations {
		target, err := rewrite(location)
		if err != nil {
			return nil, err
		}
		plan.files = append(plan.files, copiedFile{source: location, target: target})
	}

	// 5. The rewritten metadata document itself.
	if err := metadata.RewriteForCopy(sourceRoot, targetRoot, tableUUID, rewrite); err != nil {
		return nil, err
	}
	document, err := metadata.JSON()
	if err != nil {
		return nil, err
	}
	metadataLocation := joinLocation(targetRoot, metadataFilePrefix+tableUUID+metadataFileSuffix)
	plan.metadataLocation = metadataLocation
	plan.files = append(plan.files, copiedFile{target: metadataLocation, content: document})

	sort.Slice(plan.files, func(i, j int) bool { return plan.files[i].target < plan.files[j].target })
	return plan, nil
}

// readSmallFile reads a metadata-sized file (a manifest list or a manifest) through the source
// FileIO, with the buffer bound of maxAvroFile.
func readSmallFile(ctx context.Context, reader FileReader, location string) ([]byte, error) {
	body, err := reader.Open(ctx, location)
	if err != nil {
		return nil, err
	}
	defer func() { _ = body.Close() }()
	data, err := io.ReadAll(io.LimitReader(body, maxAvroFile+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read %q: %w", ErrTableCopy, location, err)
	}
	if len(data) > maxAvroFile {
		return nil, fmt.Errorf("%w: %q exceeds the %d-byte limit", ErrTableCopy, location, maxAvroFile)
	}
	return data, nil
}

// uploadCopyPlan writes every file of the plan through the object API. The key of an object is the
// file's location relative to the repository's storage namespace.
func uploadCopyPlan(ctx context.Context, uploader Uploader, reader FileReader, repository, branch, storageNamespace string, plan *copyPlan) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(defaultCopyConcurrency)
	for _, file := range plan.files {
		group.Go(func() error {
			key, err := relativeTo(storageNamespace, file.target)
			if err != nil {
				return fmt.Errorf("%w: target file %q: %w", ErrTableCopy, file.target, err)
			}
			if file.content != nil {
				if err := uploader.Upload(groupCtx, repository, branch, key, int64(len(file.content)),
					bytes.NewReader(file.content)); err != nil {
					return fmt.Errorf("%w: upload %q: %w", ErrTableCopy, file.target, err)
				}
				return nil
			}
			size, err := reader.Size(groupCtx, file.source)
			if err != nil {
				return err
			}
			body, err := reader.Open(groupCtx, file.source)
			if err != nil {
				return err
			}
			defer func() { _ = body.Close() }()
			if err := uploader.Upload(groupCtx, repository, branch, key, size, body); err != nil {
				return fmt.Errorf("%w: copy %q to %q: %w", ErrTableCopy, file.source, file.target, err)
			}
			return nil
		})
	}
	return group.Wait()
}
