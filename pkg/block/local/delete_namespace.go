package local

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/treeverse/lakefs/pkg/block"
)

const defaultDeleteNamespacePageSize = 1000

// DeleteNamespacePage deletes one page of the files under storageNamespace from the local
// filesystem. Every path is resolved through the adapter's verified namespace mapping and
// re-checked against the shared delete prefix, and the walker never enters its `_lakefs_cache`
// directory. Empty directories are removed bottom-up, so a completed deletion leaves no trace of
// the namespace either.
func (l *Adapter) DeleteNamespacePage(_ context.Context, storageID, storageNamespace string, opts block.DeleteNamespaceOpts) (*block.DeleteNamespaceResult, error) {
	_, prefix, err := block.NamespaceDeletePrefix(storageNamespace)
	if err != nil {
		return nil, err
	}
	dir, err := l.extractParamsFromObj(block.ObjectPointer{
		StorageID:        storageID,
		StorageNamespace: storageNamespace,
		Identifier:       "",
		IdentifierType:   block.IdentifierTypeRelative,
	})
	if err != nil {
		return nil, err
	}
	dir = filepath.Clean(dir)
	if err := l.verifyRelPath(dir); err != nil {
		return nil, err
	}
	cacheDir := filepath.Clean(filepath.Join(l.path, cacheDirName))

	limit := opts.Limit
	if limit <= 0 {
		limit = defaultDeleteNamespacePageSize
	}

	type candidate struct {
		path   string
		relKey string
	}
	var (
		candidates []candidate
		dirs       []string
		done       = true
	)
	walkErr := filepath.WalkDir(dir, func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		cleanPath := filepath.Clean(p)
		if entry.IsDir() {
			if cleanPath == cacheDir {
				return fs.SkipDir
			}
			dirs = append(dirs, cleanPath)
			return nil
		}
		rel, err := filepath.Rel(l.path, cleanPath)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		// The second guard (REPO_DELETE_STORAGE_DESIGN.md section 2.2): a listing bug must fail the
		// page rather than delete outside the namespace.
		if !strings.HasPrefix(rel, prefix) {
			return fmt.Errorf("%q: %w", rel, block.ErrNamespaceDeleteOutsidePrefix)
		}
		relKey := strings.TrimPrefix(rel, prefix)
		if opts.After != "" && relKey <= opts.After {
			return nil
		}
		if len(candidates) == limit {
			done = false
			return fs.SkipAll
		}
		candidates = append(candidates, candidate{path: cleanPath, relKey: relKey})
		return nil
	})
	if errors.Is(walkErr, fs.ErrNotExist) {
		return &block.DeleteNamespaceResult{LastKey: opts.After, Done: true}, nil
	}
	if walkErr != nil {
		return nil, walkErr
	}

	result := &block.DeleteNamespaceResult{LastKey: opts.After, Done: done}
	for _, c := range candidates {
		result.LastKey = c.relKey
		if isKeptDeleteNamespaceKey(c.relKey, opts.Keep) {
			continue
		}
		if err := os.Remove(c.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		result.Deleted++
	}

	// Deepest first, so a directory is empty by the time its parent is attempted. A non-empty
	// directory is left in place: it still holds objects we are not allowed to delete.
	sort.Slice(dirs, func(i, j int) bool {
		di, dj := strings.Count(dirs[i], string(filepath.Separator)), strings.Count(dirs[j], string(filepath.Separator))
		if di != dj {
			return di > dj
		}
		return dirs[i] > dirs[j]
	})
	for _, d := range dirs {
		if err := removeEmptyDir(d); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func removeEmptyDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	if len(entries) > 0 {
		return nil
	}
	if err := os.Remove(dir); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func isKeptDeleteNamespaceKey(key string, keep []string) bool {
	for _, kept := range keep {
		if key == kept {
			return true
		}
	}
	return false
}
