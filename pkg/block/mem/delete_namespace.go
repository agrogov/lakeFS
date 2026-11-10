package mem

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/treeverse/lakefs/pkg/block"
)

const defaultDeleteNamespacePageSize = 1000

// DeleteNamespacePage deletes one page of the keys under storageNamespace from the in-memory map.
// It never touches a key outside "<namespace>/": every key is re-checked against both the shared
// namespace delete prefix and the fully qualified prefix derived from the namespace.
func (a *Adapter) DeleteNamespacePage(_ context.Context, storageID, storageNamespace string, opts block.DeleteNamespaceOpts) (*block.DeleteNamespaceResult, error) {
	_, prefix, err := block.NamespaceDeletePrefix(storageNamespace)
	if err != nil {
		return nil, err
	}
	qk, err := a.ResolveNamespace(storageID, storageNamespace, "", block.IdentifierTypeRelative)
	if err != nil {
		return nil, err
	}
	fullPrefix := qk.Format()
	if !strings.HasSuffix(fullPrefix, "/") {
		fullPrefix += "/"
	}
	if !strings.HasSuffix(fullPrefix, prefix) {
		return nil, fmt.Errorf("%q is not under the namespace delete prefix %q: %w",
			fullPrefix, prefix, block.ErrNamespaceDeleteOutsidePrefix)
	}

	fullAfter := ""
	if opts.After != "" {
		fullAfter = fullPrefix + opts.After
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultDeleteNamespacePageSize
	}

	a.mutex.Lock()
	defer a.mutex.Unlock()

	keys := make([]string, 0, len(a.data[storageID]))
	for key := range a.data[storageID] {
		if !strings.HasPrefix(key, fullPrefix) {
			continue
		}
		if fullAfter != "" && key <= fullAfter {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := &block.DeleteNamespaceResult{
		LastKey: opts.After,
		Done:    len(keys) <= limit,
	}
	if len(keys) > limit {
		keys = keys[:limit]
	}

	for _, key := range keys {
		// The second guard (REPO_DELETE_STORAGE_DESIGN.md section 2.2): a listing bug must fail the
		// page rather than delete outside the namespace.
		relativeKey := strings.TrimPrefix(key, "mem://")
		if !strings.HasPrefix(relativeKey, prefix) || !strings.HasPrefix(key, fullPrefix) {
			return nil, fmt.Errorf("%q: %w", key, block.ErrNamespaceDeleteOutsidePrefix)
		}
		relativeKey = strings.TrimPrefix(key, fullPrefix)
		result.LastKey = relativeKey
		if isKeptDeleteNamespaceKey(relativeKey, opts.Keep) {
			continue
		}
		delete(a.data[storageID], key)
		if a.properties[storageID] != nil {
			delete(a.properties[storageID], key)
		}
		result.Deleted++
	}
	return result, nil
}

func isKeptDeleteNamespaceKey(key string, keep []string) bool {
	for _, kept := range keep {
		if key == kept {
			return true
		}
	}
	return false
}
