package catalog

import (
	"context"
	"fmt"
	"strings"
)

// RepositoryDeletionTarget is one repository whose storage deletion a request asks for.  The
// overlap scan excludes every target of the same request from the others' check (design §4.2).
type RepositoryDeletionTarget struct {
	Repository       string
	StorageID        string
	StorageNamespace string
}

// NamespaceConflict describes the owner of a namespace that overlaps a deletion target (R10).
type NamespaceConflict struct {
	Repository       string
	StorageID        string
	StorageNamespace string
	// FromDeletionRecord is true when the owner is a pending deletion record rather than a live
	// repository record.
	FromDeletionRecord bool
}

func (c NamespaceConflict) String() string {
	return c.Repository
}

// splitNamespace splits a storage namespace into its scheme and path, dropping trailing slashes.
// An empty path means the root of the scheme (a bucket, a container, or local:// and mem://).
func splitNamespace(storageNamespace string) (scheme, path string) {
	scheme, path, found := strings.Cut(storageNamespace, "://")
	if !found {
		return "", strings.TrimRight(storageNamespace, "/")
	}
	return scheme, strings.TrimRight(path, "/")
}

// namespacesOverlap reports whether two storage namespaces overlap: they are equal, one is inside
// the other, or one is the root of the scheme.  The comparison is on path segments, so "x" and
// "x-metadata" do not overlap and "x/y" does.  A different storage ID never conflicts, even when
// the path is the same (design §4.2 step 2, R10).
func namespacesOverlap(storageID, storageNamespace, otherStorageID, otherStorageNamespace string) bool {
	if storageID != otherStorageID {
		return false
	}
	scheme, path := splitNamespace(storageNamespace)
	otherScheme, otherPath := splitNamespace(otherStorageNamespace)
	if scheme != otherScheme {
		return false
	}
	// A root namespace contains, and is contained by, everything under its scheme.
	if path == "" || otherPath == "" {
		return true
	}
	return path == otherPath ||
		strings.HasPrefix(path+"/", otherPath+"/") ||
		strings.HasPrefix(otherPath+"/", path+"/")
}

// FindNamespaceConflict scans every repository record, in every state including IN_DELETION, and
// every repository deletion record that is not DONE, for a namespace that overlaps one of the
// targets.  Targets of the same request are excluded from each other's check (design §4.2 step 2).
func (c *Catalog) FindNamespaceConflict(ctx context.Context, targets []RepositoryDeletionTarget) (*NamespaceConflict, error) {
	if len(targets) == 0 {
		return nil, nil
	}
	excluded := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		excluded[target.Repository] = struct{}{}
	}

	conflicts := func(conflict *NamespaceConflict) bool {
		if _, ok := excluded[conflict.Repository]; ok {
			return false
		}
		for _, target := range targets {
			if namespacesOverlap(target.StorageID, target.StorageNamespace, conflict.StorageID, conflict.StorageNamespace) {
				return true
			}
		}
		return false
	}

	it, err := c.Store.ListRepositories(ctx)
	if err != nil {
		return nil, fmt.Errorf("list repositories for namespace overlap: %w", err)
	}
	defer it.Close()
	for it.Next() {
		repo := it.Value()
		conflict := &NamespaceConflict{
			Repository:       repo.RepositoryID.String(),
			StorageID:        repo.StorageID.String(),
			StorageNamespace: repo.StorageNamespace.String(),
		}
		if conflicts(conflict) {
			return conflict, nil
		}
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("list repositories for namespace overlap: %w", err)
	}

	records, err := c.ListRepositoryDeletions(ctx)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if record.State == RepositoryDeletionState_DONE {
			continue
		}
		conflict := &NamespaceConflict{
			Repository:         record.Repository,
			StorageID:          record.StorageID,
			StorageNamespace:   record.StorageNamespace,
			FromDeletionRecord: true,
		}
		if conflicts(conflict) {
			return conflict, nil
		}
	}
	return nil, nil
}

// CheckRepositoryDeletionConflict refuses creating a repository while a storage deletion record
// for the same name, or one with an overlapping namespace, is pending (R16).  It is called from
// Catalog.CreateRepository and Catalog.CreateBareRepository before graveler.
func (c *Catalog) CheckRepositoryDeletionConflict(ctx context.Context, repository, storageID, storageNamespace string) error {
	records, err := c.ListRepositoryDeletions(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.State == RepositoryDeletionState_DONE {
			continue
		}
		if record.Repository == repository {
			return fmt.Errorf("repository %s: %w", repository, ErrDeletionInProgress)
		}
		if namespacesOverlap(storageID, storageNamespace, record.StorageID, record.StorageNamespace) {
			return fmt.Errorf("storage namespace %s overlaps the pending storage deletion of repository %s: %w",
				storageNamespace, record.Repository, ErrDeletionInProgress)
		}
	}
	return nil
}
