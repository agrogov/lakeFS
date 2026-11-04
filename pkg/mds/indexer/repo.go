package indexer

// repo.go owns the companion repository of design §3.3 and §4.8: create R-metadata for a newly
// configured data repository R, derive and check its storage namespace, create the namespace
// system and the table in it, and write the initialisation commit that marks the repository as
// the indexer's.
//
// R-metadata is what makes the mirror readable through the catalog (§5), so the rules here are
// about refusing to guess: a bucket-root namespace cannot be derived (§3.3), a namespace that
// overlaps R's would be swept by R's uncommitted GC (§3.3), and a repository that an operator
// created by hand is not touched at all (§4.8).

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/client"
)

// ensureMetadataRepository makes sure R-metadata of a configured repository exists, is one this
// indexer created, and is initialised (§3.3, §4.8). It returns the name of the companion
// repository and caches its default branch and initialisation commit for the rest of the tick.
func (x *Indexer) ensureMetadataRepository(ctx context.Context, repository Repository) (string, error) {
	if icebergcatalog.IsMetadataRepository(repository.Name) {
		return "", fmt.Errorf("%w: %q ends in %q, so its companion would be %q",
			ErrDataRepositoryNamedMetadata, repository.Name, icebergcatalog.MetadataRepositorySuffix,
			icebergcatalog.MetadataRepository(repository.Name))
	}

	data, err := x.lakeFS.GetRepository(ctx, repository.Name)
	if err != nil {
		return "", fmt.Errorf("data repository %q: %w", repository.Name, err)
	}
	metadata := metadataRepository(repository.Name)

	meta, err := x.lakeFS.GetRepository(ctx, metadata)
	switch {
	case errors.Is(err, client.ErrNotFound):
		meta, err = x.createMetadataRepository(ctx, repository, data)
		if err != nil {
			return "", err
		}
	case err != nil:
		return "", fmt.Errorf("companion repository %q: %w", metadata, err)
	}

	if err := x.ensureInitialised(ctx, repository.Name, metadata, meta.DefaultBranch); err != nil {
		return "", err
	}
	return metadata, nil
}

// createMetadataRepository creates R-metadata for a configured data repository (§3.3) and returns
// it. A concurrent replica that created it first is not an error: the initialisation that follows
// then completes it.
func (x *Indexer) createMetadataRepository(ctx context.Context, repository Repository, data client.Repository) (client.Repository, error) {
	namespace, err := metadataNamespace(repository.Name, data.StorageNamespace, repository.StorageNamespace)
	if err != nil {
		return client.Repository{}, err
	}
	metadata := metadataRepository(repository.Name)
	created, err := x.lakeFS.CreateRepository(ctx, client.RepositoryOptions{
		Name:             metadata,
		StorageNamespace: namespace,
		DefaultBranch:    data.DefaultBranch,
		StorageID:        data.StorageID,
	})
	if errors.Is(err, client.ErrConflict) {
		return x.lakeFS.GetRepository(ctx, metadata)
	}
	if err != nil {
		return client.Repository{}, fmt.Errorf("create companion repository %q in %q: %w", metadata, namespace, err)
	}
	x.log.WithFields(logging.Fields{
		"repository":        repository.Name,
		"metadata":          metadata,
		"storage_namespace": namespace,
	}).Info("mds: created the metadata repository")
	return created, nil
}

// ensureInitialised makes sure R-metadata carries an initialisation commit (§4.8): it is what
// marks the repository as one the indexer created, and what §4.5 and §4.7 branch and rebuild from.
//
// The check runs once per process per repository. A repository whose default branch has no commit
// is an initialisation that was interrupted before its commit, and is completed; a repository with
// commits but no initialisation commit is not the indexer's, and is refused.
func (x *Indexer) ensureInitialised(ctx context.Context, repository, metadata, defaultBranch string) error {
	state := x.metaInfo(repository)
	if state.initialised && state.defaultBranch == defaultBranch {
		return nil
	}
	branch := defaultBranch
	if branch == "" {
		branch = state.defaultBranch
	}
	if branch == "" {
		return fmt.Errorf("%w: %q", ErrNoDefaultBranch, metadata)
	}

	_, err := x.lakeFS.GetCommit(ctx, metadata, branch)
	switch {
	case errors.Is(err, client.ErrNotFound):
		// An empty repository: R-metadata was created but its initialisation commit is missing.
		x.log.WithFields(logging.Fields{"repository": repository, "metadata": metadata}).
			Warn("mds: completing an interrupted initialisation")
		state, err = x.initialise(ctx, repository, metadata, branch)
	case err != nil:
		return fmt.Errorf("head of %q@%s: %w", metadata, branch, err)
	default:
		state, err = x.verifyInitCommit(ctx, metadata, branch)
	}
	if err != nil {
		return err
	}
	x.setMetaInfo(repository, state)
	return nil
}

// verifyInitCommit walks the first-parent log of R-metadata to its root: the initialisation commit
// is the root of the repository (§4.8), so the root's metadata is what tells a companion
// repository of this indexer from one an operator created.
func (x *Indexer) verifyInitCommit(ctx context.Context, metadata, branch string) (metadataState, error) {
	root, err := x.rootCommit(ctx, metadata, branch)
	if err != nil {
		return metadataState{}, err
	}
	if root.Metadata[MetadataKeyInit] != MetadataKeyInitValue {
		return metadataState{}, fmt.Errorf("%w: %q has no %s commit",
			ErrForeignRepository, metadata, MetadataKeyInit)
	}
	return metadataState{defaultBranch: branch, initCommit: root.ID, initialised: true}, nil
}

// initialise creates the namespace and the table of a fresh R-metadata and writes its
// initialisation commit (§4.8), returning the state of the repository.
func (x *Indexer) initialise(ctx context.Context, repository, metadata, branch string) (metadataState, error) {
	tbl, err := x.tables.CreateTable(ctx, repository, branch)
	switch {
	case errors.Is(err, ErrTableExists):
		// The table of an interrupted initialisation: only the commit is missing.
		tbl, err = x.tables.LoadTable(ctx, repository, branch)
		if err != nil {
			return metadataState{}, fmt.Errorf("load the metadata table of %q@%s: %w", metadata, branch, err)
		}
	case err != nil:
		return metadataState{}, fmt.Errorf("create the metadata table of %q@%s: %w", metadata, branch, err)
	}
	x.closeTable(tbl, metadata, branch)

	commit, err := x.lakeFS.Commit(ctx, metadata, branch, client.CommitOptions{
		Message: InitCommitMessage,
		Metadata: map[string]string{
			MetadataKeyInit:             MetadataKeyInitValue,
			MetadataKeySourceRepository: repository,
		},
		// The commit carries the table creation, so it is never empty. allow_empty keeps a
		// retried initialisation from failing when the creation was already committed by a
		// previous attempt.
		AllowEmpty: true,
	})
	if err != nil {
		return metadataState{}, fmt.Errorf("commit the initialisation of %q@%s: %w", metadata, branch, err)
	}
	x.log.WithFields(logging.Fields{
		"repository": repository,
		"metadata":   metadata,
		"commit":     commit.ID,
	}).Info("mds: initialised the metadata repository")
	return metadataState{defaultBranch: branch, initCommit: commit.ID, initialised: true}, nil
}

// initCommit returns the initialisation commit of R-metadata (§4.8): the commit that §4.5 step 2
// creates a new branch from and that §4.7 hard resets to. ensureMetadataRepository has already
// found it for the current tick.
func (x *Indexer) initCommit(repository string) (string, error) {
	state := x.metaInfo(repository)
	if state.initCommit == "" {
		return "", fmt.Errorf("%w: %q was not initialised", ErrForeignRepository, metadataRepository(repository))
	}
	return state.initCommit, nil
}

// rootCommit returns the oldest first-parent commit of a branch, stopping early at the first
// commit that carries the initialisation marker.
func (x *Indexer) rootCommit(ctx context.Context, repository, branch string) (client.Commit, error) {
	var root client.Commit
	after := ""
	for {
		commits, hasMore, err := x.lakeFS.LogCommits(ctx, repository, branch, client.LogCommitsOptions{
			FirstParent: true,
			After:       after,
		})
		if err != nil {
			return client.Commit{}, fmt.Errorf("log commits of %q@%s: %w", repository, branch, err)
		}
		for _, commit := range commits {
			if commit.Metadata[MetadataKeyInit] == MetadataKeyInitValue {
				return commit, nil
			}
			root = commit
		}
		if !hasMore {
			if root.ID == "" {
				return client.Commit{}, fmt.Errorf("%w: %q@%s", ErrNoCommit, repository, branch)
			}
			return root, nil
		}
		after = lastNameOf(len(commits), func(i int) string { return commits[i].ID })
		if after == "" {
			return client.Commit{}, fmt.Errorf("%w: log commits of %q@%s: more results without a cursor",
				client.ErrBadPagination, repository, branch)
		}
	}
}

// metadataNamespace derives the storage namespace of R-metadata (§3.3):
//
//   - an explicit metadata_search.repositories.<R>.storage_namespace is used as it is;
//   - otherwise R's namespace with "-metadata" appended, and only when R's namespace has a path
//     component: a bucket-root namespace (s3://bucket) needs the explicit value;
//   - the result must not be inside R's namespace nor contain it, because R's uncommitted GC
//     sweeps every unreferenced object under R's prefix and would delete R-metadata's files.
func metadataNamespace(repository, dataNamespace, configured string) (string, error) {
	namespace := strings.TrimRight(strings.TrimSpace(configured), "/")
	dataNamespace = strings.TrimRight(strings.TrimSpace(dataNamespace), "/")
	if namespace == "" {
		if dataNamespace == "" {
			return "", fmt.Errorf("%w: repository %q has no storage namespace", ErrBucketRootNamespace, repository)
		}
		if !namespaceHasPath(dataNamespace) {
			return "", fmt.Errorf("%w: %q of repository %q is a bucket root, set metadata_search.repositories.%s.storage_namespace",
				ErrBucketRootNamespace, dataNamespace, repository, repository)
		}
		namespace = dataNamespace + icebergcatalog.MetadataRepositorySuffix
	}
	if overlapsStorageNamespace(namespace, dataNamespace) {
		return "", fmt.Errorf("%w: %q of %q and %q of %q overlap",
			ErrOverlappingNamespace, namespace, metadataRepository(repository), dataNamespace, repository)
	}
	return namespace, nil
}

// namespaceHasPath reports whether a storage namespace has a path component after its bucket or
// host: s3://bucket has none, s3://bucket/path has "path".
func namespaceHasPath(namespace string) bool {
	rest := namespace
	if scheme := strings.Index(rest, "://"); scheme >= 0 {
		rest = rest[scheme+len("://"):]
	}
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		return false
	}
	return strings.Trim(rest[slash:], "/") != ""
}

// overlapsStorageNamespace reports whether one storage namespace contains the other, with the
// "/"-normalised prefix test the uncommitted GC uses (pkg/catalog/gc_write_uncommitted.go,
// normalizeStorageNamespace): a prefix only counts when it ends on a path boundary.
func overlapsStorageNamespace(namespace, other string) bool {
	if namespace == "" || other == "" {
		return false
	}
	normalized := normalizeNamespace(namespace)
	otherNormalized := normalizeNamespace(other)
	return strings.HasPrefix(normalized, otherNormalized) || strings.HasPrefix(otherNormalized, normalized)
}

// normalizeNamespace appends the path delimiter the GC's prefix test expects.
func normalizeNamespace(namespace string) string {
	if !strings.HasSuffix(namespace, "/") {
		return namespace + "/"
	}
	return namespace
}
