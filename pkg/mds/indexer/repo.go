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

// automaticCreationCommitMessage is the message lakeFS gives the root commit of a non-bare
// repository (pkg/graveler.FirstCommitMsg). A companion repository is created non-bare, so its
// default branch starts with this commit and the initialisation commit is the first commit after
// it. The indexer is a REST client and does not import pkg/graveler, so the string is spelled here;
// a test pins it to the real constant.
const automaticCreationCommitMessage = "Repository created"

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

	// §3.3's namespace rule, checked on every tick and not only when R-metadata is created. Both
	// repository reads above happen anyway, so the check costs no API call, and it is what catches
	// an existing companion repository whose namespace overlaps R's: one an earlier version or an
	// operator created, one a concurrent replica created with a different namespace, or one whose
	// data repository has since moved its namespace on top of it. R's uncommitted GC sweeps every
	// unreferenced object under R's prefix (pkg/catalog/gc_write_uncommitted.go) and would delete
	// the table's files.
	if err := verifyMetadataNamespace(repository.Name, metadata, data.StorageNamespace, meta.StorageNamespace); err != nil {
		return "", err
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
		state, err = x.verifyInitCommit(ctx, repository, metadata, branch)
	}
	if err != nil {
		return err
	}
	x.setMetaInfo(repository, state)
	return nil
}

// verifyInitCommit walks the first-parent log of R-metadata and decides whether the repository is
// the indexer's (§4.8). The initialisation commit is accepted in exactly two places:
//
//   - at the root, for a companion repository that was created bare (the original initialisation);
//   - as the first commit after lakeFS's automatic "Repository created" root, which is what a
//     non-bare CreateRepository writes.
//
// A repository whose only commit is that automatic root has not been initialised yet: the
// namespace, the table and the mds: init commit are written on top of it. Any other history
// without the marker at one of those two positions is not the indexer's and is refused.
func (x *Indexer) verifyInitCommit(ctx context.Context, repository, metadata, branch string) (metadataState, error) {
	history, err := x.firstParentHistory(ctx, metadata, branch)
	if err != nil {
		return metadataState{}, err
	}
	if len(history) == 0 {
		return metadataState{}, fmt.Errorf("%w: %q@%s", ErrNoCommit, metadata, branch)
	}
	root := history[len(history)-1]
	for i, commit := range history {
		if commit.Metadata[MetadataKeyInit] == MetadataKeyInitValue {
			if initCommitAccepted(history, i, root) {
				return metadataState{defaultBranch: branch, initCommit: commit.ID, initialised: true}, nil
			}
			return metadataState{}, fmt.Errorf("%w: %q carries %s on a commit that is neither its root nor its first commit after %q",
				ErrForeignRepository, metadata, MetadataKeyInit, automaticCreationCommitMessage)
		}
	}
	if len(history) == 1 && isAutomaticCreationCommit(root) {
		x.log.WithFields(logging.Fields{
			"repository": repository,
			"metadata":   metadata,
			"root":       root.ID,
		}).Info("mds: completing the initialisation of a newly created metadata repository")
		return x.initialise(ctx, repository, metadata, branch)
	}
	return metadataState{}, fmt.Errorf("%w: %q has no %s commit",
		ErrForeignRepository, metadata, MetadataKeyInit)
}

// initCommitAccepted reports whether the init marker at index i of a newest-first first-parent
// history is in one of the two accepted positions of §4.8.
func initCommitAccepted(history []client.Commit, i int, root client.Commit) bool {
	if i == len(history)-1 {
		// The marker is the root: a bare companion repository, the original form.
		return true
	}
	// The marker is the first commit after lakeFS's automatic root of a non-bare repository.
	return i == len(history)-2 && isAutomaticCreationCommit(root)
}

// isAutomaticCreationCommit reports whether commit is lakeFS's automatic root commit of a non-bare
// repository.
func isAutomaticCreationCommit(commit client.Commit) bool {
	return len(commit.Parents) == 0 && commit.Message == automaticCreationCommitMessage
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

// firstParentHistory returns the first-parent history of a branch, newest first.
func (x *Indexer) firstParentHistory(ctx context.Context, repository, branch string) ([]client.Commit, error) {
	var commits []client.Commit
	after := ""
	for {
		page, hasMore, err := x.lakeFS.LogCommits(ctx, repository, branch, client.LogCommitsOptions{
			FirstParent: true,
			After:       after,
		})
		if err != nil {
			return nil, fmt.Errorf("log commits of %q@%s: %w", repository, branch, err)
		}
		commits = append(commits, page...)
		if !hasMore {
			return commits, nil
		}
		after = lastNameOf(len(page), func(i int) string { return page[i].ID })
		if after == "" {
			return nil, fmt.Errorf("%w: log commits of %q@%s: more results without a cursor",
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

// verifyMetadataNamespace refuses a companion repository whose existing storage namespace overlaps
// R's (§3.3). metadataNamespace checks the namespace the indexer is about to create; this checks the
// one an existing R-metadata already has, which the create path never sees.
func verifyMetadataNamespace(repository, metadata, dataNamespace, metaNamespace string) error {
	if !overlapsStorageNamespace(metaNamespace, dataNamespace) {
		return nil
	}
	return fmt.Errorf("%w: %q of %q and %q of %q overlap",
		ErrOverlappingNamespace, metaNamespace, metadata, dataNamespace, repository)
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
