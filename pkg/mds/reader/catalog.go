package reader

// catalog.go is the production implementation of the seam: the lakeFS operations of the indexer
// served by an in-process *catalog.Catalog, over the same KV store and block adapter the lakeFS
// server reads (METADATA_SEARCH_REALIGN.md RA4b). Nothing here talks HTTP.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

// CatalogAPI is the part of *catalog.Catalog this package uses. It is an interface so a test can
// drive the mapping without a catalog, and so the list of catalog calls the indexer makes stays
// explicit in one place.
type CatalogAPI interface {
	GetRepository(ctx context.Context, repository string) (*catalog.Repository, error)
	CreateRepository(ctx context.Context, repository, storageID, storageNamespace, branch string, readOnly bool) (*catalog.Repository, error)

	ListBranches(ctx context.Context, repositoryID, prefix string, limit int, after string, opts ...graveler.ListOptionsFunc) ([]*catalog.Branch, bool, error)
	CreateBranch(ctx context.Context, repositoryID, branch, sourceBranch string, opts ...graveler.SetOptionsFunc) (*catalog.CommitLog, error)
	DeleteBranch(ctx context.Context, repositoryID, branch string, opts ...graveler.SetOptionsFunc) error
	ResetBranch(ctx context.Context, repositoryID, branch string, opts ...graveler.SetOptionsFunc) error
	HardResetBranch(ctx context.Context, repositoryID, branch, refExpr string, opts ...graveler.SetOptionsFunc) error
	FindMergeBase(ctx context.Context, repositoryID, destinationRef, sourceRef string) (string, string, string, error)

	ListCommits(ctx context.Context, repositoryID, ref string, params catalog.LogParams) ([]*catalog.CommitLog, bool, error)
	GetCommit(ctx context.Context, repositoryID, reference string) (*catalog.CommitLog, error)
	Commit(ctx context.Context, repositoryID, branch, message, committer string, metadata catalog.Metadata, date *int64, sourceMetarange *string, allowEmpty bool, opts ...graveler.SetOptionsFunc) (*catalog.CommitLog, error)

	ListTags(ctx context.Context, repositoryID, prefix string, limit int, after string) ([]*catalog.Tag, bool, error)
	CreateTag(ctx context.Context, repositoryID, tagID, ref string, opts ...graveler.SetOptionsFunc) (string, error)
	GetTag(ctx context.Context, repositoryID, tagID string) (string, error)
	DeleteTag(ctx context.Context, repositoryID, tagID string, opts ...graveler.SetOptionsFunc) error

	ListEntries(ctx context.Context, repositoryID, reference, prefix, after, delimiter string, limit int) ([]*catalog.DBEntry, bool, error)
	Diff(ctx context.Context, repositoryID, leftReference, rightReference string, params catalog.DiffParams) (catalog.Differences, bool, error)
	DiffUncommitted(ctx context.Context, repositoryID, branch, prefix, delimiter string, limit int, after string) (catalog.Differences, bool, error)
	GetEntry(ctx context.Context, repositoryID, reference, path string, params catalog.GetEntryParams) (*catalog.DBEntry, error)
}

// Catalog serves the indexer's lakeFS operations from an in-process catalog. It is stateless: the
// catalog owns the KV store and the block adapter, and every call resolves the repository it
// addresses.
type Catalog struct {
	catalog  CatalogAPI
	pageSize int
}

var _ LakeFS = (*Catalog)(nil)

// New returns a Catalog over an in-process *catalog.Catalog. A non-positive pageSize means
// DefaultPageSize.
//
// The indexer commits to the companion repository as the Metadata Search principal, which is the
// identity pkg/icebergcatalog's ownership proof checks (§8): the in-process writer names the same
// committer the REST API used to take from the authenticated user.
func New(cat CatalogAPI, pageSize int) *Catalog {
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}
	return &Catalog{catalog: cat, pageSize: pageSize}
}

// GetRepository implements RepositoryReader.
func (c *Catalog) GetRepository(ctx context.Context, repository string) (Repository, error) {
	repo, err := c.catalog.GetRepository(ctx, repository)
	if err != nil {
		return Repository{}, mapError("get repository", repository, err)
	}
	return repositoryFromCatalog(repo), nil
}

// CreateRepository implements RepositoryCreator.
//
// The REST endpoint answers 409 for a name that already exists before it creates anything
// (pkg/api/controller.go CreateRepository), and the catalog itself would overwrite. The check here
// keeps the indexer's create-then-read recovery working (§4.8) instead of replacing a companion
// repository by accident.
func (c *Catalog) CreateRepository(ctx context.Context, opts RepositoryOptions) (Repository, error) {
	_, err := c.catalog.GetRepository(ctx, opts.Name)
	switch {
	case err == nil:
		return Repository{}, fmt.Errorf("create repository %q: %w", opts.Name, ErrConflict)
	case !errors.Is(err, graveler.ErrNotFound):
		return Repository{}, mapError("get repository", opts.Name, err)
	}

	branch := opts.DefaultBranch
	if branch == "" {
		branch = "main"
	}
	created, err := c.catalog.CreateRepository(ctx, opts.Name, opts.StorageID, opts.StorageNamespace, branch, opts.ReadOnly)
	if err != nil {
		return Repository{}, mapError("create repository", opts.Name, err)
	}
	return repositoryFromCatalog(created), nil
}

// ListBranches implements BranchLister. The cursor of the next page is the Name of the last
// returned branch.
func (c *Catalog) ListBranches(ctx context.Context, repository, after string) ([]Branch, bool, error) {
	branches, hasMore, err := c.catalog.ListBranches(ctx, repository, "", c.pageSize, after)
	if err != nil {
		return nil, false, mapError("list branches", repository, err)
	}
	out := make([]Branch, 0, len(branches))
	for _, branch := range branches {
		out = append(out, Branch{Name: branch.Name, CommitID: branch.Reference})
	}
	return out, hasMore, nil
}

// CreateBranch implements BranchWriter. §4.5 creates a metadata branch from a tag.
func (c *Catalog) CreateBranch(ctx context.Context, repository, branch, source string) error {
	if _, err := c.catalog.CreateBranch(ctx, repository, branch, source); err != nil {
		return mapError("create branch", branch, err)
	}
	return nil
}

// DeleteBranch implements BranchWriter. §4.8 deletes the metadata branch of a deleted data
// branch; its tags stay, so commit and tag reads of the old history still work.
func (c *Catalog) DeleteBranch(ctx context.Context, repository, branch string) error {
	return mapError("delete branch", branch, c.catalog.DeleteBranch(ctx, repository, branch))
}

// ResetBranch implements BranchWriter: it discards every uncommitted change of a branch and
// is a no-op on a clean branch. §4.4 recovery uses it when a crashed Iceberg commit left
// staging behind that does not describe the next commit.
func (c *Catalog) ResetBranch(ctx context.Context, repository, branch string) error {
	return mapError("reset branch", branch, c.catalog.ResetBranch(ctx, repository, branch))
}

// HardResetBranch implements BranchWriter: it moves a branch to ref, dropping the commits
// after it. §4.7 uses it to move a metadata branch to the tag of a merge base.
func (c *Catalog) HardResetBranch(ctx context.Context, repository, branch, ref string) error {
	return mapError("hard reset branch", branch, c.catalog.HardResetBranch(ctx, repository, branch, ref))
}

// FindMergeBase implements MergeBaseFinder and returns the base commit ID. §4.2 step 4 tests
// whether the indexed position is still an ancestor of the head: findMergeBase(last, head)
// answers last exactly when it is.
//
// A force-moved branch can have no common ancestor with its old position. graveler reports that as
// ErrNoMergeBase, and §4.7 has a fallback for that state, so it is reported as the typed
// ErrNoMergeBase rather than a generic failure.
func (c *Catalog) FindMergeBase(ctx context.Context, repository, sourceRef, destinationRef string) (string, error) {
	_, _, base, err := c.catalog.FindMergeBase(ctx, repository, destinationRef, sourceRef)
	if err != nil {
		return "", mapError("find merge base", repository+"@"+sourceRef+".."+destinationRef, err)
	}
	return base, nil
}

// LogCommits implements CommitReader. §4.2 step 5 asks for at most max_commits first-parent
// commits after the indexed position, then reverses the pages. The cursor of the next page is
// the ID of the last returned commit.
func (c *Catalog) LogCommits(ctx context.Context, repository, ref string, opts LogCommitsOptions) ([]Commit, bool, error) {
	commits, hasMore, err := c.catalog.ListCommits(ctx, repository, ref, catalog.LogParams{
		Amount:        c.pageSize,
		FirstParent:   opts.FirstParent,
		StopAt:        opts.StopAt,
		FromReference: opts.After,
	})
	if err != nil {
		return nil, false, mapError("log commits", ref, err)
	}
	out := make([]Commit, 0, len(commits))
	for _, commit := range commits {
		out = append(out, commitFromCatalog(commit))
	}
	return out, hasMore, nil
}

// GetCommit implements CommitReader. The reference may be a commit ID, a branch, a tag or a
// ref expression: §4.4 reads the head commit of a metadata branch this way.
func (c *Catalog) GetCommit(ctx context.Context, repository, commitID string) (Commit, error) {
	commit, err := c.catalog.GetCommit(ctx, repository, commitID)
	if err != nil {
		return Commit{}, mapError("get commit", commitID, err)
	}
	return commitFromCatalog(commit), nil
}

// Commit implements CommitWriter. §4.3 step 4 commits the staged metadata table and the
// index position together; §4.8 writes the initialisation commit the same way.
func (c *Catalog) Commit(ctx context.Context, repository, branch string, opts CommitOptions) (Commit, error) {
	commit, err := c.catalog.Commit(ctx, repository, branch, opts.Message, table.ServiceUserName,
		catalog.Metadata(opts.Metadata), nil, nil, opts.AllowEmpty)
	if err != nil {
		return Commit{}, mapError("commit", branch, err)
	}
	return commitFromCatalog(commit), nil
}

// ListTags implements Tags. §4.9 pages the "mds-*" tags to apply tag retention. The cursor
// of the next page is the Name of the last returned tag.
func (c *Catalog) ListTags(ctx context.Context, repository, after string) ([]Tag, bool, error) {
	tags, hasMore, err := c.catalog.ListTags(ctx, repository, "", c.pageSize, after)
	if err != nil {
		return nil, false, mapError("list tags", repository, err)
	}
	out := make([]Tag, 0, len(tags))
	for _, tag := range tags {
		out = append(out, Tag{Name: tag.ID, CommitID: tag.CommitID})
	}
	return out, hasMore, nil
}

// CreateTag implements Tags. force only lets the tag be created in a read-only repository: lakeFS
// never overwrites an existing tag (pkg/graveler/graveler.go CreateTag returns
// ErrTagAlreadyExists, a conflict), so a caller that moves a tag deletes it first (§4.4 step 5).
func (c *Catalog) CreateTag(ctx context.Context, repository, tag, ref string, force bool) (Tag, error) {
	commitID, err := c.catalog.CreateTag(ctx, repository, tag, ref, graveler.WithForce(force))
	if err != nil {
		return Tag{}, mapError("create tag", tag, err)
	}
	return Tag{Name: tag, CommitID: commitID}, nil
}

// GetTag implements Tags. §4.5 uses a not-found answer (errors.Is(err, ErrNotFound)) as "this
// commit is not indexed", which is a normal answer, not a failure.
func (c *Catalog) GetTag(ctx context.Context, repository, tag string) (Tag, error) {
	commitID, err := c.catalog.GetTag(ctx, repository, tag)
	if err != nil {
		return Tag{}, mapError("get tag", tag, err)
	}
	return Tag{Name: tag, CommitID: commitID}, nil
}

// DeleteTag implements Tags. §4.9 retires an "mds-*" tag whose source commit is older than
// tag_retention, which is what lets lakeFS GC reclaim its data.
func (c *Catalog) DeleteTag(ctx context.Context, repository, tag string) error {
	return mapError("delete tag", tag, c.catalog.DeleteTag(ctx, repository, tag))
}

// DiffRefs implements ObjectLister. It returns the two-dot diff between the two references, with
// the right-side stat of every entry that has one (§4.3 step 1). The cursor of the next page is
// the Path of the last returned change.
func (c *Catalog) DiffRefs(ctx context.Context, repository, leftRef, rightRef, after string) ([]Change, bool, error) {
	diff, hasMore, err := c.catalog.Diff(ctx, repository, leftRef, rightRef, catalog.DiffParams{
		Limit: c.pageSize,
		After: after,
	})
	if err != nil {
		return nil, false, mapError("diff refs", repository+"@"+leftRef+".."+rightRef, err)
	}
	return changesFromCatalog(diff), hasMore, nil
}

// DiffBranch implements ObjectLister: it pages through the uncommitted changes of a branch.
// §4.4 uses the first page as the authoritative answer to "is anything staged on R-metadata@b?",
// which the table's lakefs.mds.source-commit property alone cannot answer. The cursor of the next
// page is the Path of the last returned change.
func (c *Catalog) DiffBranch(ctx context.Context, repository, branch, after string) ([]Change, bool, error) {
	diff, hasMore, err := c.catalog.DiffUncommitted(ctx, repository, branch, "", "", c.pageSize, after)
	if err != nil {
		return nil, false, mapError("diff branch", repository+"@"+branch, err)
	}
	return changesFromCatalog(diff), hasMore, nil
}

// ListObjects implements ObjectLister. Every row of §3.1 carries the object's user metadata, so
// the entry's metadata map is part of the answer. The cursor of the next page is the Path of the
// last returned object.
func (c *Catalog) ListObjects(ctx context.Context, repository, ref, after string) ([]ObjectStat, bool, error) {
	entries, hasMore, err := c.catalog.ListEntries(ctx, repository, ref, "", after, "", c.pageSize)
	if err != nil {
		return nil, false, mapError("list objects", ref, err)
	}
	out := make([]ObjectStat, 0, len(entries))
	for _, entry := range entries {
		out = append(out, objectStatFromEntry(entry))
	}
	return out, hasMore, nil
}

// StatObject implements ObjectStatter. §4.3 step 1 falls back to it when a right-side diff
// stat is missing a field.
func (c *Catalog) StatObject(ctx context.Context, repository, ref, path string) (ObjectStat, error) {
	entry, err := c.catalog.GetEntry(ctx, repository, ref, path, catalog.GetEntryParams{})
	if err != nil {
		return ObjectStat{}, mapError("stat object", path, err)
	}
	return objectStatFromEntry(entry), nil
}

// repositoryFromCatalog narrows one catalog repository. The trailing "/" is removed so that
// §3.3 can append "-metadata" to the namespace.
func repositoryFromCatalog(repository *catalog.Repository) Repository {
	return Repository{
		Name:             repository.Name,
		StorageNamespace: strings.TrimRight(repository.StorageNamespace, "/"),
		DefaultBranch:    repository.DefaultBranch,
		StorageID:        repository.StorageID,
		ReadOnly:         repository.ReadOnly,
	}
}

// commitFromCatalog narrows one catalog commit log.
func commitFromCatalog(commit *catalog.CommitLog) Commit {
	return Commit{
		ID:           commit.Reference,
		Message:      commit.Message,
		Parents:      append([]string(nil), commit.Parents...),
		Committer:    commit.Committer,
		CreationDate: commit.CreationDate,
		Metadata:     commitMetadata(commit.Metadata),
	}
}

// commitMetadata copies a commit metadata map, returning nil when there is none.
func commitMetadata(metadata catalog.Metadata) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		out[key] = value
	}
	return out
}

// objectStatFromEntry narrows one catalog entry.
func objectStatFromEntry(entry *catalog.DBEntry) ObjectStat {
	return ObjectStat{
		Path:         entry.Path,
		SizeBytes:    entry.Size,
		Mtime:        timeFromCatalog(entry.CreationDate),
		ETag:         entry.Checksum,
		ContentType:  entry.ContentType,
		UserMetadata: entryMetadata(entry.Metadata),
	}
}

// changesFromCatalog narrows a diff. The right-side stat is present for an added or changed
// entry and absent for a removed one, exactly as §4.3 step 1 expects.
func changesFromCatalog(diff catalog.Differences) []Change {
	out := make([]Change, 0, len(diff))
	for i := range diff {
		change := Change{
			Path: diff[i].Path,
			Type: ChangeType(changeTypeName(diff[i].Type)),
		}
		if diff[i].Type == catalog.DifferenceTypeAdded || diff[i].Type == catalog.DifferenceTypeChanged {
			stat := objectStatFromEntry(&diff[i].DBEntry)
			change.SizeBytes = stat.SizeBytes
			change.Right = &stat
		}
		out = append(out, change)
	}
	return out
}

// changeTypeName spells a catalog diff type the way the lakeFS API does
// (pkg/api/transform.go), so that the indexer sees the same values it saw through the REST API.
func changeTypeName(typ catalog.DifferenceType) string {
	switch typ {
	case catalog.DifferenceTypeAdded:
		return string(ChangeAdded)
	case catalog.DifferenceTypeRemoved:
		return string(ChangeRemoved)
	case catalog.DifferenceTypeChanged:
		return string(ChangeChanged)
	case catalog.DifferenceTypeConflict:
		return "conflict"
	case catalog.DifferenceTypePrefixChanged:
		return "prefix_changed"
	default:
		return ""
	}
}

// entryMetadata copies an entry's metadata map, returning nil when there is none.
func entryMetadata(metadata catalog.Metadata) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		out[key] = value
	}
	return out
}

// timeFromCatalog normalizes a catalog time. A missing value is the zero time, which the table
// writer converts to NULL.
func timeFromCatalog(t time.Time) time.Time {
	if t.IsZero() {
		return time.Time{}
	}
	return t.UTC()
}

// mapError turns a catalog error into this package's typed errors. The mapping is the one
// pkg/api/controller.go applies to the same errors, so the indexer's reactions (recovery,
// rebuild, retry) are the ones it took against the REST API.
func mapError(operation, subject string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, graveler.ErrNotFound), errors.Is(err, kv.ErrNotFound):
		return &NotFoundError{Operation: operation, Subject: subject}
	case errors.Is(err, graveler.ErrNoMergeBase):
		return fmt.Errorf("%s %q: %w: %w", operation, subject, ErrNoMergeBase, err)
	case errors.Is(err, graveler.ErrProtectedBranch),
		errors.Is(err, graveler.ErrReadOnlyRepository),
		errors.Is(err, graveler.ErrDeleteDefaultBranch),
		errors.Is(err, block.ErrForbidden):
		return fmt.Errorf("%s %q: %w: %w", operation, subject, ErrForbidden, err)
	case errors.Is(err, graveler.ErrNotUnique), errors.Is(err, graveler.ErrConflictFound):
		return fmt.Errorf("%s %q: %w: %w", operation, subject, ErrConflict, err)
	case errors.Is(err, graveler.ErrNoChanges):
		return fmt.Errorf("%s %q: %w: %w", operation, subject, ErrBadRequest, err)
	default:
		return fmt.Errorf("%s %q: %w", operation, subject, err)
	}
}
