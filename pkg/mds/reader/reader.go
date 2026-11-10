// Package reader is the lakeFS surface of the Metadata Search indexer
// (METADATA_SEARCH_REALIGN.md RA4b, X13).
//
// The indexer runs on the lakeFS server binary and the server's configuration
// (`lakefs-sso mds run --config <file>`). It opens the same KV store and block adapter the server
// uses, so it reads and writes data repositories and companion repositories in process through
// pkg/catalog instead of going through the REST API. This package is that seam: the types the
// indexer exchanges with lakeFS, and the production implementation (Catalog) over a
// *catalog.Catalog.
//
// Only the metadata table itself is written over REST: pkg/mds/table writes the Parquet, Avro and
// metadata files through the lakeFS Iceberg REST catalog at <lakefs_endpoint>/iceberg/api, with
// the IO properties the process derives from its own blockstore.* configuration (X6).
//
// The interfaces are the complete list of lakeFS operations the indexer performs, which is what
// keeps its behaviour checkable by reading one file. They are split by resource so a test can fake
// only the part a case needs (pkg/mds/indexer/fake_test.go is the in-memory lakeFS of those
// tests).
package reader

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	// DefaultPageSize is the page size of every listing when New is given a non-positive page
	// size: 1000 is the server's API maximum for object listings
	// (catalog.ListEntriesLimitMax is larger, but the API and the indexer's own bounds were
	// written for 1000) and a comfortable page for branch and commit listings.
	DefaultPageSize = 1000
)

// Errors this package reports. A missing resource is always a *NotFoundError that unwraps to
// ErrNotFound; the others are sentinels so a caller can react without parsing an error string.
var (
	// ErrNotFound is unwrapped by every *NotFoundError.
	ErrNotFound = errors.New("lakeFS resource not found")

	// ErrForbidden is reported when lakeFS refuses the write: a read-only repository, a
	// protected branch, or deleting a repository's default branch. Retrying will not help.
	ErrForbidden = errors.New("lakeFS operation forbidden")

	// ErrConflict is reported for a reference that already exists (a repository, branch or tag
	// name) or a conflicting reference name. The indexer reloads and retries.
	ErrConflict = errors.New("lakeFS conflict")

	// ErrBadRequest is reported when the request is wrong for the state of the repository. For
	// the indexer the interesting case is a commit without changes when allow_empty is false
	// (§4.3 step 4).
	ErrBadRequest = errors.New("lakeFS request rejected")

	// ErrBadPagination is reported when a listing says another page exists but gives no cursor
	// to reach it. Refusing that answer is what keeps a listing from looping forever.
	ErrBadPagination = errors.New("invalid pagination response")

	// ErrNoMergeBase is reported when FindMergeBase finds no common ancestor (§4.7). A
	// force-moved branch can have an unrelated history. The indexer needs the typed error to take
	// the rewrite fallback instead of failing the branch forever.
	ErrNoMergeBase = errors.New("no merge base")
)

// NotFoundError is the typed form of "not found". errors.Is(err, ErrNotFound) matches it, and
// errors.As(err, &notFound) gives the operation and the object that was missing.
type NotFoundError struct {
	// Operation is the call that answered not found: "get commit", "get tag", ...
	Operation string

	// Subject names what was missing, in the vocabulary of the operation: a repository, a
	// branch, a commit ID, a tag, an object path, or a composite like "repo@main..feature".
	Subject string
}

// Error implements error.
func (e *NotFoundError) Error() string {
	return fmt.Sprintf("%s %q: %v", e.Operation, e.Subject, ErrNotFound)
}

// Unwrap makes errors.Is(err, ErrNotFound) true for every *NotFoundError.
func (e *NotFoundError) Unwrap() error { return ErrNotFound }

// Repository is the part of a lakeFS repository the indexer needs (§3.3): where the object
// data lives, which branch is the default, and the storage ID a companion repository inherits.
type Repository struct {
	Name string

	// StorageNamespace is the repository's namespace with the trailing "/" removed, so that
	// "-metadata" can be appended to it (§3.3) without producing a double slash.
	StorageNamespace string

	DefaultBranch string
	StorageID     string
	ReadOnly      bool
}

// RepositoryOptions is the input of CreateRepository. The zero values leave the server
// defaults: no explicit storage ID, the server's default branch name, not read-only.
type RepositoryOptions struct {
	Name             string
	StorageNamespace string
	DefaultBranch    string
	StorageID        string
	ReadOnly         bool
}

// Branch is one branch of a repository. CommitID is the branch head, which §4.2 compares
// with the indexed position to decide whether there is work to do.
type Branch struct {
	Name     string
	CommitID string
}

// Tag is one tag of a repository: a name and the commit it points at. §4.4 uses
// "mds-<commit>" tags of the metadata repository as its crash-recovery marker.
type Tag struct {
	Name     string
	CommitID string
}

// Commit is one commit of a repository (§4.2, §4.3).
type Commit struct {
	ID      string
	Message string
	Parents []string

	Committer string

	// CreationDate is the commit time. It becomes the creation_date column of every row the
	// commit writes (§3.1).
	CreationDate time.Time

	// Metadata is the commit metadata. In the metadata repository it carries
	// lakefs.mds.source-commit, which is the indexing position (§3.3).
	Metadata map[string]string
}

// CommitOptions is the input of Commit.
type CommitOptions struct {
	Message string

	// Metadata is attached to the commit. §3.3 writes lakefs.mds.source-commit and
	// lakefs.mds.source-repository here.
	Metadata map[string]string

	// AllowEmpty lets a commit contain no changes. §4.3 step 4 passes false because an
	// indexed commit always stages a table property, so an empty commit is a bug.
	AllowEmpty bool
}

// LogCommitsOptions is the input of LogCommits. The cursor of a next page is the ID of the
// last returned commit.
type LogCommitsOptions struct {
	// FirstParent follows only the first parent at a merge commit. §4.2 walks the
	// first-parent chain, so the indexer passes true.
	FirstParent bool

	// StopAt stops the walk at this reference (a commit ID in §4.2). Empty means no stop.
	StopAt string

	// After is the pagination cursor. Empty asks for the first page.
	After string
}

// ObjectStat is one object of a ref (§4.3): the fields of a row of the table, minus the
// commit-derived ones.
type ObjectStat struct {
	Path      string
	SizeBytes int64

	// Mtime is the object's modification time. It is the zero time when the repository reports
	// none, which is converted to NULL by the table writer.
	Mtime time.Time

	// ETag is lakeFS's checksum of the object content.
	ETag string

	// ContentType is the object's media type, empty when it has none.
	ContentType string

	// UserMetadata is the object's user metadata map, nil when it has none. Objects written
	// through the S3 gateway use X-Amz-Meta-<lowercased name> keys (§3.1).
	UserMetadata map[string]string
}

// ChangeType is the type of one entry of a two-dot diff. The values are the strings the lakeFS
// API returns (pkg/api/transform.go); any other value is passed through unchanged so a caller
// can reject it rather than silently lose a change.
type ChangeType string

// Change types reported by DiffRefs.
const (
	ChangeAdded   ChangeType = "added"
	ChangeChanged ChangeType = "changed"
	ChangeRemoved ChangeType = "removed"
)

// Change is one entry of the diff between a commit and its first parent (§4.3 step 1).
type Change struct {
	Path string
	Type ChangeType

	// SizeBytes is the size lakeFS reports for the entry. It is present for every object
	// entry, including a removed one.
	SizeBytes int64

	// Right holds the right-side object stats. It is nil for a removed entry and for entries
	// that carry none. Fields that are absent stay zero, which is §4.3 step 1's signal to call
	// StatObject instead.
	Right *ObjectStat
}

// RepositoryReader reads one repository.
type RepositoryReader interface {
	GetRepository(ctx context.Context, repository string) (Repository, error)
}

// RepositoryCreator creates a repository.
type RepositoryCreator interface {
	CreateRepository(ctx context.Context, opts RepositoryOptions) (Repository, error)
}

// Repositories reads and creates repositories.
type Repositories interface {
	RepositoryReader
	RepositoryCreator
}

// BranchLister lists the branches of a repository, one page at a time.
type BranchLister interface {
	ListBranches(ctx context.Context, repository, after string) (branches []Branch, hasMore bool, err error)
}

// BranchWriter changes branches of the metadata repository (§4.4, §4.7, §4.8).
type BranchWriter interface {
	CreateBranch(ctx context.Context, repository, branch, source string) error
	DeleteBranch(ctx context.Context, repository, branch string) error
	ResetBranch(ctx context.Context, repository, branch string) error
	HardResetBranch(ctx context.Context, repository, branch, ref string) error
}

// MergeBaseFinder reports the merge base of two references (§4.2 step 4, §4.7).
type MergeBaseFinder interface {
	FindMergeBase(ctx context.Context, repository, sourceRef, destinationRef string) (string, error)
}

// Branches lists and changes branches and finds merge bases.
type Branches interface {
	BranchLister
	BranchWriter
	MergeBaseFinder
}

// CommitReader reads commits and commit history.
type CommitReader interface {
	LogCommits(ctx context.Context, repository, ref string, opts LogCommitsOptions) (commits []Commit, hasMore bool, err error)
	GetCommit(ctx context.Context, repository, commitID string) (Commit, error)
}

// CommitWriter writes commits.
type CommitWriter interface {
	Commit(ctx context.Context, repository, branch string, opts CommitOptions) (Commit, error)
}

// Commits reads and writes commits.
type Commits interface {
	CommitReader
	CommitWriter
}

// Tags reads and writes tags. ListTags and DeleteTag are what the tag maintenance of §4.9 needs to
// retire "mds-*" tags; keeping them here keeps the indexer's lakeFS surface in one file.
type Tags interface {
	ListTags(ctx context.Context, repository, after string) (tags []Tag, hasMore bool, err error)
	CreateTag(ctx context.Context, repository, tag, ref string, force bool) (Tag, error)
	GetTag(ctx context.Context, repository, tag string) (Tag, error)
	DeleteTag(ctx context.Context, repository, tag string) error
}

// ObjectLister pages through objects and diffs.
type ObjectLister interface {
	ListObjects(ctx context.Context, repository, ref, after string) (objects []ObjectStat, hasMore bool, err error)
	DiffRefs(ctx context.Context, repository, leftRef, rightRef, after string) (changes []Change, hasMore bool, err error)

	// DiffBranch pages through the uncommitted changes of a branch. §4.4 recovery uses it to
	// tell an empty staging from a foreign staged write whose property happens to equal the
	// recorded position.
	DiffBranch(ctx context.Context, repository, branch, after string) (changes []Change, hasMore bool, err error)
}

// ObjectStatter reads one object.
type ObjectStatter interface {
	StatObject(ctx context.Context, repository, ref, path string) (ObjectStat, error)
}

// Objects lists objects and diffs and stats one object.
type Objects interface {
	ObjectLister
	ObjectStatter
}

// LakeFS is the complete lakeFS surface the indexer uses. The indexer takes this interface; a
// test can fake it, or fake only one of the small interfaces above.
type LakeFS interface {
	Repositories
	Branches
	Commits
	Tags
	Objects
}
