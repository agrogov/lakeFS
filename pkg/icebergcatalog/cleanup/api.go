package cleanup

// api.go declares the narrow lakeFS API this package works through, in the vocabulary of
// the cleanup job rather than in the vocabulary of the generated client. There are three
// reasons for the indirection:
//
//   - the generated client (pkg/api/apigen) speaks HTTP: every call returns a response
//     struct, and a status check is repeated at every call site. apiclient.go performs
//     that mapping once, so the phases below never see a status code;
//   - the phases are testable without a server: the fake in fake_test.go implements this
//     interface over an in-memory map of branches, entries and commits, which is what
//     lets the unit tests exercise a full expire+mark+sweep run;
//   - the interface is the complete list of permissions the tool needs, which makes the
//     documentation's permission section checkable by reading one file.
//
// Errors: an absent entry or rules object is ErrNotFound (or ErrGCRulesNotFound for the
// GC rules), and everything else is a wrapped error that names the operation. The
// package never treats "not found" as a generic failure, because "this table has no
// file entry for its metadata" is a normal, reportable condition (an external-metadata
// table) rather than an error.

import (
	"context"
	"errors"
	"time"
)

// Errors the API implementation reports for the two "absent" conditions the phases
// distinguish from failures. apiclient.go maps HTTP 404 onto them.
var (
	// ErrNotFound is reported when an entry does not exist on a ref.
	ErrNotFound = errors.New("lakeFS entry not found")

	// ErrGCRulesNotFound is reported by GetGCRules when the repository has no GC rules.
	// It is not an error of the cleanup run: lakeFS never collects commits in such a
	// repository, so every reachable commit is retained (see refs.go).
	ErrGCRulesNotFound = errors.New("lakeFS garbage collection rules are not configured")

	// ErrForbidden is reported for HTTP 403, so a caller can report "the principal is
	// not allowed to do this" without parsing the server's message.
	ErrForbidden = errors.New("lakeFS operation forbidden")

	// ErrInvalidConfig is wrapped by every configuration error New rejects. It lets a
	// caller tell "the command line is wrong" from "the repository is wrong".
	ErrInvalidConfig = errors.New("invalid cleanup configuration")

	// ErrInvalidLayout is wrapped when a path cannot be used as this job's key: a location
	// outside the storage namespace, a file outside its table's root, a path with a "..".
	ErrInvalidLayout = errors.New("invalid iceberg layout")

	// ErrInvalidProperty is wrapped when a table property this phase reads does not parse.
	ErrInvalidProperty = errors.New("invalid iceberg table property")

	// ErrInvalidCatalogState is wrapped when catalog state cannot be interpreted: a metadata
	// document that does not parse, a table UUID that is not a UUID, a rewritten document
	// that does not parse back. It is a 5xx-class condition of the repository, not a
	// missing table.
	ErrInvalidCatalogState = errors.New("invalid iceberg catalog state")

	// ErrInvalidMark is wrapped when a mark report cannot be acted on: a different schema
	// version or kind, another mark's ID, or an object list that does not parse.
	ErrInvalidMark = errors.New("invalid mark report")

	// ErrBadPagination is wrapped when a server reports another page but no cursor to reach
	// it. Refusing it is what keeps a listing from looping forever.
	ErrBadPagination = errors.New("invalid pagination response")

	// ErrUnexpectedResponse is wrapped for a status this client does not expect.
	ErrUnexpectedResponse = errors.New("unexpected lakeFS response")

	// ErrSweepIncomplete is wrapped when some marked objects could not be deleted. The
	// report still exists and names each failure.
	ErrSweepIncomplete = errors.New("sweep incomplete")
)

// Repository is the part of a lakeFS repository the cleanup job needs: where the table
// files live and which branch the reports are written to.
type Repository struct {
	Name string

	// StorageNamespace is the repository's storage namespace, without a trailing slash,
	// e.g. "s3://bucket/prefix". Every table file this job considers lives under it.
	StorageNamespace string

	// DefaultBranch is the branch the reports are written to. It is also the branch a
	// run needs write permission on for its reports, in addition to the branches whose
	// tables it changes.
	DefaultBranch string
}

// ObjectInfo is one lakeFS entry: its path on the ref, the physical object it points
// at, and the entry's metadata. It is the common shape of a listing row and of a stat
// response, which is what lets the phases read a pointer's user metadata straight from
// a listing instead of paying one StatObject per table.
type ObjectInfo struct {
	// Path is the entry path relative to the ref.
	Path string

	// PhysicalAddress is the fully qualified URI of the object the entry points at.
	PhysicalAddress string

	// Checksum is the entry's checksum, as lakeFS computed it when the object was
	// written. The expire phase passes it back when it stages the new metadata file.
	Checksum string

	// SizeBytes is the object's size, or zero when the server did not report one.
	SizeBytes int64

	// Mtime is the entry's creation time (Unix epoch, second resolution on the wire).
	Mtime time.Time

	// Metadata is the entry's user metadata (iceberg.metadata-location and friends for
	// a table pointer).
	Metadata map[string]string
}

// StagedObject is what StageObject writes: an existing physical object promoted to an
// entry on a branch, optionally with user metadata. The expire phase uses it for the
// table pointer (metadata carries the new metadata-location) and for the catalog's file
// registration of the new metadata file (no metadata).
type StagedObject struct {
	PhysicalAddress string
	Checksum        string
	SizeBytes       int64
	Mtime           time.Time
	Metadata        map[string]string
}

// Tag is a lakeFS tag and the commit it points at. The commit is retained forever, so
// the files its table pointers reference are protected.
type Tag struct {
	Name     string
	CommitID string
}

// CommitInfo is the part of a lakeFS commit the retention walk needs: its first parent
// and when it was created. The parent list is lakeFS' ordered parent list, whose first
// element is the main parent (graveler's retention walk follows exactly that edge).
type CommitInfo struct {
	ID           string
	CreationDate time.Time
	Parents      []string
}

// DiffInfo is one uncommitted change on a branch. Type is one of "added", "changed" or
// "removed" (the API also reports "conflict" and "prefix_changed", which the phases
// ignore: the first names a conflict rather than an entry, the second a directory).
type DiffInfo struct {
	Path string
	Type string
}

// GCRules is the repository's GC retention policy, reduced to what the retention walk
// uses. Branches maps a branch name to its own retention period; a branch not in the
// map uses DefaultRetentionDays (pkg/graveler/retention/active_commits.go).
type GCRules struct {
	DefaultRetentionDays int
	Branches             map[string]int
}

// API is the lakeFS client the cleanup job works through. Every method is one REST
// call (or one paginated sequence of them, which is why the list methods take a cursor
// and report whether another page exists).
type API interface {
	// GetRepository reads the repository's storage namespace and default branch.
	GetRepository(ctx context.Context, repository string) (Repository, error)

	// ListBranches returns every branch name, sorted as the server returns them.
	ListBranches(ctx context.Context, repository string) ([]string, error)

	// ListTags returns every tag with the commit it points at.
	ListTags(ctx context.Context, repository string) ([]Tag, error)

	// GetGCRules returns the repository's GC rules, or ErrGCRulesNotFound.
	GetGCRules(ctx context.Context, repository string) (GCRules, error)

	// ListCommits returns one page of the commit log of ref, newest first, following
	// first parents only (the retention walk's edge). after is the previous page's
	// last commit ID, or "" for the first page.
	ListCommits(ctx context.Context, repository, ref, after string) (commits []CommitInfo, hasMore bool, err error)

	// ListObjects returns one page of the entries visible on ref under prefix, with
	// their user metadata. after is the previous page's last path, or "".
	ListObjects(ctx context.Context, repository, ref, prefix, after string) (objects []ObjectInfo, hasMore bool, err error)

	// StatObject returns one entry, or ErrNotFound.
	StatObject(ctx context.Context, repository, ref, path string) (ObjectInfo, error)

	// GetObject returns the bytes of one entry, or ErrNotFound. lakeFS resolves the
	// entry to its physical address and reads the object, so a catalog file entry
	// (which points at a fully qualified table file) is readable without any
	// blockstore credentials on this side.
	GetObject(ctx context.Context, repository, ref, path string) ([]byte, error)

	// UploadObject writes data to path on branch and stages an entry for it. The
	// returned ObjectInfo carries the physical address, checksum and size the caller
	// needs to register the same object under a second entry.
	UploadObject(ctx context.Context, repository, branch, path string, data []byte) (ObjectInfo, error)

	// StageObject stages an existing physical object as an entry on branch, with
	// optional user metadata.
	StageObject(ctx context.Context, repository, branch, path string, object StagedObject) error

	// DeleteObject stages the removal of one entry on branch.
	DeleteObject(ctx context.Context, repository, branch, path string) error

	// DiffBranch returns one page of the uncommitted changes of branch under prefix.
	DiffBranch(ctx context.Context, repository, branch, prefix, after string) (changes []DiffInfo, hasMore bool, err error)
}
