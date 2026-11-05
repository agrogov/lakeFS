// Package refmap maps a ref of a data repository to a ref of its Metadata Search companion
// repository (METADATA_SEARCH_DESIGN.md §6, milestone M5a).
//
// Metadata Search mirrors every branch of a data repository R into R-metadata and records each
// indexed commit of R as a tag `mds-<full commit ID>` of R-metadata (§3.3, §4.3). A branch of R is
// therefore served by the branch of the same name in R-metadata, which the catalog's built-in
// resolver does on its own; every other ref - a tag, a full or short commit ID, a ref expression
// such as `main~1` - has to be translated into the tag that holds its snapshot. That translation
// is this package: the icebergcatalog.RefMapper the server installs through
// icebergcatalog.MetadataSearchRefMapper (cmd/lakefs-sso/main.go).
//
// The mapper is deliberately strict: it answers only with the snapshot of the commit the ref names,
// and answers NoSuchTableException when the index has no snapshot for it. Falling back to an
// ancestor tag would serve rows of a different commit under the name the caller asked for, which is
// worse than the 404 - the caller would have no way to notice.
//
// Clean-room note: upstream Metadata Search is a lakeFS Team/Enterprise feature. This is an
// independent OSS implementation written only from the public guide
// (https://docs.lakefs.io/guides/metadata-search/), the Apache Iceberg specs and this
// repository's code.
package refmap

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	logging "github.com/treeverse/lakefs/pkg/logging"
)

// metadataTagPrefix is the prefix of the tag that records one indexed commit of a data repository
// in its companion repository (§3.3: tag `mds-<full commit ID of R>` on the commit that recorded
// it; §4.3 step 5 creates it).
const metadataTagPrefix = "mds-"

// refResolver is the lakeFS surface the mapper reads: the catalog's commit resolution - tags, full
// commit IDs, short commit IDs and ref expressions such as `main~1` all resolve to a commit - and
// the tag lookup that finds the snapshot the indexer recorded for that commit (§6).
//
// *catalog.Catalog implements both methods. The interface is declared here, and not in
// pkg/icebergcatalog, because the ref mapper is the only caller and the catalog side must not
// depend on the indexer's needs (§6).
type refResolver interface {
	GetCommit(ctx context.Context, repositoryID, reference string) (*catalog.CommitLog, error)
	GetTag(ctx context.Context, repositoryID, tagID string) (string, error)
}

// The server's catalog is the only implementation of refResolver, and New type-asserts it at
// startup: without this assertion a change to GetCommit or GetTag of *catalog.Catalog would make
// the assertion fail silently, New would return nil, and every tag and commit read of
// R.<ref>.system.object_metadata would answer NoSuchTableException with one Warn line as the only
// signal (§6, MDS_REVIEW.md TB-5). With it, the build breaks instead.
var _ refResolver = (*catalog.Catalog)(nil)

// mapper is the RefMapper of §6. It holds the resolved refResolver, never the BranchLister it was
// built from: New refuses a lister that cannot answer the two questions the mapping needs.
type mapper struct {
	refs refResolver
}

// logOnce keeps the "not supported" warning to one line per process: New runs once per server
// start, but tests and a re-registered catalog may call it again, and the condition cannot change
// between calls.
var logOnce sync.Once

// New returns the icebergcatalog.RefMapper of §6 for the given branch lister: the lakeFS catalog
// the built-in resolver was configured with.
//
// branches must implement GetCommit and GetTag - *catalog.Catalog does - because the mapping needs
// the commit a ref names and the tag that records it. A lister that does not is not a
// misconfiguration that can be repaired at request time: the alternative to a mapping is the
// documented default, in which a tag or a commit answers NoSuchTableException (§5.1). So New
// returns nil (no mapper: the default behaviour is kept) and logs that once, rather than failing
// server startup over a feature that is disabled unless iceberg.metadata_search.enabled is set.
func New(branches icebergcatalog.BranchLister) icebergcatalog.RefMapper {
	refs, ok := branches.(refResolver)
	if !ok {
		logUnsupported(branches)
		return nil
	}
	return &mapper{refs: refs}
}

// MapRef implements icebergcatalog.RefMapper: it maps ref of dataRepo - a tag, a full or short
// commit ID, or a ref expression - to the tag `mds-<commit ID>` of dataRepo-metadata that holds the
// snapshot the indexer recorded for that commit (§6).
//
// The two steps:
//
//  1. GetCommit(dataRepo, ref) resolves the ref to a commit the way lakeFS does everywhere else:
//     tag, full commit ID, short commit ID (a prefix) and ref expressions (`main~2`, `main^`) all
//     name a commit. A ref that names none is answered NoSuchTableException - the table of that ref
//     does not exist.
//  2. GetTag(dataRepo-metadata, "mds-"+commit.Reference) asks the companion repository for the tag
//     the indexer created when it indexed that commit. Found, the tag name is the answer: reads of
//     it are read-only in the catalog, which is right for a snapshot.
//
// A commit the index has not recorded - not indexed yet, not on the first-parent chain of an
// indexed branch, or its tag was retired by tag retention (§4.9) - is answered
// NoSuchTableException. The mapper never walks to an ancestor's tag: an ancestor's table holds the
// rows of a different commit, so answering with it would return wrong data under the requested ref.
//
// Any other error is returned as it is: a repository in deletion, a storage failure or a server
// fault is not "this ref has no table", and the catalog answers it accordingly (500).
func (m *mapper) MapRef(ctx context.Context, dataRepo, ref string) (string, error) {
	commit, err := m.refs.GetCommit(ctx, dataRepo, ref)
	if err != nil {
		if errors.Is(err, graveler.ErrNotFound) {
			return "", noSuchTable(dataRepo, ref)
		}
		return "", err
	}
	if commit == nil {
		// A nil commit with no error violates GetCommit's contract; treating it as an unknown ref
		// keeps the request a 404 instead of a panic in the catalog.
		return "", noSuchTable(dataRepo, ref)
	}

	metadataRepo := icebergcatalog.MetadataRepository(dataRepo)
	tag := metadataTag(commit.Reference)
	if _, err := m.refs.GetTag(ctx, metadataRepo, tag); err != nil {
		if errors.Is(err, graveler.ErrNotFound) {
			// The companion repository, or the tag in it, is not there: this commit was not
			// indexed, and no ancestor stands in for it.
			return "", noSuchTable(dataRepo, ref)
		}
		return "", err
	}
	return tag, nil
}

// metadataTag is the name of the tag that records the snapshot of one commit of a data repository
// (§3.3, §4.3 step 5).
func metadataTag(commitID string) string { return metadataTagPrefix + commitID }

// noSuchTable is the refusal of a ref the index has nothing for (§6). The address it reports is
// the virtual table the mapper was asked about - R.<ref>.system.object_metadata - because that is
// the table the client asked for and the 404 describes.
func noSuchTable(dataRepo, ref string) *icebergcatalog.NoSuchTableException {
	return &icebergcatalog.NoSuchTableException{
		Repo:      dataRepo,
		Ref:       ref,
		Namespace: icebergcatalog.Namespace{icebergcatalog.SystemNamespace},
		Table:     icebergcatalog.ObjectMetadataTable,
	}
}

// logUnsupported reports, once, that the branch lister cannot map refs and that tags and commits
// therefore answer NoSuchTableException (§5.1). The type is included because the only likely cause
// is a wiring mistake - a catalog implementation that is not *catalog.Catalog.
func logUnsupported(branches icebergcatalog.BranchLister) {
	logOnce.Do(func() {
		logging.ContextUnavailable().
			WithField("branch_lister", fmt.Sprintf("%T", branches)).
			Warn("Metadata Search ref mapping is disabled: the branch lister does not implement GetCommit and GetTag, " +
				"so tags and commits answer NoSuchTableException")
	})
}
