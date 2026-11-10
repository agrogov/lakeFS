package indexer

// repo.go owns the companion repository of design §3.3 and §4.8: create R-metadata for a newly
// configured data repository R, derive and check its storage namespace, create the namespace
// system and the table in it, and write the initialisation commit that marks the repository as
// the indexer's.
//
// R-metadata is what makes the mirror readable through the catalog (§5), so the rules here are
// about refusing to guess where guessing would lose data: a namespace that overlaps R's would be
// swept by R's uncommitted GC (§3.3), and a repository that an operator created by hand is not
// touched at all (§4.8). A bucket-root or container-root namespace is not refused: "-metadata" is
// appended to it like to any other namespace (G10/X10).

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/reader"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

// automaticCreationCommitMessage is the message lakeFS gives the root commit of a non-bare
// repository (pkg/graveler.FirstCommitMsg). A companion repository is created non-bare, so its
// default branch starts with this commit and the initialisation commit is the first commit after
// it. The indexer does not import pkg/graveler, so the string is spelled here;
// a test pins it to the real constant.
const automaticCreationCommitMessage = "Repository created"

// ErrReadOnlyMetadataRepository is reported for a companion repository that lakeFS reports as
// read-only (§3.3, §4.8). A read-only repository refuses every write of a tick with 403
// (graveler.ErrReadOnlyRepository, mapped to http.StatusForbidden in pkg/api/controller.go), so the
// branch would report the same error on every tick, /_health would stay 503 and tag retention would
// never run. A branch-protection rule that blocks COMMIT on the companion's branches has the same
// effect but is not visible in the repository record; the error names both causes, and the
// repository is refused once per tick instead of failing every branch of it. A protection rule is
// detected at the first write lakeFS refuses (Commit, ResetBranch or HardResetBranch, or a catalog
// write that stages on the branch), because the API does not report the rule on the repository.
var ErrReadOnlyMetadataRepository = errors.New("the companion repository is read-only")

// protectedBranchWriteCauses are the messages with which lakeFS names the two rules that refuse a
// write to R-metadata independently of the indexer's permissions: a protection rule that blocks
// COMMIT or STAGING_WRITE on the branch (graveler.ErrCommitToProtectedBranch and
// ErrWriteToProtectedBranch, pkg/graveler/errors.go) and a repository marked read_only
// (graveler.ErrReadOnlyRepository). Only one of them is a diagnosis of branch protection; a 401/403
// that names neither is an authorization refusal, and the difference matters because the two
// failures have different remedies.
var protectedBranchWriteCauses = []string{
	"cannot commit to protected branch",
	"cannot write to protected branch",
	"read-only repository",
}

// claimsProtectedBranchWrite reports whether err carries one of lakeFS's own protection messages.
// The wrapped cause is what decides: a protection rule, a read_only repository and (before RA10) a
// principal that lacks the catalog permission all arrive as the same 403 family
// (pkg/api/controller_authorization_status_test.go: an authorization failure is 401 by default, and
// 403 when auth.authorization_failure_status opts in), and pkg/mds/reader maps the protection
// refusals into reader.ErrForbidden. Claiming protection from the status alone told an operator to
// look for a rule when the missing grant was the indexer's (R3I-1).
func claimsProtectedBranchWrite(err error) bool {
	message := err.Error()
	for _, cause := range protectedBranchWriteCauses {
		if strings.Contains(message, cause) {
			return true
		}
	}
	return false
}

// metadataBranchWriteError answers a forbidden write to R-metadata with a repository-level error.
// lakeFS refuses a write to a branch whose protection rule blocks COMMIT or STAGING_WRITE
// (graveler.ErrCommitToProtectedBranch/ErrWriteToProtectedBranch, both wrapping ErrProtectedBranch)
// and to a read-only repository (ErrReadOnlyRepository); pkg/api/controller.go maps both to
// http.StatusForbidden. The write-sign path of the catalog answers the same status for a staging
// write (pkg/icebergcatalog/tables.go), which iceberg-go reports as rest.ErrForbidden. Naming the
// cause once per refused write is what an operator can act on; leaving the raw 403 made every tick
// look like a transient failure (TS-4, R2I-3).
//
// The message claims branch protection only when lakeFS said so. Any other forbidden answer stays in
// the same family - a fresh companion repository really is not writable, and /_health must say so -
// but it names the three candidates (the branch's protection rules, the repository's read_only flag
// and the indexer principal's own permissions) without picking one, and without the destructive
// "create the repository with read_only=false" advice. The most common member of that family is the
// documented one of design §8: a companion repository the policy refresher has not covered yet, so
// the indexer's deny/grant policy is the thing to look at (R3I-1).
func metadataBranchWriteError(metadata, branch string, err error) error {
	if err == nil || errors.Is(err, ErrReadOnlyMetadataRepository) {
		return err
	}
	if !errors.Is(err, reader.ErrForbidden) && !errors.Is(err, rest.ErrForbidden) {
		return err
	}
	if claimsProtectedBranchWrite(err) {
		return fmt.Errorf("%w: lakeFS refused a write to %q@%s: a branch protection rule blocks COMMIT or STAGING_WRITE on it, or the repository is read-only (%w); remove the rule or create the repository with read_only=false",
			ErrReadOnlyMetadataRepository, metadata, branch, err)
	}
	return fmt.Errorf("%w: lakeFS refused a write to %q@%s: %w; check branch protection on the branch, the companion repository's read_only flag, and the indexer principal's own permissions",
		ErrReadOnlyMetadataRepository, metadata, branch, err)
}

// tableOwnership is the optional part of a Table handle that knows the table's
// lakefs.mds.repository property. The Table seam predates the ownership check of §4.8; a store that
// does not expose the property is left to the existing behaviour rather than refusing every
// initialisation.
type tableOwnership interface {
	Repository() string
}

// verifyTableOwnership refuses a staged table that belongs to another data repository (R2I-4).
// §4.8 refuses a companion repository the indexer did not create; the same rule has to cover the
// table it did not create, because initialise commits whatever table is staged, with that table's
// location, schema and properties.
func verifyTableOwnership(tbl Table, repository string) error {
	owner, ok := tbl.(tableOwnership)
	if !ok {
		return nil
	}
	if owner.Repository() == repository {
		return nil
	}
	return fmt.Errorf("%w: the staged table records %s=%q, not %q",
		ErrForeignRepository, table.PropertyRepository, owner.Repository(), repository)
}

// Repository implements tableOwnership for the production table handle. It lives here, next to the
// check that consumes it, so the Table interface of tables.go stays the seam M3 defined.
func (t *icebergTable) Repository() string {
	return t.table.Properties()[table.PropertyRepository]
}

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
	case errors.Is(err, reader.ErrNotFound):
		meta, err = x.createMetadataRepository(ctx, repository, data)
		if err != nil {
			return "", err
		}
	case err != nil:
		return "", fmt.Errorf("companion repository %q: %w", metadata, err)
	}

	// A companion repository that cannot be written fails every commit, reset and rewrite of §4.4
	// and §4.7 with 403. Report that as a repository-level refusal instead of the same per-branch
	// error on every tick, which no operator could tell apart from a transient failure (TS-4).
	if meta.ReadOnly {
		return "", fmt.Errorf("%w: %q: create it with read_only=false and do not protect its branches against COMMIT",
			ErrReadOnlyMetadataRepository, metadata)
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
func (x *Indexer) createMetadataRepository(ctx context.Context, repository Repository, data reader.Repository) (reader.Repository, error) {
	namespace, err := metadataNamespace(repository.Name, data.StorageNamespace, repository.StorageNamespace)
	if err != nil {
		return reader.Repository{}, err
	}
	metadata := metadataRepository(repository.Name)
	created, err := x.lakeFS.CreateRepository(ctx, reader.RepositoryOptions{
		Name:             metadata,
		StorageNamespace: namespace,
		DefaultBranch:    data.DefaultBranch,
		StorageID:        data.StorageID,
	})
	if errors.Is(err, reader.ErrConflict) {
		return x.lakeFS.GetRepository(ctx, metadata)
	}
	if err != nil {
		return reader.Repository{}, fmt.Errorf("create companion repository %q in %q: %w", metadata, namespace, err)
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
	case errors.Is(err, reader.ErrNotFound):
		// An empty repository: R-metadata was created but its initialisation commit is missing.
		x.log.WithFields(logging.Fields{"repository": repository, "metadata": metadata}).
			Warn("mds: completing an interrupted initialisation")
		state, err = x.initialise(ctx, repository, metadata, branch)
	case err != nil:
		return fmt.Errorf("head of %q@%s: %w", metadata, branch, err)
	default:
		state, err = x.verifyInitCommit(ctx, repository, metadata, branch, true)
	}
	if err != nil {
		return err
	}
	x.setMetaInfo(repository, state)
	return nil
}

// verifyInitCommit streams the first-parent log of R-metadata and decides whether the repository is
// the indexer's (§4.8). The initialisation commit is accepted in exactly two shapes:
//
//   - a contiguous run of initialisation commits that reaches the root, for a companion repository
//     that was created bare (the original initialisation);
//   - a contiguous run of initialisation commits directly above lakeFS's automatic
//     "Repository created" root, which is what a non-bare CreateRepository writes.
//
// A run and not a single commit, because a second initialisation can reach the branch before the
// first one commits (§4.1: two indexer processes may overlap, and a rolling restart is enough):
// initialise no longer commits an empty change set (IO-1), so the loser of that race commits
// nothing, but a repository an older indexer already wrote carries the duplicate mds: init commit,
// and refusing it forever would throw the mirror away over a history the indexer itself produced.
// The commit the tick records is the run's member closest to the root: the one that created the
// table, and the one §4.5 seeds branches from.
//
// A repository whose only commit is that automatic root has not been initialised yet: the
// namespace, the table and the mds: init commit are written on top of it. Any other history
// without the marker in one of those two shapes is not the indexer's and is refused.
//
// The walk keeps a fixed number of commits and stops as soon as the answer is known (TS-5): the
// alternative read every commit of R-metadata into memory on every process start, page by page,
// before the first branch could be indexed, and R-metadata gets one indexing commit per indexed
// source commit, so its history is as long as everything the repository ever indexed. The number of
// calls is the same as before for a healthy repository - the run and the root sit at the bottom of
// its history - but a refusal is decided at the commit that makes it one instead of after reading
// the whole history into memory first.
func (x *Indexer) verifyInitCommit(ctx context.Context, repository, metadata, branch string, completeInitialisation bool) (metadataState, error) {
	scan := &initScan{}
	if err := x.walkFirstParent(ctx, metadata, branch, scan.visit); err != nil {
		return metadataState{}, err
	}
	switch {
	case scan.commits == 0:
		return metadataState{}, fmt.Errorf("%w: %q@%s", ErrNoCommit, metadata, branch)
	case scan.accepted():
		return metadataState{defaultBranch: branch, initCommit: scan.initCommit.ID, initialised: true}, nil
	case scan.freshAutomaticRoot():
		if !completeInitialisation {
			// initialise calls this after its own commit was refused with a 400. A branch that still
			// shows only the automatic root has no other replica's initialisation to adopt, so the
			// 400 was not a lost race: report it instead of entering initialise again, which would
			// recurse without a bound (R2I-1).
			return metadataState{}, nil
		}
		x.log.WithFields(logging.Fields{
			"repository": repository,
			"metadata":   metadata,
			"root":       scan.first.ID,
		}).Info("mds: completing the initialisation of a newly created metadata repository")
		return x.initialise(ctx, repository, metadata, branch)
	case scan.markerSeen:
		return metadataState{}, fmt.Errorf("%w: %q carries %s on a commit that is neither its root nor part of a run of %s commits directly above %q",
			ErrForeignRepository, metadata, MetadataKeyInit, MetadataKeyInit, automaticCreationCommitMessage)
	}
	return metadataState{}, fmt.Errorf("%w: %q has no %s commit",
		ErrForeignRepository, metadata, MetadataKeyInit)
}

// initScan is the state of verifyInitCommit's single streaming pass over the first-parent log of
// R-metadata (§4.8). It holds the commits the decision needs, never the history (TS-5).
//
// The walk is newest first, so the commits above the run are indexing commits (or a foreign
// commit), and the answer is a property of the run and of the commit immediately below it.
type initScan struct {
	// commits counts the commits visited, and first is the newest of them.
	commits int
	first   reader.Commit

	// markerSeen reports that an initialisation commit was visited, and initCommit is the one
	// closest to the root visited so far, i.e. the oldest member of the run.
	markerSeen bool
	initCommit reader.Commit

	// gap is the first commit below the run that carries no marker. A run is accepted only when
	// that commit is lakeFS's automatic creation root; a marker below the gap, or a second gap,
	// ends the walk with a refusal.
	gap     reader.Commit
	gapSeen bool
	refused bool
}

// visit consumes one commit of the walk, newest first, and reports whether the walk can stop. The
// walk visits nothing below an automatic creation root: it has no parent.
func (s *initScan) visit(commit reader.Commit) (bool, error) {
	if s.commits == 0 {
		s.first = commit
	}
	s.commits++
	switch {
	case isInitCommit(commit):
		if s.gapSeen {
			// A marker below a commit without one: the run is not contiguous, so it is neither the
			// root nor a run that starts above it.
			s.refused = true
			return true, nil
		}
		s.markerSeen = true
		s.initCommit = commit
		return false, nil
	case !s.markerSeen:
		// Commits above the run, which are indexing commits (§4.3): the initialisation commit can
		// still be anywhere below. The automatic creation root has no parent, so it is the last
		// commit there is to read.
		return isAutomaticCreationCommit(commit), nil
	case s.gapSeen:
		// A second commit without the marker below the run: the run does not end at the root.
		s.refused = true
		return true, nil
	default:
		s.gap = commit
		s.gapSeen = true
		// The accepted run ends at the automatic creation root, whose parent list is empty: stop
		// here instead of reading the rest of an indexing history that may be very long (TS-5).
		return isAutomaticCreationCommit(commit), nil
	}
}

// accepted reports whether the branch is the indexer's: the newest run of initialisation commits
// reaches the root (bare) or ends directly above the automatic creation root (non-bare).
func (s *initScan) accepted() bool {
	if !s.markerSeen || s.refused {
		return false
	}
	return !s.gapSeen || isAutomaticCreationCommit(s.gap)
}

// freshAutomaticRoot reports whether the branch carries lakeFS's automatic creation root and
// nothing else: the initialisation has not happened yet and is completed now.
func (s *initScan) freshAutomaticRoot() bool {
	return !s.markerSeen && s.commits == 1 && isAutomaticCreationCommit(s.first)
}

// isInitCommit reports whether a commit carries §4.8's initialisation marker.
func isInitCommit(commit reader.Commit) bool {
	return commit.Metadata[MetadataKeyInit] == MetadataKeyInitValue
}

// isAutomaticCreationCommit reports whether commit is lakeFS's automatic root commit of a non-bare
// repository.
func isAutomaticCreationCommit(commit reader.Commit) bool {
	return len(commit.Parents) == 0 && commit.Message == automaticCreationCommitMessage
}

// initialise creates the namespace and the table of a fresh R-metadata and writes its
// initialisation commit (§4.8), returning the state of the repository.
func (x *Indexer) initialise(ctx context.Context, repository, metadata, branch string) (metadataState, error) {
	tbl, err := x.tables.CreateTable(ctx, repository, branch)
	switch {
	case errors.Is(err, ErrTableExists):
		// The table of an interrupted initialisation: only the commit is missing. The staged table
		// must still be the one this data repository owns, or the initialisation would seal a
		// foreign table (its location, schema and properties) with the mds: init marker (R2I-4).
		tbl, err = x.tables.LoadTable(ctx, repository, branch)
		if err != nil {
			return metadataState{}, fmt.Errorf("load the metadata table of %q@%s: %w", metadata, branch, err)
		}
		if err := verifyTableOwnership(tbl, repository); err != nil {
			x.closeTable(tbl, metadata, branch)
			return metadataState{}, err
		}
	case err != nil:
		// A create is a staging write on the branch, so a COMMIT or STAGING_WRITE rule present
		// before the first tick is refused here, one step before the initialisation commit. The
		// catalog answers it 403 exactly like the other table writes (pkg/icebergcatalog/tables.go
		// maps graveler.ErrWriteToProtectedBranch/ErrReadOnlyRepository to ForbiddenException) and
		// iceberg-go reports it as rest.ErrForbidden, so it belongs to the same family and is mapped
		// the same way (R3I-2).
		return metadataState{}, metadataBranchWriteError(metadata, branch,
			fmt.Errorf("create the metadata table of %q@%s: %w", metadata, branch, err))
	}
	x.closeTable(tbl, metadata, branch)

	commit, err := x.lakeFS.Commit(ctx, metadata, branch, reader.CommitOptions{
		Message: InitCommitMessage,
		Metadata: map[string]string{
			MetadataKeyInit:             MetadataKeyInitValue,
			MetadataKeySourceRepository: repository,
		},
		// §4.8 (IO-1): the commit is the idempotency point of the initialisation. It carries the
		// table creation, so it is never legitimately empty; an initialisation that finds nothing
		// to commit lost a race to a concurrent one that committed the same staging. lakeFS answers
		// that with graveler.ErrNoChanges, a 400 (pkg/api/controller.go maps it to
		// StatusBadRequest, and pkg/graveler/graveler.go:2355 tolerates it only under
		// AllowEmpty), so AllowEmpty would let the loser write a duplicate mds: init on top of the
		// winner, which the branch then carries for ever.
		AllowEmpty: false,
	})
	if errors.Is(err, reader.ErrBadRequest) {
		// A 400 is "lost the race" only when re-verification adopts another replica's init commit.
		// A branch that still shows only the automatic root has nothing to adopt, so the 400 is a
		// real refusal (block.ErrWriteFailed, a validation error, an HTTP intermediary): return it
		// instead of entering initialise again, which recursed without a bound (R2I-1).
		state, verifyErr := x.verifyInitCommit(ctx, repository, metadata, branch, false)
		if verifyErr != nil {
			return metadataState{}, fmt.Errorf("commit the initialisation of %q@%s: %w; the branch is not initialised either: %w",
				metadata, branch, err, verifyErr)
		}
		if !state.initialised {
			return metadataState{}, fmt.Errorf("commit the initialisation of %q@%s: %w", metadata, branch, err)
		}
		x.log.WithFields(logging.Fields{
			"repository": repository,
			"metadata":   metadata,
			"commit":     state.initCommit,
		}).Info("mds: a concurrent initialisation committed the metadata repository")
		return state, nil
	}
	if err != nil {
		return metadataState{}, metadataBranchWriteError(metadata, branch,
			fmt.Errorf("commit the initialisation of %q@%s: %w", metadata, branch, err))
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

// walkFirstParent calls visit for every commit of the first-parent log of a branch, newest first,
// and stops when visit asks it to. It keeps no history of its own: callers that need one would hold
// the whole repository in memory (TS-5).
func (x *Indexer) walkFirstParent(ctx context.Context, repository, branch string, visit func(reader.Commit) (bool, error)) error {
	after := ""
	for {
		page, hasMore, err := x.lakeFS.LogCommits(ctx, repository, branch, reader.LogCommitsOptions{
			FirstParent: true,
			After:       after,
		})
		if err != nil {
			return fmt.Errorf("log commits of %q@%s: %w", repository, branch, err)
		}
		for _, commit := range page {
			stop, err := visit(commit)
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
		}
		if !hasMore {
			return nil
		}
		after = lastNameOf(len(page), func(i int) string { return page[i].ID })
		if after == "" {
			return fmt.Errorf("%w: log commits of %q@%s: more results without a cursor",
				reader.ErrBadPagination, repository, branch)
		}
	}
}

// metadataNamespace derives the storage namespace of R-metadata (§3.3):
//
//   - an explicit metadata_search.repositories.<R>.storage_namespace is used as it is;
//   - otherwise "-metadata" is appended to R's own namespace, in every case, exactly as the guide
//     says (G10/X10): s3://bucket becomes s3://bucket-metadata, and an Azure container root
//     https://acct.blob.core.windows.net/data becomes https://acct.blob.core.windows.net/data-metadata.
//     There is no bucket-root or container-root exception any more;
//   - the result must not be inside R's namespace nor contain it, because R's uncommitted GC
//     sweeps every unreferenced object under R's prefix and would delete R-metadata's files.
func metadataNamespace(repository, dataNamespace, configured string) (string, error) {
	namespace := strings.TrimRight(strings.TrimSpace(configured), "/")
	dataNamespace = strings.TrimRight(strings.TrimSpace(dataNamespace), "/")
	if namespace == "" {
		if dataNamespace == "" {
			return "", fmt.Errorf("%w: repository %q", ErrNoStorageNamespace, repository)
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

// overlapsStorageNamespace reports whether one storage namespace contains the other, with the
// "/"-normalised prefix test the uncommitted GC uses (pkg/catalog/gc_write_uncommitted.go,
// normalizeStorageNamespace): a prefix only counts when it ends on a path boundary.
//
// On Azure the scheme is not part of the physical address: the block adapter accepts http and https
// for the same container and prefix (METADATA_SEARCH_AZURE D4), so an http companion namespace and
// an https data namespace over the same prefix do overlap, and the comparison drops the scheme of
// both before it tests the prefix. The namespaces themselves stay as stored; only the comparison is
// scheme-neutral, and only for http(s) - every other blockstore's scheme is part of its address.
func overlapsStorageNamespace(namespace, other string) bool {
	if namespace == "" || other == "" {
		return false
	}
	normalized := normalizeNamespace(schemeNeutralNamespace(namespace))
	otherNormalized := normalizeNamespace(schemeNeutralNamespace(other))
	return strings.HasPrefix(normalized, otherNormalized) || strings.HasPrefix(otherNormalized, normalized)
}

// schemeNeutralNamespace drops the scheme of an Azure (http or https) namespace, whose two
// spellings name the same container and prefix. Any other namespace is returned as it is.
func schemeNeutralNamespace(namespace string) string {
	switch {
	case strings.HasPrefix(namespace, "https://"):
		return strings.TrimPrefix(namespace, "https://")
	case strings.HasPrefix(namespace, "http://"):
		return strings.TrimPrefix(namespace, "http://")
	default:
		return namespace
	}
}

// normalizeNamespace appends the path delimiter the GC's prefix test expects.
func normalizeNamespace(namespace string) string {
	if !strings.HasSuffix(namespace, "/") {
		return namespace + "/"
	}
	return namespace
}
