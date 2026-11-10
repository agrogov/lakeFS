package indexer_test

// fake_test.go is the in-memory lakeFS of the indexer tests: one type implements both seams of
// the package, reader.LakeFS (M2, now served in process by RA4b) and indexer.TableStore (M3),
// because the interesting states of
// design §4.4 are exactly the states where the two disagree - an Iceberg commit that has no
// lakeFS commit, or a lakeFS commit whose tag is missing. Nothing here talks HTTP.

import (
	"cmp"
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/indexer"
	"github.com/treeverse/lakefs/pkg/mds/reader"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

// fakeIndexerPrincipal is the name lakeFS records as the committer of every commit the indexer
// makes: the sync identity mds-service-user (pkg/mds/table.ServiceUserName, pkg/mds/bootstrap). The
// catalog does not read the committer (METADATA_SEARCH_REALIGN.md X4), so this is about the shape
// real lakeFS writes, not about an ownership proof.
const fakeIndexerPrincipal = table.ServiceUserName

// fake is the whole lakeFS of a test: repositories, refs, commits, tags, objects and the metadata
// table of every branch of the one companion repository the test indexes into.
type fake struct {
	mu sync.Mutex

	// pageSize bounds every listing; 0 returns one page.
	pageSize int

	// now is the creation date of the commits the fake makes itself.
	now time.Time

	// metadataRepo is the companion repository of this fake; the TableStore methods address it.
	metadataRepo string

	repositories map[string]reader.Repository
	// branches is repository -> branch -> head commit ID ("" for a branch without commits).
	branches map[string]map[string]string
	// commits is repository -> commit ID -> commit.
	commits map[string]map[string]reader.Commit
	// objects is repository -> ref -> the objects of that commit.
	objects map[string]map[string][]reader.ObjectStat
	// diffs is repository -> "left..right" -> the two-dot diff.
	diffs map[string]map[string][]reader.Change
	// tags is repository -> tag -> commit ID.
	tags map[string]map[string]string

	// tableExists reports whether the metadata table was created (it is created once per
	// repository, on its default branch).
	tableExists bool
	// tableAt is metadata commit ID -> the table state that commit recorded.
	tableAt map[string]*fakeState
	// staged is metadata branch -> the uncommitted table state of that branch.
	staged map[string]*fakeState

	// The calls the tests assert on.
	createdRepositories []reader.RepositoryOptions
	createdBranches     []createdBranch
	deletedBranches     []string
	resetBranches       []string
	hardResets          []hardReset
	commitsMade         []reader.CommitOptions
	tagsMade            []tagCreation
	deletedTags         []string
	statCalls           []string
	events              []string
	diffCalls           int
	listCalls           int
	applyCalls          int
	rebuildCalls        int
	compactCalls        int
	// createTableCalls counts CreateTable requests, including the ones that answer ErrTableExists:
	// an initialisation that re-enters itself calls it again (R2I-1).
	createTableCalls int
	// findMergeBaseCalls counts §4.2 step 4 reads, so a caught-up tick can assert zero (R2I-2).
	findMergeBaseCalls int

	// protectedCommit and protectedStagingWrite model branch protection: lakeFS refuses a commit
	// to a COMMIT-blocked branch and any staging write (Set, Reset, a catalog staging write) to a
	// STAGING_WRITE-blocked one with 403 (pkg/graveler/branch/protection_manager.go,
	// pkg/graveler/graveler.go Commit/Reset/HardReset, pkg/api/controller.go handleApiErrorCallback).
	protectedCommit       map[string]bool
	protectedStagingWrite map[string]bool

	// dataBranchHook, when set, is run for every head read that starts a branch tick of a data
	// repository (indexer.runBranchOnce reads it with GetCommit). It receives the commit ID the
	// read names - the branch's head commit for a branch read - and lets a test hold several branch
	// ticks in flight at once and measure how many the indexer runs in parallel (design §4.1).
	dataBranchHook func(commitID string) (done func())

	// Injected failures. Each is one-shot unless documented otherwise.
	applyErr    error
	applyErrors []error
	rebuildErr  error
	compactErr  error

	// findMergeBaseErr, when set, is returned by the next FindMergeBase call instead of an answer:
	// it is how a test injects a 404 that is not about the recorded position - a transient answer
	// about the head, most of all.
	findMergeBaseErr error

	// afterFindMergeBase, when set, runs once after the next successful FindMergeBase call, outside
	// the lock: it is how a test models a position that is collected between §4.2 step 4 and the
	// pending walk of step 5.
	afterFindMergeBase func()

	// logCalls and logCommits count the LogCommits answers per repository: the requests a walk
	// costs and the commits it reads, which is what the bounds of §4.2 step 5 are about.
	logCalls   map[string]int
	logCommits map[string]int

	// logRequests records the options of every LogCommits call, so a test can pin the
	// FirstParent flag §3.2 requires (TS-10).
	logRequests []logCommitsCall

	index int
}

type logCommitsCall struct {
	repository string
	opts       reader.LogCommitsOptions
}

type createdBranch struct {
	repository string
	branch     string
	source     string
}

type hardReset struct {
	repository string
	branch     string
	ref        string
}

type tagCreation struct {
	repository string
	tag        string
	ref        string
}

// fakeState is the table state of one branch: what a snapshot of the table holds.
type fakeState struct {
	sourceCommit string
	// repository is the table's lakefs.mds.repository property (§3.1): initialise refuses a staged
	// table whose value is not the data repository it is initialising (R2I-4).
	repository  string
	rows        map[string]table.Row
	snapshots   int
	deleteFiles int
}

func newState() *fakeState { return &fakeState{rows: make(map[string]table.Row)} }

func (s *fakeState) clone() *fakeState {
	clone := &fakeState{
		sourceCommit: s.sourceCommit,
		repository:   s.repository,
		rows:         make(map[string]table.Row, len(s.rows)),
		snapshots:    s.snapshots,
		deleteFiles:  s.deleteFiles,
	}
	for path, row := range s.rows {
		clone.rows[path] = row
	}
	return clone
}

// paths returns the paths of the state's rows, sorted.
func (s *fakeState) paths() []string {
	paths := make([]string, 0, len(s.rows))
	for path := range s.rows {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	return paths
}

func newFake() *fake {
	return &fake{
		now:                   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		repositories:          make(map[string]reader.Repository),
		branches:              make(map[string]map[string]string),
		commits:               make(map[string]map[string]reader.Commit),
		objects:               make(map[string]map[string][]reader.ObjectStat),
		diffs:                 make(map[string]map[string][]reader.Change),
		tags:                  make(map[string]map[string]string),
		tableAt:               make(map[string]*fakeState),
		staged:                make(map[string]*fakeState),
		logCalls:              make(map[string]int),
		logCommits:            make(map[string]int),
		protectedCommit:       make(map[string]bool),
		protectedStagingWrite: make(map[string]bool),
	}
}

// addRepository adds a repository and its empty default branch.
func (f *fake) addRepository(name, namespace, defaultBranch string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addRepositoryLocked(name, namespace, defaultBranch)
}

func (f *fake) addRepositoryLocked(name, namespace, defaultBranch string) {
	f.repositories[name] = reader.Repository{
		Name:             name,
		StorageNamespace: namespace,
		DefaultBranch:    defaultBranch,
		StorageID:        "storage-" + name,
	}
	f.branches[name] = map[string]string{defaultBranch: ""}
	if f.commits[name] == nil {
		f.commits[name] = make(map[string]reader.Commit)
	}
	if f.tags[name] == nil {
		f.tags[name] = make(map[string]string)
	}
	if strings.HasSuffix(name, "-metadata") {
		f.metadataRepo = name
	}
}

// addCommit adds a commit to a repository with the given parents and objects, and moves the branch
// (when branch is not empty) to it.
func (f *fake) addCommit(repository, branch, id string, parents []string, objects []reader.ObjectStat) reader.Commit {
	f.mu.Lock()
	defer f.mu.Unlock()
	commit := reader.Commit{
		ID:           id,
		Message:      "commit " + id,
		Parents:      parents,
		Committer:    "committer-" + id,
		CreationDate: f.now.Add(time.Duration(f.index) * time.Minute),
	}
	f.index++
	f.commits[repository][id] = commit
	f.setObjectsLocked(repository, id, objects)
	if branch != "" {
		f.branches[repository][branch] = id
	}
	return commit
}

// setCommit stores a commit as it is, for a test that cares about its committer or date.
func (f *fake) setCommit(repository string, commit reader.Commit) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits[repository][commit.ID] = commit
}

// deleteCommit removes a commit from a repository and leaves the rest of the history alone: it is
// how a test models committed GC collecting a position that no branch refers to any more (design
// §4.7), or a repository that was deleted and created again under the same name (design §4.8).
func (f *fake) deleteCommit(repository, commit string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.commits[repository], commit)
	delete(f.objects[repository], commit)
}

// setObjects sets the objects of one ref (a commit ID in every test here).
func (f *fake) setObjects(repository, ref string, objects []reader.ObjectStat) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setObjectsLocked(repository, ref, objects)
}

func (f *fake) setObjectsLocked(repository, ref string, objects []reader.ObjectStat) {
	if f.objects[repository] == nil {
		f.objects[repository] = make(map[string][]reader.ObjectStat)
	}
	f.objects[repository][ref] = objects
}

// setBranchHead moves a branch to a commit without making a commit: it is how a test simulates a
// hard reset, a revert by reset or a force-moved branch (design §4.7).
func (f *fake) setBranchHead(repository, branch, commit string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.branches[repository][branch] = commit
}

// deleteBranch removes a branch, as a user deleting a data branch does (design §4.8).
func (f *fake) deleteBranch(repository, branch string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.branches[repository], branch)
}

// setCommitDate overrides the creation date of a commit, which is what design §4.9's tag retention
// and §4.5's since are about.
func (f *fake) setCommitDate(repository, commit string, date time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	stored := f.commits[repository][commit]
	stored.CreationDate = date
	f.commits[repository][commit] = stored
}

// initCommitID returns the initialisation commit of the metadata repository.
func (f *fake) initCommitID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, commit := range f.commits[f.metadataRepo] {
		if commit.Metadata[indexer.MetadataKeyInit] == indexer.MetadataKeyInitValue {
			return id
		}
	}
	return ""
}

// setDiff sets the two-dot diff between two commits.
func (f *fake) setDiff(repository, left, right string, changes []reader.Change) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.diffs[repository] == nil {
		f.diffs[repository] = make(map[string][]reader.Change)
	}
	f.diffs[repository][left+".."+right] = changes
}

// addTag adds a tag.
func (f *fake) addTag(repository, tag, commit string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tags[repository] == nil {
		f.tags[repository] = make(map[string]string)
	}
	f.tags[repository][tag] = commit
}

// tagCommit returns the commit a tag points at, and whether the tag exists.
func (f *fake) tagCommit(repository, tag string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	commit, ok := f.tags[repository][tag]
	return commit, ok
}

// branchHead returns the head commit of a branch, and whether the branch exists.
func (f *fake) branchHead(repository, branch string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	head, ok := f.branches[repository][branch]
	return head, ok
}

// tableState returns the committed table state of a branch.
func (f *fake) tableState(branch string) (*fakeState, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	head := f.branches[f.metadataRepo][branch]
	if head == "" {
		return nil, false
	}
	state, ok := f.tableAt[head]
	return state, ok
}

// stage puts an uncommitted table state on a metadata branch, which is how a test injects the
// crash window between step 3 and step 4 of design §4.3.
func (f *fake) stage(branch string, state *fakeState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.staged[branch] = state
}

// resetCalls returns the metadata branches the indexer discarded the staging of.
func (f *fake) resetCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.resetBranches)
}

// setReadOnly marks a repository read-only, as an operator creating the companion repository with
// read_only: true (or flipping the flag later) would (TS-4).
func (f *fake) setReadOnly(repository string, readOnly bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	stored := f.repositories[repository]
	stored.ReadOnly = readOnly
	f.repositories[repository] = stored
}

// setDataBranchHook installs the hook that enterDataBranch runs for the head read of a data branch.
func (f *fake) setDataBranchHook(hook func(commitID string) (done func())) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dataBranchHook = hook
}

// enterDataBranch runs the concurrency probe, when one is installed, for a head read of a branch of
// a data repository. commitID is what the read names - a branch read names the head commit.
// Reads of the metadata repository are excluded: they belong to a branch tick that is already
// running, they do not start one.
func (f *fake) enterDataBranch(repository, commitID string) func() {
	f.mu.Lock()
	hook := f.dataBranchHook
	metadata := f.metadataRepo
	f.mu.Unlock()
	if hook == nil || repository == metadata {
		return nil
	}
	return hook(commitID)
}

// diffRefsCalls returns how many pages of a diff the indexer read.
func (f *fake) diffRefsCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.diffCalls
}

// listObjectCalls returns how many pages of a listing the indexer read.
func (f *fake) listObjectCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listCalls
}

// logCommitsCalls returns how many LogCommits requests the indexer made for a repository.
func (f *fake) logCommitsCalls(repository string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.logCalls[repository]
}

// logCommitsRead returns how many commits the LogCommits answers of a repository returned in total,
// which bounds what a walk of that log can hold.
func (f *fake) logCommitsRead(repository string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.logCommits[repository]
}

// madeCommits returns the lakeFS commits the indexer made.
func (f *fake) madeCommits() []reader.CommitOptions {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.commitsMade)
}

// Repository reads.

func (f *fake) GetRepository(_ context.Context, repository string) (reader.Repository, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	found, ok := f.repositories[repository]
	if !ok {
		return reader.Repository{}, &reader.NotFoundError{Operation: "get repository", Subject: repository}
	}
	return found, nil
}

func (f *fake) CreateRepository(_ context.Context, opts reader.RepositoryOptions) (reader.Repository, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.repositories[opts.Name]; ok {
		return reader.Repository{}, fmt.Errorf("create repository %q: %w", opts.Name, reader.ErrConflict)
	}
	branch := opts.DefaultBranch
	if branch == "" {
		branch = "main"
	}
	f.addRepositoryLocked(opts.Name, opts.StorageNamespace, branch)
	// lakeFS creates a non-bare repository: its default branch starts with the automatic
	// "Repository created" root commit. The indexer's initialisation commit is the first commit
	// after it, never the root (design §4.8).
	root := reader.Commit{
		ID:           fmt.Sprintf("created-%d", f.index),
		Message:      "Repository created",
		Committer:    "lakefs",
		CreationDate: f.now.Add(time.Duration(f.index) * time.Minute),
	}
	f.index++
	f.commits[opts.Name][root.ID] = root
	f.setObjectsLocked(opts.Name, root.ID, nil)
	f.branches[opts.Name][branch] = root.ID
	f.createdRepositories = append(f.createdRepositories, opts)
	return f.repositories[opts.Name], nil
}

// Branch reads and writes.

func (f *fake) ListBranches(_ context.Context, repository, after string) ([]reader.Branch, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	names := make([]string, 0, len(f.branches[repository]))
	for name := range f.branches[repository] {
		names = append(names, name)
	}
	slices.Sort(names)
	return pageOf(names, after, f.pageSize, func(name string) string { return name }, func(name string) reader.Branch {
		return reader.Branch{Name: name, CommitID: f.branches[repository][name]}
	})
}

func (f *fake) CreateBranch(_ context.Context, repository, branch, source string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.branches[repository][branch]; ok {
		return fmt.Errorf("create branch %q: %w", branch, reader.ErrConflict)
	}
	commit, err := f.resolveLocked(repository, source)
	if err != nil {
		return err
	}
	f.branches[repository][branch] = commit
	f.createdBranches = append(f.createdBranches, createdBranch{repository: repository, branch: branch, source: source})
	return nil
}

func (f *fake) DeleteBranch(_ context.Context, repository, branch string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.requireWritableBranchLocked(repository, branch); err != nil {
		return err
	}
	if f.repositories[repository].DefaultBranch == branch {
		// Real lakeFS refuses to delete a repository's default branch: graveler.DeleteBranch
		// returns ErrDeleteDefaultBranch, which pkg/api/controller.go maps to 403, not 400
		// (handleApiErrorCallback, errors.Is(err, graveler.ErrDeleteDefaultBranch)). Model the
		// status so an indexer that tries it fails the test instead of silently "succeeding".
		return fmt.Errorf("delete branch %q: default branch: %w", branch, reader.ErrForbidden)
	}
	delete(f.branches[repository], branch)
	delete(f.staged, branch)
	f.deletedBranches = append(f.deletedBranches, branch)
	return nil
}

func (f *fake) ResetBranch(_ context.Context, repository, branch string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Real lakeFS resolves the branch first and answers 404 for one that does not exist, so a
	// reset that "succeeds" on a missing branch would hide a metadata branch deleted under a
	// tick (graveler.ErrBranchNotFound and ErrRepositoryNotFound are 404 in pkg/api/controller.go).
	if err := f.requireWritableBranchLocked(repository, branch, fakeProtectionStagingWrite); err != nil {
		return err
	}
	delete(f.staged, branch)
	f.resetBranches = append(f.resetBranches, branch)
	f.events = append(f.events, "reset:"+branch)
	return nil
}

func (f *fake) HardResetBranch(_ context.Context, repository, branch, ref string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.requireWritableBranchLocked(repository, branch, fakeProtectionCommit, fakeProtectionStagingWrite); err != nil {
		return err
	}
	commit, err := f.resolveLocked(repository, ref)
	if err != nil {
		return err
	}
	f.branches[repository][branch] = commit
	// Real lakeFS ResetHard does not discard staging: pkg/graveler/graveler.go ResetHard checks
	// only the sealed tokens and leaves a TODO for the rest. Keeping f.staged here is what makes
	// the fake model that, so an indexer that rewrites before discarding staging is caught.
	f.hardResets = append(f.hardResets, hardReset{repository: repository, branch: branch, ref: ref})
	f.events = append(f.events, "hard-reset:"+branch+":"+ref)
	return nil
}

// requireWritableBranchLocked reports the answers real lakeFS gives before it writes to a branch:
// 404 for a repository or branch that does not exist (pkg/api/controller.go maps
// graveler.ErrRepositoryNotFound and ErrBranchNotFound to http.StatusNotFound), and 403 for a
// read-only repository (graveler.ErrReadOnlyRepository comes before the branch lookup in Commit,
// Reset, ResetHard and DeleteBranch, and is http.StatusForbidden in pkg/api/controller.go). A branch
// that exists but has no commit yet is not missing: lakeFS lets a root commit be written to it.
//
// The optional actions model branch protection: graveler.Commit/ResetHard check the COMMIT rule
// before ReadOnly and the branch lookup, Reset/HardReset check STAGING_WRITE, and pkg/api maps
// ErrProtectedBranch to 403 (pkg/graveler/graveler.go:2245-2257, :2577-2595, :2638-2650).
func (f *fake) requireWritableBranchLocked(repository, branch string, actions ...string) error {
	if err := f.protectionErrorLocked(repository, branch, actions...); err != nil {
		return err
	}
	if _, ok := f.repositories[repository]; !ok {
		return &reader.NotFoundError{Operation: "get repository", Subject: repository}
	}
	if f.repositories[repository].ReadOnly {
		return fmt.Errorf("write to branch %q: read-only repository: %w", branch, reader.ErrForbidden)
	}
	if _, ok := f.branches[repository][branch]; !ok {
		return &reader.NotFoundError{Operation: "get branch", Subject: branch}
	}
	return nil
}

// catalogWriteError answers a refused table write the way the indexer's TableStore sees it. The
// production table is written through the Iceberg REST catalog, not through the lakeFS API client:
// a protection rule or a read_only repository makes pkg/icebergcatalog/tables.go writeError answer
// 403 ForbiddenException over the graveler message, and iceberg-go's REST client turns that status
// into rest.ErrForbidden (iceberg-go catalog/rest/rest.go). So a catalog write never produces
// reader.ErrForbidden, and a fake that answered the API sentinel left metadataBranchWriteError's
// rest.ErrForbidden arm - the one production uses - untested (R3I-5). The graveler message stays in
// the text so a failure still reads the way lakeFS wrote it.
func catalogWriteError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", rest.ErrForbidden, err)
}

const (
	fakeProtectionCommit       = "commit"
	fakeProtectionStagingWrite = "staging_write"
)

// protectionErrorLocked answers a blocked action the way graveler does: the two messages are
// graveler.ErrCommitToProtectedBranch and graveler.ErrWriteToProtectedBranch, both wrapping
// ErrProtectedBranch, which pkg/api/controller.go maps to http.StatusForbidden.
func (f *fake) protectionErrorLocked(repository, branch string, actions ...string) error {
	key := repository + "@" + branch
	for _, action := range actions {
		switch action {
		case fakeProtectionCommit:
			if f.protectedCommit[key] {
				return fmt.Errorf("commit to branch %q: cannot commit to protected branch: %w", branch, reader.ErrForbidden)
			}
		case fakeProtectionStagingWrite:
			if f.protectedStagingWrite[key] {
				return fmt.Errorf("write to branch %q: cannot write to protected branch: %w", branch, reader.ErrForbidden)
			}
		}
	}
	return nil
}

// protectBranch installs a branch protection rule for one branch: commit blocks COMMIT, staging
// blocks STAGING_WRITE (a catalog staging write, Set, or Reset).
func (f *fake) protectBranch(repository, branch string, commit, staging bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := repository + "@" + branch
	if commit {
		f.protectedCommit[key] = true
	}
	if staging {
		f.protectedStagingWrite[key] = true
	}
}

func (f *fake) FindMergeBase(_ context.Context, repository, sourceRef, destinationRef string) (string, error) {
	f.mu.Lock()
	f.findMergeBaseCalls++
	if f.findMergeBaseErr != nil {
		err := f.findMergeBaseErr
		f.findMergeBaseErr = nil
		f.mu.Unlock()
		return "", err
	}
	base, err := f.findMergeBaseLocked(repository, sourceRef, destinationRef)
	// The hook runs after the answer and outside the lock: it models the recorded position being
	// collected between step 4 of §4.2 and the pending walk of step 5.
	hook := f.afterFindMergeBase
	f.afterFindMergeBase = nil
	f.mu.Unlock()
	if err != nil {
		return "", err
	}
	if hook != nil {
		hook()
	}
	return base, nil
}

func (f *fake) findMergeBaseLocked(repository, sourceRef, destinationRef string) (string, error) {
	source, err := f.resolveLocked(repository, sourceRef)
	if err != nil {
		return "", err
	}
	destination, err := f.resolveLocked(repository, destinationRef)
	if err != nil {
		return "", err
	}
	for _, commit := range []string{source, destination} {
		if _, ok := f.commits[repository][commit]; !ok {
			return "", &reader.NotFoundError{Operation: "get commit", Subject: commit}
		}
	}
	if source == destination {
		return source, nil
	}

	// The best common ancestor, as graveler's ref.FindMergeBase computes it
	// (pkg/graveler/ref/merge_base_finder.go): walk both sides in generation order - highest
	// generation first, newest commit first on a tie - and report the first commit reached from
	// both. A breadth-first walk from the source instead answers the first common ancestor it
	// meets, which on a criss-cross or nested-merge history is an ancestor of the best one, so a
	// test could pin a tag real lakeFS never produces (TS-10).
	const (
		fromSource uint8 = 1 << iota
		fromDestination
	)
	reached := map[string]uint8{source: fromSource, destination: fromDestination}
	queue := []string{source, destination}
	for len(queue) > 0 {
		current := f.popNewestLocked(repository, &queue)
		flags := reached[current]
		for _, parent := range f.commits[repository][current].Parents {
			if _, ok := reached[parent]; !ok {
				queue = append(queue, parent)
			}
			reached[parent] |= flags
			if reached[parent]&fromSource != 0 && reached[parent]&fromDestination != 0 {
				if _, ok := f.commits[repository][parent]; !ok {
					return "", &reader.NotFoundError{Operation: "get commit", Subject: parent}
				}
				return parent, nil
			}
		}
	}
	// graveler.FindMergeBase returns ErrNoMergeBase for unrelated histories; pkg/api turns that
	// into a 500. The fake reports the same typed error pkg/mds/reader must produce.
	return "", reader.ErrNoMergeBase
}

// popNewestLocked removes the queued commit graveler's commit-generation priority queue would pop
// first: the highest generation, and the newest commit on a tie (pkg/graveler/ref/
// commit_generation_priority_queue.go).
func (f *fake) popNewestLocked(repository string, queue *[]string) string {
	best := 0
	bestGeneration := f.generationLocked(repository, (*queue)[0])
	for i := 1; i < len(*queue); i++ {
		generation := f.generationLocked(repository, (*queue)[i])
		if generation > bestGeneration ||
			(generation == bestGeneration &&
				f.commits[repository][(*queue)[i]].CreationDate.After(f.commits[repository][(*queue)[best]].CreationDate)) {
			best, bestGeneration = i, generation
		}
	}
	current := (*queue)[best]
	*queue = append((*queue)[:best], (*queue)[best+1:]...)
	return current
}

// generationLocked returns a commit's generation: 1 for a root, and one more than the highest
// generation of its parents. graveler stores it on the commit; the fake derives it, so a test can
// build a history with addCommit alone.
func (f *fake) generationLocked(repository, commitID string) int {
	commit, ok := f.commits[repository][commitID]
	if !ok || len(commit.Parents) == 0 {
		return 1
	}
	generation := 0
	for _, parent := range commit.Parents {
		if candidate := f.generationLocked(repository, parent) + 1; candidate > generation {
			generation = candidate
		}
	}
	if generation == 0 {
		generation = 1
	}
	return generation
}

// Commit reads and writes.

func (f *fake) LogCommits(_ context.Context, repository, ref string, opts reader.LogCommitsOptions) ([]reader.Commit, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logCalls[repository]++
	f.logRequests = append(f.logRequests, logCommitsCall{repository: repository, opts: opts})
	head, err := f.resolveLocked(repository, ref)
	if err != nil {
		return nil, false, err
	}
	// pkg/catalog dereferences stop_at before it walks (pkg/catalog/catalog.go, ListCommits), so a
	// stop_at that no longer exists answers 404 - a position collected by committed GC, or a
	// repository that was deleted and created again - and never a short log.
	if opts.StopAt != "" {
		if _, err := f.resolveLocked(repository, opts.StopAt); err != nil {
			return nil, false, err
		}
	}
	var chain []reader.Commit
	if opts.FirstParent {
		for current := head; current != ""; {
			commit, ok := f.commits[repository][current]
			if !ok {
				break
			}
			chain = append(chain, commit)
			// The lakeFS API's stop_at is inclusive: the stop commit is the last item returned.
			if opts.StopAt != "" && current == opts.StopAt {
				break
			}
			if len(commit.Parents) == 0 {
				break
			}
			current = commit.Parents[0]
		}
	} else {
		// Without first_parent graveler's ListCommits walks every parent. The fake walks them
		// breadth first, which is enough to make a dropped FirstParent: true visible (§3.2,
		// TS-10); the order is not graveler's generation order, and nothing the indexer does
		// reads a log without first_parent (pkg/mds/reader passes the flag through to
		// pkg/catalog's log).
		queue := []string{head}
		seen := map[string]bool{}
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			if current == "" || seen[current] {
				continue
			}
			seen[current] = true
			commit, ok := f.commits[repository][current]
			if !ok {
				continue
			}
			chain = append(chain, commit)
			if opts.StopAt != "" && current == opts.StopAt {
				continue
			}
			queue = append(queue, commit.Parents...)
		}
	}
	if opts.After != "" {
		for i, commit := range chain {
			if commit.ID == opts.After {
				chain = chain[i+1:]
				break
			}
		}
	}
	page, hasMore, err := pageOf(chain, "", f.pageSize, func(commit reader.Commit) string { return commit.ID },
		func(commit reader.Commit) reader.Commit { return commit })
	f.logCommits[repository] += len(page)
	return page, hasMore, err
}

func (f *fake) GetCommit(_ context.Context, repository, commitID string) (reader.Commit, error) {
	if done := f.enterDataBranch(repository, commitID); done != nil {
		defer done()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	id, err := f.resolveLocked(repository, commitID)
	if err != nil {
		return reader.Commit{}, err
	}
	commit, ok := f.commits[repository][id]
	if !ok {
		return reader.Commit{}, &reader.NotFoundError{Operation: "get commit", Subject: commitID}
	}
	return commit, nil
}

func (f *fake) Commit(_ context.Context, repository, branch string, opts reader.CommitOptions) (reader.Commit, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Real lakeFS resolves the branch before it commits and answers 404 when it (or its
	// repository) is gone. Re-creating a branch under a commit instead produced a repository state
	// lakeFS cannot have - a metadata branch whose root commit is an indexing commit - and hid the
	// 404 a branch deleted under a tick must surface.
	if err := f.requireWritableBranchLocked(repository, branch, fakeProtectionCommit); err != nil {
		return reader.Commit{}, err
	}
	var state *fakeState
	if repository == f.metadataRepo {
		staged, ok := f.staged[branch]
		switch {
		case ok:
			state = staged
		case !opts.AllowEmpty:
			return reader.Commit{}, fmt.Errorf("commit %q@%s: nothing staged: %w", repository, branch, reader.ErrBadRequest)
		default:
			if head := f.branches[repository][branch]; head != "" && f.tableAt[head] != nil {
				state = f.tableAt[head].clone()
			} else {
				state = newState()
			}
		}
	}

	head := f.branches[repository][branch]
	var parents []string
	if head != "" {
		parents = []string{head}
	}
	commit := reader.Commit{
		ID:           fmt.Sprintf("index-%d", f.index),
		Message:      opts.Message,
		Parents:      parents,
		Committer:    fakeIndexerPrincipal,
		CreationDate: f.now,
		Metadata:     opts.Metadata,
	}
	f.index++
	f.commits[repository][commit.ID] = commit
	if state != nil {
		f.tableAt[commit.ID] = state
		delete(f.staged, branch)
	}
	f.branches[repository][branch] = commit.ID
	f.commitsMade = append(f.commitsMade, opts)
	return commit, nil
}

// Tag reads and writes.

func (f *fake) ListTags(_ context.Context, repository, after string) ([]reader.Tag, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	names := make([]string, 0, len(f.tags[repository]))
	for name := range f.tags[repository] {
		names = append(names, name)
	}
	slices.Sort(names)
	return pageOf(names, after, f.pageSize, func(name string) string { return name }, func(name string) reader.Tag {
		return reader.Tag{Name: name, CommitID: f.tags[repository][name]}
	})
}

func (f *fake) CreateTag(_ context.Context, repository, tag, ref string, force bool) (reader.Tag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Like lakeFS, force never overwrites: pkg/graveler/graveler.go CreateTag uses it only to
	// pass the read-only check, then returns ErrTagAlreadyExists (409) for any existing tag.
	if _, ok := f.tags[repository][tag]; ok {
		return reader.Tag{}, fmt.Errorf("create tag %q: %w", tag, reader.ErrConflict)
	}
	commit, err := f.resolveLocked(repository, ref)
	if err != nil {
		return reader.Tag{}, err
	}
	if f.tags[repository] == nil {
		f.tags[repository] = make(map[string]string)
	}
	f.tags[repository][tag] = commit
	f.tagsMade = append(f.tagsMade, tagCreation{repository: repository, tag: tag, ref: ref})
	return reader.Tag{Name: tag, CommitID: commit}, nil
}

func (f *fake) GetTag(_ context.Context, repository, tag string) (reader.Tag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	commit, ok := f.tags[repository][tag]
	if !ok {
		return reader.Tag{}, &reader.NotFoundError{Operation: "get tag", Subject: tag}
	}
	return reader.Tag{Name: tag, CommitID: commit}, nil
}

func (f *fake) DeleteTag(_ context.Context, repository, tag string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.tags[repository][tag]; !ok {
		return &reader.NotFoundError{Operation: "delete tag", Subject: tag}
	}
	delete(f.tags[repository], tag)
	f.deletedTags = append(f.deletedTags, tag)
	return nil
}

// Object reads.

func (f *fake) ListObjects(_ context.Context, repository, ref, after string) ([]reader.ObjectStat, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCalls++
	objects, ok := f.objects[repository][ref]
	if !ok {
		return nil, false, &reader.NotFoundError{Operation: "list objects", Subject: ref}
	}
	sorted := slices.Clone(objects)
	slices.SortFunc(sorted, func(a, b reader.ObjectStat) int { return cmp.Compare(a.Path, b.Path) })
	return pageOf(sorted, after, f.pageSize, func(object reader.ObjectStat) string { return object.Path },
		func(object reader.ObjectStat) reader.ObjectStat { return object })
}

func (f *fake) DiffRefs(_ context.Context, repository, leftRef, rightRef, after string) ([]reader.Change, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.diffCalls++
	changes, ok := f.diffs[repository][leftRef+".."+rightRef]
	if !ok {
		return nil, false, &reader.NotFoundError{Operation: "diff refs", Subject: leftRef + ".." + rightRef}
	}
	sorted := slices.Clone(changes)
	slices.SortFunc(sorted, func(a, b reader.Change) int { return cmp.Compare(a.Path, b.Path) })
	return pageOf(sorted, after, f.pageSize, func(change reader.Change) string { return change.Path },
		func(change reader.Change) reader.Change { return change })
}

// DiffBranch reports the uncommitted changes of a metadata branch. A staged table pointer is
// itself an object change on the branch, so any staged state is at least one change.
func (f *fake) DiffBranch(_ context.Context, repository, branch, after string) ([]reader.Change, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.branches[repository][branch]; !ok {
		return nil, false, &reader.NotFoundError{Operation: "diff branch", Subject: branch}
	}
	staged, ok := f.staged[branch]
	if !ok {
		return nil, false, nil
	}
	committed := f.tableAt[f.branches[repository][branch]]
	var changes []reader.Change
	for path, row := range staged.rows {
		old, ok := committedRow(committed, path)
		switch {
		case !ok:
			changes = append(changes, reader.Change{Path: path, Type: reader.ChangeAdded, SizeBytes: row.SizeBytes})
		case !reflect.DeepEqual(old, row):
			changes = append(changes, reader.Change{Path: path, Type: reader.ChangeChanged, SizeBytes: row.SizeBytes})
		}
	}
	if committed != nil {
		for path := range committed.rows {
			if _, ok := staged.rows[path]; !ok {
				changes = append(changes, reader.Change{Path: path, Type: reader.ChangeRemoved})
			}
		}
	}
	if len(changes) == 0 {
		changes = append(changes, reader.Change{
			Path: "_lakefs_iceberg/tables/system/object_metadata",
			Type: reader.ChangeChanged,
		})
	}
	slices.SortFunc(changes, func(a, b reader.Change) int { return cmp.Compare(a.Path, b.Path) })
	return pageOf(changes, after, f.pageSize, func(change reader.Change) string { return change.Path },
		func(change reader.Change) reader.Change { return change })
}

func committedRow(state *fakeState, path string) (table.Row, bool) {
	if state == nil {
		return table.Row{}, false
	}
	row, ok := state.rows[path]
	return row, ok
}

func (f *fake) StatObject(_ context.Context, repository, ref, path string) (reader.ObjectStat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statCalls = append(f.statCalls, path)
	for _, object := range f.objects[repository][ref] {
		if object.Path == path {
			return object, nil
		}
	}
	return reader.ObjectStat{}, &reader.NotFoundError{Operation: "stat object", Subject: path}
}

// Metadata table reads and writes.

func (f *fake) LoadTable(_ context.Context, _, branch string) (indexer.Table, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	state, ok := f.tableStateLocked(branch)
	if !ok {
		return nil, indexer.ErrTableNotFound
	}
	return &fakeTable{f: f, branch: branch, state: state.clone()}, nil
}

// tableStateLocked returns the table state a load of the branch sees: the staging when it has any
// (the catalog reads the staged pointer) and the committed state of the branch head otherwise.
func (f *fake) tableStateLocked(branch string) (*fakeState, bool) {
	if state, ok := f.staged[branch]; ok {
		return state, true
	}
	head, ok := f.branches[f.metadataRepo][branch]
	if !ok || head == "" {
		return nil, false
	}
	state, ok := f.tableAt[head]
	return state, ok
}

func (f *fake) CreateTable(_ context.Context, dataRepository, branch string) (indexer.Table, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createTableCalls++
	// The catalog's create is a staging write on the branch (pkg/icebergcatalog/tables.go,
	// CreateTableWithMetadata -> stage), so it meets the same COMMIT/STAGING_WRITE protection and
	// the same read-only refusal the other table writes do - one step before the initialisation
	// commit. The fake used to accept it unconditionally and hid the path the indexer did not map
	// (R3I-2).
	if err := f.protectionErrorLocked(f.metadataRepo, branch, fakeProtectionStagingWrite); err != nil {
		return nil, catalogWriteError(err)
	}
	if f.repositories[f.metadataRepo].ReadOnly {
		return nil, catalogWriteError(fmt.Errorf("write to branch %q: read-only repository: %w", branch, reader.ErrForbidden))
	}
	if f.tableExists {
		return nil, indexer.ErrTableExists
	}
	f.tableExists = true
	state := newState()
	state.repository = dataRepository
	f.staged[branch] = state
	return &fakeTable{f: f, branch: branch, state: state.clone()}, nil
}

// resolveLocked resolves a reference to a commit ID: a commit, a branch, or a tag.
func (f *fake) resolveLocked(repository, ref string) (string, error) {
	if _, ok := f.commits[repository][ref]; ok {
		return ref, nil
	}
	if commit, ok := f.branches[repository][ref]; ok {
		if commit == "" {
			return "", &reader.NotFoundError{Operation: "resolve", Subject: ref}
		}
		return commit, nil
	}
	if commit, ok := f.tags[repository][ref]; ok {
		return commit, nil
	}
	return "", &reader.NotFoundError{Operation: "resolve", Subject: ref}
}

// pageOf returns one page of a listing sorted by key, starting strictly after `after`: the cursor
// is the key of the last item of the previous page, which is the cursor the lakeFS API uses.
func pageOf[T, R any](items []T, after string, pageSize int, key func(T) string, convert func(T) R) ([]R, bool, error) {
	start := 0
	if after != "" {
		for start < len(items) && key(items[start]) <= after {
			start++
		}
	}
	remaining := items[start:]
	if pageSize > 0 && len(remaining) > pageSize {
		remaining = remaining[:pageSize]
	}
	converted := make([]R, 0, len(remaining))
	for _, item := range remaining {
		converted = append(converted, convert(item))
	}
	return converted, start+len(remaining) < len(items), nil
}

// fakeTable is the indexer.Table handle of one branch of the fake's metadata repository.
type fakeTable struct {
	f      *fake
	branch string
	state  *fakeState
}

func (t *fakeTable) SourceCommit() string { return t.state.sourceCommit }
func (t *fakeTable) Repository() string   { return t.state.repository }
func (t *fakeTable) SnapshotCount() int   { return t.state.snapshots }
func (t *fakeTable) DeleteFiles() int     { return t.state.deleteFiles }

// Refresh re-reads the branch the way a TableStore.LoadTable would, through the same handle: the
// staging if the branch has any, the committed state of its head otherwise.
func (t *fakeTable) Refresh(context.Context) error {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	state, ok := t.f.tableStateLocked(t.branch)
	if !ok {
		return indexer.ErrTableNotFound
	}
	t.state = state.clone()
	return nil
}

func (t *fakeTable) ApplyDelta(_ context.Context, upserts []table.Row, deletes []string, sourceCommit string) error {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	// A catalog staging write on a STAGING_WRITE-blocked branch is refused with 403
	// (pkg/icebergcatalog/tables.go maps graveler.ErrWriteToProtectedBranch to ForbiddenException).
	if err := t.f.protectionErrorLocked(t.f.metadataRepo, t.branch, fakeProtectionStagingWrite); err != nil {
		return catalogWriteError(err)
	}
	t.f.applyCalls++
	if len(t.f.applyErrors) > 0 {
		err := t.f.applyErrors[0]
		t.f.applyErrors = t.f.applyErrors[1:]
		return err
	}
	if t.f.applyErr != nil {
		err := t.f.applyErr
		t.f.applyErr = nil
		return err
	}
	next := t.state.clone()
	for _, path := range deletes {
		delete(next.rows, path)
	}
	if len(deletes) > 0 {
		next.deleteFiles++
	}
	for _, row := range upserts {
		next.rows[row.Path] = row
	}
	next.sourceCommit = sourceCommit
	next.snapshots++
	t.state = next
	t.f.staged[t.branch] = next
	return nil
}

func (t *fakeTable) Rebuild(_ context.Context, rows table.Rows, sourceCommit string) error {
	t.f.mu.Lock()
	if err := t.f.protectionErrorLocked(t.f.metadataRepo, t.branch, fakeProtectionStagingWrite); err != nil {
		t.f.mu.Unlock()
		return catalogWriteError(err)
	}
	t.f.rebuildCalls++
	if t.f.rebuildErr != nil {
		err := t.f.rebuildErr
		t.f.rebuildErr = nil
		t.f.mu.Unlock()
		return err
	}
	t.f.mu.Unlock()

	// The row source reads the fake's objects, so it runs without the lock: a rebuild streams its
	// rows through the writer, and holding the lock here would deadlock the lakeFS it reads from.
	next := newState()
	next.repository = t.state.repository
	for row, err := range rows {
		if err != nil {
			return err
		}
		next.rows[row.Path] = row
	}

	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	next.sourceCommit = sourceCommit
	next.snapshots++
	t.state = next
	t.f.staged[t.branch] = next
	return nil
}

func (t *fakeTable) Compact(_ context.Context, sourceCommit string) error {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	if err := t.f.protectionErrorLocked(t.f.metadataRepo, t.branch, fakeProtectionStagingWrite); err != nil {
		return catalogWriteError(err)
	}
	t.f.compactCalls++
	if t.f.compactErr != nil {
		err := t.f.compactErr
		t.f.compactErr = nil
		return err
	}
	next := t.state.clone()
	next.deleteFiles = 0
	next.sourceCommit = sourceCommit
	next.snapshots++
	t.state = next
	t.f.staged[t.branch] = next
	return nil
}

func (t *fakeTable) Close() error { return nil }

// recordingLogger records the Info messages of the indexer, so a test can pin a line that must be
// logged once per process rather than once per tick.
type recordingLogger struct {
	logging.DummyLogger

	mu    sync.Mutex
	infos []string
}

func (l *recordingLogger) WithContext(context.Context) logging.Logger { return l }
func (l *recordingLogger) WithField(string, any) logging.Logger       { return l }
func (l *recordingLogger) WithFields(logging.Fields) logging.Logger   { return l }
func (l *recordingLogger) WithError(error) logging.Logger             { return l }

func (l *recordingLogger) Info(args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infos = append(l.infos, fmt.Sprint(args...))
}

// countInfos returns how many Info messages contain substr.
func (l *recordingLogger) countInfos(substr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	count := 0
	for _, message := range l.infos {
		if strings.Contains(message, substr) {
			count++
		}
	}
	return count
}

// fakeHealth records what the indexer reported on /_health (design §9).
type fakeHealth struct {
	mu       sync.Mutex
	begins   []string
	finishes []finishRecord
	branches map[string]branchRecord
}

type finishRecord struct {
	repository string
	err        error
}

type branchRecord struct {
	lastIndexed string
	head        string
	lag         int64
	err         error
}

func newFakeHealth() *fakeHealth {
	return &fakeHealth{branches: make(map[string]branchRecord)}
}

func (h *fakeHealth) BeginTick(repository string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.begins = append(h.begins, repository)
}

func (h *fakeHealth) FinishTick(repository string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.finishes = append(h.finishes, finishRecord{repository: repository, err: err})
}

func (h *fakeHealth) SetBranchState(repository, branch, lastIndexed, head string, lagCommits int64, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.branches[repository+"@"+branch] = branchRecord{
		lastIndexed: lastIndexed,
		head:        head,
		lag:         lagCommits,
		err:         err,
	}
}

func (h *fakeHealth) RemoveBranch(repository, branch string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.branches, repository+"@"+branch)
}

// branch returns the recorded state of one branch.
func (h *fakeHealth) branch(repository, branch string) branchRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.branches[repository+"@"+branch]
}

// hasBranch reports whether /_health still has a record of one branch.
func (h *fakeHealth) hasBranch(repository, branch string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.branches[repository+"@"+branch]
	return ok
}

// repositoryError returns the error the repository tick finished with, if any.
func (h *fakeHealth) repositoryError(repository string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := len(h.finishes) - 1; i >= 0; i-- {
		if h.finishes[i].repository == repository {
			return h.finishes[i].err
		}
	}
	return nil
}

// fakeMetrics records the counters of design §9.
type fakeMetrics struct {
	mu          sync.Mutex
	indexed     map[string]int
	lags        map[string]int64
	errors      []string
	rows        int64
	rebuilds    int
	compactions int
	cleared     []string
}

func newFakeMetrics() *fakeMetrics {
	return &fakeMetrics{indexed: make(map[string]int), lags: make(map[string]int64)}
}

func (m *fakeMetrics) IndexedCommit(repository string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.indexed[repository]++
}

func (m *fakeMetrics) SetLag(repository, branch string, commits int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lags[repository+"@"+branch] = commits
}

func (m *fakeMetrics) ClearLag(repository, branch string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleared = append(m.cleared, repository+"@"+branch)
}

func (m *fakeMetrics) Error(repository, stage string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors = append(m.errors, repository+"/"+stage)
}

func (m *fakeMetrics) RowsWritten(rows int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows += rows
}

func (m *fakeMetrics) Rebuild() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rebuilds++
}

func (m *fakeMetrics) Compaction() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.compactions++
}

// env is one indexer over one fake lakeFS.
type env struct {
	t       *testing.T
	fake    *fake
	health  *fakeHealth
	metrics *fakeMetrics
	indexer *indexer.Indexer
}

// newEnv builds an indexer over a fresh fake lakeFS with the repository "repo" in
// s3://bucket/data and the branch selector "main".
func newEnv(t *testing.T, configure ...func(*indexer.Options)) *env {
	t.Helper()
	f := newFake()
	f.addRepository("repo", "s3://bucket/data", "main")

	e := &env{t: t, fake: f, health: newFakeHealth(), metrics: newFakeMetrics()}
	opts := indexer.Options{
		LakeFS:       f,
		Tables:       f,
		Repositories: []indexer.Repository{{Name: "repo", Branches: []string{"main"}}},
		Health:       e.health,
		Metrics:      e.metrics,
		Log:          logging.ContextUnavailable(),
	}
	for _, apply := range configure {
		apply(&opts)
	}
	idx, err := indexer.New(opts)
	require.NoError(t, err)
	e.indexer = idx
	return e
}

// tick runs one tick of the indexer.
func (e *env) tick() {
	e.t.Helper()
	require.NoError(e.t, e.indexer.Tick(e.t.Context()))
}

// object returns an ObjectStat.
func object(path string, size int64) reader.ObjectStat {
	return reader.ObjectStat{
		Path:         path,
		SizeBytes:    size,
		Mtime:        time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC),
		ETag:         "etag-" + path,
		ContentType:  "text/plain",
		UserMetadata: map[string]string{"X-Amz-Meta-Owner": "alice"},
	}
}

// added is a diff entry that adds an object.
func added(path string, size int64) reader.Change {
	stat := object(path, size)
	return reader.Change{Path: path, Type: reader.ChangeAdded, SizeBytes: size, Right: &stat}
}

// removed is a diff entry that removes an object.
func removed(path string) reader.Change {
	return reader.Change{Path: path, Type: reader.ChangeRemoved}
}

// TestFakeWriteMethodsRefuseAMissingBranchOrRepository pins the fake against the answers real
// lakeFS gives, so an indexer bug the fake would otherwise hide fails a test (TS-3).
// pkg/api/controller.go maps graveler.ErrNotFound - ErrBranchNotFound, ErrRepositoryNotFound,
// ErrCommitNotFound - to http.StatusNotFound, and graveler.ErrDeleteDefaultBranch to
// http.StatusForbidden; Commit, ResetHard and the reset path all resolve the branch before they
// write (pkg/graveler/graveler.go Commit, ResetHard; pkg/catalog/catalog.go ResetBranch), so none
// of them invents a branch lakeFS would answer 404 for.
func TestFakeWriteMethodsRefuseAMissingBranchOrRepository(t *testing.T) {
	ctx := t.Context()
	f := newFake()
	f.addRepository("repo-metadata", "s3://bucket/data-metadata", "main")
	init := f.addCommit("repo-metadata", "main", "init", nil, nil)

	_, err := f.Commit(ctx, "repo-metadata", "gone", reader.CommitOptions{AllowEmpty: true})
	require.ErrorIs(t, err, reader.ErrNotFound, "commit to a branch that does not exist")
	require.ErrorIs(t, f.ResetBranch(ctx, "repo-metadata", "gone"), reader.ErrNotFound,
		"reset of a branch that does not exist")
	require.ErrorIs(t, f.HardResetBranch(ctx, "repo-metadata", "gone", init.ID), reader.ErrNotFound,
		"hard reset of a branch that does not exist")
	_, created := f.branchHead("repo-metadata", "gone")
	require.False(t, created, "a refused write does not create the branch")

	_, err = f.Commit(ctx, "gone", "main", reader.CommitOptions{AllowEmpty: true})
	require.ErrorIs(t, err, reader.ErrNotFound, "commit in a repository that does not exist")
	require.ErrorIs(t, f.ResetBranch(ctx, "gone", "main"), reader.ErrNotFound,
		"reset in a repository that does not exist")
	require.ErrorIs(t, f.HardResetBranch(ctx, "gone", "main", init.ID), reader.ErrNotFound,
		"hard reset in a repository that does not exist")
	_, registered := f.branches["gone"]
	require.False(t, registered, "a refused write does not create the repository")

	// The refusal of a default-branch delete carries the status lakeFS answers: 403, not 400.
	err = f.DeleteBranch(ctx, "repo-metadata", "main")
	require.ErrorIs(t, err, reader.ErrForbidden, "lakeFS refuses a default-branch delete with 403")
	require.NotErrorIs(t, err, reader.ErrBadRequest)
	head, ok := f.branchHead("repo-metadata", "main")
	require.True(t, ok)
	require.Equal(t, init.ID, head)

	// An existing branch without a commit is not a missing branch: a root commit is written.
	f.addRepository("bare", "s3://bucket/bare", "main")
	root, err := f.Commit(ctx, "bare", "main", reader.CommitOptions{AllowEmpty: true})
	require.NoError(t, err)
	require.Empty(t, root.Parents, "a root commit has no parent")
}

// TestFakeFindMergeBaseIsTheBestCommonAncestor pins the rule of pkg/graveler/ref/merge_base_finder.go
// - the best common ancestor, one that is not an ancestor of another common ancestor - instead of
// the first common ancestor a walk from the source meets (TS-10).
func TestFakeFindMergeBaseIsTheBestCommonAncestor(t *testing.T) {
	ctx := t.Context()
	f := newFake()
	f.addRepository("repo", "s3://bucket/data", "main")
	f.addCommit("repo", "", "root", nil, nil)
	f.addCommit("repo", "", "x", []string{"root"}, nil)
	f.addCommit("repo", "", "y", []string{"x"}, nil)
	// tip's first parent is x, and its second parent y - and x is an ancestor of y.
	f.addCommit("repo", "", "tip", []string{"x", "y"}, nil)

	base, err := f.FindMergeBase(ctx, "repo", "tip", "y")
	require.NoError(t, err)
	require.Equal(t, "y", base,
		"y is the best common ancestor: a walk from tip meets x first, but x is an ancestor of y")
	base, err = f.FindMergeBase(ctx, "repo", "y", "tip")
	require.NoError(t, err)
	require.Equal(t, "y", base, "the answer does not depend on the argument order")

	// A linear chain keeps answering the ancestor.
	base, err = f.FindMergeBase(ctx, "repo", "root", "tip")
	require.NoError(t, err)
	require.Equal(t, "root", base)
}

// TestFakeLogCommitsHonoursFirstParent pins TS-10(b): the fake walks every parent unless the caller
// asks for the first-parent chain, so dropping FirstParent: true from any of the indexer's walks
// (pending, seedTag, backfillStart, firstParentHistory) changes what a test sees.
func TestFakeLogCommitsHonoursFirstParent(t *testing.T) {
	ctx := t.Context()
	f := newFake()
	f.addRepository("repo", "s3://bucket/data", "main")
	f.addCommit("repo", "main", "s1", nil, nil)
	f.addCommit("repo", "main", "s2", []string{"s1"}, nil)
	f.addCommit("repo", "", "side", []string{"s1"}, nil)
	f.addCommit("repo", "main", "m", []string{"s2", "side"}, nil)

	firstParent, _, err := f.LogCommits(ctx, "repo", "m", reader.LogCommitsOptions{FirstParent: true})
	require.NoError(t, err)
	require.Equal(t, []string{"m", "s2", "s1"}, commitIDs(firstParent), "the first-parent chain only")

	everyParent, _, err := f.LogCommits(ctx, "repo", "m", reader.LogCommitsOptions{FirstParent: false})
	require.NoError(t, err)
	require.Equal(t, []string{"m", "s2", "side", "s1"}, commitIDs(everyParent),
		"every parent's history, so a dropped FirstParent is visible")
}

func commitIDs(commits []reader.Commit) []string {
	ids := make([]string, 0, len(commits))
	for _, commit := range commits {
		ids = append(ids, commit.ID)
	}
	return ids
}
