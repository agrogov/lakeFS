package indexer_test

// fake_test.go is the in-memory lakeFS of the indexer tests: one type implements both seams of
// the package, client.LakeFS (M2) and indexer.TableStore (M3), because the interesting states of
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

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/client"
	"github.com/treeverse/lakefs/pkg/mds/indexer"
	"github.com/treeverse/lakefs/pkg/mds/table"
)

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

	repositories map[string]client.Repository
	// branches is repository -> branch -> head commit ID ("" for a branch without commits).
	branches map[string]map[string]string
	// commits is repository -> commit ID -> commit.
	commits map[string]map[string]client.Commit
	// objects is repository -> ref -> the objects of that commit.
	objects map[string]map[string][]client.ObjectStat
	// diffs is repository -> "left..right" -> the two-dot diff.
	diffs map[string]map[string][]client.Change
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
	createdRepositories []client.RepositoryOptions
	createdBranches     []createdBranch
	deletedBranches     []string
	resetBranches       []string
	hardResets          []hardReset
	commitsMade         []client.CommitOptions
	tagsMade            []tagCreation
	deletedTags         []string
	statCalls           []string
	events              []string
	diffCalls           int
	listCalls           int
	applyCalls          int
	rebuildCalls        int
	compactCalls        int

	// dataBranchHook, when set, is run for every head read that starts a branch tick of a data
	// repository (indexer.runBranchOnce reads it with GetCommit). It lets a test hold several branch
	// ticks in flight at once and measure how many the indexer runs in parallel (design §4.1).
	dataBranchHook func(branch string) (done func())

	// Injected failures. Each is one-shot unless documented otherwise.
	applyErr    error
	applyErrors []error
	rebuildErr  error
	compactErr  error

	index int
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
	rows         map[string]table.Row
	snapshots    int
	deleteFiles  int
}

func newState() *fakeState { return &fakeState{rows: make(map[string]table.Row)} }

func (s *fakeState) clone() *fakeState {
	clone := &fakeState{
		sourceCommit: s.sourceCommit,
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
		now:          time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		repositories: make(map[string]client.Repository),
		branches:     make(map[string]map[string]string),
		commits:      make(map[string]map[string]client.Commit),
		objects:      make(map[string]map[string][]client.ObjectStat),
		diffs:        make(map[string]map[string][]client.Change),
		tags:         make(map[string]map[string]string),
		tableAt:      make(map[string]*fakeState),
		staged:       make(map[string]*fakeState),
	}
}

// addRepository adds a repository and its empty default branch.
func (f *fake) addRepository(name, namespace, defaultBranch string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addRepositoryLocked(name, namespace, defaultBranch)
}

func (f *fake) addRepositoryLocked(name, namespace, defaultBranch string) {
	f.repositories[name] = client.Repository{
		Name:             name,
		StorageNamespace: namespace,
		DefaultBranch:    defaultBranch,
		StorageID:        "storage-" + name,
	}
	f.branches[name] = map[string]string{defaultBranch: ""}
	if f.commits[name] == nil {
		f.commits[name] = make(map[string]client.Commit)
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
func (f *fake) addCommit(repository, branch, id string, parents []string, objects []client.ObjectStat) client.Commit {
	f.mu.Lock()
	defer f.mu.Unlock()
	commit := client.Commit{
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
func (f *fake) setCommit(repository string, commit client.Commit) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits[repository][commit.ID] = commit
}

// setObjects sets the objects of one ref (a commit ID in every test here).
func (f *fake) setObjects(repository, ref string, objects []client.ObjectStat) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setObjectsLocked(repository, ref, objects)
}

func (f *fake) setObjectsLocked(repository, ref string, objects []client.ObjectStat) {
	if f.objects[repository] == nil {
		f.objects[repository] = make(map[string][]client.ObjectStat)
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
func (f *fake) setDiff(repository, left, right string, changes []client.Change) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.diffs[repository] == nil {
		f.diffs[repository] = make(map[string][]client.Change)
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

// setDataBranchHook installs the hook that enterDataBranch runs for the head read of a data branch.
func (f *fake) setDataBranchHook(hook func(branch string) (done func())) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dataBranchHook = hook
}

// enterDataBranch runs the concurrency probe, when one is installed, for a head read of a branch of
// a data repository. Reads of the metadata repository are excluded: they belong to a branch tick
// that is already running, they do not start one.
func (f *fake) enterDataBranch(repository, branch string) func() {
	f.mu.Lock()
	hook := f.dataBranchHook
	metadata := f.metadataRepo
	f.mu.Unlock()
	if hook == nil || repository == metadata {
		return nil
	}
	return hook(branch)
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

// madeCommits returns the lakeFS commits the indexer made.
func (f *fake) madeCommits() []client.CommitOptions {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.commitsMade)
}

// Repository reads.

func (f *fake) GetRepository(_ context.Context, repository string) (client.Repository, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	found, ok := f.repositories[repository]
	if !ok {
		return client.Repository{}, &client.NotFoundError{Operation: "get repository", Subject: repository}
	}
	return found, nil
}

func (f *fake) CreateRepository(_ context.Context, opts client.RepositoryOptions) (client.Repository, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.repositories[opts.Name]; ok {
		return client.Repository{}, fmt.Errorf("create repository %q: %w", opts.Name, client.ErrConflict)
	}
	branch := opts.DefaultBranch
	if branch == "" {
		branch = "main"
	}
	f.addRepositoryLocked(opts.Name, opts.StorageNamespace, branch)
	// lakeFS creates a non-bare repository: its default branch starts with the automatic
	// "Repository created" root commit. The indexer's initialisation commit is the first commit
	// after it, never the root (design §4.8).
	root := client.Commit{
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

func (f *fake) ListBranches(_ context.Context, repository, after string) ([]client.Branch, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	names := make([]string, 0, len(f.branches[repository]))
	for name := range f.branches[repository] {
		names = append(names, name)
	}
	slices.Sort(names)
	return pageOf(names, after, f.pageSize, func(name string) string { return name }, func(name string) client.Branch {
		return client.Branch{Name: name, CommitID: f.branches[repository][name]}
	})
}

func (f *fake) CreateBranch(_ context.Context, repository, branch, source string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.branches[repository][branch]; ok {
		return fmt.Errorf("create branch %q: %w", branch, client.ErrConflict)
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
	if _, ok := f.branches[repository][branch]; !ok {
		return &client.NotFoundError{Operation: "delete branch", Subject: branch}
	}
	if f.repositories[repository].DefaultBranch == branch {
		// Real lakeFS refuses to delete a repository's default branch: pkg/graveler/graveler.go
		// DeleteBranch returns ErrDeleteDefaultBranch before touching the branch. Model that so an
		// indexer that tries it fails the test instead of silently "succeeding".
		return fmt.Errorf("delete branch %q: default branch: %w", branch, client.ErrBadRequest)
	}
	delete(f.branches[repository], branch)
	delete(f.staged, branch)
	f.deletedBranches = append(f.deletedBranches, branch)
	return nil
}

func (f *fake) ResetBranch(_ context.Context, repository, branch string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.staged, branch)
	f.resetBranches = append(f.resetBranches, branch)
	f.events = append(f.events, "reset:"+branch)
	return nil
}

func (f *fake) HardResetBranch(_ context.Context, repository, branch, ref string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
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

func (f *fake) FindMergeBase(_ context.Context, repository, sourceRef, destinationRef string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	source, err := f.resolveLocked(repository, sourceRef)
	if err != nil {
		return "", err
	}
	destination, err := f.resolveLocked(repository, destinationRef)
	if err != nil {
		return "", err
	}
	reached := f.ancestorsLocked(repository, destination)
	queue := []string{source}
	seen := map[string]bool{}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if seen[current] {
			continue
		}
		seen[current] = true
		if reached[current] {
			return current, nil
		}
		queue = append(queue, f.commits[repository][current].Parents...)
	}
	// graveler.FindMergeBase returns ErrNoMergeBase for unrelated histories; pkg/api turns that
	// into a 500. The fake reports the same typed error the client must produce.
	return "", client.ErrNoMergeBase
}

func (f *fake) ancestorsLocked(repository, commit string) map[string]bool {
	reached := map[string]bool{}
	queue := []string{commit}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if current == "" || reached[current] {
			continue
		}
		reached[current] = true
		queue = append(queue, f.commits[repository][current].Parents...)
	}
	return reached
}

// Commit reads and writes.

func (f *fake) LogCommits(_ context.Context, repository, ref string, opts client.LogCommitsOptions) ([]client.Commit, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	head, err := f.resolveLocked(repository, ref)
	if err != nil {
		return nil, false, err
	}
	var chain []client.Commit
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
	if opts.After != "" {
		for i, commit := range chain {
			if commit.ID == opts.After {
				chain = chain[i+1:]
				break
			}
		}
	}
	return pageOf(chain, "", f.pageSize, func(commit client.Commit) string { return commit.ID },
		func(commit client.Commit) client.Commit { return commit })
}

func (f *fake) GetCommit(_ context.Context, repository, commitID string) (client.Commit, error) {
	if done := f.enterDataBranch(repository, commitID); done != nil {
		defer done()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	id, err := f.resolveLocked(repository, commitID)
	if err != nil {
		return client.Commit{}, err
	}
	commit, ok := f.commits[repository][id]
	if !ok {
		return client.Commit{}, &client.NotFoundError{Operation: "get commit", Subject: commitID}
	}
	return commit, nil
}

func (f *fake) Commit(_ context.Context, repository, branch string, opts client.CommitOptions) (client.Commit, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var state *fakeState
	if repository == f.metadataRepo {
		staged, ok := f.staged[branch]
		switch {
		case ok:
			state = staged
		case !opts.AllowEmpty:
			return client.Commit{}, fmt.Errorf("commit %q@%s: nothing staged: %w", repository, branch, client.ErrBadRequest)
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
	commit := client.Commit{
		ID:           fmt.Sprintf("index-%d", f.index),
		Message:      opts.Message,
		Parents:      parents,
		Committer:    "indexer",
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

func (f *fake) ListTags(_ context.Context, repository, after string) ([]client.Tag, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	names := make([]string, 0, len(f.tags[repository]))
	for name := range f.tags[repository] {
		names = append(names, name)
	}
	slices.Sort(names)
	return pageOf(names, after, f.pageSize, func(name string) string { return name }, func(name string) client.Tag {
		return client.Tag{Name: name, CommitID: f.tags[repository][name]}
	})
}

func (f *fake) CreateTag(_ context.Context, repository, tag, ref string, force bool) (client.Tag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Like lakeFS, force never overwrites: pkg/graveler/graveler.go CreateTag uses it only to
	// pass the read-only check, then returns ErrTagAlreadyExists (409) for any existing tag.
	if _, ok := f.tags[repository][tag]; ok {
		return client.Tag{}, fmt.Errorf("create tag %q: %w", tag, client.ErrConflict)
	}
	commit, err := f.resolveLocked(repository, ref)
	if err != nil {
		return client.Tag{}, err
	}
	if f.tags[repository] == nil {
		f.tags[repository] = make(map[string]string)
	}
	f.tags[repository][tag] = commit
	f.tagsMade = append(f.tagsMade, tagCreation{repository: repository, tag: tag, ref: ref})
	return client.Tag{Name: tag, CommitID: commit}, nil
}

func (f *fake) GetTag(_ context.Context, repository, tag string) (client.Tag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	commit, ok := f.tags[repository][tag]
	if !ok {
		return client.Tag{}, &client.NotFoundError{Operation: "get tag", Subject: tag}
	}
	return client.Tag{Name: tag, CommitID: commit}, nil
}

func (f *fake) DeleteTag(_ context.Context, repository, tag string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.tags[repository][tag]; !ok {
		return &client.NotFoundError{Operation: "delete tag", Subject: tag}
	}
	delete(f.tags[repository], tag)
	f.deletedTags = append(f.deletedTags, tag)
	return nil
}

// Object reads.

func (f *fake) ListObjects(_ context.Context, repository, ref, after string) ([]client.ObjectStat, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCalls++
	objects, ok := f.objects[repository][ref]
	if !ok {
		return nil, false, &client.NotFoundError{Operation: "list objects", Subject: ref}
	}
	sorted := slices.Clone(objects)
	slices.SortFunc(sorted, func(a, b client.ObjectStat) int { return cmp.Compare(a.Path, b.Path) })
	return pageOf(sorted, after, f.pageSize, func(object client.ObjectStat) string { return object.Path },
		func(object client.ObjectStat) client.ObjectStat { return object })
}

func (f *fake) DiffRefs(_ context.Context, repository, leftRef, rightRef, after string) ([]client.Change, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.diffCalls++
	changes, ok := f.diffs[repository][leftRef+".."+rightRef]
	if !ok {
		return nil, false, &client.NotFoundError{Operation: "diff refs", Subject: leftRef + ".." + rightRef}
	}
	sorted := slices.Clone(changes)
	slices.SortFunc(sorted, func(a, b client.Change) int { return cmp.Compare(a.Path, b.Path) })
	return pageOf(sorted, after, f.pageSize, func(change client.Change) string { return change.Path },
		func(change client.Change) client.Change { return change })
}

// DiffBranch reports the uncommitted changes of a metadata branch. A staged table pointer is
// itself an object change on the branch, so any staged state is at least one change.
func (f *fake) DiffBranch(_ context.Context, repository, branch, after string) ([]client.Change, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.branches[repository][branch]; !ok {
		return nil, false, &client.NotFoundError{Operation: "diff branch", Subject: branch}
	}
	staged, ok := f.staged[branch]
	if !ok {
		return nil, false, nil
	}
	committed := f.tableAt[f.branches[repository][branch]]
	var changes []client.Change
	for path, row := range staged.rows {
		old, ok := committedRow(committed, path)
		switch {
		case !ok:
			changes = append(changes, client.Change{Path: path, Type: client.ChangeAdded, SizeBytes: row.SizeBytes})
		case !reflect.DeepEqual(old, row):
			changes = append(changes, client.Change{Path: path, Type: client.ChangeChanged, SizeBytes: row.SizeBytes})
		}
	}
	if committed != nil {
		for path := range committed.rows {
			if _, ok := staged.rows[path]; !ok {
				changes = append(changes, client.Change{Path: path, Type: client.ChangeRemoved})
			}
		}
	}
	if len(changes) == 0 {
		changes = append(changes, client.Change{
			Path: "_lakefs_iceberg/tables/system/object_metadata",
			Type: client.ChangeChanged,
		})
	}
	slices.SortFunc(changes, func(a, b client.Change) int { return cmp.Compare(a.Path, b.Path) })
	return pageOf(changes, after, f.pageSize, func(change client.Change) string { return change.Path },
		func(change client.Change) client.Change { return change })
}

func committedRow(state *fakeState, path string) (table.Row, bool) {
	if state == nil {
		return table.Row{}, false
	}
	row, ok := state.rows[path]
	return row, ok
}

func (f *fake) StatObject(_ context.Context, repository, ref, path string) (client.ObjectStat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statCalls = append(f.statCalls, path)
	for _, object := range f.objects[repository][ref] {
		if object.Path == path {
			return object, nil
		}
	}
	return client.ObjectStat{}, &client.NotFoundError{Operation: "stat object", Subject: path}
}

// Metadata table reads and writes.

func (f *fake) LoadTable(_ context.Context, _, branch string) (indexer.Table, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if state, ok := f.staged[branch]; ok {
		return &fakeTable{f: f, branch: branch, state: state.clone()}, nil
	}
	head, ok := f.branches[f.metadataRepo][branch]
	if !ok || head == "" {
		return nil, indexer.ErrTableNotFound
	}
	state, ok := f.tableAt[head]
	if !ok {
		return nil, indexer.ErrTableNotFound
	}
	return &fakeTable{f: f, branch: branch, state: state.clone()}, nil
}

func (f *fake) CreateTable(_ context.Context, _, branch string) (indexer.Table, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tableExists {
		return nil, indexer.ErrTableExists
	}
	f.tableExists = true
	state := newState()
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
			return "", &client.NotFoundError{Operation: "resolve", Subject: ref}
		}
		return commit, nil
	}
	if commit, ok := f.tags[repository][ref]; ok {
		return commit, nil
	}
	return "", &client.NotFoundError{Operation: "resolve", Subject: ref}
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
func (t *fakeTable) SnapshotCount() int   { return t.state.snapshots }
func (t *fakeTable) DeleteFiles() int     { return t.state.deleteFiles }

func (t *fakeTable) ApplyDelta(_ context.Context, upserts []table.Row, deletes []string, sourceCommit string) error {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
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
func object(path string, size int64) client.ObjectStat {
	return client.ObjectStat{
		Path:         path,
		SizeBytes:    size,
		Mtime:        time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC),
		ETag:         "etag-" + path,
		ContentType:  "text/plain",
		UserMetadata: map[string]string{"X-Amz-Meta-Owner": "alice"},
	}
}

// added is a diff entry that adds an object.
func added(path string, size int64) client.Change {
	stat := object(path, size)
	return client.Change{Path: path, Type: client.ChangeAdded, SizeBytes: size, Right: &stat}
}

// removed is a diff entry that removes an object.
func removed(path string) client.Change {
	return client.Change{Path: path, Type: client.ChangeRemoved}
}
