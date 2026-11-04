// Package indexer is the core of the Metadata Search indexer
// (METADATA_SEARCH_DESIGN.md §3.2, §3.3, §4.2–§4.5, §4.7–§4.9).
//
// For every configured data repository R it keeps the companion repository R-metadata in sync
// with the commits of R: R-metadata holds one branch per indexed branch of R, that branch holds
// the Iceberg table system.object_metadata, and the position of the mirror is the
// lakefs.mds.source-commit metadata of the head commit of R-metadata@b (§3.3). There is no state
// file: the position moves atomically with the table, because both are in the same lakeFS commit
// (§4.3 steps 3 and 4).
//
// The package is the second half of a REST client. It never imports pkg/catalog or pkg/graveler:
// every operation on R and on R-metadata goes through pkg/mds/client (M2), and every operation on
// the table goes through the TableStore seam, whose production implementation (IcebergStore, in
// iceberg.go) talks to the lakeFS Iceberg REST catalog through pkg/mds/table (M3). Both seams are
// interfaces, so this package's tests drive every crash window of §4.4 and every state machine of
// §4.5, §4.7 and §4.8 with fakes and without a lakeFS server.
//
// A tick is started by *mds.Service (design §4.1); see Tick.
//
// Clean-room note: upstream Metadata Search is a lakeFS Enterprise feature. This is an independent
// OSS implementation written from METADATA_SEARCH_DESIGN.md, the public Metadata Search guide
// (https://docs.lakefs.io/guides/metadata-search/) and the Apache Iceberg specs only. No upstream
// Enterprise code was used.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/icebergcatalog"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/client"
)

const (
	// TagPrefix names the tag of R-metadata that records an indexed source commit (§3.3, §4.3
	// step 5). Tag mds-<C> points at the lakeFS commit of R-metadata that applied commit C of R,
	// which is what makes a commit or a tag of R addressable through the virtual table (§6).
	TagPrefix = "mds-"

	// MetadataKeyInit marks the initialisation commit of R-metadata (§4.8). Its presence is what
	// distinguishes a companion repository this indexer created from one an operator created by
	// hand, which the indexer refuses to touch.
	MetadataKeyInit = "lakefs.mds.init"

	// MetadataKeyInitValue is the value MetadataKeyInit carries.
	MetadataKeyInitValue = "true"

	// MetadataKeySourceCommit is the commit of R the commit of R-metadata recorded (§3.3). On the
	// head commit of R-metadata@b it is the indexing position of branch b.
	MetadataKeySourceCommit = "lakefs.mds.source-commit"

	// MetadataKeySourceRepository is the data repository the recorded commit belongs to (§3.3).
	MetadataKeySourceRepository = "lakefs.mds.source-repository"

	// InitCommitMessage is the message of the initialisation commit of R-metadata (§4.8).
	InitCommitMessage = "mds: init"

	// IndexCommitMessage is the message of the lakeFS commit that records one indexed commit
	// (§3.3: "mds: index <R>@<commit>").
	IndexCommitMessage = "mds: index %s@%s"

	// CompactCommitMessage is the message of the lakeFS commit that records one compaction (§4.9:
	// "mds: compact <R>@<C>").
	CompactCommitMessage = "mds: compact %s@%s"

	// BackfillHead indexes a branch at its head when it is first indexed (§4.5 step 2). It is
	// the documented default of metadata_search.backfill.
	BackfillHead = "head"

	// BackfillHistory starts at the branch's oldest first-parent commit (§4.5 step 2).
	BackfillHistory = "history"
)

// The stages the indexer reports failures with, as errors_total{repository,stage} (§9). The
// values are the labels pkg/mds declares, spelled here because this package must not import it.
const (
	// StageRepository is a repository-level failure: the repository is missing, R-metadata
	// cannot be created, its storage namespace cannot be derived or overlaps R's, or a branch of
	// it could not be listed.
	StageRepository = "repository"

	// StageBranch is a failure while indexing one branch (§4.2).
	StageBranch = "branch"
)

// The defaults of an Options field that is left zero. They are the documented defaults of
// metadata_search (§7); *mds.Service always passes the validated configuration, so these only
// matter to a caller that builds an Indexer directly.
const (
	DefaultConcurrency      = 1
	DefaultMaxCommits       = 1000
	DefaultSeedLookback     = 1000
	DefaultRebuildThreshold = 100_000
	DefaultMaxDeleteFiles   = 200
	DefaultEveryCommits     = 500
)

// conflictRetries bounds how often one branch tick restarts after the Iceberg commit answered 409
// (§4.4: "A 409 from the Iceberg commit means another writer moved the table: reload, re-run
// recovery, continue"). One restart is enough for the documented cause - a second indexer replica
// that is one commit ahead - and a bounded number keeps a foreign writer from spinning a tick.
const conflictRetries = 1

// The errors of this package. They are static so that a caller can match them with errors.Is and
// so that no message is built from a value the caller cannot see.
var (
	// ErrTableNotFound is reported by a TableStore for a branch of R-metadata that holds no
	// table. It is a normal answer: §4.5 indexes such a branch from scratch.
	ErrTableNotFound = errors.New("the metadata table does not exist on the branch")

	// ErrConflict is reported by a TableStore when the Iceberg commit was refused because the
	// table moved. §4.4 makes the indexer reload the table and re-run recovery.
	ErrConflict = errors.New("the metadata table was moved by another writer")

	// ErrForeignRepository is reported for a companion repository that carries no initialisation
	// commit (§4.8). The indexer never writes into a repository it did not create.
	ErrForeignRepository = errors.New("the companion repository was not created by the indexer")

	// ErrBucketRootNamespace is reported for a data repository whose storage namespace is a bucket
	// root and whose metadata repository has no explicit storage_namespace (§3.3): deriving
	// "-metadata" would need a path component that does not exist.
	ErrBucketRootNamespace = errors.New("a bucket-root storage namespace cannot be derived, set storage_namespace")

	// ErrOverlappingNamespace is reported when R-metadata's storage namespace is inside R's or
	// contains it (§3.3): R's uncommitted GC sweeps every unreferenced object under R's prefix and
	// would delete R-metadata's files.
	ErrOverlappingNamespace = errors.New("the metadata storage namespace overlaps the data repository storage namespace")

	// ErrUnknownChangeType is reported for a diff entry whose type is neither added, changed nor
	// removed. Refusing it is what keeps a new API value from silently losing a change (§4.3).
	ErrUnknownChangeType = errors.New("unknown diff change type")

	// ErrDataRepositoryNamedMetadata is reported for a configured data repository whose own name
	// ends in "-metadata" (§3.3): the catalog would treat it as a companion repository, and its
	// own companion would be <name>-metadata-metadata.
	ErrDataRepositoryNamedMetadata = errors.New("a data repository name must not end in the companion repository suffix")

	// ErrNoEndpoint is reported by New when it has to build a client and no endpoint is given.
	ErrNoEndpoint = errors.New("no lakeFS endpoint")

	// ErrNoDefaultBranch is reported for a companion repository without a default branch: the
	// indexer cannot address a table, a commit or an initialisation without one.
	ErrNoDefaultBranch = errors.New("the companion repository has no default branch")

	// ErrNoCommit is reported by rootCommit for a branch that has no commit at all, which is not
	// the state of an initialised companion repository.
	ErrNoCommit = errors.New("the branch has no commit")
)

// Metrics is the metric sink of the indexer (§9). *mds.Recorder implements it; the interface is
// declared here so that this package does not import pkg/mds.
type Metrics interface {
	IndexedCommit(repository string)
	SetLag(repository, branch string, commits int64)
	ClearLag(repository, branch string)
	Error(repository, stage string)
	RowsWritten(rows int64)
	Rebuild()
	Compaction()
}

// Health is the /_health sink of the indexer (§9). *mds.Health implements it.
type Health interface {
	BeginTick(repository string)
	FinishTick(repository string, err error)
	SetBranchState(repository, branch, lastIndexed, head string, lagCommits int64, err error)
}

// Repository is one configured data repository (§7): its name, the branch selectors to index and
// the optional storage namespace override of R-metadata (§3.3).
type Repository struct {
	Name string

	// Branches are the selectors of §4.1: exact branch names, or a prefix followed by a single
	// trailing "*".
	Branches []string

	// StorageNamespace overrides the derived storage namespace of R-metadata (§3.3). Empty means
	// "derive it from R's namespace".
	StorageNamespace string
}

// Compaction is the maintenance configuration of metadata_search.compaction (§4.9).
type Compaction struct {
	// MaxDeleteFiles is the number of position-delete files of a branch's table above which the
	// table is compacted.
	MaxDeleteFiles int

	// EveryCommits is the number of indexed commits after which a branch's table is compacted.
	EveryCommits int
}

// Options configures an Indexer. The zero value of every optional field means the documented
// default of §7.
type Options struct {
	// LakeFS is the lakeFS client of the indexer (§4.2–§4.9). When nil, New builds one from
	// Endpoint, AccessKeyID and SecretKey.
	LakeFS client.LakeFS

	// Tables reads and writes the metadata tables (§4.3, §4.9). When nil, New builds an
	// IcebergStore from Endpoint, AccessKeyID, SecretKey and Storage.
	Tables TableStore

	// Endpoint is the lakeFS base URL (metadata_search.lakefs_mds_endpoint): the API lives at
	// /api/v1 and the catalog at /iceberg (§4.1, §4.3 step 3).
	Endpoint string

	// AccessKeyID and SecretKey are the credentials of the indexer principal (§8). They are sent
	// as HTTP basic authentication to both the API and the catalog.
	AccessKeyID string
	SecretKey   string

	// Storage holds the iceberg-go IO properties used when the catalog does not vend write
	// credentials (§4.6), e.g. s3.endpoint and s3.access-key-id.
	Storage map[string]string

	// Concurrency is how many branches one repository tick processes in parallel (§4.1).
	Concurrency int

	// MaxCommits bounds the number of source commits one branch applies per tick (§4.2).
	MaxCommits int

	// Since bounds a backfill of a branch: only commits created at or after it are listed (§4.5
	// step 2). Nil means "no lower bound".
	Since *time.Time

	// Backfill is BackfillHead (default) or BackfillHistory (§4.5 step 2).
	Backfill string

	// SeedLookback bounds how far back the first indexing of a branch looks for an indexed
	// ancestor to seed from (§4.5 step 1).
	SeedLookback int

	// RebuildThreshold is the number of upserts plus deletes above which a commit is indexed by a
	// full rebuild of the branch's table instead of a delta (§4.3 step 2).
	RebuildThreshold int

	// DeleteBranches mirrors the deletion of a matching branch to R-metadata (§4.8).
	DeleteBranches bool

	// TagRetention is how long mds-* tags are kept; 0 keeps them forever (§4.9).
	TagRetention time.Duration

	// Compaction configures the table maintenance of §4.9.
	Compaction Compaction

	// Repositories are the data repositories to index (§7).
	Repositories []Repository

	// Log receives the indexer's log lines. Nil means the process logger without fields.
	Log logging.Logger

	// Health and Metrics receive the state and the counters of §9. Nil means "do not report".
	Health  Health
	Metrics Metrics
}

// metadataState is what one repository tick learns about R-metadata that a later step of the same
// tick needs: its default branch and the commit that initialised it (§4.8). It is cached on the
// Indexer because both are properties of the repository, not of a branch, and a tick processes
// many branches.
type metadataState struct {
	// defaultBranch is the branch R-metadata was created with: the default branch of R (§3.3).
	defaultBranch string

	// initCommit is the commit of R-metadata that created it (§4.8). Tag and branch reads of an
	// older position need it for a full rebuild (§4.5 step 2, §4.7).
	initCommit string

	// initialised reports that R-metadata was verified to be one this indexer created (§4.8).
	// It is checked once per process: the refusal is about a repository that was not created by
	// the indexer, not about a repository that changes owner while the indexer runs.
	initialised bool
}

// Indexer is the Metadata Search indexer of design §4. It is safe for concurrent use: one tick
// processes the branches of a repository in parallel, and the health and metrics sinks are
// concurrency-safe.
type Indexer struct {
	lakeFS  client.LakeFS
	tables  TableStore
	health  Health
	metrics Metrics
	log     logging.Logger

	concurrency      int
	maxCommits       int
	since            *time.Time
	backfill         string
	seedLookback     int
	rebuildThreshold int
	deleteBranches   bool
	tagRetention     time.Duration
	compaction       Compaction
	repositories     []Repository

	mu          sync.Mutex
	metadata    map[string]metadataState
	maintenance map[string]int
}

// New returns an Indexer for opts. When opts.LakeFS or opts.Tables is nil it builds the production
// implementation from the endpoint and the credentials of §4.6; a test passes fakes instead.
func New(opts Options) (*Indexer, error) {
	x := &Indexer{
		lakeFS:           opts.LakeFS,
		tables:           opts.Tables,
		health:           opts.Health,
		metrics:          opts.Metrics,
		log:              opts.Log,
		concurrency:      opts.Concurrency,
		maxCommits:       opts.MaxCommits,
		since:            opts.Since,
		backfill:         opts.Backfill,
		seedLookback:     opts.SeedLookback,
		rebuildThreshold: opts.RebuildThreshold,
		deleteBranches:   opts.DeleteBranches,
		tagRetention:     opts.TagRetention,
		compaction:       opts.Compaction,
		repositories:     append([]Repository(nil), opts.Repositories...),
		metadata:         make(map[string]metadataState, len(opts.Repositories)),
		maintenance:      make(map[string]int),
	}
	x.applyDefaults()

	if x.log == nil {
		x.log = logging.ContextUnavailable()
	}
	if x.health == nil {
		x.health = nopHealth{}
	}
	if x.metrics == nil {
		x.metrics = nopMetrics{}
	}
	if x.lakeFS == nil || x.tables == nil {
		endpoint := strings.TrimSpace(opts.Endpoint)
		if endpoint == "" {
			return nil, fmt.Errorf("%w: cannot build the lakeFS client of the indexer", ErrNoEndpoint)
		}
		base, err := apiBaseURL(endpoint)
		if err != nil {
			return nil, err
		}
		if x.lakeFS == nil {
			lakeFS, err := newLakeFSClient(base, opts.AccessKeyID, opts.SecretKey)
			if err != nil {
				return nil, err
			}
			x.lakeFS = lakeFS
		}
		if x.tables == nil {
			x.tables = &IcebergStore{
				Endpoint:        base,
				AccessKeyID:     opts.AccessKeyID,
				SecretAccessKey: opts.SecretKey,
				Storage:         iceberg.Properties(opts.Storage),
			}
		}
	}
	return x, nil
}

// applyDefaults fills the zero values of the configuration with the documented defaults of §7.
func (x *Indexer) applyDefaults() {
	if x.concurrency <= 0 {
		x.concurrency = DefaultConcurrency
	}
	if x.maxCommits <= 0 {
		x.maxCommits = DefaultMaxCommits
	}
	if x.seedLookback <= 0 {
		x.seedLookback = DefaultSeedLookback
	}
	if x.rebuildThreshold <= 0 {
		x.rebuildThreshold = DefaultRebuildThreshold
	}
	if x.compaction.MaxDeleteFiles <= 0 {
		x.compaction.MaxDeleteFiles = DefaultMaxDeleteFiles
	}
	if x.compaction.EveryCommits <= 0 {
		x.compaction.EveryCommits = DefaultEveryCommits
	}
	if x.backfill == "" {
		x.backfill = BackfillHead
	}
}

// Tick is one tick of the indexer (§4.1): it visits every configured repository, brings every
// matching branch of it up to date and maintains the tables.
//
// It never fails as a whole: a repository-level failure is recorded on the Health sink of that
// repository and does not stop the other repositories (§7: "Per-repository runtime errors ...
// mark that repository failed in /_health and metrics, and the others continue"), and a cancelled
// tick is the in-flight work of a graceful stop (§4.1). The error return is the one mds.TickFunc
// has, for a future whole-tick failure such as a broken configuration.
func (x *Indexer) Tick(ctx context.Context) error {
	for _, repository := range x.repositories {
		if err := ctx.Err(); err != nil {
			return nil
		}
		x.tickRepository(ctx, repository)
	}
	return nil
}

// tickRepository is one repository of a tick: the health state machine of §9 around
// indexRepository.
func (x *Indexer) tickRepository(ctx context.Context, repository Repository) {
	x.health.BeginTick(repository.Name)
	err := x.indexRepository(ctx, repository)
	if ctx.Err() != nil {
		// The in-flight work of a graceful stop is not a failure (§4.1): the next process starts
		// from the position the last lakeFS commit recorded.
		return
	}
	if err != nil {
		x.metrics.Error(repository.Name, StageRepository)
		x.log.WithError(err).WithField("repository", repository.Name).
			Warn("mds: repository skipped")
	}
	x.health.FinishTick(repository.Name, err)
}

// indexRepository brings every matching branch of one repository up to date and runs the
// repository-wide maintenance of §4.8 and §4.9.
func (x *Indexer) indexRepository(ctx context.Context, repository Repository) error {
	metadata, err := x.ensureMetadataRepository(ctx, repository)
	if err != nil {
		return err
	}

	branches, err := x.listBranches(ctx, repository.Name)
	if err != nil {
		return err
	}
	matching := selectBranches(repository.Branches, branches)

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, x.concurrency)
	for _, branch := range matching {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		semaphore <- struct{}{}
		go func(branch string) {
			defer wg.Done()
			defer func() { <-semaphore }()
			x.indexBranch(ctx, repository.Name, branch)
		}(branch)
	}
	wg.Wait()

	if x.deleteBranches {
		if err := x.retireDeletedBranches(ctx, repository.Name, metadata, branches); err != nil {
			return err
		}
	}
	return x.retireTags(ctx, repository.Name)
}

// indexBranch indexes one branch and reports its state on /_health and /metrics (§9). Branch
// failures stay inside the branch: the next tick resumes from the recorded position (§4.2 step 6).
func (x *Indexer) indexBranch(ctx context.Context, repository, branch string) {
	state := branchState{}
	err := x.runBranch(ctx, repository, branch, &state, conflictRetries)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		x.metrics.Error(repository, StageBranch)
		x.log.WithError(err).WithFields(logging.Fields{
			"repository": repository,
			"branch":     branch,
			"last":       state.last,
			"head":       state.head,
		}).Warn("mds: branch not indexed")
	}
	x.metrics.SetLag(repository, branch, state.lag)
	x.health.SetBranchState(repository, branch, state.last, state.head, state.lag, err)
}

// branchState is the progress of one branch of one tick: what /_health reports (§9).
type branchState struct {
	// last is the source commit the branch's table is at, empty when it has no indexed commit.
	last string

	// head is the source commit the tick saw as the branch head.
	head string

	// lag is the number of first-parent commits of R@b that are not indexed yet.
	lag int64
}

// listBranches pages through every branch of a repository.
func (x *Indexer) listBranches(ctx context.Context, repository string) ([]string, error) {
	var names []string
	after := ""
	for {
		branches, hasMore, err := x.lakeFS.ListBranches(ctx, repository, after)
		if err != nil {
			return nil, fmt.Errorf("list branches of %q: %w", repository, err)
		}
		for _, branch := range branches {
			names = append(names, branch.Name)
		}
		if !hasMore {
			return names, nil
		}
		after = lastNameOf(len(branches), func(i int) string { return branches[i].Name })
		if after == "" {
			return nil, fmt.Errorf("%w: list branches of %q: more results without a cursor", client.ErrBadPagination, repository)
		}
	}
}

// selectBranches returns the branches of names that the selectors of §4.1 match: an exact name, or
// a prefix followed by a single trailing "*". A "*" anywhere else matches nothing, which is what
// config validation already refuses.
func selectBranches(selectors []string, branches []string) []string {
	selected := make([]string, 0, len(branches))
	for _, branch := range branches {
		for _, selector := range selectors {
			if matchBranch(selector, branch) {
				selected = append(selected, branch)
				break
			}
		}
	}
	return selected
}

// matchBranch reports whether branch matches one selector of §4.1.
func matchBranch(selector, branch string) bool {
	if prefix, ok := strings.CutSuffix(selector, "*"); ok {
		return strings.HasPrefix(branch, prefix)
	}
	return selector == branch
}

// lastNameOf returns the cursor of the page that follows a listing: the name or path of its last
// item, which is the cursor pkg/mds/client documents. It is empty for an empty page, which is what
// makes a "more results" answer without an item an error instead of a loop.
func lastNameOf(count int, name func(int) string) string {
	if count == 0 {
		return ""
	}
	return name(count - 1)
}

// apiBaseURL returns the lakeFS API base URL of an endpoint: the endpoint with its path reduced to
// /api/v1, so that an endpoint that already carries the API path is not doubled.
func apiBaseURL(endpoint string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("%w: endpoint %q: %w", ErrNoEndpoint, endpoint, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("%w: endpoint %q is not an absolute URL", ErrNoEndpoint, endpoint)
	}
	path := strings.TrimRight(parsed.Path, "/")
	path = strings.TrimSuffix(path, apiutil.BaseURL)
	parsed.Path = strings.TrimRight(path, "/")
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/") + apiutil.BaseURL, nil
}

// newLakeFSClient builds the typed lakeFS client of pkg/mds/client over the generated client,
// authenticated as the indexer principal with HTTP basic (§8).
func newLakeFSClient(baseURL, accessKeyID, secretKey string) (client.LakeFS, error) {
	api, err := apigen.NewClientWithResponses(baseURL, apigen.WithRequestEditorFn(
		func(_ context.Context, req *http.Request) error {
			req.SetBasicAuth(accessKeyID, secretKey)
			return nil
		}))
	if err != nil {
		return nil, fmt.Errorf("build the lakeFS client of %q: %w", baseURL, err)
	}
	return client.New(api, 0), nil
}

// metadataRepository returns the name of the companion repository of a data repository (§3.3),
// spelled the way pkg/icebergcatalog spells it so that the catalog and the indexer cannot drift.
func metadataRepository(repository string) string {
	return icebergcatalog.MetadataRepository(repository)
}

// tagName returns the tag of R-metadata that records source commit commit (§3.3).
func tagName(commit string) string {
	return TagPrefix + commit
}

// sourceMetadata returns the commit metadata of §3.3: the source commit and its repository.
func sourceMetadata(repository, commit string) map[string]string {
	return map[string]string{
		MetadataKeySourceCommit:     commit,
		MetadataKeySourceRepository: repository,
	}
}

// metaInfo returns what this tick knows about the companion repository of a data repository.
func (x *Indexer) metaInfo(repository string) metadataState {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.metadata[repository]
}

// setMetaInfo records what this tick learned about the companion repository of a data repository.
func (x *Indexer) setMetaInfo(repository string, state metadataState) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.metadata[repository] = state
}

// closeTable closes a table handle, logging a failure to release it: the handle's writes are
// already durable, so a close error is worth a line and not a failed tick.
func (x *Indexer) closeTable(tbl Table, repository, branch string) {
	if tbl == nil {
		return
	}
	if err := tbl.Close(); err != nil {
		x.log.WithError(err).WithFields(logging.Fields{
			"repository": repository,
			"branch":     branch,
		}).Debug("mds: cannot close the metadata table")
	}
}

// nopMetrics is the Metrics sink of an Indexer built without one.
type nopMetrics struct{}

func (nopMetrics) IndexedCommit(string)         {}
func (nopMetrics) SetLag(string, string, int64) {}
func (nopMetrics) ClearLag(string, string)      {}
func (nopMetrics) Error(string, string)         {}
func (nopMetrics) RowsWritten(int64)            {}
func (nopMetrics) Rebuild()                     {}
func (nopMetrics) Compaction()                  {}

// nopHealth is the Health sink of an Indexer built without one.
type nopHealth struct{}

func (nopHealth) BeginTick(string)         {}
func (nopHealth) FinishTick(string, error) {}
func (nopHealth) SetBranchState(string, string, string, string, int64, error) {
}
