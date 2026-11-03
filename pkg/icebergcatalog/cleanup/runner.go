package cleanup

// runner.go is the entry point of the package: it validates a run's configuration, reads
// the repository, and runs the phases in order.
//
// The phase order is the one the Enterprise job exposes and the one an operator expects:
//
//	expire      rewrite every branch's table metadata, staging the result
//	mark        decide which registrations no live reference protects
//	sweep       delete the marked registrations, after checking the references again
//
// --mark-only stops after mark; --sweep-only runs only sweep against an earlier mark's ID.
// A plain run does all three. --dry-run computes everything and writes nothing at all: no
// metadata file, no staged pointer, no deleted registration, no report. A dry run
// therefore cannot be resumed by --sweep-only, which is the point - it is a rehearsal, not
// a mark.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

const (
	// DefaultMaxSnapshotAge is the snapshot retention of a run that neither the command
	// line nor the table properties override: 5 days, the default of the Enterprise job
	// and of Iceberg's own history.expire.max-snapshot-age-ms.
	DefaultMaxSnapshotAge = 5 * 24 * time.Hour

	// DefaultMinSnapshotsToKeep is the number of most recent snapshots a run keeps
	// whatever their age. Two allows a reader that resolves the previous snapshot to keep
	// working while a new one is written, which is why it is the command's default.
	DefaultMinSnapshotsToKeep = 2

	// DefaultSafetyAge is how old a table file must be before the sweep may delete its
	// registration: 72 hours. It covers the window between a client writing files for a
	// snapshot and the catalog staging that snapshot's entries, the same window the
	// uncommitted-GC grace closes on the server (pkg/icebergcatalog/gc.go).
	DefaultSafetyAge = 72 * time.Hour

	// hoursPerDay is the number of hours in a day, used to quote the snapshot retention in
	// the report in the unit the command line takes.
	hoursPerDay = 24

	// MinSafetyAge is the smallest safety age a run accepts: 12 hours. Allowing a smaller
	// value would make it easy to delete files of an in-flight writer, and no supported
	// client holds a committed snapshot's files for less than that.
	MinSafetyAge = 12 * time.Hour
)

// ErrEmbeddedManifests is returned when the repository contains a v1 table whose snapshots
// embed their manifest locations. The expire phase refuses to rewrite such metadata (see
// planExpire) and aborts the whole run before it writes anything, so a repository is never
// left half-expired because of a table the tool cannot read safely.
var ErrEmbeddedManifests = errors.New("a v1 table embeds manifest locations")

// Config is one cleanup run's configuration.
type Config struct {
	// Repository is the lakeFS repository to clean up.
	Repository string

	// MaxSnapshotAge is the default snapshot retention, used for a table that does not
	// set history.expire.max-snapshot-age-ms. Zero means DefaultMaxSnapshotAge.
	MaxSnapshotAge time.Duration

	// MinSnapshotsToKeep is the default number of most recent snapshots to keep, used for
	// a table that does not set history.expire.min-snapshots-to-keep. Zero means
	// DefaultMinSnapshotsToKeep.
	MinSnapshotsToKeep int

	// SafetyAge is the youngest a table file may be when the sweep deletes its
	// registration. Zero means DefaultSafetyAge; anything below MinSafetyAge is refused.
	SafetyAge time.Duration

	// DryRun computes every decision and writes nothing.
	DryRun bool

	// MarkOnly runs expire and mark, and stops before sweep.
	MarkOnly bool

	// SweepOnly runs sweep only, against the mark named by MarkID.
	SweepOnly bool

	// MarkID is the run ID of the mark a --sweep-only run resumes.
	MarkID string

	// Now is the clock, for tests. Zero means time.Now. Every age decision of one run uses
	// this single instant, so a run's report is internally consistent even if it takes
	// hours.
	Now func() time.Time

	// RunID overrides the generated run ID, for tests and for a caller that wants the
	// report path before the run starts.
	RunID string

	// ReportsPrefix overrides where the reports are written. Zero means ReportsPrefix
	// (the package constant, _lakefs/retention/iceberg). It exists so a test can write
	// reports to its own prefix instead of the reserved one, and so an installation that
	// reserves _lakefs/retention/ for something else can still use the tool.
	ReportsPrefix string
}

// Runner executes one cleanup run. It is not safe for concurrent use.
type Runner struct {
	api  API
	cfg  Config
	now  time.Time
	repo Repository

	// index caches Iceberg documents across phases. A run reads a table's metadata once
	// even when a hundred retained commits point at it.
	index *catalogIndex

	reportsPrefix string
	runID         string
	dryRun        bool
}

// Result is what a completed run reports to its caller.
type Result struct {
	Expire *ExpireReport
	Mark   *MarkReport
	Sweep  *SweepReport
}

// New validates config and returns a runner ready for Run. It performs no I/O: the
// repository is read by Run, and every configuration error a user can make is caught here.
func New(api API, config Config) (*Runner, error) {
	if api == nil {
		return nil, fmt.Errorf("%w: the lakeFS API client is required", ErrInvalidConfig)
	}
	if config.Repository == "" {
		return nil, fmt.Errorf("%w: --repo is required", ErrInvalidConfig)
	}
	if config.MaxSnapshotAge < 0 {
		return nil, fmt.Errorf("%w: --max-snapshot-age-days must not be negative", ErrInvalidConfig)
	}
	if config.MaxSnapshotAge == 0 {
		config.MaxSnapshotAge = DefaultMaxSnapshotAge
	}
	if config.MinSnapshotsToKeep < 0 {
		return nil, fmt.Errorf("%w: --min-snapshots-to-keep must not be negative", ErrInvalidConfig)
	}
	if config.MinSnapshotsToKeep == 0 {
		config.MinSnapshotsToKeep = DefaultMinSnapshotsToKeep
	}
	if config.SafetyAge == 0 {
		config.SafetyAge = DefaultSafetyAge
	}
	if config.SafetyAge < MinSafetyAge {
		return nil, fmt.Errorf("%w: --safety-age must be at least %s (got %s)", ErrInvalidConfig, MinSafetyAge, config.SafetyAge)
	}
	switch {
	case config.MarkOnly && config.SweepOnly:
		return nil, fmt.Errorf("%w: --mark-only and --sweep-only are mutually exclusive", ErrInvalidConfig)
	case config.SweepOnly && config.MarkID == "":
		return nil, fmt.Errorf("%w: --sweep-only requires --mark-id", ErrInvalidConfig)
	case config.MarkOnly && config.MarkID != "":
		return nil, fmt.Errorf("%w: --mark-id is only used with --sweep-only", ErrInvalidConfig)
	}

	now := time.Now()
	if config.Now != nil {
		now = config.Now()
	}
	now = now.UTC()

	runID := config.RunID
	if runID == "" {
		runID = newRunID(now)
	}
	prefix := config.ReportsPrefix
	if prefix == "" {
		prefix = ReportsPrefix
	}
	return &Runner{
		api:           api,
		cfg:           config,
		now:           now,
		reportsPrefix: prefix,
		runID:         runID,
		dryRun:        config.DryRun,
	}, nil
}

// init reads the repository and prepares the run's caches. It is separate from Run so a
// test can plan one phase against the same initialized state a run uses.
func (r *Runner) init(ctx context.Context) error {
	repository, err := r.api.GetRepository(ctx, r.cfg.Repository)
	if err != nil {
		return err
	}
	if repository.Name == "" {
		repository.Name = r.cfg.Repository
	}
	if repository.DefaultBranch == "" {
		return fmt.Errorf("%w: repository %q has no default branch to write reports to", ErrInvalidConfig, repository.Name)
	}
	r.repo = Repository{
		Name:             repository.Name,
		StorageNamespace: trimTrailingSlashes(repository.StorageNamespace),
		DefaultBranch:    repository.DefaultBranch,
	}
	r.index = newCatalogIndex(r.api, r.repo.Name)
	return nil
}

// Run reads the repository and runs the configured phases.
func (r *Runner) Run(ctx context.Context) (*Result, error) {
	if err := r.init(ctx); err != nil {
		return nil, err
	}

	result := &Result{}
	if r.cfg.SweepOnly {
		sweep, err := r.sweep(ctx, r.cfg.MarkID, nil)
		result.Sweep = sweep
		return result, err
	}

	expire, err := r.expire(ctx)
	result.Expire = expire
	if err != nil {
		return result, err
	}

	mark, marked, err := r.mark(ctx)
	result.Mark = mark
	if err != nil {
		return result, err
	}
	if r.cfg.MarkOnly {
		return result, nil
	}
	sweep, err := r.sweep(ctx, mark.RunID, marked)
	result.Sweep = sweep
	return result, err
}

// optionsReport quotes the effective configuration in a report.
func (r *Runner) optionsReport() OptionsReport {
	return OptionsReport{
		MaxSnapshotAge:     r.cfg.MaxSnapshotAge.String(),
		MaxSnapshotAgeDays: int(r.cfg.MaxSnapshotAge / hoursPerDay),
		MinSnapshotsToKeep: r.cfg.MinSnapshotsToKeep,
		SafetyAge:          r.cfg.SafetyAge.String(),
		DryRun:             r.cfg.DryRun,
		MarkOnly:           r.cfg.MarkOnly,
		SweepOnly:          r.cfg.SweepOnly,
		MarkID:             r.cfg.MarkID,
	}
}

// newRunID returns a run ID that sorts chronologically and is unique enough for one
// repository's retention reports: <UTC time>-<random suffix>.
func newRunID(now time.Time) string {
	var suffix [4]byte
	// crypto/rand.Read never returns an error on a supported platform; the ignored error
	// would only make the ID less random, never wrong.
	_, _ = rand.Read(suffix[:])
	return now.Format("20060102T150405Z") + "-" + hex.EncodeToString(suffix[:])
}

// trimTrailingSlashes is strings.TrimRight(s, "/") without importing strings just for this
// once; it is also where a future namespace normalization would live.
func trimTrailingSlashes(value string) string {
	for len(value) > 0 && value[len(value)-1] == '/' {
		value = value[:len(value)-1]
	}
	return value
}
