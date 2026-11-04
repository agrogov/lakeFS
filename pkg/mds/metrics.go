package mds

// metrics.go registers the Prometheus metrics of design §9 under the lakefs_mds_ prefix:
//
//	lakefs_mds_indexed_commits_total{repository}   source commits applied to the mirror
//	lakefs_mds_lag_commits{repository,branch}      source commits not indexed yet
//	lakefs_mds_errors_total{repository,stage}      failures, by stage
//	lakefs_mds_tick_duration_seconds               duration of one tick
//	lakefs_mds_rows_written_total                  rows written to the metadata tables
//	lakefs_mds_rebuilds_total                      rebuilds of a branch's table
//	lakefs_mds_compactions_total                   compactions of a branch's table
//
// They are registered on the default Prometheus registry at package initialisation, the way the
// rest of the repository registers its metrics, so the process's /metrics endpoint shows them
// without further wiring.

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	// metricsNamespace and metricsSubsystem produce the lakefs_mds_ prefix of §9.
	metricsNamespace = "lakefs"
	metricsSubsystem = "mds"

	labelRepository = "repository"
	labelBranch     = "branch"
	labelStage      = "stage"

	// AllRepositories is the repository label of a failure that could not be attributed to one
	// repository, such as a tick that failed before it read any repository.
	AllRepositories = "*"

	// StageTick is the stage label of a failure of a whole tick (§9).
	StageTick = "tick"

	// StageRepository is the stage label of a repository-level failure (§9): the repository is
	// missing, R-metadata cannot be created, its storage namespace cannot be derived or overlaps
	// the data repository's.
	StageRepository = "repository"

	// StageBranch is the stage label of a failure while indexing one branch (§9).
	StageBranch = "branch"
)

// tickDurationBuckets cover a tick from an empty repository to a large backfill; the longest
// documented tick is bounded by max_commits, not by this histogram.
var tickDurationBuckets = []float64{0.1, 0.5, 1, 5, 15, 30, 60, 120, 300, 600, 1800}

var (
	indexedCommitsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "indexed_commits_total",
			Help:      "Number of source commits applied to a metadata table, by repository",
		},
		[]string{labelRepository},
	)

	lagCommits = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "lag_commits",
			Help:      "Number of source commits of a branch that are not indexed yet",
		},
		[]string{labelRepository, labelBranch},
	)

	errorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "errors_total",
			Help:      "Number of Metadata Search indexer failures, by repository and stage",
		},
		[]string{labelRepository, labelStage},
	)

	tickDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "tick_duration_seconds",
			Help:      "Duration of one tick of the Metadata Search indexer in seconds",
			Buckets:   tickDurationBuckets,
		},
	)

	rowsWrittenTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "rows_written_total",
			Help:      "Number of object metadata rows written to metadata tables",
		},
	)

	rebuildsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "rebuilds_total",
			Help:      "Number of times a branch's metadata table was rebuilt from a full listing",
		},
	)

	compactionsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "compactions_total",
			Help:      "Number of times a branch's metadata table was compacted",
		},
	)
)

// Recorder writes the metrics of §9. The zero value is ready to use; it exists as a type so that
// the parts of the indexer that live in other packages can take a small interface instead of the
// package-level collectors.
type Recorder struct{}

// IndexedCommit counts one source commit applied to the metadata table of a repository.
func (Recorder) IndexedCommit(repository string) {
	indexedCommitsTotal.WithLabelValues(repository).Inc()
}

// SetLag records how many source commits of a branch are not indexed yet.
func (Recorder) SetLag(repository, branch string, commits int64) {
	lagCommits.WithLabelValues(repository, branch).Set(float64(commits))
}

// Error counts one failure at the given stage of the given repository.
func (Recorder) Error(repository, stage string) {
	errorsTotal.WithLabelValues(repository, stage).Inc()
}

// TickDuration records the duration of one tick.
func (Recorder) TickDuration(d time.Duration) {
	tickDuration.Observe(d.Seconds())
}

// RowsWritten counts rows written to the metadata tables.
func (Recorder) RowsWritten(rows int64) {
	rowsWrittenTotal.Add(float64(rows))
}

// Rebuild counts one rebuild of a branch's metadata table.
func (Recorder) Rebuild() {
	rebuildsTotal.Inc()
}

// Compaction counts one compaction of a branch's metadata table.
func (Recorder) Compaction() {
	compactionsTotal.Inc()
}
