package catalog

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Repository storage deletion metrics (REPO_DELETE_STORAGE_SPEC.md R21).  They all live in
// pkg/catalog because the worker is part of the catalog and every lakeFS process registers the
// same set.

var (
	// repositoryStorageDeletions is a gauge by state, set on every worker scan.
	repositoryStorageDeletions = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lakefs_repository_storage_deletions",
			Help: "Number of repository storage deletion records by state",
		},
		[]string{"state"},
	)

	// repositoryStorageDeletedObjects counts every object a namespace deletion removed, over every
	// repository, page and retry.
	repositoryStorageDeletedObjects = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "lakefs_repository_storage_deleted_objects_total",
			Help: "Total number of objects deleted by repository storage deletions",
		},
	)

	// repositoryStorageDeletionErrors counts failed attempts by the step they failed in.
	repositoryStorageDeletionErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "lakefs_repository_storage_deletion_errors_total",
			Help: "Total number of failed repository storage deletion attempts by step",
		},
		[]string{"step"},
	)
)

// repositoryDeletionStateLabel maps a state to its metric label.  The metric labels are the
// lowercase names the API uses (R14).
func repositoryDeletionStateLabel(state RepositoryDeletionState) string {
	switch state {
	case RepositoryDeletionState_PENDING:
		return "pending"
	case RepositoryDeletionState_DELETING:
		return "deleting"
	case RepositoryDeletionState_DONE:
		return "done"
	case RepositoryDeletionState_FAILED:
		return "failed"
	default:
		return "unknown"
	}
}

// setRepositoryStorageDeletionsGauge resets the gauge from a full scan: every known state gets the
// count of records in it, including zero.
func setRepositoryStorageDeletionsGauge(counts map[RepositoryDeletionState]int) {
	for _, state := range []RepositoryDeletionState{
		RepositoryDeletionState_PENDING,
		RepositoryDeletionState_DELETING,
		RepositoryDeletionState_DONE,
		RepositoryDeletionState_FAILED,
	} {
		repositoryStorageDeletions.WithLabelValues(repositoryDeletionStateLabel(state)).Set(float64(counts[state]))
	}
}

// recordRepositoryStorageDeletedObjects adds one page's deletion count to the counter.
func recordRepositoryStorageDeletedObjects(deleted int) {
	if deleted <= 0 {
		return
	}
	repositoryStorageDeletedObjects.Add(float64(deleted))
}

// recordRepositoryStorageDeletionError counts a failed attempt of a step.
func recordRepositoryStorageDeletionError(step RepositoryDeletionStep) {
	repositoryStorageDeletionErrors.WithLabelValues(step.String()).Inc()
}
