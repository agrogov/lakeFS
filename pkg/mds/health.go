package mds

// health.go is the /_health endpoint of design §9: it answers 200 while every repository's last
// tick succeeded (or is still in progress) and 503 as soon as one repository or one branch
// carries an error, with a JSON body that says which one.
//
// The indexer owns one Health for the life of the process. The tick reports into it: a
// repository-level failure (the repository is missing, R-metadata cannot be created, its storage
// namespace cannot be derived or overlaps R's) marks that repository failed while the other
// repositories keep indexing; the branch fields say how far the mirror of one branch has come.

import (
	"encoding/json"
	"net/http"
	"sync"
)

const (
	// HealthPath is where the health report is served (§9).
	HealthPath = "/_health"

	// MetricsPath is where the Prometheus metrics of §9 are served.
	MetricsPath = "/metrics"

	// PPROFPath is where the Go runtime profiles are served.
	PPROFPath = "/_pprof/"
)

// BranchHealth is the indexed state of one branch of one data repository (§9): the last source
// commit the mirror covers, the head the last tick saw, how many first-parent commits are still
// to apply, and the error of the last attempt.
type BranchHealth struct {
	// LastIndexed is the full ID of the source commit the table of this branch is at, i.e. the
	// lakefs.mds.source-commit of the head of R-metadata@b; empty when the branch has never been
	// indexed.
	LastIndexed string `json:"last_indexed"`

	// Head is the full ID of the source branch head the last tick saw.
	Head string `json:"head"`

	// LagCommits is the number of first-parent commits between LastIndexed and Head.
	LagCommits int64 `json:"lag_commits"`

	// LastError is the error of the last indexing attempt of this branch, empty when it
	// succeeded.
	LastError string `json:"last_error,omitempty"`
}

// RepositoryHealth is the state of one configured data repository (§9).
type RepositoryHealth struct {
	// InProgress reports that a tick of this repository is running.
	InProgress bool `json:"in_progress"`

	// Branches is the per-branch state, by branch name.
	Branches map[string]BranchHealth `json:"branches"`

	// LastError is the repository-level error of the last tick (repository missing, R-metadata
	// not created by the indexer, namespace not derivable or overlapping, ...), empty when the
	// last tick succeeded.
	LastError string `json:"last_error,omitempty"`
}

// HealthReport is the body of /_health (§9).
type HealthReport struct {
	// Healthy is true when no repository and no branch carries an error. It is the condition the
	// HTTP status follows.
	Healthy bool `json:"healthy"`

	// Repositories is the per-repository state, by repository name.
	Repositories map[string]RepositoryHealth `json:"repositories"`
}

// Health collects the health of the indexer for /_health (§9). It is safe for concurrent use:
// the tick may process branches in parallel while a probe reads the report.
type Health struct {
	mu    sync.Mutex
	repos map[string]*repositoryHealth
}

// repositoryHealth is the mutable form of RepositoryHealth.
type repositoryHealth struct {
	inProgress bool
	lastError  string
	branches   map[string]BranchHealth
}

// NewHealth returns a Health that knows the configured repositories. A repository is added on
// demand as well, so a report can never lose a repository the configuration does not mention.
func NewHealth(repositories []string) *Health {
	h := &Health{repos: make(map[string]*repositoryHealth, len(repositories))}
	for _, repository := range repositories {
		h.repos[repository] = newRepositoryHealth()
	}
	return h
}

func newRepositoryHealth() *repositoryHealth {
	return &repositoryHealth{branches: make(map[string]BranchHealth)}
}

// BeginTick marks the tick of one repository as in progress. It keeps the previous outcome: a
// probe that arrives mid-tick sees the state of the previous tick plus in_progress.
func (h *Health) BeginTick(repository string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.repositoryLocked(repository).inProgress = true
}

// FinishTick records the outcome of the tick of one repository: a nil error clears the
// repository-level error of the previous tick, a non-nil one marks the repository failed.
func (h *Health) FinishTick(repository string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	repo := h.repositoryLocked(repository)
	repo.inProgress = false
	repo.lastError = errorString(err)
}

// SetBranch records the state of one mirrored branch.
func (h *Health) SetBranch(repository, branch string, health BranchHealth) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.repositoryLocked(repository).branches[branch] = health
}

// SetBranchState records the whole state of one mirrored branch (§9): the commit its table is at,
// the head the tick saw, how many commits are still to apply, and the error of the attempt.
//
// It takes the fields rather than a BranchHealth so that the indexer (pkg/mds/indexer) can report
// through an interface without importing this package: the indexer is a client of the indexer
// process, and the dependency runs the other way.
func (h *Health) SetBranchState(repository, branch, lastIndexed, head string, lagCommits int64, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.repositoryLocked(repository).branches[branch] = BranchHealth{
		LastIndexed: lastIndexed,
		Head:        head,
		LagCommits:  lagCommits,
		LastError:   errorString(err),
	}
}

// SetBranchError records that indexing one branch failed, leaving the rest of its state as it
// was.
func (h *Health) SetBranchError(repository, branch string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	repo := h.repositoryLocked(repository)
	branchHealth := repo.branches[branch]
	branchHealth.LastError = errorString(err)
	repo.branches[branch] = branchHealth
}

// FailAll marks every repository failed with the same error. It is what the service records when
// a tick fails as a whole, before it could attribute the failure to one repository.
func (h *Health) FailAll(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, repo := range h.repos {
		repo.inProgress = false
		repo.lastError = errorString(err)
	}
}

// Report returns a consistent snapshot of the health of every repository (§9).
func (h *Health) Report() HealthReport {
	h.mu.Lock()
	defer h.mu.Unlock()
	report := HealthReport{
		Healthy:      true,
		Repositories: make(map[string]RepositoryHealth, len(h.repos)),
	}
	for name, repo := range h.repos {
		branches := make(map[string]BranchHealth, len(repo.branches))
		for branch, health := range repo.branches {
			branches[branch] = health
			if health.LastError != "" {
				report.Healthy = false
			}
		}
		if repo.lastError != "" {
			report.Healthy = false
		}
		report.Repositories[name] = RepositoryHealth{
			InProgress: repo.inProgress,
			Branches:   branches,
			LastError:  repo.lastError,
		}
	}
	return report
}

// ServeHTTP answers /_health (§9): 200 while the report is healthy and 503 otherwise, with the
// report itself as the JSON body.
func (h *Health) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodHead)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	report := h.Report()
	w.Header().Set("Content-Type", "application/json")
	status := http.StatusOK
	if !report.Healthy {
		status = http.StatusServiceUnavailable
	}
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}
	_ = json.NewEncoder(w).Encode(report)
}

// repositoryLocked returns the state of a repository, creating it when the configuration did not
// list it. The caller must hold h.mu.
func (h *Health) repositoryLocked(repository string) *repositoryHealth {
	repo, ok := h.repos[repository]
	if !ok {
		repo = newRepositoryHealth()
		h.repos[repository] = repo
	}
	return repo
}

// errorString is the reported form of an error: empty for nil, so that a clean state and a state
// that failed with an empty message are the same thing.
func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
