package indexer_test

// timeout_test.go pins the bound of design §4.1 on the one production HTTP client the indexer
// still has (RA4b: the repository reads are in-process, the table writes are not): a connection
// that accepts the request and never answers must fail the branch it belongs to, so the tick loop
// keeps running and /_health (§9) turns 503. Without the bound the tick of the stuck branch never
// returns, Service.tickUntilDone never starts the next one, and /_health keeps answering 200
// because the tick is only "in progress" (MDS_REVIEW.md IX-2).
//
// The test uses the production Iceberg catalog client - indexer.New with an endpoint and no Tables
// seam - rather than the fakes of fake_test.go, because the defect is exactly that a client built
// without an http.Client timeout stalls forever.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds"
	"github.com/treeverse/lakefs/pkg/mds/indexer"
	"github.com/treeverse/lakefs/pkg/mds/reader"
)

const (
	// shortRequestTimeout is the request bound of the clients in these tests: short enough that a
	// stalled request fails well inside the test's own bound, long enough not to race the local
	// stack.
	shortRequestTimeout = 200 * time.Millisecond

	// tickBound bounds one Tick of these tests. It is far above shortRequestTimeout on purpose:
	// the assertion is "the tick returns at all", which is what the client timeout buys and what
	// the unbounded client did not do (the probe of IX-2 was still in progress after 3s).
	tickBound = 10 * time.Second
)

// hangingServer is an HTTP server that accepts the request, reads it and never answers, which is
// the shape of a wedged load balancer or a blackholed replica. The handler returns when the client
// gives up, so closing the server in t.Cleanup cannot block.
func hangingServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)
	return server
}

// tickWithin runs one tick and fails the test when it has not returned within bound. The tick's
// context is bounded by the same deadline, so a client without a timeout cannot leak a goroutine
// past the test.
func tickWithin(t *testing.T, idx *indexer.Indexer, bound time.Duration) time.Duration {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), bound)
	defer cancel()
	start := time.Now()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = idx.Tick(ctx)
	}()
	select {
	case <-done:
		return time.Since(start)
	case <-ctx.Done():
		t.Fatalf("the tick did not return within %s: a request that never answers is not bounded", bound)
		return 0
	}
}

// healthStatus returns the status /_health answers (§9): 200 while every repository and branch is
// healthy, 503 as soon as one carries an error.
func healthStatus(t *testing.T, health *mds.Health) int {
	t.Helper()
	recorder := httptest.NewRecorder()
	health.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, mds.HealthPath, nil))
	return recorder.Code
}

// requireDeadlineError requires that a reported error came from the request bound and not from
// something else (a refused connection would return immediately, which is not what these tests
// pin).
func requireDeadlineError(t *testing.T, message string) {
	t.Helper()
	require.NotEmpty(t, message)
	require.True(t,
		strings.Contains(message, "deadline exceeded") || strings.Contains(message, "Timeout exceeded"),
		"the reported error is the request timeout, got %q", message)
}

// TestTickAgainstACatalogThatNeverAnswers pins where the failure is attributed: the in-process
// reader of the fake answers (repository stage succeeds), the Iceberg REST catalog hangs, so the
// *branch* is reported failed while the repository tick completes. Without the catalog bound the
// tick of that branch never returns.
func TestTickAgainstACatalogThatNeverAnswers(t *testing.T) {
	// Seed the fake with one indexed branch, so the second, production-catalog indexer starts at
	// the branch stage instead of creating the companion repository through the catalog.
	f := newFake()
	f.addRepository("repo", "s3://bucket/data", "main")
	f.addCommit("repo", "main", "commit-1", nil, []reader.ObjectStat{object("orders/a.parquet", 10)})
	seedHealth := newFakeHealth()
	seed, err := indexer.New(indexer.Options{
		LakeFS:       f,
		Tables:       f,
		Repositories: []indexer.Repository{{Name: "repo", Branches: []string{"main"}}},
		Health:       seedHealth,
		Log:          logging.ContextUnavailable(),
	})
	require.NoError(t, err)
	require.NoError(t, seed.Tick(t.Context()))
	require.NoError(t, seedHealth.repositoryError("repo"))
	require.NoError(t, seedHealth.branch("repo", "main").err, "the seed tick indexed the branch")
	require.Equal(t, "commit-1", seedHealth.branch("repo", "main").lastIndexed)

	// The production Iceberg REST catalog client, built by indexer.New from the endpoint.
	server := hangingServer(t)
	health := mds.NewHealth([]string{"repo"})
	idx, err := indexer.New(indexer.Options{
		LakeFS:         f,
		Endpoint:       server.URL,
		AccessKeyID:    "AKIAEXAMPLE",
		SecretKey:      "secret",
		RequestTimeout: shortRequestTimeout,
		Repositories:   []indexer.Repository{{Name: "repo", Branches: []string{"main"}}},
		Health:         health,
		Log:            logging.ContextUnavailable(),
	})
	require.NoError(t, err)

	elapsed := tickWithin(t, idx, tickBound)
	require.GreaterOrEqual(t, elapsed, shortRequestTimeout,
		"the branch waited for the request bound of the catalog client")

	report := health.Report()
	branchHealth := report.Repositories["repo"].Branches["main"]
	requireDeadlineError(t, branchHealth.LastError)
	require.Empty(t, report.Repositories["repo"].LastError,
		"the failure stays inside the branch: the repository tick itself completed")
	require.False(t, report.Healthy, "a branch whose catalog never answers is not healthy")
	require.Equal(t, http.StatusServiceUnavailable, healthStatus(t, health))
}
