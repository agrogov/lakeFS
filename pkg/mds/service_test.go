package mds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/mds/indexer"
)

// testConfig returns a validated configuration with every default of §7, so a test can change
// only the settings it is about.
func testConfig(t *testing.T) *Config {
	t.Helper()
	useConfig(t, fullConfigYAML)
	cfg, err := LoadConfig()
	require.NoError(t, err)
	return cfg
}

// TestHealthReport pins the health state machine of §9: a repository is healthy while its last
// tick succeeded, a tick in progress does not clear a previous failure (IO-5), and one failing
// repository or branch makes the whole report unhealthy.
func TestHealthReport(t *testing.T) {
	health := NewHealth([]string{"acme", "globex"})

	report := health.Report()
	require.True(t, report.Healthy, "a fresh indexer has nothing to report and is healthy")
	require.Len(t, report.Repositories, 2)

	health.BeginTick("acme")
	report = health.Report()
	require.True(t, report.Healthy)
	require.True(t, report.Repositories["acme"].InProgress, "in progress with nothing to report is healthy")

	health.FinishTick("acme", nil)
	report = health.Report()
	require.True(t, report.Healthy)
	require.False(t, report.Repositories["acme"].InProgress)

	health.SetBranchState("acme", "main", "aaa", "bbb", 1, nil)
	report = health.Report()
	require.True(t, report.Healthy)
	require.Equal(t, BranchHealth{LastIndexed: "aaa", Head: "bbb", LagCommits: 1},
		report.Repositories["acme"].Branches["main"])

	health.SetBranchState("acme", "main", "aaa", "bbb", 1, errors.New("position delete failed"))
	report = health.Report()
	require.False(t, report.Healthy)
	require.Equal(t, "aaa", report.Repositories["acme"].Branches["main"].LastIndexed,
		"a branch error keeps the rest of the branch state")
	require.Equal(t, "position delete failed", report.Repositories["acme"].Branches["main"].LastError)

	// A later successful tick of the branch clears that error again.
	health.SetBranchState("acme", "main", "bbb", "bbb", 0, nil)
	require.True(t, health.Report().Healthy)

	health.FinishTick("acme", errors.New("repository is missing"))
	report = health.Report()
	require.False(t, report.Healthy)
	require.Equal(t, "repository is missing", report.Repositories["acme"].LastError)
	require.Empty(t, report.Repositories["globex"].LastError, "one repository does not affect another")

	health.FinishTick("acme", nil)
	require.True(t, health.Report().Healthy, "a successful tick clears the repository error")

	health.FailAll(errors.New("cannot list repositories"))
	report = health.Report()
	require.False(t, report.Healthy)
	require.Equal(t, "cannot list repositories", report.Repositories["globex"].LastError)
}

// TestHealthServeHTTP pins the wire format of §9: 200/503 and the documented JSON fields of the
// per-repository and per-branch body.
func TestHealthServeHTTP(t *testing.T) {
	health := NewHealth([]string{"acme"})
	health.SetBranchState("acme", "main", "last", "head", 7, nil)

	recorder := httptest.NewRecorder()
	health.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, HealthPath, nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "application/json", recorder.Header().Get("Content-Type"))

	var body map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.Equal(t, true, body["healthy"])
	branches := body["repositories"].(map[string]any)["acme"].(map[string]any)["branches"].(map[string]any)
	main := branches["main"].(map[string]any)
	require.Equal(t, "last", main["last_indexed"])
	require.Equal(t, "head", main["head"])
	require.EqualValues(t, 7, main["lag_commits"])

	health.FinishTick("acme", errors.New("boom"))
	recorder = httptest.NewRecorder()
	health.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, HealthPath, nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.Equal(t, false, body["healthy"])
	require.Equal(t, "boom", body["repositories"].(map[string]any)["acme"].(map[string]any)["last_error"])
}

// TestHealthRemoveBranch pins §4.8's health side: a branch that was deleted (or stopped matching
// the configuration) is removed from the report, so its last error cannot keep /_health at 503
// until the process restarts.
func TestHealthRemoveBranch(t *testing.T) {
	health := NewHealth([]string{"acme"})
	health.SetBranchState("acme", "feature", "a", "b", 1, errors.New("the branch could not be indexed"))
	health.FinishTick("acme", nil)
	require.False(t, health.Report().Healthy, "a failed branch makes the report unhealthy")

	health.RemoveBranch("acme", "feature")

	report := health.Report()
	require.NotContains(t, report.Repositories["acme"].Branches, "feature")
	require.True(t, report.Healthy, "removing the branch removes its error")

	// Removing a branch or a repository that was never reported is harmless.
	health.RemoveBranch("acme", "missing")
	health.RemoveBranch("globex", "main")
	require.True(t, health.Report().Healthy)
}

// TestHealthInProgressVersusPreviousError pins the precedence IO-5 asks for: a tick in progress
// never masks the outcome of the previous one, so a failed repository stays 503 while its retry
// runs, and a cancelled tick ends the in-progress flag without touching that outcome.
func TestHealthInProgressVersusPreviousError(t *testing.T) {
	health := NewHealth([]string{"acme"})
	health.FinishTick("acme", errors.New("repository is missing"))
	require.False(t, health.Report().Healthy)

	health.BeginTick("acme")
	report := health.Report()
	require.True(t, report.Repositories["acme"].InProgress, "the retry is in progress")
	require.False(t, report.Healthy, "a retry in progress does not clear the failure of the previous tick")
	require.Equal(t, "repository is missing", report.Repositories["acme"].LastError)

	health.CancelTick("acme")
	report = health.Report()
	require.False(t, report.Repositories["acme"].InProgress, "a cancelled tick is not in progress any more")
	require.Equal(t, "repository is missing", report.Repositories["acme"].LastError,
		"a cancelled tick is not an outcome, so it keeps the previous one")
	require.False(t, report.Healthy)

	health.FinishTick("acme", nil)
	require.True(t, health.Report().Healthy, "only a tick that succeeded clears the error")
}

func TestHealthServeHTTPRejectsOtherMethods(t *testing.T) {
	recorder := httptest.NewRecorder()
	NewHealth(nil).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, HealthPath, nil))
	require.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
	require.Equal(t, http.MethodGet+", "+http.MethodHead, recorder.Header().Get("Allow"))
}

// TestServiceHandler pins the three endpoints of §9 on the service's own handler.
func TestServiceHandler(t *testing.T) {
	svc := NewService(testConfig(t), nil)

	// The §9 metrics are CounterVecs/Gauges: they appear on /metrics only once a label value was
	// used, so record one sample of each before the endpoint is checked.
	var recorder Recorder
	recorder.IndexedCommit("my-repo")
	recorder.SetLag("my-repo", "main", 3)
	recorder.Error("my-repo", indexer.StageBranch)
	recorder.TickDuration(time.Second)
	recorder.RowsWritten(10)
	recorder.Rebuild()
	recorder.Compaction()

	server := httptest.NewServer(svc.handler())
	t.Cleanup(server.Close)

	resp, err := server.Client().Get(server.URL + HealthPath)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "a configured repository with no error is healthy")
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), `"repositories"`)

	resp, err = server.Client().Get(server.URL + MetricsPath)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	for _, metric := range []string{
		"lakefs_mds_indexed_commits_total",
		"lakefs_mds_lag_commits",
		"lakefs_mds_errors_total",
		"lakefs_mds_tick_duration_seconds",
		"lakefs_mds_rows_written_total",
		"lakefs_mds_rebuilds_total",
		"lakefs_mds_compactions_total",
	} {
		require.Contains(t, string(body), metric, "§9 metric %s must be registered and served", metric)
	}

	for _, path := range []string{PPROFPath, PPROFPath + "heap", PPROFPath + "goroutine"} {
		resp, err := server.Client().Get(server.URL + path)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s", path)
		resp.Body.Close()
	}
}

// TestMetricsRegistered pins the metric names of §9 independently of the HTTP server: they are
// registered on the default Prometheus registry at package initialisation.
func TestMetricsRegistered(t *testing.T) {
	var recorder Recorder
	recorder.IndexedCommit("registration-check")
	recorder.SetLag("registration-check", "main", 1)
	recorder.Error("registration-check", StageTick)
	recorder.TickDuration(time.Millisecond)
	recorder.RowsWritten(1)
	recorder.Rebuild()
	recorder.Compaction()

	require.EqualValues(t, 1, testutil.ToFloat64(indexedCommitsTotal.WithLabelValues("registration-check")))
	require.EqualValues(t, 1, testutil.ToFloat64(lagCommits.WithLabelValues("registration-check", "main")))
	require.EqualValues(t, 1, testutil.ToFloat64(errorsTotal.WithLabelValues("registration-check", StageTick)))
	require.Positive(t, testutil.CollectAndCount(tickDuration))
	require.Positive(t, testutil.ToFloat64(rowsWrittenTotal))
	require.Positive(t, testutil.ToFloat64(rebuildsTotal))
	require.Positive(t, testutil.ToFloat64(compactionsTotal))
}

// TestServiceNoopTick pins the skeleton tick of §4.1: every configured repository reports a
// successful tick and no error.
func TestServiceNoopTick(t *testing.T) {
	svc := NewService(testConfig(t), nil)
	svc.tickOnce(context.Background())

	report := svc.Health().Report()
	require.True(t, report.Healthy)
	require.Contains(t, report.Repositories, "my-repo")
}

// TestServiceTickFailure pins what a failed tick leaves behind: the error metric of §9 and a 503
// answer from /_health, for every repository (a tick error is not attributable to one).
func TestServiceTickFailure(t *testing.T) {
	boom := errors.New("cannot reach lakeFS")
	svc := NewService(testConfig(t), nil, WithTickFunc(func(context.Context) error { return boom }))

	before := testutil.ToFloat64(errorsTotal.WithLabelValues(AllRepositories, StageTick))
	svc.tickOnce(context.Background())
	require.EqualValues(t, before+1, testutil.ToFloat64(errorsTotal.WithLabelValues(AllRepositories, StageTick)))

	report := svc.Health().Report()
	require.False(t, report.Healthy)
	require.Equal(t, boom.Error(), report.Repositories["my-repo"].LastError)

	recorder := httptest.NewRecorder()
	svc.health.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, HealthPath, nil))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
}

// TestServiceCancelledTickIsNotAFailure pins the graceful-stop rule: the in-flight tick of a
// stopped process is neither counted as an error nor reported on /_health.
func TestServiceCancelledTickIsNotAFailure(t *testing.T) {
	svc := NewService(testConfig(t), nil, WithTickFunc(func(ctx context.Context) error {
		return ctx.Err()
	}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	before := testutil.ToFloat64(errorsTotal.WithLabelValues(AllRepositories, StageTick))
	svc.tickOnce(ctx)
	require.EqualValues(t, before, testutil.ToFloat64(errorsTotal.WithLabelValues(AllRepositories, StageTick)))
	require.True(t, svc.Health().Report().Healthy)
}

// TestServiceRunServesAndStopsGracefully starts the service on an ephemeral port, reads
// /_health through the real socket and stops it by cancelling the context, which is what
// signal.NotifyContext does on SIGTERM.
func TestServiceRunServesAndStopsGracefully(t *testing.T) {
	cfg := testConfig(t)
	cfg.ListenAddress = "127.0.0.1:0"
	cfg.Period = 10 * time.Millisecond
	var ticks atomic.Int64
	svc := NewService(cfg, nil, WithTickFunc(func(context.Context) error {
		ticks.Add(1)
		return nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(ctx)
	}()
	t.Cleanup(cancel)

	require.Eventually(t, func() bool { return svc.Addr() != "" }, 5*time.Second, 5*time.Millisecond,
		"the service reports the address it bound")
	baseURL := "http://" + svc.Addr()

	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get(baseURL + HealthPath)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// The tick ran on the period.
	require.Eventually(t, func() bool { return ticks.Load() > 0 }, 5*time.Second, 5*time.Millisecond,
		"the service ticks on the configured period")

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "a cancelled context is a graceful stop")
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}

	// The listener is closed, so the endpoint no longer answers.
	_, err = (&http.Client{Timeout: time.Second}).Get(baseURL + HealthPath)
	require.Error(t, err)
}

// TestServiceStopsOnSIGTERM pins the process model of §4.1 end to end: SIGTERM reaches the
// service through signal.NotifyContext and Run returns nil. The signal is sent to the test
// process itself while the handler is installed, so it is delivered to this test and not to the
// default action of the signal.
func TestServiceStopsOnSIGTERM(t *testing.T) {
	cfg := testConfig(t)
	cfg.ListenAddress = "127.0.0.1:0"
	cfg.Period = 10 * time.Millisecond
	// The process model is what this test pins; the indexer itself is installed by Run only
	// without a TickFunc (RA4b: and only with an in-process reader, see
	// TestServiceRunRequiresTheInProcessReader).
	svc := NewService(cfg, nil, WithTickFunc(func(context.Context) error { return nil }))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	t.Cleanup(stop)

	done := make(chan error, 1)
	go func() {
		done <- svc.Run(ctx)
	}()

	require.Eventually(t, func() bool { return svc.Addr() != "" }, 5*time.Second, 5*time.Millisecond)
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGTERM))

	select {
	case err := <-done:
		require.NoError(t, err, "SIGTERM is a graceful stop")
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after SIGTERM")
	}
}

// TestServiceRunRequiresConfiguration pins the two startup guards of Run: a Service built without
// a configuration, and one whose period was never validated.
func TestServiceRunRequiresConfiguration(t *testing.T) {
	err := NewService(nil, nil).Run(context.Background())
	require.ErrorIs(t, err, ErrNotConfigured)

	cfg := testConfig(t)
	cfg.Period = 0
	err = NewService(cfg, nil).Run(context.Background())
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.ErrorContains(t, err, "period must be positive")
}

// TestServiceRunReportsListenError pins that an address the process cannot bind fails startup
// instead of leaving a half-running indexer.
func TestServiceRunReportsListenError(t *testing.T) {
	cfg := testConfig(t)
	cfg.ListenAddress = "127.0.0.1:70000" // not a valid TCP port
	err := NewService(cfg, nil, WithTickFunc(func(context.Context) error { return nil })).Run(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "listen on")
}

// TestServiceRunRequiresTheInProcessReader pins the RA4b startup requirement: the indexer reads the
// repositories in process, so a Service that was not given the reader `mds run` builds cannot
// install the indexer. Startup fails instead of the first tick.
func TestServiceRunRequiresTheInProcessReader(t *testing.T) {
	cfg := testConfig(t)
	cfg.ListenAddress = "127.0.0.1:0"
	err := NewService(cfg, nil).Run(context.Background())
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.ErrorIs(t, err, indexer.ErrNoLakeFS)
}

// TestRecorderIncrements pins the Recorder methods of §9 against the collectors.
func TestRecorderIncrements(t *testing.T) {
	var recorder Recorder
	repository := "recorder-" + fmt.Sprint(time.Now().UnixNano())

	recorder.IndexedCommit(repository)
	require.EqualValues(t, 1, testutil.ToFloat64(indexedCommitsTotal.WithLabelValues(repository)))

	recorder.SetLag(repository, "main", 42)
	require.EqualValues(t, 42, testutil.ToFloat64(lagCommits.WithLabelValues(repository, "main")))

	recorder.Error(repository, StageTick)
	require.EqualValues(t, 1, testutil.ToFloat64(errorsTotal.WithLabelValues(repository, StageTick)))
}
