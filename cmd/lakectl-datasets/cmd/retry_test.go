package cmd

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	lakectlcmd "github.com/treeverse/lakefs/cmd/lakectl/cmd"
)

// TestDatasetsRetryPolicy pins CLI-2: only GET and HEAD are retried, so a publish POST whose
// response was lost is never replayed.
func TestDatasetsRetryPolicy(t *testing.T) {
	request := func(method string) *http.Request {
		return httptest.NewRequest(method, "/datasets/customer-360", nil)
	}
	tests := []struct {
		name string
		resp *http.Response
		err  error
		want bool
	}{
		{name: "GET on 500", resp: &http.Response{StatusCode: http.StatusInternalServerError, Request: request(http.MethodGet)}, want: true},
		{name: "GET on 503", resp: &http.Response{StatusCode: http.StatusServiceUnavailable, Request: request(http.MethodGet)}, want: true},
		{name: "GET on 429", resp: &http.Response{StatusCode: http.StatusTooManyRequests, Request: request(http.MethodGet)}, want: true},
		{name: "HEAD on 500", resp: &http.Response{StatusCode: http.StatusInternalServerError, Request: request(http.MethodHead)}, want: true},
		{name: "GET on 200", resp: &http.Response{StatusCode: http.StatusOK, Request: request(http.MethodGet)}, want: false},
		{name: "GET on 404", resp: &http.Response{StatusCode: http.StatusNotFound, Request: request(http.MethodGet)}, want: false},
		{name: "POST on 500", resp: &http.Response{StatusCode: http.StatusInternalServerError, Request: request(http.MethodPost)}, want: false},
		{name: "PUT on 503", resp: &http.Response{StatusCode: http.StatusServiceUnavailable, Request: request(http.MethodPut)}, want: false},
		{name: "DELETE on 500", resp: &http.Response{StatusCode: http.StatusInternalServerError, Request: request(http.MethodDelete)}, want: false},
		{name: "transport error", err: errors.New("connection reset"), want: false},
		{name: "response without a request", resp: &http.Response{StatusCode: http.StatusInternalServerError}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := datasetsRetryPolicy(context.Background(), tt.resp, tt.err)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestRetryTransportRetriesGetAndHeadOnly drives the real retry transport with the datasets
// policy against a server that fails the first request of every method: GET and HEAD recover,
// POST keeps the error status and is sent exactly once.
func TestRetryTransportRetriesGetAndHeadOnly(t *testing.T) {
	var (
		mu    sync.Mutex
		calls = map[string]int{}
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls[r.Method]++
		attempt := calls[r.Method]
		mu.Unlock()
		if attempt == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	client := lakectlcmd.NewRetryClient(lakectlcmd.RetriesCfg{
		Enabled:         true,
		MaxAttempts:     2,
		MinWaitInterval: time.Millisecond,
		MaxWaitInterval: time.Millisecond,
	}, nil, datasetsRetryPolicy)

	do := func(t *testing.T, method string) int {
		t.Helper()
		req, err := http.NewRequestWithContext(context.Background(), method, server.URL+controlPlanePath, nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode
	}
	callsOf := func(method string) int {
		mu.Lock()
		defer mu.Unlock()
		return calls[method]
	}

	require.Equal(t, http.StatusOK, do(t, http.MethodGet))
	require.Equal(t, 2, callsOf(http.MethodGet), "GET is retried")
	require.Equal(t, http.StatusOK, do(t, http.MethodHead))
	require.Equal(t, 2, callsOf(http.MethodHead), "HEAD is retried")
	require.Equal(t, http.StatusInternalServerError, do(t, http.MethodPost))
	require.Equal(t, 1, callsOf(http.MethodPost), "POST is not retried")
}
