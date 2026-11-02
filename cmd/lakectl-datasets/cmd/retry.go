package cmd

import (
	"context"
	"net/http"

	lakectlcmd "github.com/treeverse/lakefs/cmd/lakectl/cmd"
)

// datasetsRetryStatuses are the transient statuses worth retrying, the same ones lakectl retries
// (§10.4): 429 too many requests, 500 internal server error and 503 service unavailable.
var datasetsRetryStatuses = lakectlcmd.ShouldRetryer{
	http.StatusTooManyRequests,
	http.StatusInternalServerError,
	http.StatusServiceUnavailable,
}

// datasetsRetryPolicy is the retry policy of the datasets client: it retries GET and HEAD only
// (CLI-2). The control plane has no idempotency key, so replaying a publish POST after a lost
// response would create a duplicate version; `datasets publish` therefore sends the version it
// expects (--expected-version, fetched first by default), so that a replay is answered with 409
// instead of publishing twice.
//
// A transport error arrives without a response, so the request method is unknown. Such a request
// is not retried: replaying a write whose outcome we cannot see is exactly the failure this
// policy exists to prevent.
func datasetsRetryPolicy(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if resp == nil || resp.Request == nil {
		return false, nil
	}
	if !idempotentMethod(resp.Request.Method) {
		return false, nil
	}
	return lakectlcmd.CheckRetry(ctx, resp, err, datasetsRetryStatuses)
}

// idempotentMethod reports whether an HTTP method is safe to replay: GET and HEAD have no
// side effects, so a retry cannot change the server's state.
func idempotentMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead:
		return true
	default:
		return false
	}
}
