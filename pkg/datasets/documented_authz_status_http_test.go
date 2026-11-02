package datasets

// documented_authz_status_http_test.go covers DOC-17: a permission denial answers the
// configured auth.authorization_failure_status instead of a hard-coded 403.

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/auth/model"
)

// TestDatasetsAuthorizationFailureStatus is DOC-17.
func TestDatasetsAuthorizationFailureStatus(t *testing.T) {
	const datasetsConfig = "datasets:\n  enabled: true\n  storage_namespace_prefix: \"mem://datasets\"\n"

	for _, tt := range []struct {
		name       string
		configured int
		wantStatus int
	}{
		{name: "unset uses the core default", configured: 0, wantStatus: http.StatusUnauthorized},
		{name: "configured 401 is honoured", configured: http.StatusUnauthorized, wantStatus: http.StatusUnauthorized},
		{name: "configured 403 is honoured", configured: http.StatusForbidden, wantStatus: http.StatusForbidden},
	} {
		t.Run(tt.name, func(t *testing.T) {
			handler, deps, _, _ := newControlPlaneWithConfigAndAuthzStatus(t, datasetsConfig, tt.configured)

			const nobody = "doc-17-nobody"
			_, err := deps.AuthService.CreateUser(t.Context(), &model.User{Username: nobody})
			require.NoError(t, err)
			cred, err := deps.AuthService.AddCredentials(t.Context(), nobody, "AKIADOC17NOBODY0001", "nobody-secret")
			require.NoError(t, err)

			rec := doRequest(t, handler, http.MethodGet, "/metadata", "", cred.AccessKeyID, cred.SecretAccessKey)
			require.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}
