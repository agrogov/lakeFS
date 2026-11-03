package icebergcatalog

// Tests for the degradation of Bootstrap in the default auth mode (conformance/RESULTS.md D-3,
// §3): the reserved-prefix deny is a hardening measure, so an auth service that has no policy
// backend at all - auth.BasicAuthService, which both lakefs and lakefs-sso run until an external
// auth service or SSO is configured - must not stop the server from starting. Every other failure
// of the policy machinery must still stop it, because a backend that can store policies and failed
// leaves the reserved prefix writable for a reason an operator has to fix.
//
// The three cases are driven through Bootstrap itself, on the real catalog of store_test.go's
// fixture and (for the capable backend) the real simplified-ACL auth service of authz_test.go,
// because the behaviour that matters is "does the server start, and is the deny in force", not the
// return value of one helper. The log capture reads what the package logger actually wrote, so the
// warning is asserted as an operator would see it.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/logging"
)

// policyStub is an auth.Service whose only implemented method is the one EnsureReservedPrefixProtectPolicy
// calls first: GetPolicy. The embedded nil interface is deliberate - a method Bootstrap does not
// call panics loudly instead of pretending to work, so a future call path this test does not cover
// cannot pass by accident.
type policyStub struct {
	auth.Service
	policyErr error
}

func (s policyStub) GetPolicy(context.Context, string) (*model.Policy, error) {
	return nil, s.policyErr
}

// TestBootstrapPolicyBackend covers D-3: what Bootstrap does when the auth service cannot manage
// policies at all, when it can but the policy call fails, and when it works.
func TestBootstrapPolicyBackend(t *testing.T) {
	// Not parallel: Bootstrap installs process-wide hooks (the uncommitted-GC protector and the
	// merge-conflict resolver), LoadConfig reads the process's viper state, and the log capture
	// redirects the package logger.

	t.Run("a backend without policies degrades to a warning and the server starts", func(t *testing.T) {
		deps := bootstrapPolicyDeps(t, policyStub{policyErr: fmt.Errorf("basic auth service: %w", auth.ErrNotImplemented)})
		readLog := capturePackageLog(t)

		// The exact startup the default auth mode used to abort: it must succeed.
		require.NoError(t, Bootstrap(t.Context(), deps, []string{"custom-operators"}),
			"a service that cannot store policies must not refuse to start the catalog")

		output := readLog()
		require.Contains(t, output, "level=warning", "the degradation is logged loudly, not silently")
		require.Contains(t, output, "policy="+ReservedPrefixProtectPolicyName)
		require.Contains(t, output, "NOT attached", "the warning says the deny is not in force")
		require.Contains(t, output, ReservedPrefix+"/", "the warning names what is not protected")
		require.Contains(t, output, auth.ErrNotImplemented.Error(), "the warning carries the reason")
	})

	t.Run("a genuine policy error is still fatal", func(t *testing.T) {
		genuine := errors.New("kv: connection refused")
		deps := bootstrapPolicyDeps(t, policyStub{policyErr: fmt.Errorf("get policy: %w", genuine)})

		err := Bootstrap(t.Context(), deps, nil)
		require.Error(t, err, "a backend that stores policies and failed must still fail startup")
		require.ErrorIs(t, err, genuine, "the real cause is preserved")
		require.EqualError(t, err, fmt.Sprintf("icebergcatalog: read policy %s: get policy: %v",
			ReservedPrefixProtectPolicyName, genuine),
			"a real error is not softened into a warning")
	})

	t.Run("a capable backend still writes and attaches the deny", func(t *testing.T) {
		f := newAuthzFixture(t)
		// One group of the union list exists in this installation; the rest are expected to be
		// absent and skipped, which Bootstrap must neither fail nor warn about as degraded.
		_, err := f.svc.CreateGroup(t.Context(), &model.Group{DisplayName: "Admins", CreatedAt: gcNow})
		require.NoError(t, err, "create group Admins")
		deps := bootstrapPolicyDeps(t, f.svc)

		require.NoError(t, Bootstrap(t.Context(), deps, []string{"custom-operators"}))

		stored, err := f.svc.GetPolicy(t.Context(), ReservedPrefixProtectPolicyName)
		require.NoError(t, err, "Bootstrap creates the deny policy on a backend that stores policies")
		require.True(t, reservedPrefixProtectStatementsEqual(ReservedPrefixProtectPolicy(stored.CreatedAt).Statement, stored.Statement),
			"the stored policy is the built-in deny: %+v", stored.Statement)

		policies, _, err := f.svc.ListGroupPolicies(t.Context(), "Admins", &model.PaginationParams{Amount: -1})
		require.NoError(t, err)
		names := make([]string, 0, len(policies))
		for _, policy := range policies {
			names = append(names, policy.DisplayName)
		}
		require.Contains(t, names, ReservedPrefixProtectPolicyName, "Bootstrap attaches the deny to Admins")
	})
}

// bootstrapPolicyDeps builds the minimum ServerDeps Bootstrap accepts, around the store fixture's
// real catalog so the §6.3 grace and the §6.4 hook install against a working catalog instead of a
// nil one. It also installs the viper state that enables the catalog, and clears the process-wide
// hooks again when the subtest ends.
func bootstrapPolicyDeps(t *testing.T, svc auth.Service) cmdpkg.ServerDeps {
	t.Helper()
	// The catalog is built before the configuration is installed: pkg/icebergcatalog's store
	// fixture sets the blockstore type through viper once per process, and useConfig resets viper.
	f := newStoreFixture(t)
	useConfig(t, "iceberg:\n  enabled: true\n")
	t.Cleanup(func() {
		catalog.SetUncommittedGCProtector(nil)
		catalog.SetMergeConflictResolver(nil)
	})
	return cmdpkg.ServerDeps{
		Catalog:      f.catalog,
		BlockAdapter: f.catalog.BlockAdapter,
		AuthService:  svc,
		Logger:       logging.DummyLogger{},
	}
}

// capturePackageLog redirects the package logger (the one logging.FromContext returns, where
// EnsureReservedPrefixProtectPolicy writes its warning) to a file and returns a reader for it.
func capturePackageLog(t *testing.T) func() string {
	t.Helper()
	logFile := filepath.Join(t.TempDir(), "icebergcatalog.log")
	require.NoError(t, logging.SetOutputs([]string{logFile}, 0, 0), "redirect the package logger")
	t.Cleanup(func() {
		_ = logging.CloseWriters()
		require.NoError(t, logging.SetOutputs([]string{"-"}, 0, 0), "restore the package logger")
	})
	return func() string {
		require.NoError(t, logging.CloseWriters(), "flush the captured log")
		data, err := os.ReadFile(logFile)
		require.NoError(t, err, "read the captured log")
		return string(data)
	}
}
