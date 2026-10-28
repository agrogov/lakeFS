package sso

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/setup"
	"github.com/treeverse/lakefs/pkg/kv"
	logging "github.com/treeverse/lakefs/pkg/logging"
)

const (
	// RBACBootstrapVersion is the version of the RBAC base template compiled into this
	// binary: the Admins/SuperUsers/Developers/Viewers groups, the policies created by
	// setup.CreateRBACBasePolicies and their attachments.
	//
	// Bump it whenever that template changes, so existing installations apply the change
	// once (on the first boot of the new binary) instead of re-running the bootstrap on
	// every boot.
	RBACBootstrapVersion = 1

	// RBACBootstrapVersionKeyName is the key, inside the "auth" KV partition, that records
	// the last successfully applied RBAC template version. It is read with the same name by
	// the ACL policy reconciler, so template changes apply once per version there too.
	RBACBootstrapVersionKeyName = "rbac-bootstrap-version"
)

// RBACBootstrapVersionKey returns the KV key that holds the applied RBAC template version.
// The version lives in the auth partition (model.PartitionKey), next to the groups and
// policies it describes.
func RBACBootstrapVersionKey() []byte {
	return []byte(RBACBootstrapVersionKeyName)
}

// EnsureRBACBase bootstraps the policy-based RBAC base template (groups, policies and
// attachments) when the version recorded in the auth partition is behind
// RBACBootstrapVersion, and then records the version it applied.
//
// The bootstrap itself never modifies an existing policy, so built-ins that an admin edited
// survive both re-runs and version bumps. Recording the version means a new binary whose
// template changed applies that change exactly once rather than on every boot, and gives
// the policy reconciler a single place to check what was already applied.
//
// A stored version newer than this binary's constant is left untouched: an older binary
// rolling back must not downgrade the record (or the template) of a newer one.
//
// store must be the KV store that backs svc; acl.AuthService deliberately does not expose
// it, and this bootstrap is the only pkg/sso code that needs to write outside the service.
func EnsureRBACBase(ctx context.Context, svc auth.Service, store kv.Store, logger logging.Logger) error {
	applied, err := rbacBootstrapVersion(ctx, store)
	if err != nil {
		return err
	}
	if applied >= RBACBootstrapVersion {
		logger.WithField("version", applied).Debug("rbac: base template already applied")
		return nil
	}

	if err := setup.EnsureRBACBaseGroups(ctx, svc, time.Now()); err != nil {
		return fmt.Errorf("ensure rbac base groups: %w", err)
	}

	// Only record the version after a fully successful bootstrap, so a partial failure is
	// retried on the next boot.
	if err := store.Set(ctx, []byte(model.PartitionKey), RBACBootstrapVersionKey(), []byte(strconv.Itoa(RBACBootstrapVersion))); err != nil {
		return fmt.Errorf("record rbac bootstrap version: %w", err)
	}
	logger.WithFields(logging.Fields{
		"version":         RBACBootstrapVersion,
		"previousVersion": applied,
	}).Info("rbac: base template applied")
	return nil
}

// rbacBootstrapVersion reads the applied template version. A missing key means "never
// bootstrapped" and yields 0. An unparseable value is an error rather than a silent
// re-bootstrap: the bootstrap may be skipped, and failing loudly is safer than pretending
// the template is current.
func rbacBootstrapVersion(ctx context.Context, store kv.Store) (int, error) {
	value, err := store.Get(ctx, []byte(model.PartitionKey), RBACBootstrapVersionKey())
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("read rbac bootstrap version: %w", err)
	}
	version, err := strconv.Atoi(string(value.Value))
	if err != nil {
		return 0, fmt.Errorf("parse rbac bootstrap version %q: %w", value.Value, err)
	}
	return version, nil
}
