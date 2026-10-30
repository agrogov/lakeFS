package sso

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/treeverse/lakefs/contrib/auth/acl"
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
	//
	// Version 2: setup.CreateRBACBasePolicies gained the DatasetsFullAccess and
	// DatasetsReadAll policies (card C4), so installations that bootstrapped version 1
	// re-apply the template once to pick them up.
	//
	// Version 3: SuperUsers gained the PRReadWriteAll attachment, so a Super keeps pull-request
	// write access after `rbac-migrate` moves it out of the ACL Supers group (GAP-06).
	RBACBootstrapVersion = 3

	// RBACBootstrapVersionKeyName is the name of the record, inside the "auth" KV partition,
	// that holds the last successfully applied RBAC template version. It is recorded through
	// acl.TemplateVersionKey, i.e. under the same "metadata/<name>" path and in the same
	// partition as the ACL policy reconciler's own version record, so both records follow one
	// convention.
	//
	// The two records are nevertheless distinct keys: they version two independent template
	// sets, applied by two independent guards and bumped independently. See
	// acl.TemplateVersionKey for why a single shared record was rejected, and RBAC_DESIGN.md
	// §4.5 #2 for the original request it deviates from.
	RBACBootstrapVersionKeyName = "rbac-bootstrap-version"

	// legacyRBACBootstrapVersionKeyName is the pre-R1 key: a bare key in the auth partition
	// root, "rbac-bootstrap-version", written without the "metadata/" path. It is only read, so
	// an installation that recorded its version there is migrated to the current key instead of
	// being bootstrapped again, and a rollback to an older binary still finds its own record.
	legacyRBACBootstrapVersionKeyName = "rbac-bootstrap-version"
)

// RBACBootstrapVersionKey returns the KV key that holds the applied RBAC template version.
// The version lives in the auth partition (model.PartitionKey), next to the groups and
// policies it describes, under the shared template-version path convention.
func RBACBootstrapVersionKey() []byte {
	return acl.TemplateVersionKey(RBACBootstrapVersionKeyName)
}

// EnsureRBACBase bootstraps the policy-based RBAC base template (groups, policies and
// attachments) when the version recorded in the auth partition is behind
// RBACBootstrapVersion, and then records the version it applied.
//
// The bootstrap itself never modifies an existing policy, so built-ins that an admin edited
// survive both re-runs and version bumps. Recording the version means a new binary whose
// template changed applies that change exactly once rather than on every boot, and it lets an
// operator see what was already applied.
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
//
// A version recorded under the pre-R1 bare key is migrated to the current key and used as is,
// so upgrading does not re-run the bootstrap for installations that already recorded one.
func rbacBootstrapVersion(ctx context.Context, store kv.Store) (int, error) {
	value, found, err := acl.ReadTemplateVersion(ctx, store, RBACBootstrapVersionKeyName,
		[]byte(legacyRBACBootstrapVersionKeyName))
	if err != nil {
		return 0, fmt.Errorf("read rbac bootstrap version: %w", err)
	}
	if !found {
		return 0, nil
	}
	version, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse rbac bootstrap version %q: %w", value, err)
	}
	return version, nil
}
