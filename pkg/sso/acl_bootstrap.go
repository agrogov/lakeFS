package sso

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
)

// EnsureACLBaseGroups creates the Admins/Supers/Writers/Readers ACL groups when they
// are missing. acl.SetupACLServer alone is not enough: it skips group creation when a
// setup timestamp already exists, and "lakefs setup" (basic auth) writes that same
// timestamp, so an installation set up before SSO was enabled would never get groups.
// Idempotent: safe to call on every startup and from sso-migrate.
//
// It deliberately does not call acl.SetupACLServer, because that also writes the setup
// timestamp, and the timestamp is the installation's "initialized" marker
// (auth.KVMetadataManager.IsInitialized and acl.IsInitialized both read it). The builder runs
// before setupLakeFS decides whether the KV is initialized, so writing it here would make a
// fresh installation look complete before its first admin user exists (GAP-01). Creating the
// groups is all this bootstrap needs to do.
func EnsureACLBaseGroups(ctx context.Context, svc *acl.AuthService) error {
	_, err := svc.GetGroup(ctx, acl.AdminsGroup)
	if err == nil {
		return nil
	}
	if !errors.Is(err, auth.ErrNotFound) {
		return fmt.Errorf("check %s group: %w", acl.AdminsGroup, err)
	}
	if err = acl.CreateACLBaseGroups(ctx, svc, time.Now()); err != nil {
		return fmt.Errorf("create base ACL groups: %w", err)
	}
	return nil
}
