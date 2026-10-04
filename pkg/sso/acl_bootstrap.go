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
func EnsureACLBaseGroups(ctx context.Context, svc *acl.AuthService) error {
	if err := acl.SetupACLServer(ctx, svc); err != nil {
		return fmt.Errorf("acl setup: %w", err)
	}
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
