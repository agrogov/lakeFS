package sso

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	logging "github.com/treeverse/lakefs/pkg/logging"
)

const (
	breakGlassDefaultUser = "admin"
	breakGlassUserSource  = "internal"
)

// breakGlassProvisioner is the subset of the ACL auth service used to seed the local admin.
type breakGlassProvisioner interface {
	GetUser(ctx context.Context, username string) (*model.User, error)
	CreateUser(ctx context.Context, user *model.User) (string, error)
	AddCredentials(ctx context.Context, username, accessKeyID, secretAccessKey string) (*model.Credential, error)
	AddUserToGroup(ctx context.Context, username, groupDisplayName string) error
	DeleteUser(ctx context.Context, username string) error
}

// EnsureBreakGlassAdmin seeds a local admin with a fixed access key pair so there is a way in
// when SSO is unavailable (e.g. the identity provider app is not approved yet). It reads
// installation.user_name (default "admin"), installation.access_key_id and
// installation.secret_access_key, i.e. LAKEFS_INSTALLATION_*.
//
// It does nothing unless both keys are set, and nothing when the user already exists, so it is
// safe on every startup and never touches SSO users (those are named after the IdP user ID).
// Remove the settings and delete the user once SSO admins exist. Returns whether it created the user.
func EnsureBreakGlassAdmin(ctx context.Context, svc breakGlassProvisioner, logger logging.Logger) (bool, error) {
	accessKeyID := viper.GetString("installation.access_key_id")
	secretAccessKey := viper.GetString("installation.secret_access_key")
	if accessKeyID == "" || secretAccessKey == "" {
		return false, nil
	}
	username := viper.GetString("installation.user_name")
	if username == "" {
		username = breakGlassDefaultUser
	}

	_, err := svc.GetUser(ctx, username)
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, auth.ErrNotFound) {
		return false, fmt.Errorf("break-glass admin: get user %q: %w", username, err)
	}

	if _, err = svc.CreateUser(ctx, &model.User{CreatedAt: time.Now().UTC(), Source: breakGlassUserSource, Username: username}); err != nil {
		if errors.Is(err, auth.ErrAlreadyExists) {
			return false, nil
		}
		return false, fmt.Errorf("break-glass admin: create user %q: %w", username, err)
	}
	// A user without keys or group would be skipped on the next start, so undo a partial setup.
	rollback := func() {
		if delErr := svc.DeleteUser(ctx, username); delErr != nil {
			logger.WithField("username", username).WithError(delErr).Error("break-glass admin: rollback failed")
		}
	}
	if _, err = svc.AddCredentials(ctx, username, accessKeyID, secretAccessKey); err != nil {
		rollback()
		return false, fmt.Errorf("break-glass admin: add credentials for %q: %w", username, err)
	}
	if err = svc.AddUserToGroup(ctx, username, acl.AdminsGroup); err != nil {
		rollback()
		return false, fmt.Errorf("break-glass admin: add %q to %s: %w", username, acl.AdminsGroup, err)
	}
	logger.WithField("username", username).Warn("break-glass local admin created from installation.* settings; remove them once SSO admins exist")
	return true, nil
}
