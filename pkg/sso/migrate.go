package sso

import (
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/kv"
	logging "github.com/treeverse/lakefs/pkg/logging"
	"google.golang.org/protobuf/proto"
)

// MigrateBasicToACL copies the single admin user (and their credentials) from the
// BasicAuthService KV partition ("basicAuth") into the ACL auth service partition ("auth").
// It is idempotent: if the user already exists in the ACL partition the function returns nil.
// After migration the user is placed in the "Admins" ACL group.
//
// KV layout:
//   basicAuth / model.UserPath("superAdmin")          → model.UserData
//   basicAuth / model.CredentialPath("superAdmin", *) → model.CredentialData
//   auth      / model.UserPath(username)              → model.UserData  (ACL)
//   auth      / model.CredentialPath(username, *)     → model.CredentialData (ACL)
func MigrateBasicToACL(ctx context.Context, kvStore kv.Store, secretStore crypt.SecretStore, logger logging.Logger) error {
	// --- 1. Read admin user from basicAuth partition ---
	var userData model.UserData
	userKey := model.UserPath(auth.SuperAdminKey)
	_, err := kv.GetMsg(ctx, kvStore, auth.BasicPartitionKey, userKey, &userData)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			// Nothing to migrate — basicAuth partition has no user yet.
			logger.Info("migrate: no user found in basicAuth partition, skipping")
			return nil
		}
		return fmt.Errorf("migrate: read admin user: %w", err)
	}
	user := model.UserFromProto(&userData)
	logger.WithField("username", user.Username).Info("migrate: found admin user in basicAuth partition")

	// --- 2. Read credentials from basicAuth partition ---
	// Credentials are keyed under "superAdmin" (not the real username) in basicAuth.
	var credData model.CredentialData
	credPrefix := model.CredentialPath(auth.SuperAdminKey, "")
	it, err := kv.NewPrimaryIterator(ctx, kvStore, (&credData).ProtoReflect().Type(),
		auth.BasicPartitionKey, credPrefix, kv.IteratorOptionsAfter([]byte("")))
	if err != nil {
		return fmt.Errorf("migrate: iterate credentials: %w", err)
	}
	defer it.Close()

	var rawCreds []proto.Message
	for it.Next() {
		v := it.Entry().Value
		rawCreds = append(rawCreds, v)
	}
	if err = it.Err(); err != nil {
		return fmt.Errorf("migrate: read credentials: %w", err)
	}

	creds, err := model.ConvertCredDataList(secretStore, rawCreds, true)
	if err != nil {
		return fmt.Errorf("migrate: decrypt credentials: %w", err)
	}
	logger.WithField("count", len(creds)).Info("migrate: found credentials in basicAuth partition")

	// --- 3. Instantiate ACL service to write into "auth" partition ---
	noCache := authparams.ServiceCache{Enabled: false}
	aclSvc := acl.NewAuthService(kvStore, secretStore, noCache)

	// --- 4. Create user in ACL partition (idempotent) ---
	if _, err = aclSvc.CreateUser(ctx, user); err != nil {
		if !errors.Is(err, auth.ErrAlreadyExists) {
			return fmt.Errorf("migrate: create user in ACL: %w", err)
		}
		logger.WithField("username", user.Username).Info("migrate: user already exists in ACL partition, skipping user creation")
	}

	// --- 5. Copy credentials (idempotent) ---
	for _, c := range creds {
		if _, err = aclSvc.AddCredentials(ctx, user.Username, c.AccessKeyID, c.SecretAccessKey); err != nil {
			if !errors.Is(err, auth.ErrAlreadyExists) {
				return fmt.Errorf("migrate: add credentials (%s): %w", c.AccessKeyID, err)
			}
		}
	}

	// --- 6. Add to Admins group ---
	if err = aclSvc.AddUserToGroup(ctx, user.Username, acl.AdminsGroup); err != nil {
		if !errors.Is(err, auth.ErrAlreadyExists) {
			return fmt.Errorf("migrate: add user to Admins group: %w", err)
		}
	}

	logger.WithField("username", user.Username).Info("migrate: admin user migrated to ACL partition successfully")
	return nil
}
