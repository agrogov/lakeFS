package sso

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
)

// oidcUserSource is the Source value upstream's UserFromOIDCSession gives to users it
// creates from an OIDC session. Users provisioned here must look the same.
const oidcUserSource = "oidc"

// userProvisioner is the subset of auth.Service used to provision a user at login.
type userProvisioner interface {
	GetUserByExternalID(ctx context.Context, externalID string) (*model.User, error)
	CreateUser(ctx context.Context, user *model.User) (string, error)
}

// EnsureUser makes sure a user with the given external ID exists, creating it the same
// way upstream's UserFromOIDCSession would on the user's first request. It reports
// whether the user was created by this call. A concurrent creation is not an error.
//
// OauthCallback needs this because group sync runs at login, before the first
// authenticated request has had a chance to create the user.
func EnsureUser(ctx context.Context, svc userProvisioner, externalID string) (bool, error) {
	_, err := svc.GetUserByExternalID(ctx, externalID)
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, auth.ErrNotFound) {
		return false, fmt.Errorf("get user %q: %w", externalID, err)
	}
	u := model.User{CreatedAt: time.Now().UTC(), Source: oidcUserSource, Username: externalID, ExternalID: &externalID}
	if _, err = svc.CreateUser(ctx, &u); err != nil {
		if errors.Is(err, auth.ErrAlreadyExists) {
			return false, nil
		}
		return false, fmt.Errorf("create user %q: %w", externalID, err)
	}
	return true, nil
}

// UserProfile is the human-readable identity taken from the ID token. The user ID itself
// stays the immutable object ID (sso.user_id_claim), so renames in Entra never orphan a user.
type UserProfile struct {
	Email        string
	FriendlyName string
}

// ProfileFromClaims picks the email (email claim, else a UPN-looking preferred_username)
// and display name (name claim, else the configured friendly name claim).
func ProfileFromClaims(claims map[string]any, cfg *SSOConfig) UserProfile {
	str := func(k string) string { s, _ := claims[k].(string); return strings.TrimSpace(s) }
	p := UserProfile{Email: str("email"), FriendlyName: str("name")}
	if upn := str("preferred_username"); p.Email == "" && strings.Contains(upn, "@") {
		p.Email = upn
	}
	if p.FriendlyName == "" {
		p.FriendlyName = str(cfg.FriendlyNameClaim)
	}
	return p
}

// kvProfileStore writes email/friendly name straight onto the ACL user record:
// acl.AuthService has no update call (UpdateUserFriendlyName is not implemented).
type kvProfileStore struct {
	store kv.Store
}

// UpdateUserProfile stores the non-empty profile fields that differ from the user record.
func (k *kvProfileStore) UpdateUserProfile(ctx context.Context, username string, p UserProfile) error {
	key := model.UserPath(username)
	data := model.UserData{}
	pred, err := kv.GetMsg(ctx, k.store, model.PartitionKey, key, &data)
	if err != nil {
		return fmt.Errorf("get user %q: %w", username, err)
	}
	changed := false
	if p.Email != "" && data.Email != p.Email {
		data.Email, changed = p.Email, true
	}
	if p.FriendlyName != "" && data.FriendlyName != p.FriendlyName {
		data.FriendlyName, changed = p.FriendlyName, true
	}
	if !changed {
		return nil
	}
	// a concurrent login writes the same values, so losing the race is fine
	if err = kv.SetMsgIf(ctx, k.store, model.PartitionKey, key, &data, pred); err != nil && !errors.Is(err, kv.ErrPredicateFailed) {
		return fmt.Errorf("update user %q: %w", username, err)
	}
	return nil
}

// ApplyFriendlyNameClaim makes upstream show the configured sso.friendly_name_claim.
// The ACL auth service does not persist friendly names; upstream reads the claim named
// by auth.oidc.friendly_name_claim_name from the session on every request. When that key
// is unset it is filled in from the SSO config; an explicit value always wins.
func ApplyFriendlyNameClaim(baseAuth *config.BaseAuth, ssoCfg *SSOConfig) {
	if baseAuth.OIDC.FriendlyNameClaimName == "" {
		baseAuth.OIDC.FriendlyNameClaimName = ssoCfg.FriendlyNameClaim
	}
}
