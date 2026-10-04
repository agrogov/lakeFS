package sso

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/config"
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

// ApplyFriendlyNameClaim makes upstream show the configured sso.friendly_name_claim.
// The ACL auth service does not persist friendly names; upstream reads the claim named
// by auth.oidc.friendly_name_claim_name from the session on every request. When that key
// is unset it is filled in from the SSO config; an explicit value always wins.
func ApplyFriendlyNameClaim(baseAuth *config.BaseAuth, ssoCfg *SSOConfig) {
	if baseAuth.OIDC.FriendlyNameClaimName == "" {
		baseAuth.OIDC.FriendlyNameClaimName = ssoCfg.FriendlyNameClaim
	}
}
