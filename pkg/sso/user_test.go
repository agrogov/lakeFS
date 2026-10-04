package sso_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/sso"
)

// fakeUserStore implements the user and group methods used by the OIDC callback.
// Embedding auth.Service lets it be passed as one; unimplemented methods panic.
type fakeUserStore struct {
	auth.Service
	*fakeGroupManager
	users     map[string]*model.User // by external ID
	createErr error
	creates   int
}

func newFakeUserStore(groups map[string][]string) *fakeUserStore {
	return &fakeUserStore{fakeGroupManager: newFakeGroupManager(groups), users: map[string]*model.User{}}
}

func (f *fakeUserStore) GetUserByExternalID(_ context.Context, id string) (*model.User, error) {
	if u, ok := f.users[id]; ok {
		return u, nil
	}
	return nil, auth.ErrNotFound
}

func (f *fakeUserStore) CreateUser(_ context.Context, u *model.User) (string, error) {
	f.creates++
	if f.createErr != nil {
		return "", f.createErr
	}
	f.users[*u.ExternalID] = u
	return u.Username, nil
}

// Resolve the ambiguity between auth.Service and *fakeGroupManager for the group methods.
func (f *fakeUserStore) AddUserToGroup(ctx context.Context, u, g string) error {
	return f.fakeGroupManager.AddUserToGroup(ctx, u, g)
}

func (f *fakeUserStore) RemoveUserFromGroup(ctx context.Context, u, g string) error {
	return f.fakeGroupManager.RemoveUserFromGroup(ctx, u, g)
}

func (f *fakeUserStore) ListUserGroups(ctx context.Context, u string, p *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	return f.fakeGroupManager.ListUserGroups(ctx, u, p)
}

func TestEnsureUser_CreatesMissingUser(t *testing.T) {
	store := newFakeUserStore(nil)
	created, err := sso.EnsureUser(context.Background(), store, "oid-1")
	if err != nil || !created {
		t.Fatalf("EnsureUser = (%v, %v), want (true, nil)", created, err)
	}
	u := store.users["oid-1"]
	if u == nil || u.Username != "oid-1" || u.Source != "oidc" || *u.ExternalID != "oid-1" {
		t.Fatalf("unexpected user created: %+v", u)
	}
}

func TestEnsureUser_ExistingUserUntouched(t *testing.T) {
	store := newFakeUserStore(nil)
	store.users["oid-1"] = &model.User{Username: "oid-1"}
	created, err := sso.EnsureUser(context.Background(), store, "oid-1")
	if err != nil || created || store.creates != 0 {
		t.Fatalf("EnsureUser = (%v, %v), creates=%d; want (false, nil), 0", created, err, store.creates)
	}
}

func TestEnsureUser_ConcurrentCreateIsNotAnError(t *testing.T) {
	store := newFakeUserStore(nil)
	store.createErr = auth.ErrAlreadyExists
	created, err := sso.EnsureUser(context.Background(), store, "oid-1")
	if err != nil || created {
		t.Fatalf("EnsureUser = (%v, %v), want (false, nil)", created, err)
	}
}

func TestEnsureUser_CreateFailureReported(t *testing.T) {
	store := newFakeUserStore(nil)
	store.createErr = errors.New("boom")
	if _, err := sso.EnsureUser(context.Background(), store, "oid-1"); err == nil {
		t.Fatal("expected error")
	}
}

func TestApplyFriendlyNameClaim(t *testing.T) {
	tests := []struct {
		name, existing, sso, want string
	}{
		{"unset upstream key takes sso claim", "", "preferred_username", "preferred_username"},
		{"explicit upstream key wins", "name", "preferred_username", "name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := &config.BaseAuth{}
			base.OIDC.FriendlyNameClaimName = tt.existing
			sso.ApplyFriendlyNameClaim(base, &sso.SSOConfig{FriendlyNameClaim: tt.sso})
			if got := base.OIDC.FriendlyNameClaimName; got != tt.want {
				t.Fatalf("FriendlyNameClaimName = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestOauthCallbackFirstLoginSyncsGroups is the regression test for the first-login gap:
// the user does not exist yet when the callback runs, and must still get its groups.
func TestOauthCallbackFirstLoginSyncsGroups(t *testing.T) {
	mock := newMockOIDCServer(t)
	defer mock.close()
	mock.extraClaims = map[string]any{"roles": []string{"Readers"}}

	authSvc := newFakeUserStore(map[string][]string{"Readers": nil, "Writers": nil})
	svc := buildServiceWithAuth(t, mock, authSvc, true)
	store := buildSessionStore()

	state, nonce, cookies := seedFlowSession(t, svc, store)
	oid := "azure-oid-1"
	callbackURL := fmt.Sprintf("/api/v1/oidc/callback?code=%s&state=%s",
		url.QueryEscape(nonce+":"+oid), url.QueryEscape(state))
	req := httptest.NewRequest(http.MethodGet, callbackURL, nil)
	for _, c := range cookies {
		req.AddCookie(c)
	}
	w := httptest.NewRecorder()
	svc.OauthCallback(w, req, store)

	if w.Code != http.StatusFound {
		t.Fatalf("callback: got %d, body %s", w.Code, w.Body.String())
	}
	if _, err := authSvc.GetUserByExternalID(context.Background(), oid); err != nil {
		t.Fatalf("user was not provisioned at login: %v", err)
	}
	if got := authSvc.groupsOf(oid); len(got) != 1 || got[0] != "Readers" {
		t.Fatalf("groups after first login = %v, want [Readers]", got)
	}
}
