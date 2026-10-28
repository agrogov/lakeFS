package sso_test

import (
	"context"
	"errors"
	"testing"

	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/sso"
)

type fakeBreakGlass struct {
	users  map[string]*model.User
	creds  map[string]string // username -> access key ID
	groups map[string][]string
	err    error // returned by AddCredentials
}

func newFakeBreakGlass() *fakeBreakGlass {
	return &fakeBreakGlass{users: map[string]*model.User{}, creds: map[string]string{}, groups: map[string][]string{}}
}

func (f *fakeBreakGlass) GetUser(_ context.Context, name string) (*model.User, error) {
	if u, ok := f.users[name]; ok {
		return u, nil
	}
	return nil, auth.ErrNotFound
}

func (f *fakeBreakGlass) CreateUser(_ context.Context, u *model.User) (string, error) {
	if _, ok := f.users[u.Username]; ok {
		return "", auth.ErrAlreadyExists
	}
	f.users[u.Username] = u
	return u.Username, nil
}

func (f *fakeBreakGlass) AddCredentials(_ context.Context, name, keyID, _ string) (*model.Credential, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.creds[name] = keyID
	return &model.Credential{}, nil
}

func (f *fakeBreakGlass) AddUserToGroup(_ context.Context, name, group string) error {
	f.groups[name] = append(f.groups[name], group)
	return nil
}

func (f *fakeBreakGlass) DeleteUser(_ context.Context, name string) error {
	delete(f.users, name)
	return nil
}

func setInstallation(t *testing.T, user, keyID, secret string) {
	t.Helper()
	for k, v := range map[string]string{"installation.user_name": user, "installation.access_key_id": keyID, "installation.secret_access_key": secret} {
		viper.Set(k, v)
	}
	t.Cleanup(func() {
		viper.Set("installation.user_name", "")
		viper.Set("installation.access_key_id", "")
		viper.Set("installation.secret_access_key", "")
	})
}

func TestEnsureBreakGlassAdmin_DisabledWithoutKeys(t *testing.T) {
	setInstallation(t, "", "AKIAEXAMPLE", "")
	svc := newFakeBreakGlass()
	created, err := sso.EnsureBreakGlassAdmin(context.Background(), svc, logging.ContextUnavailable())
	if err != nil || created || len(svc.users) != 0 {
		t.Fatalf("got (%v, %v), users=%d; want (false, nil), 0", created, err, len(svc.users))
	}
}

func TestEnsureBreakGlassAdmin_CreatesAdmin(t *testing.T) {
	setInstallation(t, "", "AKIAEXAMPLE", "secret")
	svc := newFakeBreakGlass()
	created, err := sso.EnsureBreakGlassAdmin(context.Background(), svc, logging.ContextUnavailable())
	if err != nil || !created {
		t.Fatalf("got (%v, %v), want (true, nil)", created, err)
	}
	if svc.users["admin"] == nil || svc.creds["admin"] != "AKIAEXAMPLE" {
		t.Fatalf("admin user/credentials not created: users=%v creds=%v", svc.users, svc.creds)
	}
	if g := svc.groups["admin"]; len(g) != 1 || g[0] != "Admins" {
		t.Fatalf("groups = %v, want [Admins]", g)
	}
}

func TestEnsureBreakGlassAdmin_CustomUserName(t *testing.T) {
	setInstallation(t, "ops", "AKIAEXAMPLE", "secret")
	svc := newFakeBreakGlass()
	if _, err := sso.EnsureBreakGlassAdmin(context.Background(), svc, logging.ContextUnavailable()); err != nil || svc.users["ops"] == nil {
		t.Fatalf("err=%v users=%v", err, svc.users)
	}
}

func TestEnsureBreakGlassAdmin_ExistingUserUntouched(t *testing.T) {
	setInstallation(t, "", "AKIAEXAMPLE", "secret")
	svc := newFakeBreakGlass()
	svc.users["admin"] = &model.User{Username: "admin"}
	created, err := sso.EnsureBreakGlassAdmin(context.Background(), svc, logging.ContextUnavailable())
	if err != nil || created || len(svc.creds) != 0 || len(svc.groups) != 0 {
		t.Fatalf("got (%v, %v) creds=%v groups=%v; want nothing changed", created, err, svc.creds, svc.groups)
	}
}

func TestEnsureBreakGlassAdmin_CredentialFailureReported(t *testing.T) {
	setInstallation(t, "", "AKIAEXAMPLE", "secret")
	svc := newFakeBreakGlass()
	svc.err = errors.New("boom")
	if _, err := sso.EnsureBreakGlassAdmin(context.Background(), svc, logging.ContextUnavailable()); err == nil {
		t.Fatal("expected error")
	}
	if len(svc.users) != 0 {
		t.Fatalf("partial user not rolled back: %v", svc.users)
	}
}
