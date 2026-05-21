package sso_test

import (
	"context"
	"sort"
	"testing"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	logging "github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/sso"
)

// fakeGroupManager is a minimal in-memory implementation of the groupManager interface.
type fakeGroupManager struct {
	members map[string]map[string]bool // groupName → set of usernames
}

func newFakeGroupManager(initialMemberships map[string][]string) *fakeGroupManager {
	f := &fakeGroupManager{members: make(map[string]map[string]bool)}
	for group, users := range initialMemberships {
		f.members[group] = make(map[string]bool)
		for _, u := range users {
			f.members[group][u] = true
		}
	}
	return f
}

func (f *fakeGroupManager) ListUserGroups(_ context.Context, username string, _ *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	var groups []*model.Group
	for gname, users := range f.members {
		if users[username] {
			groups = append(groups, &model.Group{DisplayName: gname})
		}
	}
	return groups, &model.Paginator{}, nil
}

func (f *fakeGroupManager) AddUserToGroup(_ context.Context, username, groupID string) error {
	if _, exists := f.members[groupID]; !exists {
		return auth.ErrNotFound
	}
	f.members[groupID][username] = true
	return nil
}

func (f *fakeGroupManager) RemoveUserFromGroup(_ context.Context, username, groupID string) error {
	if users, exists := f.members[groupID]; exists {
		delete(users, username)
	}
	return nil
}

// groupsOf returns a sorted list of group names the user belongs to.
func (f *fakeGroupManager) groupsOf(username string) []string {
	var out []string
	for gname, users := range f.members {
		if users[username] {
			out = append(out, gname)
		}
	}
	sort.Strings(out)
	return out
}

func baseCfg() *sso.SSOConfig {
	return &sso.SSOConfig{
		SyncGroupsOnLogin: true,
		GroupsClaim:       "roles",
		UserIDClaim:       "oid",
	}
}

func TestSyncGroups_AddAndRemove(t *testing.T) {
	svc := newFakeGroupManager(map[string][]string{
		"Admins":  {"alice"},
		"Writers": {},
		"Readers": {"alice"},
	})
	cfg := baseCfg()

	// Token says alice should be in Admins and Writers (not Readers).
	err := sso.SyncGroups(context.Background(), svc, logging.Dummy(), "alice", []string{"Admins", "Writers"}, cfg)
	if err != nil {
		t.Fatalf("SyncGroups: %v", err)
	}

	got := svc.groupsOf("alice")
	want := []string{"Admins", "Writers"}
	if !equalSlices(got, want) {
		t.Errorf("groups after sync: got %v, want %v", got, want)
	}
}

func TestSyncGroups_Prefix_PreservesUnmanaged(t *testing.T) {
	svc := newFakeGroupManager(map[string][]string{
		"sso-Admins":  {"bob"},
		"sso-Writers": {},
		"Readers":     {"bob"}, // not managed (no prefix)
	})
	cfg := baseCfg()
	cfg.ManagedGroupPrefix = "sso-"

	// Token says bob is in sso-Writers only (not sso-Admins).
	err := sso.SyncGroups(context.Background(), svc, logging.Dummy(), "bob", []string{"sso-Writers"}, cfg)
	if err != nil {
		t.Fatalf("SyncGroups: %v", err)
	}

	got := svc.groupsOf("bob")
	want := []string{"Readers", "sso-Writers"} // sso-Admins removed, Readers preserved
	if !equalSlices(got, want) {
		t.Errorf("groups after sync: got %v, want %v", got, want)
	}
}

func TestSyncGroups_Prefix_IgnoresUnprefixedTokenGroups(t *testing.T) {
	svc := newFakeGroupManager(map[string][]string{
		"sso-Admins": {},
		"Admins":     {},
	})
	cfg := baseCfg()
	cfg.ManagedGroupPrefix = "sso-"

	// Token lists both "sso-Admins" and "Admins" (no prefix) — only sso-Admins should be applied.
	err := sso.SyncGroups(context.Background(), svc, logging.Dummy(), "carol", []string{"sso-Admins", "Admins"}, cfg)
	if err != nil {
		t.Fatalf("SyncGroups: %v", err)
	}

	got := svc.groupsOf("carol")
	want := []string{"sso-Admins"}
	if !equalSlices(got, want) {
		t.Errorf("groups after sync: got %v, want %v", got, want)
	}
}

func TestSyncGroups_Disabled(t *testing.T) {
	svc := newFakeGroupManager(map[string][]string{
		"Admins": {"dave"},
	})
	cfg := baseCfg()
	cfg.SyncGroupsOnLogin = false

	err := sso.SyncGroups(context.Background(), svc, logging.Dummy(), "dave", []string{"Writers"}, cfg)
	if err != nil {
		t.Fatalf("SyncGroups: %v", err)
	}

	// No changes — sync was disabled.
	got := svc.groupsOf("dave")
	want := []string{"Admins"}
	if !equalSlices(got, want) {
		t.Errorf("expected no changes, got %v", got)
	}
}

func TestSyncGroups_GroupNotFound_Skipped(t *testing.T) {
	svc := newFakeGroupManager(map[string][]string{
		"Admins": {},
		// "NonExistent" is not registered in the fake → AddUserToGroup returns ErrNotFound
	})
	cfg := baseCfg()

	// Should not return error even though "NonExistent" group is missing.
	err := sso.SyncGroups(context.Background(), svc, logging.Dummy(), "eve", []string{"Admins", "NonExistent"}, cfg)
	if err != nil {
		t.Fatalf("SyncGroups returned error on missing group: %v", err)
	}

	got := svc.groupsOf("eve")
	want := []string{"Admins"}
	if !equalSlices(got, want) {
		t.Errorf("groups after sync: got %v, want %v", got, want)
	}
}

func TestSyncGroups_EmptyToken_RemovesAll(t *testing.T) {
	svc := newFakeGroupManager(map[string][]string{
		"Admins":  {"frank"},
		"Writers": {"frank"},
	})
	cfg := baseCfg()

	// Empty token → remove all managed groups.
	err := sso.SyncGroups(context.Background(), svc, logging.Dummy(), "frank", nil, cfg)
	if err != nil {
		t.Fatalf("SyncGroups: %v", err)
	}

	got := svc.groupsOf("frank")
	if len(got) != 0 {
		t.Errorf("expected no groups after empty token, got %v", got)
	}
}

func TestExtractStringSlice(t *testing.T) {
	cases := []struct {
		name  string
		input any
		want  []string
	}{
		{"nil", nil, nil},
		{"string", "Admins", []string{"Admins"}},
		{"empty string", "", nil},
		{"string slice", []string{"A", "B"}, []string{"A", "B"}},
		{"interface slice", []interface{}{"X", "Y"}, []string{"X", "Y"}},
		{"interface slice with non-string", []interface{}{"X", 42, "Y"}, []string{"X", "Y"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sso.ExtractStringSlice(tc.input)
			if !equalSlices(got, tc.want) {
				t.Errorf("ExtractStringSlice(%v) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
