package sso

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

func TestProfileFromClaims(t *testing.T) {
	cfg := &SSOConfig{FriendlyNameClaim: "preferred_username"}
	tests := []struct {
		name   string
		claims map[string]any
		want   UserProfile
	}{
		{"email and name", map[string]any{"email": "a@x.com", "name": "Alice", "preferred_username": "alice@x.com"}, UserProfile{"a@x.com", "Alice"}},
		{"upn as email", map[string]any{"name": "Alice", "preferred_username": "alice@x.com"}, UserProfile{"alice@x.com", "Alice"}},
		{"non-upn username", map[string]any{"preferred_username": "alice"}, UserProfile{"", "alice"}},
		{"nothing", map[string]any{"oid": "123"}, UserProfile{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ProfileFromClaims(tt.claims, cfg); got != tt.want {
				t.Fatalf("ProfileFromClaims = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestKVProfileStoreUpdateUserProfile(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)
	key := model.UserPath("oid-1")
	if err := kv.SetMsg(ctx, store, model.PartitionKey, key, &model.UserData{Username: "oid-1", ExternalId: "oid-1", Source: "oidc"}); err != nil {
		t.Fatal(err)
	}
	ps := &kvProfileStore{store: store}

	if err := ps.UpdateUserProfile(ctx, "oid-1", UserProfile{Email: "a@x.com", FriendlyName: "Alice"}); err != nil {
		t.Fatalf("UpdateUserProfile: %v", err)
	}
	// empty fields leave stored values alone
	if err := ps.UpdateUserProfile(ctx, "oid-1", UserProfile{FriendlyName: "Alice B"}); err != nil {
		t.Fatalf("UpdateUserProfile: %v", err)
	}

	var got model.UserData
	if _, err := kv.GetMsg(ctx, store, model.PartitionKey, key, &got); err != nil {
		t.Fatal(err)
	}
	if got.Email != "a@x.com" || got.FriendlyName != "Alice B" || got.ExternalId != "oid-1" || got.Source != "oidc" {
		t.Fatalf("user = %+v", &got)
	}

	if err := ps.UpdateUserProfile(ctx, "missing", UserProfile{Email: "b@x.com"}); err == nil {
		t.Fatal("expected error for missing user")
	}
}
