package sso

import (
	"testing"
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
