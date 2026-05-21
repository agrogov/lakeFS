package sso

import "github.com/spf13/viper"

// SSOConfig holds Azure Entra ID OIDC configuration, read from the "sso:" YAML namespace.
// The upstream config.ConfigImpl absorbs the "sso" key via its SSO field so that
// viper.UnmarshalExact does not reject it; this struct is populated by LoadSSOConfig via
// viper.UnmarshalKey independently.
type SSOConfig struct {
	Enabled           bool              `mapstructure:"enabled"`
	ClientID          string            `mapstructure:"client_id"`
	ClientSecret      string            `mapstructure:"client_secret"`
	IssuerURL         string            `mapstructure:"issuer_url"`
	CallbackBaseURL   string            `mapstructure:"callback_base_url"`
	Scopes            []string          `mapstructure:"scopes"`
	UserIDClaim       string            `mapstructure:"user_id_claim"`
	FriendlyNameClaim string            `mapstructure:"friendly_name_claim"`
	GroupsClaim       string            `mapstructure:"groups_claim"`
	DefaultGroups     []string          `mapstructure:"default_groups"`
	SyncGroupsOnLogin bool              `mapstructure:"sync_groups_on_login"`
	ManagedGroupPrefix string           `mapstructure:"managed_group_prefix"`
	AuthorizeParams   map[string]string `mapstructure:"authorize_params"`
	LogoutURL         string            `mapstructure:"logout_url"`
}

func LoadSSOConfig() *SSOConfig {
	cfg := &SSOConfig{}
	if err := viper.UnmarshalKey("sso", cfg); err != nil {
		// A structurally invalid sso: section (wrong field type, etc.) would leave cfg at
		// zero values, including Enabled=false, causing silent fallback to basic auth.
		// Panic here so the misconfiguration is never silently hidden.
		panic("sso: config parse error: " + err.Error())
	}
	if len(cfg.Scopes) == 0 {
		cfg.Scopes = []string{"openid", "profile", "email"}
	}
	if cfg.UserIDClaim == "" {
		cfg.UserIDClaim = "oid"
	}
	if cfg.FriendlyNameClaim == "" {
		cfg.FriendlyNameClaim = "preferred_username"
	}
	if cfg.GroupsClaim == "" {
		cfg.GroupsClaim = "roles"
	}
	return cfg
}
