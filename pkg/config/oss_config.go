package config

import (
	"fmt"
)

type ConfigImpl struct {
	BaseConfig `mapstructure:",squash"`
	Auth       Auth                   `mapstructure:"auth"`
	UI         UI                     `mapstructure:"ui"`
	SSO        SecretSection          `mapstructure:"sso"`      // absorbed so viper.UnmarshalExact accepts the "sso:" YAML key; parsed by pkg/sso
	Datasets   map[string]interface{} `mapstructure:"datasets"` // absorbed so viper.UnmarshalExact accepts the "datasets:" YAML key; parsed by pkg/datasets
}

// SecretSection is a config section kept only so viper.UnmarshalExact accepts its key; it is
// parsed elsewhere and may hold secrets (e.g. sso.client_secret), so it is masked when logged.
type SecretSection map[string]any

func (c *ConfigImpl) AuthConfig() AuthConfig {
	return &c.Auth
}

func (c *ConfigImpl) UIConfig() UIConfig {
	return &c.UI
}

func (c *ConfigImpl) Validate() error {
	if err := c.Auth.ValidateAuthorizationFailureStatus(); err != nil {
		return err
	}
	if err := c.ValidateTrustedProxies(); err != nil {
		return err
	}
	missingKeys := ValidateMissingRequiredKeys(c, "mapstructure", "squash")
	if len(missingKeys) > 0 {
		return fmt.Errorf("%w: %v", ErrMissingRequiredKeys, missingKeys)
	}
	return ValidateBlockstore(&c.Blockstore)
}

func BuildConfig(cfgType string) (Config, error) {
	c := &ConfigImpl{}
	_, err := NewConfig(cfgType, c)
	if err != nil {
		return nil, err
	}

	// Perform required validations
	if err = c.Validate(); err != nil {
		return nil, err
	}

	err = c.ValidateDomainNames()
	if err != nil {
		return nil, err
	}

	return c, nil
}
