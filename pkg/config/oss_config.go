package config

import (
	"fmt"
)

type ConfigImpl struct {
	BaseConfig `mapstructure:",squash"`
	Auth       Auth                   `mapstructure:"auth"`
	UI         UI                     `mapstructure:"ui"`
	SSO        map[string]interface{} `mapstructure:"sso"` // absorbed so viper.UnmarshalExact accepts the "sso:" YAML key; parsed by pkg/sso
}

func (c *ConfigImpl) AuthConfig() AuthConfig {
	return &c.Auth
}

func (c *ConfigImpl) UIConfig() UIConfig {
	return &c.UI
}

func (c *ConfigImpl) Validate() error {
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
