// Package config provides PPM configuration settings
package config

import (
	"errors"
	"fmt"

	"github.com/go-playground/validator/v10"
	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/spf13/viper"
)

var ErrConfigValidationFailed = errors.New("configuration validation failed")

type Config struct {
	Debug            bool                               `mapstructure:"debug"`
	LogFormat        string                             `mapstructure:"log-format"`
	ConnectionURL    string                             `mapstructure:"connection-url"`
	StatementTimeout int                                `mapstructure:"statement-timeout" validate:"required"`
	LockTimeout      int                                `mapstructure:"lock-timeout" validate:"required"`
	Partitions       map[string]partition.Configuration `mapstructure:"partitions" validate:"required,dive,keys,endkeys,required"`
}

// FromViper explicitly builds a Config from the given viper instance and applies defaults.
// It does not validate: call Check() on the result.
func FromViper(v *viper.Viper) (*Config, error) {
	var c Config

	if err := v.Unmarshal(&c); err != nil {
		return nil, fmt.Errorf("unable to load configuration: %w", err)
	}

	// "initialRetention" defaults to "retention" only when the key is absent:
	// 0 is a valid explicit value. Viper lowercases keys.
	raw := v.GetStringMap("partitions")

	for name, p := range c.Partitions {
		if m, ok := raw[name].(map[string]any); ok {
			if _, set := m["initialretention"]; !set {
				p.InitialRetention = p.Retention
				c.Partitions[name] = p
			}
		}
	}

	return &c, nil
}

func (c *Config) Check() error {
	validate := validator.New()

	err := validate.Struct(c)
	if err != nil {
		formatConfigurationError(err)

		return fmt.Errorf("configuration validation failed: %w", err)
	}
	// check that retention periods are consistent
	for _, partConfig := range c.Partitions {
		if partConfig.InitialRetention > partConfig.Retention {
			return fmt.Errorf("%w: \"initialRetention\" (%d) must be lower or equal to \"retention\" (%d)", ErrConfigValidationFailed, partConfig.InitialRetention, partConfig.Retention)
		}
	}

	return nil
}

func formatConfigurationError(err error) {
	var invalidValidation *validator.InvalidValidationError

	if errors.As(err, &invalidValidation) {
		fmt.Println("ERROR: The provided configuration is invalid and cannot be processed. Please check your configuration for any structural errors.")
	}

	var validationErrors validator.ValidationErrors

	if errors.As(err, &validationErrors) {
		for _, e := range validationErrors {
			switch e.Tag() {
			case "required":
				fmt.Printf("ERROR: The '%s' field is required and cannot be empty.\n", e.StructNamespace())
			case "oneof":
				fmt.Printf("ERROR: The '%s' field must be one of [%s], but got '%s'.\n", e.StructNamespace(), e.Param(), e.Value())
			default:
				// Generic error message for any other validation tags
				fmt.Printf("ERROR: The '%s' field is not valid: %s\n", e.Field(), e.Error())
			}
		}
	}
}
