// Package config provides PPM configuration settings
package config

import (
	"errors"
	"fmt"

	"github.com/go-playground/validator/v10"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
)

type Config struct {
	Debug            bool                                         `mapstructure:"debug"`
	LogFormat        string                                       `mapstructure:"log-format"`
	ConnectionURL    string                                       `mapstructure:"connection-url"`
	StatementTimeout int                                          `mapstructure:"statement-timeout" validate:"required"`
	LockTimeout      int                                          `mapstructure:"lock-timeout" validate:"required"`
	Partitions       map[string]postgresql.PartitionConfiguration `mapstructure:"partitions" validate:"required,dive,keys,endkeys,required"`
}

func (c *Config) Check() error {
	validate := validator.New()

	err := validate.Struct(c)
	if err != nil {
		formatConfigurationError(err)

		return fmt.Errorf("configuration validation failed: %w", err)
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
