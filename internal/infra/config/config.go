// Package config provides PPM configuration settings
package config

import (
	"errors"
	"fmt"

	"github.com/go-playground/validator/v10"
	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
)

type Config struct {
	Debug            bool                               `mapstructure:"debug"`
	LogFormat        string                             `mapstructure:"log-format"`
	ConnectionURL    string                             `mapstructure:"connection-url"`
	StatementTimeout int                                `mapstructure:"statement-timeout" validate:"required"`
	LockTimeout      int                                `mapstructure:"lock-timeout" validate:"required"`
	Partitions       map[string]partition.Configuration `mapstructure:"partitions" validate:"required,dive,keys,endkeys,required"`
	Conversions      map[string]partition.Configuration `mapstructure:"conversions" validate:"omitempty"`
}

func (c *Config) Check() error {
	validate := validator.New()

	err := validate.Struct(c)
	if err != nil {
		formatConfigurationError(err)

		return fmt.Errorf("configuration validation failed: %w", err)
	}

	// Validate conversion entries with relaxed rules (CleanupPolicy not required)
	// and apply default values for conversion-specific fields
	if err := c.checkConversions(); err != nil {
		return err
	}

	return nil
}

func (c *Config) checkConversions() error {
	for name, conv := range c.Conversions {
		// Validate conversion-specific fields that have constraints
		if conv.Schema == "" {
			return fmt.Errorf("configuration validation failed: conversions.%s.schema is required", name)
		}

		if conv.Table == "" {
			return fmt.Errorf("configuration validation failed: conversions.%s.table is required", name)
		}

		if conv.PartitionKey == "" {
			return fmt.Errorf("configuration validation failed: conversions.%s.partitionKey is required", name)
		}

		if conv.Interval == "" {
			return fmt.Errorf("configuration validation failed: conversions.%s.interval is required", name)
		}

		if conv.Retention <= 0 {
			return fmt.Errorf("configuration validation failed: conversions.%s.retention must be greater than 0", name)
		}

		if conv.PreProvisioned <= 0 {
			return fmt.Errorf("configuration validation failed: conversions.%s.preProvisioned must be greater than 0", name)
		}

		// Validate batch sizes
		if conv.BatchSize != 0 && (conv.BatchSize < 1 || conv.BatchSize > 1000000) {
			return fmt.Errorf("configuration validation failed: conversions.%s.batchSize must be between 1 and 1000000", name)
		}

		if conv.ReplayBatchSize != 0 && (conv.ReplayBatchSize < 1 || conv.ReplayBatchSize > 1000000) {
			return fmt.Errorf("configuration validation failed: conversions.%s.replayBatchSize must be between 1 and 1000000", name)
		}

		// Validate timeouts
		if conv.LockTimeout != 0 && (conv.LockTimeout < 1 || conv.LockTimeout > 60) {
			return fmt.Errorf("configuration validation failed: conversions.%s.lockTimeout must be between 1 and 60", name)
		}

		if conv.StatementTimeout != 0 && (conv.StatementTimeout < 5 || conv.StatementTimeout > 120) {
			return fmt.Errorf("configuration validation failed: conversions.%s.statementTimeout must be between 5 and 120", name)
		}

		// Apply defaults for conversion-specific fields
		conv.ApplyConvertDefaults()
		c.Conversions[name] = conv
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
