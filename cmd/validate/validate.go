// Package validate provides Cobra command to validate the configuration file
package validate

import (
	"fmt"
	"os"

	"github.com/qonto/postgresql-partition-manager/internal/infra/config"
	"github.com/qonto/postgresql-partition-manager/internal/infra/logger"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	InvalidConfigurationExitCode = 1
	InternalErrorExitCode        = 2
)

func ValidateCmd() *cobra.Command {
	validateCmd := &cobra.Command{
		Use:              "validate",
		Short:            "Check configuration file",
		Long:             `Check configuration file and exit with an error if configuration is invalid`,
		TraverseChildren: true,
		Run: func(cmd *cobra.Command, args []string) {
			var config config.Config

			if err := viper.Unmarshal(&config); err != nil {
				fmt.Printf("Unable to load configuration, %v", err)
				os.Exit(InvalidConfigurationExitCode)
			}

			logger, err := logger.New(config.Debug, config.LogFormat)
			if err != nil {
				fmt.Println("ERROR: Fail to initialize logger: %w", err)
				os.Exit(InternalErrorExitCode)
			}

			if err := config.Check(); err != nil {
				os.Exit(InvalidConfigurationExitCode)
			}

			logger.Info("Configuration is valid")
		},
	}

	return validateCmd
}
