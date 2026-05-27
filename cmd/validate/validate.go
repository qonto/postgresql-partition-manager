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
			var cfg config.Config

			if err := viper.Unmarshal(&cfg); err != nil {
				fmt.Printf("Unable to load configuration, %v", err)
				os.Exit(InvalidConfigurationExitCode)
			}

			log, err := logger.New(cfg.Debug, cfg.LogFormat)
			if err != nil {
				fmt.Println("ERROR: Fail to initialize logger: %w", err)
				os.Exit(InternalErrorExitCode)
			}

			if err := cfg.Check(); err != nil {
				os.Exit(InvalidConfigurationExitCode)
			}

			log.Info("Configuration is valid")

			for name, partition := range cfg.Partitions {
				log.Info("Partition configuration",
					"partition", name,
					"schema", partition.Schema,
					"table", partition.Table,
					"partitionKey", partition.PartitionKey,
					"interval", partition.Interval,
					"retention", partition.Retention,
					"preProvisioned", partition.PreProvisioned,
					"cleanupPolicy", partition.CleanupPolicy,
				)

				if partition.Convert != nil {
					log.Info("Convert settings",
						"partition", name,
						"backfillBatchSize", partition.Convert.BackfillBatchSize,
						"replayBatchSize", partition.Convert.ReplayBatchSize,
						"lockTimeout", partition.Convert.LockTimeout,
						"statementTimeout", partition.Convert.StatementTimeout,
					)
				}
			}
		},
	}

	return validateCmd
}
