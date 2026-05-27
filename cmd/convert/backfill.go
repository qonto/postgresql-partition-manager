package convert

import (
	"context"
	"fmt"
	"os"

	"github.com/qonto/postgresql-partition-manager/internal/infra/config"
	convertpg "github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
	"github.com/qonto/postgresql-partition-manager/internal/infra/logger"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"github.com/qonto/postgresql-partition-manager/pkg/convert"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	BackfillFailedExitCode = 7
)

// BackfillCmd returns the "convert backfill" sub-command.
func BackfillCmd() *cobra.Command {
	var dryRun bool

	cmd := &cobra.Command{
		Use:   "backfill [table-name]",
		Short: "Copy historical data from source to target partitioned table",
		Long:  "Copy rows from the source table to the target partitioned table in batches using primary key order",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			tableName := args[0]

			var cfg config.Config

			if err := viper.Unmarshal(&cfg); err != nil {
				fmt.Println("ERROR: Unable to load configuration:", err)
				os.Exit(InvalidConfigurationExitCode)
			}

			if err := cfg.Check(); err != nil {
				os.Exit(InvalidConfigurationExitCode)
			}

			log, err := logger.New(cfg.Debug, cfg.LogFormat)
			if err != nil {
				fmt.Println("ERROR: Failed to initialize logger:", err)
				os.Exit(InternalErrorExitCode)
			}

			// Look up the partition configuration by table name (with convert defaults applied)
			convConfig, err := cfg.GetConvertConfig(tableName)
			if err != nil {
				log.Error(err.Error())
				os.Exit(InvalidConfigurationExitCode)
			}

			// Connect to the database
			databaseConfiguration := postgresql.ConnectionSettings{
				URL:              cfg.ConnectionURL,
				LockTimeout:      cfg.LockTimeout,
				StatementTimeout: cfg.StatementTimeout,
			}

			conn, err := postgresql.GetDatabaseConnection(databaseConfiguration)
			if err != nil {
				log.Error("Could not connect to database", "error", err)
				os.Exit(DatabaseErrorExitCode)
			}

			log.Info("Connected to database",
				"url", convert.RedactConnectionURL(cfg.ConnectionURL),
			)

			// Create the convert DB client with conversion-specific timeouts
			db := convertpg.NewWithTimeouts(*log, conn, convConfig.Convert.LockTimeout, convConfig.Convert.StatementTimeout)

			// Ensure metadata table exists
			if err := db.EnsureMetadataTable(); err != nil {
				log.Error("Failed to ensure metadata table", "error", err)
				os.Exit(DatabaseErrorExitCode)
			}

			// Create and run the converter
			converter := convert.New(*log, db, convConfig, dryRun)

			err = converter.Backfill(context.Background())
			if err != nil {
				log.Error("Backfill failed", "error", err)
				os.Exit(BackfillFailedExitCode)
			}

			log.Info("Backfill completed successfully",
				"schema", convConfig.Schema,
				"table", convConfig.Table,
				"dryRun", dryRun,
			)
		},
	}

	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview what would be done without making changes")

	return cmd
}
