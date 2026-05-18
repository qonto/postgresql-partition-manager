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

// CleanupCmd returns the "convert cleanup" sub-command.
func CleanupCmd() *cobra.Command {
	var (
		dryRun  bool
		confirm bool
		force   bool
	)

	cmd := &cobra.Command{
		Use:   "cleanup [table-name]",
		Short: "Remove migration artifacts after a successful cutover",
		Long:  "Drop the CDC trigger, CDC queue, source_old table, and migration metadata",
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

			// Look up the conversion configuration by table name
			convConfig, ok := cfg.Conversions[tableName]
			if !ok {
				log.Error("Conversion configuration not found", "table", tableName)
				fmt.Printf("ERROR: No conversion configuration found for %q. Check your configuration file.\n", tableName)
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
			db := convertpg.NewWithTimeouts(*log, conn, convConfig.LockTimeout, convConfig.StatementTimeout)

			// Ensure metadata table exists
			if err := db.EnsureMetadataTable(); err != nil {
				log.Error("Failed to ensure metadata table", "error", err)
				os.Exit(DatabaseErrorExitCode)
			}

			// Create and run the converter
			converter := convert.New(*log, db, convConfig, dryRun)

			err = converter.Cleanup(context.Background(), confirm, force)
			if err != nil {
				log.Error("Cleanup failed", "error", err)
				os.Exit(CleanupFailedExitCode)
			}

			log.Info("Cleanup completed successfully",
				"schema", convConfig.Schema,
				"table", convConfig.Table,
				"dryRun", dryRun,
			)
		},
	}

	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview what would be done without making changes")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "Confirm cleanup of migration artifacts")
	cmd.Flags().BoolVar(&force, "force", false, "Force cleanup regardless of migration phase")

	return cmd
}
