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

// VerifyCmd returns the "convert verify" sub-command.
func VerifyCmd() *cobra.Command {
	var dryRun bool

	cmd := &cobra.Command{
		Use:   "verify [table-name]",
		Short: "Verify convergence between source and target tables",
		Long:  "Check row counts, replay lag, and readiness for cutover",
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

			result, err := converter.Verify(context.Background())
			if err != nil {
				log.Error("Verify failed", "error", err)
				os.Exit(VerifyNotReadyExitCode)
			}

			// Print verification results
			fmt.Printf("Verification Results:\n")
			fmt.Printf("  Source row count:  %d\n", result.SourceRowCount)
			fmt.Printf("  Target row count:  %d\n", result.TargetRowCount)
			fmt.Printf("  Row difference:    %d\n", result.RowDifference)
			fmt.Printf("  Replay lag:        %d\n", result.ReplayLag)
			fmt.Printf("  Ready for cutover: %t\n", result.ReadyForCutover)

			if !result.ReadyForCutover {
				log.Warn("Table is not ready for cutover",
					"schema", convConfig.Schema,
					"table", convConfig.Table,
					"rowDifference", result.RowDifference,
					"replayLag", result.ReplayLag,
				)
				os.Exit(VerifyNotReadyExitCode)
			}

			log.Info("Verify completed successfully",
				"schema", convConfig.Schema,
				"table", convConfig.Table,
				"readyForCutover", result.ReadyForCutover,
				"dryRun", dryRun,
			)
		},
	}

	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview what would be done without making changes")

	return cmd
}
