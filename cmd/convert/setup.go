package convert

import (
	"context"
	"errors"
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
	InvalidConfigurationExitCode  = 1
	InternalErrorExitCode         = 2
	DatabaseErrorExitCode         = 3
	MissingTableExitCode          = 4
	NoPrimaryKeyExitCode          = 5
	SetupFailedExitCode           = 6
	LockTimeoutExitCode           = 7
	AssertionFailedExitCode       = 8
	RollbackNotApplicableExitCode = 9
	VerifyNotReadyExitCode        = 10
	CutoverFailedExitCode         = 11
	RollbackFailedExitCode        = 12
	CleanupFailedExitCode         = 13
)

// SetupCmd returns the "convert setup" sub-command.
func SetupCmd() *cobra.Command {
	var dryRun bool

	cmd := &cobra.Command{
		Use:   "setup [table-name]",
		Short: "Set up CDC queue, trigger, and target partitioned table",
		Long:  "Create the CDC queue, install the CDC trigger, and create the target partitioned table for the specified conversion",
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

			err = converter.Setup(context.Background())
			if err != nil {
				if errors.Is(err, convert.ErrSourceTableNotFound) {
					log.Error("Source table does not exist", "schema", convConfig.Schema, "table", convConfig.Table)
					os.Exit(MissingTableExitCode)
				}

				if errors.Is(err, convert.ErrNoPrimaryKey) {
					log.Error("Source table has no primary key", "schema", convConfig.Schema, "table", convConfig.Table)
					os.Exit(NoPrimaryKeyExitCode)
				}

				log.Error("Setup failed", "error", err)
				os.Exit(SetupFailedExitCode)
			}

			log.Info("Setup completed successfully",
				"schema", convConfig.Schema,
				"table", convConfig.Table,
				"dryRun", dryRun,
			)
		},
	}

	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview what would be done without making changes")

	return cmd
}
