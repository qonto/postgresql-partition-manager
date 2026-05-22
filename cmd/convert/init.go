package convert

import (
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

// InitCmd returns the "convert init" sub-command.
func InitCmd() *cobra.Command {
	var dryRun bool

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize the migration metadata table",
		Long:  "Create the ppm_migration_metadata table used to track conversion state",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
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

			if dryRun {
				log.Info("[DRY-RUN] Would create ppm_migration_metadata table")
				return
			}

			db := convertpg.New(*log, conn)

			if err := db.EnsureMetadataTable(); err != nil {
				log.Error("Failed to create metadata table", "error", err)
				os.Exit(DatabaseErrorExitCode)
			}

			log.Info("Migration metadata table initialized successfully")
		},
	}

	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview what would be done without making changes")

	return cmd
}
