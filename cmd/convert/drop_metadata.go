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

// DropMetadataCmd returns the "convert drop-metadata" sub-command.
func DropMetadataCmd() *cobra.Command {
	var (
		confirm bool
		force   bool
	)

	cmd := &cobra.Command{
		Use:   "drop-metadata",
		Short: "Drop the migration metadata table",
		Long:  "Remove the ppm_migration_metadata table entirely. Requires --confirm flag. Refuses to drop if active migrations exist unless --force is used.",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			if !confirm {
				fmt.Println("ERROR: This will drop the ppm_migration_metadata table. Use --confirm to proceed.")
				os.Exit(InvalidConfigurationExitCode)
			}

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

			db := convertpg.New(*log, conn)

			// Safety check: refuse to drop if there are active migrations
			if !force {
				count, err := db.CountMigrationStates()
				if err != nil {
					log.Error("Failed to check migration states", "error", err)
					os.Exit(DatabaseErrorExitCode)
				}

				if count > 0 {
					log.Error("Cannot drop metadata table: active migrations exist. Use --force to override.",
						"activeMigrations", count,
					)
					os.Exit(InvalidConfigurationExitCode)
				}
			}

			if err := db.DropMetadataTable(); err != nil {
				log.Error("Failed to drop metadata table", "error", err)
				os.Exit(DatabaseErrorExitCode)
			}

			log.Info("Migration metadata table dropped successfully")
		},
	}

	cmd.Flags().BoolVar(&confirm, "confirm", false, "Confirm dropping the metadata table")
	cmd.Flags().BoolVar(&force, "force", false, "Force drop even if active migrations exist")

	return cmd
}
