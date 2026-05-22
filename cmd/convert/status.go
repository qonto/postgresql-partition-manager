package convert

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/qonto/postgresql-partition-manager/internal/infra/config"
	convertpg "github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
	"github.com/qonto/postgresql-partition-manager/internal/infra/logger"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"github.com/qonto/postgresql-partition-manager/pkg/convert"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// StatusCmd returns the "convert status" sub-command.
func StatusCmd() *cobra.Command {
	var all bool

	cmd := &cobra.Command{
		Use:   "status [table-name]",
		Short: "Show the current status of a migration",
		Long:  "Display the current phase and metadata for a table migration. Use --all to list all tracked migrations.",
		Args:  cobra.MaximumNArgs(1),
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

			db := convertpg.New(*log, conn)

			if all {
				printAllMigrations(db, log)
				return
			}

			// Single table status
			if len(args) == 0 {
				fmt.Println("ERROR: table-name is required (or use --all to list all migrations)")
				os.Exit(InvalidConfigurationExitCode)
			}

			tableName := args[0]

			convConfig, ok := cfg.Conversions[tableName]
			if !ok {
				log.Error("Conversion configuration not found", "table", tableName)
				fmt.Printf("ERROR: No conversion configuration found for %q. Check your configuration file.\n", tableName)
				os.Exit(InvalidConfigurationExitCode)
			}

			printMigrationStatus(db, log, convConfig.Schema, convConfig.Table)
		},
	}

	cmd.Flags().BoolVarP(&all, "all", "a", false, "List all tracked migrations")

	return cmd
}

func printAllMigrations(db *convertpg.Client, log interface{ Error(string, ...any) }) {
	states, err := db.ListMigrationStates()
	if err != nil {
		fmt.Println("No migration metadata table found. Run 'convert init' to initialize it.")
		os.Exit(DatabaseErrorExitCode)
	}

	if len(states) == 0 {
		fmt.Println("No migrations found.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SCHEMA\tTABLE\tPHASE\tSTARTED AT\tUPDATED AT")
	fmt.Fprintln(w, "------\t-----\t-----\t----------\t----------")

	for _, s := range states {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			s.Schema,
			s.Table,
			s.Phase,
			s.PhaseStartedAt.Format("2006-01-02 15:04:05"),
			s.UpdatedAt.Format("2006-01-02 15:04:05"),
		)
	}

	w.Flush()
}

func printMigrationStatus(db *convertpg.Client, log interface{ Error(string, ...any) }, schema, table string) {
	state, err := db.GetMigrationState(schema, table)
	if err != nil {
		fmt.Println("No migration metadata table found. Run 'convert init' to initialize it.")
		os.Exit(DatabaseErrorExitCode)
	}

	if state == nil {
		fmt.Printf("No migration found for %s.%s\n", schema, table)
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "Schema:\t%s\n", state.Schema)
	fmt.Fprintf(w, "Table:\t%s\n", state.Table)
	fmt.Fprintf(w, "Phase:\t%s\n", state.Phase)
	fmt.Fprintf(w, "Phase started at:\t%s\n", state.PhaseStartedAt.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(w, "Updated at:\t%s\n", state.UpdatedAt.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(w, "Last replay seq:\t%d\n", state.LastReplaySeq)

	if len(state.LastBackfillPK) > 0 {
		fmt.Fprintf(w, "Last backfill PK:\t%v\n", state.LastBackfillPK)
	}

	if len(state.DroppedForeignKeys) > 0 {
		fmt.Fprintf(w, "Dropped FKs:\t%d\n", len(state.DroppedForeignKeys))

		for _, fk := range state.DroppedForeignKeys {
			fmt.Fprintf(w, "  - %s\t%s.%s(%v) → %s(%v)\n",
				fk.Name,
				fk.ReferencedSchema, fk.ReferencedTable, fk.Columns,
				state.Table, fk.ReferencedColumns,
			)
		}
	}

	w.Flush()
}
