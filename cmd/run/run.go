// Package run provides Cobra commands to execute PPM
package run

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/config"
	"github.com/qonto/postgresql-partition-manager/internal/infra/logger"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"github.com/qonto/postgresql-partition-manager/pkg/ppm"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	InvalidConfigurationExitCode         = 1
	InternalErrorExitCode                = 2
	DatabaseErrorExitCode                = 3
	PartitionsProvisioningFailedExitCode = 4
	PartitionsCheckFailedExitCode        = 5
	PartitionsCleanupFailedExitCode      = 6
	InvalidDateExitCode                  = 7
)

var ErrUnsupportedPostgreSQLVersion = errors.New("unsupported PostgreSQL version")

func RunCmd() *cobra.Command {
	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Perform partition operations",
		Long:  "Perform partition operations",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}

	runCmd.AddCommand(AllCmd)
	runCmd.AddCommand(CheckCmd)
	runCmd.AddCommand(ProvisioningCmd)
	runCmd.AddCommand(CleanupCmd)

	return runCmd
}

var AllCmd = &cobra.Command{
	Use:   "all",
	Short: "Perform partitions provisioning, cleanup, and check",
	Long:  "Perform partitions provisioning, cleanup, and check",
	Run: func(cmd *cobra.Command, args []string) {
		client := initCmd()

		provisioningCmd(client)
		cleanupCmd(client)
		checkCmd(client)
	},
}

var CheckCmd = &cobra.Command{
	Use:   "check",
	Short: "Check existing partitions",
	Long:  "Check existing partitions",
	Run: func(cmd *cobra.Command, args []string) {
		client := initCmd()
		checkCmd(client)
	},
}

var CleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Remove outdated partitions",
	Long:  "Remove outdated partitions",
	Run: func(cmd *cobra.Command, args []string) {
		client := initCmd()
		cleanupCmd(client)
	},
}

var ProvisioningCmd = &cobra.Command{
	Use:   "provisioning",
	Short: "Create and attach new partitions",
	Long:  "Create and attach new partitions",
	Run: func(cmd *cobra.Command, args []string) {
		client := initCmd()
		provisioningCmd(client)
	},
}

func initCmd() *ppm.PPM {
	var config config.Config

	if err := viper.Unmarshal(&config); err != nil {
		fmt.Println("ERROR: Unable to load configuration", "error", err)
		os.Exit(InvalidConfigurationExitCode)
	}

	err := config.Check()
	if err != nil {
		os.Exit(InvalidConfigurationExitCode)
	}

	log, err := logger.New(config.Debug, config.LogFormat)
	if err != nil {
		fmt.Println("ERROR: Fail to initialize logger: %w", err)
		os.Exit(InternalErrorExitCode)
	}

	databaseConfiguration := postgresql.ConnectionSettings{
		URL:              config.ConnectionURL,
		LockTimeout:      config.LockTimeout,
		StatementTimeout: config.StatementTimeout,
	}

	conn, err := postgresql.GetDatabaseConnection(databaseConfiguration)
	if err != nil {
		log.Error("Could not connect to database", "error", err)
		os.Exit(DatabaseErrorExitCode)
	}

	db := postgresql.New(*log, conn)

	workDate := time.Now()
	stringDate, useExternalDate := os.LookupEnv("PPM_WORK_DATE")

	if useExternalDate {
		workDate, err = time.Parse(time.DateOnly, stringDate)
		if err != nil {
			log.Error("Could not parse PPM_WORK_DATE environment variable", "error", err)
			os.Exit(InvalidDateExitCode)
		}
	}

	log.Info("Work date", "work-date", workDate)

	client := ppm.New(context.TODO(), *log, db, config.Partitions, workDate)

	if err = client.CheckServerRequirements(); err != nil {
		log.Error("Server is incompatible", "error", err)
		os.Exit(DatabaseErrorExitCode)
	}

	return client
}

func checkCmd(client *ppm.PPM) {
	if err := client.CheckPartitions(); err != nil {
		os.Exit(PartitionsCheckFailedExitCode)
	}
}

func cleanupCmd(client *ppm.PPM) {
	if err := client.CleanupPartitions(); err != nil {
		os.Exit(PartitionsCleanupFailedExitCode)
	}
}

func provisioningCmd(client *ppm.PPM) {
	if err := client.ProvisioningPartitions(); err != nil {
		os.Exit(PartitionsProvisioningFailedExitCode)
	}
}
