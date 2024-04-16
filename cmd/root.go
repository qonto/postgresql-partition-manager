// Package cmd implements command to start the PostgreSQL Partition Manager
package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/qonto/postgresql-partition-manager/cmd/run"
	"github.com/qonto/postgresql-partition-manager/cmd/validate"
	"github.com/qonto/postgresql-partition-manager/internal/infra/build"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	configErrorExitCode = 1
	cmdErrorExitCode    = 3
)

var (
	cfgFile          string
	logFormat        string
	debug            bool
	connectionURL    string
	lockTimeout      string
	statementTimeout string
)

func NewRootCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:              "postgresql-partition-manager ",
		Version:          fmt.Sprintf("%s, commit %s, built at %s", build.Version, build.CommitSHA, build.Date),
		Short:            "PostgreSQL partition manager",
		Long:             "Simplified PostgreSQL partioning management",
		TraverseChildren: true,
	}

	cobra.OnInitialize(initConfig)

	cmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/postgresql-partition-manager.yaml)")
	cmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "Enable debug mode")
	cmd.PersistentFlags().StringVarP(&logFormat, "log-format", "l", "json", "Log format (text or json)")
	cmd.PersistentFlags().StringVarP(&connectionURL, "connection-url", "u", "", "Path under which to expose metrics")
	cmd.PersistentFlags().StringVarP(&lockTimeout, "lock-timeout", "", "100", "Set lock_timeout (ms)")
	cmd.PersistentFlags().StringVarP(&statementTimeout, "statement-timeout", "", "3000", "Set statement_timeout (ms)")

	err := viper.BindPFlag("debug", cmd.PersistentFlags().Lookup("debug"))
	if err != nil {
		return cmd, fmt.Errorf("failed to bind 'debug' parameter: %w", err)
	}

	err = viper.BindPFlag("log-format", cmd.PersistentFlags().Lookup("log-format"))
	if err != nil {
		return cmd, fmt.Errorf("failed to bind 'log-format' parameter: %w", err)
	}

	err = viper.BindPFlag("connection-url", cmd.PersistentFlags().Lookup("connection-url"))
	if err != nil {
		return cmd, fmt.Errorf("failed to bind 'connection-url' parameter: %w", err)
	}

	err = viper.BindPFlag("lock-timeout", cmd.PersistentFlags().Lookup("lock-timeout"))
	if err != nil {
		return cmd, fmt.Errorf("failed to bind 'lock-timeout' parameter: %w", err)
	}

	err = viper.BindPFlag("statement-timeout", cmd.PersistentFlags().Lookup("statement-timeout"))
	if err != nil {
		return cmd, fmt.Errorf("failed to bind 'statement-timeout' parameter: %w", err)
	}

	return cmd, nil
}

func Execute() {
	cmd, err := NewRootCommand()
	if err != nil {
		fmt.Println("ERROR: Failed to load configuration: %w", err)
		os.Exit(configErrorExitCode)
	}

	cmd.AddCommand(validate.ValidateCmd())
	cmd.AddCommand(run.RunCmd())

	err = cmd.Execute()
	if err != nil {
		fmt.Println("ERROR: Command failed: %w", err)
		os.Exit(cmdErrorExitCode)
	}
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory or current directory with name "postgresql-partition-manager.yaml"

		configurationFilename := "postgresql-partition-manager.yaml"
		currentPathFilename := configurationFilename
		homeFilename := filepath.Join(home, configurationFilename)

		if _, err := os.Stat(homeFilename); err == nil {
			viper.SetConfigFile(homeFilename)
		}

		if _, err := os.Stat(currentPathFilename); err == nil {
			viper.SetConfigFile(currentPathFilename)
		}
	}

	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}

	viper.SetEnvPrefix("postgresql_partition_manager") // will be uppercased automatically
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
}
