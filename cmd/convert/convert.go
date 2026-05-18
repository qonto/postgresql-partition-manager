// Package convert provides Cobra commands for table partition conversion
package convert

import (
	"github.com/spf13/cobra"
)

// ConvertCmd returns the parent "convert" command that groups all conversion sub-commands.
func ConvertCmd() *cobra.Command {
	convertCmd := &cobra.Command{
		Use:   "convert",
		Short: "Convert a non-partitioned table to a partitioned table",
		Long:  "Convert a non-partitioned table to a partitioned table using CDC-based online migration",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}

	convertCmd.AddCommand(SetupCmd())
	convertCmd.AddCommand(BackfillCmd())
	convertCmd.AddCommand(ReplayCmd())
	convertCmd.AddCommand(VerifyCmd())
	convertCmd.AddCommand(CutoverCmd())
	convertCmd.AddCommand(RollbackCmd())
	convertCmd.AddCommand(CleanupCmd())

	return convertCmd
}
