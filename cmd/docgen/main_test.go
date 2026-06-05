package main

import (
	"os"
	"strings"
	"testing"

	"github.com/qonto/postgresql-partition-manager/cmd"
	cmdRun "github.com/qonto/postgresql-partition-manager/cmd/run"
	cmdValidate "github.com/qonto/postgresql-partition-manager/cmd/validate"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"pgregory.net/rapid"
)

// **Validates: Requirements 9.1**

// buildCommandTree constructs the full PPM Cobra command tree,
// identical to how the doc generator builds it.
func buildCommandTree(t *testing.T) *cobra.Command {
	t.Helper()

	rootCmd, err := cmd.NewRootCommand()
	if err != nil {
		t.Fatalf("failed to create root command: %v", err)
	}

	rootCmd.AddCommand(cmdValidate.ValidateCmd())
	rootCmd.AddCommand(cmdRun.RunCmd())
	rootCmd.DisableAutoGenTag = true

	return rootCmd
}

// generateCLIReference runs the doc generator and returns the output content.
func generateCLIReference(t *testing.T, rootCmd *cobra.Command) string {
	t.Helper()

	tmpFile, err := os.CreateTemp(t.TempDir(), "cli-reference-*.md")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer tmpFile.Close()

	generateCommandDoc(tmpFile, rootCmd, 2)

	content, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to read generated file: %v", err)
	}

	return string(content)
}

// collectCommands recursively collects all commands in the Cobra tree.
func collectCommands(command *cobra.Command) []*cobra.Command {
	commands := []*cobra.Command{command}
	for _, sub := range command.Commands() {
		if sub.IsAdditionalHelpTopicCommand() {
			continue
		}
		commands = append(commands, collectCommands(sub)...)
	}

	return commands
}

// TestPropertyCLIReferenceCompleteness verifies that for any command or subcommand
// registered in the PPM command tree, the generated CLI reference contains the
// command's name, description, and all its flags (name, default value, description).
func TestPropertyCLIReferenceCompleteness(t *testing.T) {
	rootCmd := buildCommandTree(t)
	output := generateCLIReference(t, rootCmd)
	allCommands := collectCommands(rootCmd)

	rapid.Check(t, func(t *rapid.T) {
		// Pick a random command from the full command tree
		idx := rapid.IntRange(0, len(allCommands)-1).Draw(t, "commandIndex")
		command := allCommands[idx]

		// Property: command path must appear in the output
		commandPath := command.CommandPath()
		if !strings.Contains(output, commandPath) {
			t.Fatalf("command path %q not found in CLI reference output", commandPath)
		}

		// Property: command description must appear in the output
		description := command.Long
		if description == "" {
			description = command.Short
		}
		if description != "" && !strings.Contains(output, description) {
			t.Fatalf("description %q for command %q not found in CLI reference output", description, commandPath)
		}

		// Property: all non-hidden flags must appear in the output
		command.NonInheritedFlags().VisitAll(func(flag *pflag.Flag) {
			if flag.Hidden {
				return
			}
			if !strings.Contains(output, flag.Name) {
				t.Fatalf("flag name %q for command %q not found in CLI reference output", flag.Name, commandPath)
			}

			defValue := flag.DefValue
			if defValue == "" {
				defValue = `""`
			}
			if !strings.Contains(output, defValue) {
				t.Fatalf("flag default value %q for flag %q of command %q not found in CLI reference output", defValue, flag.Name, commandPath)
			}

			if flag.Usage != "" && !strings.Contains(output, flag.Usage) {
				t.Fatalf("flag usage %q for flag %q of command %q not found in CLI reference output", flag.Usage, flag.Name, commandPath)
			}
		})

		// Property: inherited flags must also appear in the output (they appear in inherited flags section)
		command.InheritedFlags().VisitAll(func(flag *pflag.Flag) {
			if flag.Hidden {
				return
			}
			if !strings.Contains(output, flag.Name) {
				t.Fatalf("inherited flag name %q for command %q not found in CLI reference output", flag.Name, commandPath)
			}

			defValue := flag.DefValue
			if defValue == "" {
				defValue = `""`
			}
			if !strings.Contains(output, defValue) {
				t.Fatalf("inherited flag default value %q for flag %q of command %q not found in CLI reference output", defValue, flag.Name, commandPath)
			}

			if flag.Usage != "" && !strings.Contains(output, flag.Usage) {
				t.Fatalf("inherited flag usage %q for flag %q of command %q not found in CLI reference output", flag.Usage, flag.Name, commandPath)
			}
		})
	})
}
