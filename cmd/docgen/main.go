// Package main implements the CLI reference documentation generator.
// It traverses the Cobra command tree and produces a Markdown file
// at docs/cli-reference.md.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/qonto/postgresql-partition-manager/cmd"
	cmdRun "github.com/qonto/postgresql-partition-manager/cmd/run"
	cmdValidate "github.com/qonto/postgresql-partition-manager/cmd/validate"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const outputFile = "docs/cli-reference.md"

func main() {
	rootCmd, err := cmd.NewRootCommand()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating root command: %v\n", err)
		os.Exit(1)
	}

	rootCmd.AddCommand(cmdValidate.ValidateCmd())
	rootCmd.AddCommand(cmdRun.RunCmd())
	rootCmd.DisableAutoGenTag = true

	dir := filepath.Dir(outputFile)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating directory %s: %v\n", dir, err)
		os.Exit(1)
	}

	f, err := os.Create(outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating file %s: %v\n", outputFile, err)
		os.Exit(1)
	}
	defer f.Close()

	fmt.Fprintln(f, "# CLI Reference")
	fmt.Fprintln(f)
	fmt.Fprintln(f, "<!-- This file is auto-generated from the Cobra command tree. Do not edit manually. -->")
	fmt.Fprintln(f)

	generateCommandDoc(f, rootCmd, 2)

	fmt.Printf("CLI reference generated at %s\n", outputFile)
}

func generateCommandDoc(f *os.File, command *cobra.Command, headingLevel int) {
	heading := strings.Repeat("#", headingLevel)
	fmt.Fprintf(f, "%s %s\n\n", heading, command.CommandPath())

	if command.Long != "" {
		fmt.Fprintf(f, "%s\n\n", command.Long)
	} else if command.Short != "" {
		fmt.Fprintf(f, "%s\n\n", command.Short)
	}

	if command.UseLine() != "" {
		fmt.Fprintf(f, "**Usage:**\n\n")
		fmt.Fprintf(f, "```\n%s\n```\n\n", command.UseLine())
	}

	writeFlags(f, "Flags", command.NonInheritedFlags())
	writeFlags(f, "Inherited Flags", command.InheritedFlags())

	for _, sub := range command.Commands() {
		if sub.IsAdditionalHelpTopicCommand() {
			continue
		}
		generateCommandDoc(f, sub, headingLevel+1)
	}
}

func writeFlags(f *os.File, title string, flags *pflag.FlagSet) {
	if !flags.HasFlags() {
		return
	}

	hasVisible := false
	flags.VisitAll(func(flag *pflag.Flag) {
		if !flag.Hidden {
			hasVisible = true
		}
	})

	if !hasVisible {
		return
	}

	fmt.Fprintf(f, "**%s:**\n\n", title)
	fmt.Fprintln(f, "| Flag | Shorthand | Default | Description |")
	fmt.Fprintln(f, "|------|-----------|---------|-------------|")

	flags.VisitAll(func(flag *pflag.Flag) {
		if flag.Hidden {
			return
		}

		shorthand := ""
		if flag.Shorthand != "" {
			shorthand = fmt.Sprintf("-%s", flag.Shorthand)
		}

		defValue := flag.DefValue
		if defValue == "" {
			defValue = `""`
		}

		fmt.Fprintf(f, "| --%s | %s | %s | %s |\n",
			flag.Name, shorthand, defValue, flag.Usage)
	})

	fmt.Fprintln(f)
}
