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

const (
	outputFile          = "docs/cli-reference.md"
	initialHeadingLevel = 2
	dirPermissions      = 0o750
)

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
	if err := os.MkdirAll(dir, dirPermissions); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating directory %s: %v\n", dir, err)
		os.Exit(1)
	}

	f, err := os.Create(outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating file %s: %v\n", outputFile, err)
		os.Exit(1)
	}

	defer func() {
		if cerr := f.Close(); cerr != nil {
			fmt.Fprintf(os.Stderr, "Error closing file %s: %v\n", outputFile, cerr)
		}
	}()

	_, _ = fmt.Fprintln(f, "# CLI Reference")
	_, _ = fmt.Fprintln(f)
	_, _ = fmt.Fprintln(f, "<!-- This file is auto-generated from the Cobra command tree. Do not edit manually. -->")
	_, _ = fmt.Fprintln(f)

	generateCommandDoc(f, rootCmd, initialHeadingLevel)

	fmt.Printf("CLI reference generated at %s\n", outputFile)
}

func generateCommandDoc(f *os.File, command *cobra.Command, headingLevel int) {
	heading := strings.Repeat("#", headingLevel)
	_, _ = fmt.Fprintf(f, "%s %s\n\n", heading, command.CommandPath())

	if command.Long != "" {
		_, _ = fmt.Fprintf(f, "%s\n\n", command.Long)
	} else if command.Short != "" {
		_, _ = fmt.Fprintf(f, "%s\n\n", command.Short)
	}

	if command.UseLine() != "" {
		_, _ = fmt.Fprintf(f, "**Usage:**\n\n")
		_, _ = fmt.Fprintf(f, "```\n%s\n```\n\n", command.UseLine())
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

	_, _ = fmt.Fprintf(f, "**%s:**\n\n", title)
	_, _ = fmt.Fprintln(f, "| Flag | Shorthand | Default | Description |")
	_, _ = fmt.Fprintln(f, "|------|-----------|---------|-------------|")

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

		_, _ = fmt.Fprintf(f, "| --%s | %s | %s | %s |\n",
			flag.Name, shorthand, defValue, flag.Usage)
	})

	_, _ = fmt.Fprintln(f)
}
