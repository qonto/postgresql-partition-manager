package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/qonto/postgresql-partition-manager/cmd"
	cmdRun "github.com/qonto/postgresql-partition-manager/cmd/run"
	cmdValidate "github.com/qonto/postgresql-partition-manager/cmd/validate"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"
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

// **Validates: Requirements 3.1**

// mkdocsConfig represents the relevant fields of mkdocs.yml for navigation parsing.
type mkdocsConfig struct {
	Nav []map[string]interface{} `yaml:"nav"`
}

// extractNavPages recursively extracts all page file paths from the MkDocs nav structure.
func extractNavPages(nav []map[string]interface{}) []string {
	var pages []string

	for _, entry := range nav {
		for _, value := range entry {
			switch v := value.(type) {
			case string:
				pages = append(pages, v)
			case []interface{}:
				// Nested nav section
				subNav := make([]map[string]interface{}, 0, len(v))
				for _, item := range v {
					if m, ok := item.(map[string]interface{}); ok {
						subNav = append(subNav, m)
					}
				}
				pages = append(pages, extractNavPages(subNav)...)
			}
		}
	}

	return pages
}

// findProjectRoot walks up from the current directory to find the repository root
// by looking for mkdocs.yml.
func findProjectRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "mkdocs.yml")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find project root (no mkdocs.yml found)")
		}
		dir = parent
	}
}

// loadNavPages reads mkdocs.yml and returns all page paths listed in the navigation.
func loadNavPages(t *testing.T, projectRoot string) []string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(projectRoot, "mkdocs.yml"))
	if err != nil {
		t.Fatalf("failed to read mkdocs.yml: %v", err)
	}

	var config mkdocsConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		t.Fatalf("failed to parse mkdocs.yml: %v", err)
	}

	pages := extractNavPages(config.Nav)
	if len(pages) == 0 {
		t.Fatal("no pages found in mkdocs.yml nav")
	}

	return pages
}

// TestPropertyDocumentationPageStructure verifies that for any Markdown file listed
// in the MkDocs navigation, the file begins with a level-1 heading followed by at
// least one non-empty paragraph of introductory text.
func TestPropertyDocumentationPageStructure(t *testing.T) {
	projectRoot := findProjectRoot(t)
	navPages := loadNavPages(t, projectRoot)

	rapid.Check(t, func(t *rapid.T) {
		// Pick a random page from the navigation
		idx := rapid.IntRange(0, len(navPages)-1).Draw(t, "pageIndex")
		page := navPages[idx]

		// Resolve the full path to the documentation file
		fullPath := filepath.Join(projectRoot, "docs", page)

		// Property: the file must exist
		content, err := os.ReadFile(fullPath)
		if err != nil {
			t.Fatalf("page %q listed in nav does not exist at %q: %v", page, fullPath, err)
		}

		// Split content into lines for analysis
		lines := strings.Split(string(content), "\n")

		// Property: file must begin with a level-1 heading (# Title)
		if len(lines) == 0 {
			t.Fatalf("page %q is empty", page)
		}

		firstLine := strings.TrimSpace(lines[0])
		if !strings.HasPrefix(firstLine, "# ") {
			t.Fatalf("page %q does not start with a level-1 heading (# Title), got: %q", page, firstLine)
		}

		// Verify the heading has actual text after the #
		headingText := strings.TrimSpace(strings.TrimPrefix(firstLine, "#"))
		if headingText == "" {
			t.Fatalf("page %q has an empty level-1 heading", page)
		}

		// Property: after the heading, there must be at least one non-empty paragraph
		// of introductory text (skip blank lines between heading and paragraph)
		foundParagraph := false
		for i := 1; i < len(lines); i++ {
			line := strings.TrimSpace(lines[i])

			// Skip empty lines between heading and first content
			if line == "" {
				continue
			}

			// If we hit another heading before finding paragraph text, fail
			if strings.HasPrefix(line, "#") {
				break
			}

			// Found non-empty, non-heading text — this is the introductory paragraph
			foundParagraph = true

			break
		}

		if !foundParagraph {
			t.Fatalf("page %q does not have an introductory paragraph after the level-1 heading", page)
		}
	})
}
