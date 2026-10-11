// Feature: partition-hooks, Property 15: Template Variable Rendering
package hook

import (
	"fmt"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

// **Validates: Requirements 7.1, 7.2, 7.8, 3.5, 4.3**
//
// Property 15: Template Variable Rendering
// For any valid partition context and any template string containing {{.VariableName}}
// references to defined variables (Schema, Table, ParentTable, LowerBound, UpperBound,
// PartitionName, DatabaseName, Hostname, Retention, Interval), the rendered output SHALL
// contain the actual values substituted for each variable reference.

// allTemplateVariables lists all valid template variable names in PartitionContext.
var allTemplateVariables = []string{
	"Schema",
	"Table",
	"ParentTable",
	"LowerBound",
	"UpperBound",
	"PartitionName",
	"DatabaseName",
	"Hostname",
	"Retention",
	"Interval",
}

// genRenderingPartitionContext generates a random PartitionContext with non-empty values for all fields.
func genRenderingPartitionContext(t *rapid.T) PartitionContext {
	return PartitionContext{
		Schema:        rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]{0,15}`).Draw(t, "schema"),
		Table:         rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]{0,20}`).Draw(t, "table"),
		ParentTable:   rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]{0,20}`).Draw(t, "parentTable"),
		LowerBound:    rapid.StringMatching(`\d{4}-\d{2}-\d{2}`).Draw(t, "lowerBound"),
		UpperBound:    rapid.StringMatching(`\d{4}-\d{2}-\d{2}`).Draw(t, "upperBound"),
		PartitionName: rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]{0,15}`).Draw(t, "partitionName"),
		DatabaseName:  rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]{0,15}`).Draw(t, "databaseName"),
		Hostname:      rapid.StringMatching(`[a-z][a-z0-9\-]{0,10}\.[a-z]{2,5}`).Draw(t, "hostname"),
		Retention:     rapid.StringMatching(`\d{1,4}`).Draw(t, "retention"),
		Interval:      rapid.SampledFrom([]string{"daily", "weekly", "monthly", "quarterly", "yearly"}).Draw(t, "interval"),
	}
}

// getContextFieldValue returns the value of a PartitionContext field by name.
func getContextFieldValue(ctx PartitionContext, fieldName string) string {
	switch fieldName {
	case "Schema":
		return ctx.Schema
	case "Table":
		return ctx.Table
	case "ParentTable":
		return ctx.ParentTable
	case "LowerBound":
		return ctx.LowerBound
	case "UpperBound":
		return ctx.UpperBound
	case "PartitionName":
		return ctx.PartitionName
	case "DatabaseName":
		return ctx.DatabaseName
	case "Hostname":
		return ctx.Hostname
	case "Retention":
		return ctx.Retention
	case "Interval":
		return ctx.Interval
	default:
		return ""
	}
}

func TestProperty_TemplateVariableRendering_SingleVariable(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ctx := genRenderingPartitionContext(t)

		// Pick a random variable to test
		varName := rapid.SampledFrom(allTemplateVariables).Draw(t, "varName")
		templateStr := fmt.Sprintf("prefix-{{.%s}}-suffix", varName)

		result, err := Render(templateStr, ctx)
		if err != nil {
			t.Fatalf("Render returned unexpected error: %v", err)
		}

		expectedValue := getContextFieldValue(ctx, varName)
		expectedOutput := fmt.Sprintf("prefix-%s-suffix", expectedValue)

		if result != expectedOutput {
			t.Fatalf("expected %q, got %q", expectedOutput, result)
		}
	})
}

func TestProperty_TemplateVariableRendering_MultipleVariables(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ctx := genRenderingPartitionContext(t)

		// Pick a random number of variables (2 to all)
		numVars := rapid.IntRange(2, len(allTemplateVariables)).Draw(t, "numVars")

		// Select a random subset of variables
		selectedVars := make([]string, numVars)
		used := make(map[int]bool)

		for i := 0; i < numVars; i++ {
			for {
				idx := rapid.IntRange(0, len(allTemplateVariables)-1).Draw(t, fmt.Sprintf("idx_%d", i))
				if !used[idx] {
					used[idx] = true
					selectedVars[i] = allTemplateVariables[idx]

					break
				}
			}
		}

		// Build a template string with all selected variables separated by "/"
		var templateParts []string
		for _, varName := range selectedVars {
			templateParts = append(templateParts, fmt.Sprintf("{{.%s}}", varName))
		}

		templateStr := strings.Join(templateParts, "/")

		result, err := Render(templateStr, ctx)
		if err != nil {
			t.Fatalf("Render returned unexpected error: %v", err)
		}

		// Verify each variable value appears in the output
		for _, varName := range selectedVars {
			expectedValue := getContextFieldValue(ctx, varName)
			if !strings.Contains(result, expectedValue) {
				t.Fatalf("rendered output %q does not contain value %q for variable %s",
					result, expectedValue, varName)
			}
		}

		// Verify the full expected output matches exactly
		var expectedParts []string
		for _, varName := range selectedVars {
			expectedParts = append(expectedParts, getContextFieldValue(ctx, varName))
		}

		expectedOutput := strings.Join(expectedParts, "/")
		if result != expectedOutput {
			t.Fatalf("expected %q, got %q", expectedOutput, result)
		}
	})
}

func TestProperty_TemplateVariableRendering_AllVariables(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ctx := genRenderingPartitionContext(t)

		// Build a template using all variables in a realistic format
		templateStr := "{{.Schema}}.{{.Table}} parent={{.ParentTable}} bounds=[{{.LowerBound}},{{.UpperBound}}) partition={{.PartitionName}} db={{.DatabaseName}} host={{.Hostname}} retention={{.Retention}} interval={{.Interval}}"

		result, err := Render(templateStr, ctx)
		if err != nil {
			t.Fatalf("Render returned unexpected error: %v", err)
		}

		// Verify each variable value is present in the output
		for _, varName := range allTemplateVariables {
			expectedValue := getContextFieldValue(ctx, varName)
			if !strings.Contains(result, expectedValue) {
				t.Fatalf("rendered output %q does not contain value %q for variable %s",
					result, expectedValue, varName)
			}
		}

		// Verify exact expected output
		expectedOutput := fmt.Sprintf("%s.%s parent=%s bounds=[%s,%s) partition=%s db=%s host=%s retention=%s interval=%s",
			ctx.Schema, ctx.Table, ctx.ParentTable, ctx.LowerBound, ctx.UpperBound,
			ctx.PartitionName, ctx.DatabaseName, ctx.Hostname, ctx.Retention, ctx.Interval)

		if result != expectedOutput {
			t.Fatalf("expected %q, got %q", expectedOutput, result)
		}
	})
}

func TestProperty_TemplateVariableRendering_WithStaticText(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ctx := genRenderingPartitionContext(t)

		// Generate random static prefix and suffix
		prefix := rapid.StringMatching(`[a-zA-Z0-9/\-_]{1,20}`).Draw(t, "prefix")
		suffix := rapid.StringMatching(`[a-zA-Z0-9/\-_]{1,20}`).Draw(t, "suffix")
		varName := rapid.SampledFrom(allTemplateVariables).Draw(t, "varName")

		templateStr := fmt.Sprintf("%s{{.%s}}%s", prefix, varName, suffix)

		result, err := Render(templateStr, ctx)
		if err != nil {
			t.Fatalf("Render returned unexpected error: %v", err)
		}

		expectedValue := getContextFieldValue(ctx, varName)
		expectedOutput := fmt.Sprintf("%s%s%s", prefix, expectedValue, suffix)

		if result != expectedOutput {
			t.Fatalf("expected %q, got %q", expectedOutput, result)
		}
	})
}
