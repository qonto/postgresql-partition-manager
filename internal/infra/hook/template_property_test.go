// Feature: partition-hooks, Property 16: Undefined Template Variable Error
package hook

import (
	"testing"

	"pgregory.net/rapid"
)

// **Validates: Requirements 7.9**
//
// Property 16: Undefined Template Variable Error
// For any template string containing a reference to an undefined variable name,
// the template rendering SHALL return an error and the hook SHALL NOT be executed.

// undefinedFieldNames contains field names that do NOT exist in PartitionContext.
var undefinedFieldNames = []string{
	"NonExistentField",
	"Foo",
	"Bar",
	"Baz",
	"Unknown",
	"InvalidField",
	"Missing",
	"Port",
	"Password",
	"User",
	"Host",
	"Bucket",
	"Region",
	"Prefix",
	"Format",
	"Command",
	"Args",
	"Query",
	"Name",
	"Type",
	"Enabled",
	"Timeout",
}

// genPartitionContext generates a random valid PartitionContext.
func genPartitionContext(t *rapid.T) PartitionContext {
	return PartitionContext{
		Schema:        rapid.StringMatching(`[a-z][a-z0-9_]{0,15}`).Draw(t, "schema"),
		Table:         rapid.StringMatching(`[a-z][a-z0-9_]{0,20}`).Draw(t, "table"),
		ParentTable:   rapid.StringMatching(`[a-z][a-z0-9_]{0,20}`).Draw(t, "parentTable"),
		LowerBound:    rapid.StringMatching(`\d{4}-\d{2}-\d{2}`).Draw(t, "lowerBound"),
		UpperBound:    rapid.StringMatching(`\d{4}-\d{2}-\d{2}`).Draw(t, "upperBound"),
		PartitionName: rapid.StringMatching(`[a-z][a-z0-9_]{0,15}`).Draw(t, "partitionName"),
		Retention:     rapid.StringMatching(`\d{1,3}`).Draw(t, "retention"),
		Interval:      rapid.SampledFrom([]string{"daily", "weekly", "monthly"}).Draw(t, "interval"),
		DatabaseName:  rapid.StringMatching(`[a-z][a-z0-9_]{0,15}`).Draw(t, "databaseName"),
		Hostname:      rapid.StringMatching(`[a-z][a-z0-9\-]{0,20}\.example\.com`).Draw(t, "hostname"),
	}
}

func TestProperty_UndefinedTemplateVariableError(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Pick a random undefined field name
		undefinedField := rapid.SampledFrom(undefinedFieldNames).Draw(t, "undefinedField")

		// Generate a template string that references the undefined field
		// Optionally include some valid fields mixed in
		includeValidPrefix := rapid.Bool().Draw(t, "includeValidPrefix")

		var templateStr string
		if includeValidPrefix {
			templateStr = "{{.Schema}}.{{." + undefinedField + "}}"
		} else {
			templateStr = "{{." + undefinedField + "}}"
		}

		ctx := genPartitionContext(t)

		// Render must return an error for undefined variables
		result, err := Render(templateStr, ctx)
		if err == nil {
			t.Fatalf("expected error for undefined template variable %q, got result: %q", undefinedField, result)
		}
	})
}

func TestProperty_UndefinedTemplateVariableError_InComplexTemplate(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a more complex template with an undefined variable embedded
		undefinedField := rapid.SampledFrom(undefinedFieldNames).Draw(t, "undefinedField")

		// Build a template with a prefix, the undefined variable, and a suffix
		prefix := rapid.SampledFrom([]string{
			"pg_dump --host={{.Hostname}} --dbname={{.DatabaseName}} ",
			"VACUUM ANALYZE {{.Schema}}.",
			"/usr/local/bin/archive --table=",
			"SELECT * FROM {{.Schema}}.",
		}).Draw(t, "prefix")

		suffix := rapid.SampledFrom([]string{
			" --output=/tmp/backup",
			"",
			" WHERE 1=1",
			" | gzip > /tmp/out.gz",
		}).Draw(t, "suffix")

		templateStr := prefix + "{{." + undefinedField + "}}" + suffix

		ctx := genPartitionContext(t)

		// Render must return an error for undefined variables
		result, err := Render(templateStr, ctx)
		if err == nil {
			t.Fatalf("expected error for undefined template variable %q in complex template %q, got result: %q",
				undefinedField, templateStr, result)
		}
	})
}
