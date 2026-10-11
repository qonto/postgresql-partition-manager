package hook

import (
	"strings"
	"testing"
)

// **Validates: Requirements 7.1, 7.2, 7.8, 7.9**

func TestRender_AllVariablesPresent(t *testing.T) {
	ctx := PartitionContext{
		Schema:        "public",
		Table:         "events_2024_01",
		ParentTable:   "events",
		LowerBound:    "2024-01-01",
		UpperBound:    "2024-02-01",
		PartitionName: "events",
		Retention:     "30",
		Interval:      "daily",
		DatabaseName:  "mydb",
		Hostname:      "db.example.com",
	}

	templateStr := "{{.Schema}}.{{.Table}} parent={{.ParentTable}} bounds=[{{.LowerBound}},{{.UpperBound}}) partition={{.PartitionName}} retention={{.Retention}} interval={{.Interval}} db={{.DatabaseName}} host={{.Hostname}}"

	result, err := Render(templateStr, ctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	expected := "public.events_2024_01 parent=events bounds=[2024-01-01,2024-02-01) partition=events retention=30 interval=daily db=mydb host=db.example.com"
	if result != expected {
		t.Fatalf("expected %q, got %q", expected, result)
	}
}

func TestRender_PartialVariables(t *testing.T) {
	ctx := PartitionContext{
		Schema: "analytics",
		Table:  "metrics_2024_03",
	}

	templateStr := "VACUUM ANALYZE {{.Schema}}.{{.Table}}"

	result, err := Render(templateStr, ctx)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	expected := "VACUUM ANALYZE analytics.metrics_2024_03"
	if result != expected {
		t.Fatalf("expected %q, got %q", expected, result)
	}
}

func TestRender_SpecialCharactersInValues(t *testing.T) {
	tests := []struct {
		name     string
		ctx      PartitionContext
		template string
		expected string
	}{
		{
			name: "quotes in values",
			ctx: PartitionContext{
				Schema: `"public"`,
				Table:  `events'2024`,
			},
			template: "{{.Schema}}.{{.Table}}",
			expected: `"public".events'2024`,
		},
		{
			name: "slashes and dots",
			ctx: PartitionContext{
				Hostname:     "db.prod.example.com/path",
				DatabaseName: "my.db/name",
			},
			template: "host={{.Hostname}} db={{.DatabaseName}}",
			expected: "host=db.prod.example.com/path db=my.db/name",
		},
		{
			name: "special characters in bounds",
			ctx: PartitionContext{
				LowerBound: "2024-01-01 00:00:00+00",
				UpperBound: "2024-02-01 00:00:00+00",
			},
			template: "[{{.LowerBound}}, {{.UpperBound}})",
			expected: "[2024-01-01 00:00:00+00, 2024-02-01 00:00:00+00)",
		},
		{
			name: "backslashes and dollar signs",
			ctx: PartitionContext{
				Schema: `my\schema`,
				Table:  `table$name`,
			},
			template: "{{.Schema}}.{{.Table}}",
			expected: `my\schema.table$name`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Render(tt.template, tt.ctx)
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}

			if result != tt.expected {
				t.Fatalf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestRender_UndefinedVariableError(t *testing.T) {
	ctx := PartitionContext{
		Schema: "public",
		Table:  "events_2024_01",
	}

	templateStr := "{{.Schema}}.{{.NonExistent}}"

	_, err := Render(templateStr, ctx)
	if err == nil {
		t.Fatal("expected error for undefined variable, got nil")
	}

	if !strings.Contains(err.Error(), "NonExistent") {
		t.Fatalf("expected error to mention 'NonExistent', got: %v", err)
	}
}

func TestRender_InvalidTemplateSyntax(t *testing.T) {
	ctx := PartitionContext{
		Schema: "public",
	}

	templateStr := "{{.Schema}.{{invalid"

	_, err := Render(templateStr, ctx)
	if err == nil {
		t.Fatal("expected error for invalid template syntax, got nil")
	}
}

func TestRender_EmptyTemplateString(t *testing.T) {
	ctx := PartitionContext{
		Schema: "public",
		Table:  "events_2024_01",
	}

	result, err := Render("", ctx)
	if err != nil {
		t.Fatalf("expected no error for empty template, got: %v", err)
	}

	if result != "" {
		t.Fatalf("expected empty string, got %q", result)
	}
}
