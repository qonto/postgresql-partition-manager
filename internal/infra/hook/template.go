package hook

import (
	"bytes"
	"text/template"
)

// PartitionContext holds all template variables available during hook rendering.
type PartitionContext struct {
	// Partition metadata
	Schema      string // Partition schema
	Table       string // Partition table name (child)
	ParentTable string // Parent table name
	LowerBound  string // Partition lower bound (formatted)
	UpperBound  string // Partition upper bound (formatted)

	// Configuration metadata
	PartitionName string // Partition identifier from config file
	Retention     string // Configured retention value
	Interval      string // Configured interval value

	// Connection metadata
	DatabaseName string // Database name from connection URL
	Hostname     string // Database hostname from connection URL
}

// Render parses and executes a Go text/template string with the given PartitionContext.
// It returns the rendered string or an error if the template is invalid or references undefined variables.
func Render(templateStr string, ctx PartitionContext) (string, error) {
	tmpl, err := template.New("hook").Option("missingkey=error").Parse(templateStr)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer

	if err := tmpl.Execute(&buf, ctx); err != nil {
		return "", err
	}

	return buf.String(), nil
}
