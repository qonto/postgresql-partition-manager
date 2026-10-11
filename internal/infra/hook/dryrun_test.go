package hook

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Dry-run orchestrator unit tests ---
// Validates: Requirements 17.2, 17.3, 17.4, 17.5

func newDryRunTestLogger(buf *bytes.Buffer) slog.Logger {
	handler := slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})

	return *slog.New(handler)
}

// TestDryRun_HooksNotExecuted verifies that in dry-run mode, no runner is invoked.
// The dry-run orchestrator has a nil executor, making it structurally impossible to call a runner.
// Validates: Requirement 17.3
func TestDryRun_HooksNotExecuted(t *testing.T) {
	t.Parallel()

	hooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:    "shell-before-detach",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "/usr/bin/backup", "args": []interface{}{"--table", "{{.Table}}"}},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
		AfterDetach: []HookEntry{
			{
				Name:    "pg-after-detach",
				Type:    PostgreSQLType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"sql_query": "VACUUM ANALYZE {{.Schema}}.{{.Table}}"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
		BeforeDrop: []HookEntry{
			{
				Name:    "shell-before-drop",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "/usr/bin/notify"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
		AfterDrop: []HookEntry{
			{
				Name:    "shell-after-drop",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "/usr/bin/cleanup"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	var buf bytes.Buffer
	logger := newDryRunTestLogger(&buf)
	metrics := NewMetricsCollector(logger)
	orch := NewDryRunOrchestrator(hooks, metrics, logger, "postgresql://user:pass@localhost:5432/mydb")
	ctx := context.Background()
	partition := newTestPartitionContext()

	// Execute all lifecycle events
	require.NoError(t, orch.ExecuteBeforeDetach(ctx, partition))
	require.NoError(t, orch.ExecuteAfterDetach(ctx, partition))
	require.NoError(t, orch.ExecuteBeforeDrop(ctx, partition))
	require.NoError(t, orch.ExecuteAfterDrop(ctx, partition))

	// Verify executor is nil (structurally cannot invoke a runner)
	assert.Nil(t, orch.executor, "dry-run orchestrator should have nil executor")
	assert.True(t, orch.dryRun, "dry-run flag should be set")
	assert.False(t, orch.HasFailures(), "no failures should be reported")
}

// TestDryRun_TemplateVariablesResolved verifies that template variables in shell commands,
// args, env vars, and SQL queries are resolved correctly in dry-run mode.
// Validates: Requirement 17.2
func TestDryRun_TemplateVariablesResolved(t *testing.T) {
	t.Parallel()

	hooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:    "template-shell-hook",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config: map[string]interface{}{
					"command": "/usr/bin/archive-{{.ParentTable}}",
					"args":    []interface{}{"--schema", "{{.Schema}}", "--table", "{{.Table}}", "--retention", "{{.Retention}}"},
					"env": map[string]interface{}{
						"DB_NAME":  "{{.DatabaseName}}",
						"DB_HOST":  "{{.Hostname}}",
						"INTERVAL": "{{.Interval}}",
					},
				},
				Retry: RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
		AfterDetach: []HookEntry{
			{
				Name:    "template-pg-hook",
				Type:    PostgreSQLType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config: map[string]interface{}{
					"sql_query": "VACUUM ANALYZE {{.Schema}}.{{.Table}}",
				},
				Retry: RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	var buf bytes.Buffer
	logger := newDryRunTestLogger(&buf)
	metrics := NewMetricsCollector(logger)
	orch := NewDryRunOrchestrator(hooks, metrics, logger, "postgresql://user:pass@localhost:5432/mydb")
	ctx := context.Background()
	partition := newTestPartitionContext()

	// Execute hooks in dry-run mode
	require.NoError(t, orch.ExecuteBeforeDetach(ctx, partition))
	require.NoError(t, orch.ExecuteAfterDetach(ctx, partition))

	// Verify resolved templates appear in the log output
	logOutput := buf.String()

	// Shell hook: command should be resolved
	assert.Contains(t, logOutput, "/usr/bin/archive-events", "command template should resolve ParentTable")
	// Shell hook: args should be resolved
	assert.Contains(t, logOutput, "public", "args template should resolve Schema")
	assert.Contains(t, logOutput, "events_2024_01", "args template should resolve Table")
	// Shell hook: env should be resolved
	assert.Contains(t, logOutput, "mydb", "env template should resolve DatabaseName")
	assert.Contains(t, logOutput, "localhost", "env template should resolve Hostname")
	// PostgreSQL hook: sql_query should be resolved
	assert.Contains(t, logOutput, "VACUUM ANALYZE public.events_2024_01", "sql_query template should resolve Schema and Table")
}

// TestDryRun_ResolvedConfigLogged verifies that [DRY-RUN] log messages contain
// the expected hook details: hook name, type, lifecycle event, partition name, and resolved config.
// Validates: Requirement 17.5
func TestDryRun_ResolvedConfigLogged(t *testing.T) {
	t.Parallel()

	hooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:    "notify-before-detach",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config: map[string]interface{}{
					"command": "/usr/bin/notify",
					"args":    []interface{}{"--partition", "{{.PartitionName}}"},
				},
				Retry: RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
		AfterDrop: []HookEntry{
			{
				Name:    "vacuum-after-drop",
				Type:    PostgreSQLType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config: map[string]interface{}{
					"sql_query": "SELECT pg_stat_reset_single_table_counters('{{.Schema}}.{{.Table}}')",
				},
				Retry: RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	var buf bytes.Buffer
	logger := newDryRunTestLogger(&buf)
	metrics := NewMetricsCollector(logger)
	orch := NewDryRunOrchestrator(hooks, metrics, logger, "postgresql://user:pass@localhost:5432/mydb")
	ctx := context.Background()
	partition := newTestPartitionContext()

	require.NoError(t, orch.ExecuteBeforeDetach(ctx, partition))
	require.NoError(t, orch.ExecuteAfterDrop(ctx, partition))

	logOutput := buf.String()

	// Verify [DRY-RUN] prefix is present
	assert.Contains(t, logOutput, "[DRY-RUN] Would execute hook", "log should contain dry-run prefix")

	// Verify hook name is logged
	assert.Contains(t, logOutput, "notify-before-detach", "log should contain hook name")
	assert.Contains(t, logOutput, "vacuum-after-drop", "log should contain hook name")

	// Verify type is logged
	assert.Contains(t, logOutput, "shell", "log should contain hook type")
	assert.Contains(t, logOutput, "postgresql", "log should contain hook type")

	// Verify lifecycle event is logged
	assert.Contains(t, logOutput, "before-detach", "log should contain lifecycle event")
	assert.Contains(t, logOutput, "after-drop", "log should contain lifecycle event")

	// Verify partition name is logged
	assert.Contains(t, logOutput, "events", "log should contain partition name")

	// Verify resolved command/sql is logged
	assert.Contains(t, logOutput, "/usr/bin/notify", "log should contain resolved command")
	assert.Contains(t, logOutput, "pg_stat_reset_single_table_counters", "log should contain resolved sql")
}

// TestDryRun_DisabledHooksSkipped verifies that disabled hooks do not produce dry-run log messages.
// Validates: Requirement 17.3 (disabled hooks are not executed, even in dry-run)
func TestDryRun_DisabledHooksSkipped(t *testing.T) {
	t.Parallel()

	hooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:    "disabled-hook",
				Type:    ShellType,
				Enabled: boolPtr(false),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "/usr/bin/should-not-appear"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
			{
				Name:    "enabled-hook",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "/usr/bin/should-appear"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	var buf bytes.Buffer
	logger := newDryRunTestLogger(&buf)
	metrics := NewMetricsCollector(logger)
	orch := NewDryRunOrchestrator(hooks, metrics, logger, "postgresql://user:pass@localhost:5432/mydb")

	require.NoError(t, orch.ExecuteBeforeDetach(context.Background(), newTestPartitionContext()))

	logOutput := buf.String()

	// Disabled hook should NOT appear in log
	assert.NotContains(t, logOutput, "disabled-hook", "disabled hook should not produce dry-run log")
	assert.NotContains(t, logOutput, "/usr/bin/should-not-appear", "disabled hook command should not appear")

	// Enabled hook SHOULD appear in log
	assert.Contains(t, logOutput, "enabled-hook", "enabled hook should produce dry-run log")
	assert.Contains(t, logOutput, "/usr/bin/should-appear", "enabled hook command should appear")
}

// TestDryRun_UndefinedTemplateVariableReportsError verifies that undefined template variables
// still produce errors in dry-run mode (consistent with normal mode).
// Validates: Requirement 17.2 (template resolution errors still reported)
func TestDryRun_UndefinedTemplateVariableReportsError(t *testing.T) {
	t.Parallel()

	hooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:    "bad-template-hook",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config: map[string]interface{}{
					"command": "echo {{.NonExistentVariable}}",
				},
				Retry: RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	var buf bytes.Buffer
	logger := newDryRunTestLogger(&buf)
	metrics := NewMetricsCollector(logger)
	orch := NewDryRunOrchestrator(hooks, metrics, logger, "postgresql://user:pass@localhost:5432/mydb")

	err := orch.ExecuteBeforeDetach(context.Background(), newTestPartitionContext())

	// Should return an error for undefined template variable
	require.Error(t, err)
	assert.Contains(t, err.Error(), "template rendering failed")
	assert.True(t, orch.HasFailures(), "orchestrator should track the failure")
}

// TestDryRun_PostgreSQLUndefinedTemplateVariable verifies that undefined template variables
// in PostgreSQL hook SQL queries also produce errors in dry-run mode.
func TestDryRun_PostgreSQLUndefinedTemplateVariable(t *testing.T) {
	t.Parallel()

	hooks := &HooksConfig{
		AfterDetach: []HookEntry{
			{
				Name:    "bad-pg-hook",
				Type:    PostgreSQLType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config: map[string]interface{}{
					"sql_query": "SELECT * FROM {{.InvalidField}}",
				},
				Retry: RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	var buf bytes.Buffer
	logger := newDryRunTestLogger(&buf)
	metrics := NewMetricsCollector(logger)
	orch := NewDryRunOrchestrator(hooks, metrics, logger, "postgresql://user:pass@localhost:5432/mydb")

	err := orch.ExecuteAfterDetach(context.Background(), newTestPartitionContext())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "template rendering failed")
	assert.True(t, orch.HasFailures())
}
