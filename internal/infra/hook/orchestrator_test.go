package hook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errSimulatedFailure is a static error used by the mock runner.
var errSimulatedFailure = errors.New("simulated hook failure")

// orchestratorMockRunner tracks execution order and can be configured to fail on specific hooks.
type orchestratorMockRunner struct {
	callCount     atomic.Int32
	executedHooks []string
	failOnHook    string // hook name that should fail
}

func (r *orchestratorMockRunner) Run(_ context.Context, hook *ResolvedHook) error {
	r.callCount.Add(1)
	r.executedHooks = append(r.executedHooks, hook.Name)

	if r.failOnHook != "" && hook.Name == r.failOnHook {
		return fmt.Errorf("hook %q: %w", hook.Name, errSimulatedFailure)
	}

	return nil
}

func newOrchestratorTestLogger() slog.Logger {
	return *slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func newTestPartitionContext() PartitionContext {
	return PartitionContext{
		Schema:        "public",
		Table:         "events_2024_01",
		ParentTable:   "events",
		LowerBound:    "2024-01-01",
		UpperBound:    "2024-02-01",
		PartitionName: "events",
		Retention:     "30",
		Interval:      "daily",
		DatabaseName:  "mydb",
		Hostname:      "localhost",
	}
}

func boolPtr(b bool) *bool {
	return &b
}

func newTestOrchestrator(hooks *HooksConfig, runner Runner) *Orchestrator {
	logger := newOrchestratorTestLogger()
	executor := NewExecutor(runner, logger)
	metrics := NewMetricsCollector(logger)

	return NewOrchestrator(hooks, executor, metrics, logger, "postgresql://user:pass@localhost:5432/mydb")
}

// --- Nil hooks (no-op) ---

func TestOrchestrator_NilHooks_NoOp(t *testing.T) {
	t.Parallel()

	runner := &orchestratorMockRunner{}
	orch := newTestOrchestrator(nil, runner)
	ctx := context.Background()
	partition := newTestPartitionContext()

	assert.NoError(t, orch.ExecuteBeforeDetach(ctx, partition))
	assert.NoError(t, orch.ExecuteAfterDetach(ctx, partition))
	assert.NoError(t, orch.ExecuteBeforeDrop(ctx, partition))
	assert.NoError(t, orch.ExecuteAfterDrop(ctx, partition))
	assert.False(t, orch.HasFailures())
	assert.Equal(t, int32(0), runner.callCount.Load())
}

// --- Disabled hook skipping ---
// Validates: Requirement 2.7

func TestOrchestrator_DisabledHookSkipped(t *testing.T) {
	t.Parallel()

	runner := &orchestratorMockRunner{}
	hooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:    "disabled-hook",
				Type:    ShellType,
				Enabled: boolPtr(false),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo hello"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
			{
				Name:    "enabled-hook",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo world"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	orch := newTestOrchestrator(hooks, runner)

	err := orch.ExecuteBeforeDetach(context.Background(), newTestPartitionContext())

	require.NoError(t, err)
	assert.Equal(t, int32(1), runner.callCount.Load())
	assert.Equal(t, []string{"enabled-hook"}, runner.executedHooks)
	assert.False(t, orch.HasFailures())
}

func TestOrchestrator_AllDisabledHooksSkipped(t *testing.T) {
	t.Parallel()

	runner := &orchestratorMockRunner{}
	hooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:    "hook-1",
				Type:    ShellType,
				Enabled: boolPtr(false),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo 1"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
			{
				Name:    "hook-2",
				Type:    ShellType,
				Enabled: boolPtr(false),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo 2"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	orch := newTestOrchestrator(hooks, runner)

	err := orch.ExecuteBeforeDetach(context.Background(), newTestPartitionContext())

	require.NoError(t, err)
	assert.Equal(t, int32(0), runner.callCount.Load())
	assert.False(t, orch.HasFailures())
}

// --- Sequential execution order ---
// Validates: Requirements 2.14, 9.1, 10.1, 11.1, 12.1

func TestOrchestrator_SequentialExecution(t *testing.T) {
	t.Parallel()

	runner := &orchestratorMockRunner{}
	hooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:    "hook-a",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo a"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
			{
				Name:    "hook-b",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo b"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
			{
				Name:    "hook-c",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo c"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	orch := newTestOrchestrator(hooks, runner)

	err := orch.ExecuteBeforeDetach(context.Background(), newTestPartitionContext())

	require.NoError(t, err)
	assert.Equal(t, []string{"hook-a", "hook-b", "hook-c"}, runner.executedHooks)
}

// --- Before-hook failure cancels operation ---
// Validates: Requirements 6.1, 9.2, 11.2

func TestOrchestrator_BeforeDetachFailure_CancelsOperation(t *testing.T) {
	t.Parallel()

	runner := &orchestratorMockRunner{failOnHook: "failing-hook"}
	hooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:    "failing-hook",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo fail"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	orch := newTestOrchestrator(hooks, runner)

	err := orch.ExecuteBeforeDetach(context.Background(), newTestPartitionContext())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cancelling")
	assert.True(t, orch.HasFailures())
}

func TestOrchestrator_BeforeDropFailure_CancelsOperation(t *testing.T) {
	t.Parallel()

	runner := &orchestratorMockRunner{failOnHook: "failing-hook"}
	hooks := &HooksConfig{
		BeforeDrop: []HookEntry{
			{
				Name:    "failing-hook",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo fail"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	orch := newTestOrchestrator(hooks, runner)

	err := orch.ExecuteBeforeDrop(context.Background(), newTestPartitionContext())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cancelling")
	assert.True(t, orch.HasFailures())
}

// --- Hook failure short-circuits event ---
// Validates: Requirements 6.2, 6.6, 9.3, 10.4, 11.3, 12.3

func TestOrchestrator_FailureShortCircuits_RemainingHooks(t *testing.T) {
	t.Parallel()

	runner := &orchestratorMockRunner{failOnHook: "hook-b"}
	hooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:    "hook-a",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo a"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
			{
				Name:    "hook-b",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo b"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
			{
				Name:    "hook-c",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo c"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	orch := newTestOrchestrator(hooks, runner)

	err := orch.ExecuteBeforeDetach(context.Background(), newTestPartitionContext())

	require.Error(t, err)
	// hook-a executed, hook-b failed, hook-c should NOT be executed
	assert.Equal(t, []string{"hook-a", "hook-b"}, runner.executedHooks)
	assert.True(t, orch.HasFailures())
}

// --- on_failure=abort stops entire process ---
// Validates: Requirement 6.8

func TestOrchestrator_OnFailureAbort_ReturnsAbortError(t *testing.T) {
	t.Parallel()

	runner := &orchestratorMockRunner{failOnHook: "abort-hook"}
	hooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:      "abort-hook",
				Type:      ShellType,
				Enabled:   boolPtr(true),
				Timeout:   30 * time.Second,
				OnFailure: OnFailureAbort,
				Config:    map[string]interface{}{"command": "echo abort"},
				Retry:     RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	orch := newTestOrchestrator(hooks, runner)

	err := orch.ExecuteBeforeDetach(context.Background(), newTestPartitionContext())

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrAbort))
	assert.True(t, orch.HasFailures())
}

// --- on_failure=continue proceeds with operation ---
// Validates: Requirement 6.9

func TestOrchestrator_OnFailureContinue_ReturnsNil(t *testing.T) {
	t.Parallel()

	runner := &orchestratorMockRunner{failOnHook: "continue-hook"}
	hooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:      "continue-hook",
				Type:      ShellType,
				Enabled:   boolPtr(true),
				Timeout:   30 * time.Second,
				OnFailure: OnFailureContinue,
				Config:    map[string]interface{}{"command": "echo continue"},
				Retry:     RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	orch := newTestOrchestrator(hooks, runner)

	err := orch.ExecuteBeforeDetach(context.Background(), newTestPartitionContext())

	// on_failure=continue means the operation should proceed (nil error)
	require.NoError(t, err)
	// But the failure is still tracked
	assert.True(t, orch.HasFailures())
}

// --- After-hook failure returns error (informational) ---
// Validates: Requirement 6.4

func TestOrchestrator_AfterDetachFailure_ReturnsError(t *testing.T) {
	t.Parallel()

	runner := &orchestratorMockRunner{failOnHook: "after-hook"}
	hooks := &HooksConfig{
		AfterDetach: []HookEntry{
			{
				Name:    "after-hook",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo after"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	orch := newTestOrchestrator(hooks, runner)

	err := orch.ExecuteAfterDetach(context.Background(), newTestPartitionContext())

	// After-hook failure returns error (caller decides what to do)
	require.Error(t, err)
	assert.True(t, orch.HasFailures())
	// Should NOT be an abort error
	assert.False(t, errors.Is(err, ErrAbort))
}

func TestOrchestrator_AfterDropFailure_ReturnsError(t *testing.T) {
	t.Parallel()

	runner := &orchestratorMockRunner{failOnHook: "after-drop-hook"}
	hooks := &HooksConfig{
		AfterDrop: []HookEntry{
			{
				Name:    "after-drop-hook",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo after-drop"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	orch := newTestOrchestrator(hooks, runner)

	err := orch.ExecuteAfterDrop(context.Background(), newTestPartitionContext())

	require.Error(t, err)
	assert.True(t, orch.HasFailures())
	assert.False(t, errors.Is(err, ErrAbort))
}

// --- Template rendering in hooks ---

func TestOrchestrator_TemplateRendering_ShellHook(t *testing.T) {
	t.Parallel()

	runner := &orchestratorMockRunner{}
	hooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:    "template-hook",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config: map[string]interface{}{
					"command": "/usr/bin/archive",
					"args":    []interface{}{"--schema", "{{.Schema}}", "--table", "{{.Table}}"},
					"env": map[string]interface{}{
						"DB_NAME": "{{.DatabaseName}}",
					},
				},
				Retry: RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	orch := newTestOrchestrator(hooks, runner)

	err := orch.ExecuteBeforeDetach(context.Background(), newTestPartitionContext())

	require.NoError(t, err)
	assert.Equal(t, int32(1), runner.callCount.Load())
}

func TestOrchestrator_TemplateRendering_UndefinedVariable(t *testing.T) {
	t.Parallel()

	runner := &orchestratorMockRunner{}
	hooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:    "bad-template-hook",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config: map[string]interface{}{
					"command": "echo {{.UndefinedVar}}",
				},
				Retry: RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	orch := newTestOrchestrator(hooks, runner)

	err := orch.ExecuteBeforeDetach(context.Background(), newTestPartitionContext())

	require.Error(t, err)
	assert.Equal(t, int32(0), runner.callCount.Load())
	assert.True(t, orch.HasFailures())
}

// --- PostgreSQL hook template rendering ---

func TestOrchestrator_TemplateRendering_PostgreSQLHook(t *testing.T) {
	t.Parallel()

	runner := &orchestratorMockRunner{}
	hooks := &HooksConfig{
		AfterDetach: []HookEntry{
			{
				Name:    "vacuum-hook",
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

	orch := newTestOrchestrator(hooks, runner)

	err := orch.ExecuteAfterDetach(context.Background(), newTestPartitionContext())

	require.NoError(t, err)
	assert.Equal(t, int32(1), runner.callCount.Load())
}

// --- Summary and metrics ---

func TestOrchestrator_Summary_TracksMetrics(t *testing.T) {
	t.Parallel()

	runner := &orchestratorMockRunner{failOnHook: "hook-2"}
	hooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:    "hook-1",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo 1"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
			{
				Name:    "hook-2",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo 2"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	orch := newTestOrchestrator(hooks, runner)

	_ = orch.ExecuteBeforeDetach(context.Background(), newTestPartitionContext())

	summary := orch.Summary()
	assert.Equal(t, 2, summary.TotalExecuted)
	assert.Equal(t, 1, summary.TotalSuccess)
	assert.Equal(t, 1, summary.TotalFailures)
}

// --- HasFailures tracks across multiple lifecycle events ---

func TestOrchestrator_HasFailures_AcrossEvents(t *testing.T) {
	t.Parallel()

	runner := &orchestratorMockRunner{failOnHook: "after-hook"}
	hooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:    "before-hook",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo before"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
		AfterDetach: []HookEntry{
			{
				Name:    "after-hook",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo after"},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	orch := newTestOrchestrator(hooks, runner)
	ctx := context.Background()
	partition := newTestPartitionContext()

	// Before-detach succeeds
	err := orch.ExecuteBeforeDetach(ctx, partition)
	require.NoError(t, err)
	assert.False(t, orch.HasFailures())

	// After-detach fails
	err = orch.ExecuteAfterDetach(ctx, partition)
	require.Error(t, err)
	assert.True(t, orch.HasFailures())
}

// --- Credential propagation flag ---

func TestOrchestrator_PropagateCredentials_SetOnResolvedHook(t *testing.T) {
	t.Parallel()

	// Custom runner that captures the resolved hook
	var capturedHook *ResolvedHook

	capturingRunner := &capturingMockRunner{capturedHook: &capturedHook}
	hooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:    "cred-hook",
				Type:    ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config:  map[string]interface{}{"command": "echo creds", "propagate-credentials": true},
				Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			},
		},
	}

	logger := newOrchestratorTestLogger()
	executor := NewExecutor(capturingRunner, logger)
	metrics := NewMetricsCollector(logger)
	orch := NewOrchestrator(hooks, executor, metrics, logger, "postgresql://user:pass@localhost:5432/mydb")

	err := orch.ExecuteBeforeDetach(context.Background(), newTestPartitionContext())

	require.NoError(t, err)
	require.NotNil(t, capturedHook)
	shellCfg, ok := capturedHook.Config.(*ShellConfig)
	require.True(t, ok, "resolved config should be *ShellConfig")
	assert.True(t, shellCfg.PropagateCredentials)
	assert.Equal(t, "postgresql://user:pass@localhost:5432/mydb", capturedHook.ConnectionURL)
}

// capturingMockRunner captures the resolved hook for inspection.
type capturingMockRunner struct {
	capturedHook **ResolvedHook
}

func (r *capturingMockRunner) Run(_ context.Context, hook *ResolvedHook) error {
	*r.capturedHook = hook

	return nil
}
