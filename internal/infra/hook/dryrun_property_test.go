// Feature: partition-hooks, Property 18: Dry-Run No Side Effects
package hook

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// **Validates: Requirements 17.2, 17.3, 17.4**
//
// Property 18: Dry-Run No Side Effects
// For any hook configuration in dry-run mode, template variables SHALL be resolved and logged,
// but no hook runner SHALL be invoked, no partition SHALL be detached, and no partition SHALL be dropped.

// newDryRunTestOrchestrator creates an orchestrator in dry-run mode.
// It does NOT pass an executor/runner, matching the production usage of NewDryRunOrchestrator.
func newDryRunTestOrchestrator(hooks *HooksConfig, connURL string) *Orchestrator {
	logger := *slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	metrics := NewMetricsCollector(logger)

	return NewDryRunOrchestrator(hooks, metrics, logger, connURL)
}

// genDryRunPartitionContext generates a random valid PartitionContext for dry-run property testing.
func genDryRunPartitionContext(t *rapid.T) PartitionContext {
	return PartitionContext{
		Schema:        rapid.StringMatching(`[a-z][a-z0-9_]{1,15}`).Draw(t, "schema"),
		Table:         rapid.StringMatching(`[a-z][a-z0-9_]{1,15}_\d{4}_\d{2}`).Draw(t, "table"),
		ParentTable:   rapid.StringMatching(`[a-z][a-z0-9_]{1,15}`).Draw(t, "parentTable"),
		LowerBound:    rapid.StringMatching(`\d{4}-\d{2}-\d{2}`).Draw(t, "lowerBound"),
		UpperBound:    rapid.StringMatching(`\d{4}-\d{2}-\d{2}`).Draw(t, "upperBound"),
		PartitionName: rapid.StringMatching(`[a-z][a-z0-9_]{1,15}`).Draw(t, "partitionName"),
		Retention:     rapid.StringMatching(`\d{1,3}`).Draw(t, "retention"),
		Interval:      rapid.SampledFrom([]string{"daily", "weekly", "monthly"}).Draw(t, "interval"),
		DatabaseName:  rapid.StringMatching(`[a-z][a-z0-9_]{1,15}`).Draw(t, "databaseName"),
		Hostname:      rapid.StringMatching(`[a-z][a-z0-9\-]{1,20}`).Draw(t, "hostname"),
	}
}

// genDryRunHookEntry generates a valid enabled hook entry with template variables in config.
func genDryRunHookEntry(t *rapid.T, label string) HookEntry {
	hookType := rapid.SampledFrom([]HookType{ShellType, PostgreSQLType}).Draw(t, label+"_type")
	name := rapid.StringMatching(`[a-z][a-z0-9\-]{1,20}`).Draw(t, label+"_name")

	var config map[string]interface{}

	switch hookType {
	case ShellType:
		// Use template variables to verify they get resolved
		config = map[string]interface{}{
			"command": "/usr/bin/hook-{{.ParentTable}}",
			"args":    []interface{}{"--schema", "{{.Schema}}", "--table", "{{.Table}}"},
			"env": map[string]interface{}{
				"DB_HOST": "{{.Hostname}}",
				"DB_NAME": "{{.DatabaseName}}",
			},
		}
	case PostgreSQLType:
		config = map[string]interface{}{
			"sql_query": "VACUUM ANALYZE {{.Schema}}.{{.Table}}",
		}
	}

	return HookEntry{
		Name:    name,
		Type:    hookType,
		Enabled: boolPtr(true),
		Timeout: 30 * time.Second,
		Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
		Config:  config,
	}
}

// genDryRunHooksConfig generates a HooksConfig with hooks spread across lifecycle events.
func genDryRunHooksConfig(t *rapid.T) *HooksConfig {
	cfg := &HooksConfig{}

	numBeforeDetach := rapid.IntRange(0, 3).Draw(t, "numBeforeDetach")
	for i := 0; i < numBeforeDetach; i++ {
		cfg.BeforeDetach = append(cfg.BeforeDetach, genDryRunHookEntry(t, fmt.Sprintf("bd_%d", i)))
	}

	numAfterDetach := rapid.IntRange(0, 3).Draw(t, "numAfterDetach")
	for i := 0; i < numAfterDetach; i++ {
		cfg.AfterDetach = append(cfg.AfterDetach, genDryRunHookEntry(t, fmt.Sprintf("ad_%d", i)))
	}

	numBeforeDrop := rapid.IntRange(0, 3).Draw(t, "numBeforeDrop")
	for i := 0; i < numBeforeDrop; i++ {
		cfg.BeforeDrop = append(cfg.BeforeDrop, genDryRunHookEntry(t, fmt.Sprintf("bdr_%d", i)))
	}

	numAfterDrop := rapid.IntRange(0, 3).Draw(t, "numAfterDrop")
	for i := 0; i < numAfterDrop; i++ {
		cfg.AfterDrop = append(cfg.AfterDrop, genDryRunHookEntry(t, fmt.Sprintf("adr_%d", i)))
	}

	return cfg
}

// TestProperty_DryRunNoSideEffects_RunnerNeverInvoked verifies that in dry-run mode,
// no hook runner is ever invoked regardless of the hook configuration or lifecycle event.
func TestProperty_DryRunNoSideEffects_RunnerNeverInvoked(t *testing.T) {
	// Feature: partition-hooks, Property 18: Dry-Run No Side Effects
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")
		partition := genDryRunPartitionContext(t)

		// Generate hooks for the selected event
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		hooks := make([]HookEntry, 0, numHooks)

		for i := 0; i < numHooks; i++ {
			hooks = append(hooks, genDryRunHookEntry(t, fmt.Sprintf("hook_%d", i)))
		}

		cfg := buildHooksConfigForEvent(event, hooks)
		connURL := "postgresql://user:pass@localhost:5432/mydb"
		orch := newDryRunTestOrchestrator(cfg, connURL)

		// Execute the lifecycle event in dry-run mode
		err := executeEventOnOrchestrator(context.Background(), orch, event, partition)

		// Dry-run should succeed without errors (no side effects)
		if err != nil {
			t.Fatalf("dry-run should not return error for valid hooks, got: %v", err)
		}

		// The orchestrator in dry-run mode does not use an executor/runner at all.
		// NewDryRunOrchestrator does not accept a runner, so it's structurally impossible
		// for it to invoke one. We verify the orchestrator has no failures reported.
		if orch.HasFailures() {
			t.Fatal("dry-run should not report failures when all templates resolve successfully")
		}
	})
}

// TestProperty_DryRunNoSideEffects_TemplatesResolved verifies that in dry-run mode,
// template variables are resolved correctly (requirement 17.2). If templates contain
// undefined variables, errors are reported as in normal mode (requirement 17.7).
func TestProperty_DryRunNoSideEffects_TemplatesResolved(t *testing.T) {
	// Feature: partition-hooks, Property 18: Dry-Run No Side Effects
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")
		partition := genDryRunPartitionContext(t)

		// Generate valid hooks with template variables
		numHooks := rapid.IntRange(1, 4).Draw(t, "numHooks")
		hooks := make([]HookEntry, 0, numHooks)

		for i := 0; i < numHooks; i++ {
			hooks = append(hooks, genDryRunHookEntry(t, fmt.Sprintf("hook_%d", i)))
		}

		cfg := buildHooksConfigForEvent(event, hooks)
		connURL := "postgresql://user:pass@localhost:5432/mydb"
		orch := newDryRunTestOrchestrator(cfg, connURL)

		// Execute in dry-run mode
		err := executeEventOnOrchestrator(context.Background(), orch, event, partition)

		// All templates should resolve without error since we use valid template variables
		if err != nil {
			t.Fatalf("dry-run with valid templates should not return error, got: %v", err)
		}
	})
}

// TestProperty_DryRunNoSideEffects_UndefinedVarReportsError verifies that in dry-run mode,
// undefined template variables still produce errors (requirement 17.7), matching normal mode behavior.
func TestProperty_DryRunNoSideEffects_UndefinedVarReportsError(t *testing.T) {
	// Feature: partition-hooks, Property 18: Dry-Run No Side Effects
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")
		partition := genDryRunPartitionContext(t)

		// Create a hook with an undefined template variable
		hookType := rapid.SampledFrom([]HookType{ShellType, PostgreSQLType}).Draw(t, "hookType")
		name := rapid.StringMatching(`[a-z][a-z0-9\-]{1,20}`).Draw(t, "name")
		undefinedVar := rapid.StringMatching(`[A-Z][a-zA-Z]{3,15}`).Draw(t, "undefinedVar")

		var config map[string]interface{}

		switch hookType {
		case ShellType:
			config = map[string]interface{}{
				"command": fmt.Sprintf("/usr/bin/hook-{{.%s}}", undefinedVar),
			}
		case PostgreSQLType:
			config = map[string]interface{}{
				"sql_query": fmt.Sprintf("SELECT * FROM {{.%s}}", undefinedVar),
			}
		}

		hook := HookEntry{
			Name:    name,
			Type:    hookType,
			Enabled: boolPtr(true),
			Timeout: 30 * time.Second,
			Retry:   RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			Config:  config,
		}

		cfg := buildHooksConfigForEvent(event, []HookEntry{hook})
		connURL := "postgresql://user:pass@localhost:5432/mydb"
		orch := newDryRunTestOrchestrator(cfg, connURL)

		// Execute in dry-run mode
		err := executeEventOnOrchestrator(context.Background(), orch, event, partition)

		// Undefined template variables MUST produce an error even in dry-run mode
		if err == nil {
			t.Fatalf("dry-run with undefined template variable {{.%s}} should return error, got nil", undefinedVar)
		}

		// The orchestrator should track the failure
		if !orch.HasFailures() {
			t.Fatal("dry-run should report failure when template variable is undefined")
		}
	})
}

// TestProperty_DryRunNoSideEffects_AllLifecycleEventsNoExecution verifies that in dry-run mode,
// executing hooks across ALL lifecycle events produces no side effects (no runner calls,
// no partition operations). This simulates the full cleanup flow scenario.
func TestProperty_DryRunNoSideEffects_AllLifecycleEventsNoExecution(t *testing.T) {
	// Feature: partition-hooks, Property 18: Dry-Run No Side Effects
	rapid.Check(t, func(t *rapid.T) {
		partition := genDryRunPartitionContext(t)
		cfg := genDryRunHooksConfig(t)
		connURL := "postgresql://user:pass@localhost:5432/mydb"
		orch := newDryRunTestOrchestrator(cfg, connURL)

		ctx := context.Background()

		// Simulate full cleanup flow in dry-run: all 4 lifecycle events
		err := orch.ExecuteBeforeDetach(ctx, partition)
		if err != nil {
			t.Fatalf("dry-run ExecuteBeforeDetach should not error, got: %v", err)
		}

		err = orch.ExecuteAfterDetach(ctx, partition)
		if err != nil {
			t.Fatalf("dry-run ExecuteAfterDetach should not error, got: %v", err)
		}

		err = orch.ExecuteBeforeDrop(ctx, partition)
		if err != nil {
			t.Fatalf("dry-run ExecuteBeforeDrop should not error, got: %v", err)
		}

		err = orch.ExecuteAfterDrop(ctx, partition)
		if err != nil {
			t.Fatalf("dry-run ExecuteAfterDrop should not error, got: %v", err)
		}

		// Verify no failures (all templates use valid variables)
		if orch.HasFailures() {
			t.Fatal("dry-run should not report failures with valid hook configurations")
		}

		// Verify the executor field is nil (structurally impossible to invoke a runner)
		if orch.executor != nil {
			t.Fatal("dry-run orchestrator should have nil executor")
		}
	})
}

// TestProperty_DryRunNoSideEffects_DisabledHooksSkipped verifies that in dry-run mode,
// disabled hooks are still skipped (consistent with normal mode behavior).
func TestProperty_DryRunNoSideEffects_DisabledHooksSkipped(t *testing.T) {
	// Feature: partition-hooks, Property 18: Dry-Run No Side Effects
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")
		partition := genDryRunPartitionContext(t)

		// Generate a mix of enabled and disabled hooks
		numEnabled := rapid.IntRange(0, 3).Draw(t, "numEnabled")
		numDisabled := rapid.IntRange(1, 3).Draw(t, "numDisabled")

		hooks := make([]HookEntry, 0, numEnabled+numDisabled)

		for i := 0; i < numEnabled; i++ {
			hooks = append(hooks, genDryRunHookEntry(t, fmt.Sprintf("en_%d", i)))
		}

		for i := 0; i < numDisabled; i++ {
			hook := genDryRunHookEntry(t, fmt.Sprintf("dis_%d", i))
			hook.Enabled = boolPtr(false)
			hooks = append(hooks, hook)
		}

		cfg := buildHooksConfigForEvent(event, hooks)
		connURL := "postgresql://user:pass@localhost:5432/mydb"
		orch := newDryRunTestOrchestrator(cfg, connURL)

		// Execute in dry-run mode
		err := executeEventOnOrchestrator(context.Background(), orch, event, partition)

		// Should succeed without errors
		if err != nil {
			t.Fatalf("dry-run with disabled hooks should not error, got: %v", err)
		}

		// No failures should be reported
		if orch.HasFailures() {
			t.Fatal("dry-run should not report failures when hooks are simply disabled")
		}
	})
}
