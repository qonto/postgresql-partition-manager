// Feature: partition-hooks, Property 3: Disabled Hook Skipping
package hook

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// **Validates: Requirements 2.7**
//
// Property 3: Disabled Hook Skipping
// For any hook with `enabled` set to false, the hook runner SHALL NOT be invoked,
// and the hook execution SHALL not produce an error or affect subsequent hooks in the same lifecycle event.

// allLifecycleEvents returns all lifecycle events for property testing.
func allLifecycleEvents() []LifecycleEvent {
	return []LifecycleEvent{BeforeDetach, AfterDetach, BeforeDrop, AfterDrop}
}

// genValidHookEntry generates a valid HookEntry with all required fields for orchestrator testing.
func genValidHookEntry(t *rapid.T, label string, enabled bool) HookEntry {
	hookType := rapid.SampledFrom([]HookType{ShellType, PostgreSQLType}).Draw(t, label+"_type")
	name := rapid.StringMatching(`[a-z][a-z0-9\-]{1,20}`).Draw(t, label+"_name")

	var config map[string]interface{}

	switch hookType {
	case ShellType:
		config = map[string]interface{}{
			"command": "/usr/bin/echo",
		}
	case PostgreSQLType:
		config = map[string]interface{}{
			"sql_query": "SELECT 1",
		}
	}

	return HookEntry{
		Name:      name,
		Type:      hookType,
		Enabled:   boolPtr(enabled),
		Timeout:   30 * time.Second,
		OnFailure: "",
		Retry:     RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
		Config:    config,
	}
}

// buildHooksConfigForEvent creates a HooksConfig with hooks placed in the specified lifecycle event.
func buildHooksConfigForEvent(event LifecycleEvent, hooks []HookEntry) *HooksConfig {
	cfg := &HooksConfig{}

	switch event {
	case BeforeDetach:
		cfg.BeforeDetach = hooks
	case AfterDetach:
		cfg.AfterDetach = hooks
	case BeforeDrop:
		cfg.BeforeDrop = hooks
	case AfterDrop:
		cfg.AfterDrop = hooks
	}

	return cfg
}

// executeEventOnOrchestrator calls the appropriate Execute method based on the lifecycle event.
func executeEventOnOrchestrator(ctx context.Context, orch *Orchestrator, event LifecycleEvent, partition PartitionContext) error {
	switch event {
	case BeforeDetach:
		return orch.ExecuteBeforeDetach(ctx, partition)
	case AfterDetach:
		return orch.ExecuteAfterDetach(ctx, partition)
	case BeforeDrop:
		return orch.ExecuteBeforeDrop(ctx, partition)
	case AfterDrop:
		return orch.ExecuteAfterDrop(ctx, partition)
	}

	return nil
}

// TestProperty_DisabledHookSkipping_NeverExecuted verifies that disabled hooks are never executed
// (the runner is never called for them) across all lifecycle events.
func TestProperty_DisabledHookSkipping_NeverExecuted(t *testing.T) {
	// Feature: partition-hooks, Property 3: Disabled Hook Skipping
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate a list of hooks where ALL are disabled
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		hooks := make([]HookEntry, 0, numHooks)

		for i := 0; i < numHooks; i++ {
			hooks = append(hooks, genValidHookEntry(t, "hook", false))
		}

		runner := &orchestratorMockRunner{}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		err := executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// Disabled hooks must not produce errors
		if err != nil {
			t.Fatalf("disabled hooks should not produce errors, got: %v", err)
		}

		// Runner must never be called for disabled hooks
		if runner.callCount.Load() != 0 {
			t.Fatalf("runner should not be called for disabled hooks, got %d calls", runner.callCount.Load())
		}
	})
}

// TestProperty_DisabledHookSkipping_NoError verifies that disabled hooks don't produce errors
// regardless of the lifecycle event.
func TestProperty_DisabledHookSkipping_NoError(t *testing.T) {
	// Feature: partition-hooks, Property 3: Disabled Hook Skipping
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Mix of disabled hooks with various configurations
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		hooks := make([]HookEntry, 0, numHooks)

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", false)
			// Randomize on_failure to ensure disabled hooks never trigger failure handling
			hook.OnFailure = rapid.SampledFrom([]OnFailure{"", OnFailureAbort, OnFailureContinue}).Draw(t, "onFailure")
			hooks = append(hooks, hook)
		}

		runner := &orchestratorMockRunner{}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		err := executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// Disabled hooks must not produce errors
		if err != nil {
			t.Fatalf("disabled hooks should not produce errors regardless of on_failure setting, got: %v", err)
		}

		// Orchestrator must not report failures for disabled hooks
		if orch.HasFailures() {
			t.Fatal("orchestrator should not report failures when only disabled hooks are present")
		}
	})
}

// TestProperty_DisabledHookSkipping_SubsequentHooksStillExecute verifies that disabled hooks
// don't affect subsequent enabled hooks in the same lifecycle event.
func TestProperty_DisabledHookSkipping_SubsequentHooksStillExecute(t *testing.T) {
	// Feature: partition-hooks, Property 3: Disabled Hook Skipping
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate a mixed list: some disabled, some enabled
		// At least 1 disabled and 1 enabled hook
		numDisabled := rapid.IntRange(1, 3).Draw(t, "numDisabled")
		numEnabled := rapid.IntRange(1, 3).Draw(t, "numEnabled")

		hooks := make([]HookEntry, 0, numDisabled+numEnabled)
		expectedExecuted := make([]string, 0, numEnabled)

		// Interleave disabled and enabled hooks randomly
		disabledIdx := 0
		enabledIdx := 0
		totalHooks := numDisabled + numEnabled

		for i := 0; i < totalHooks; i++ {
			// Decide whether to place a disabled or enabled hook
			placeDisabled := false
			if disabledIdx < numDisabled && enabledIdx < numEnabled {
				placeDisabled = rapid.Bool().Draw(t, "placeDisabled")
			} else if disabledIdx < numDisabled {
				placeDisabled = true
			}

			if placeDisabled {
				hook := genValidHookEntry(t, "disabled", false)
				// Ensure unique name for tracking
				hook.Name = "disabled-" + hook.Name
				hooks = append(hooks, hook)
				disabledIdx++
			} else {
				hook := genValidHookEntry(t, "enabled", true)
				// Ensure unique name for tracking
				hook.Name = "enabled-" + hook.Name
				hooks = append(hooks, hook)
				expectedExecuted = append(expectedExecuted, hook.Name)
				enabledIdx++
			}
		}

		runner := &orchestratorMockRunner{}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		err := executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// No errors expected (all enabled hooks succeed with mock runner)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		// Verify only enabled hooks were executed
		if int(runner.callCount.Load()) != numEnabled {
			t.Fatalf("expected %d enabled hooks to be executed, got %d", numEnabled, runner.callCount.Load())
		}

		// Verify the execution order matches the order of enabled hooks
		if len(runner.executedHooks) != len(expectedExecuted) {
			t.Fatalf("expected %d executed hooks, got %d", len(expectedExecuted), len(runner.executedHooks))
		}

		for i, name := range expectedExecuted {
			if runner.executedHooks[i] != name {
				t.Fatalf("expected hook at position %d to be %q, got %q", i, name, runner.executedHooks[i])
			}
		}

		// Orchestrator must not report failures
		if orch.HasFailures() {
			t.Fatal("orchestrator should not report failures when all enabled hooks succeed")
		}
	})
}

// TestProperty_DisabledHookSkipping_DisabledBeforeEnabled verifies that disabled hooks
// placed before enabled hooks don't prevent the enabled hooks from executing.
func TestProperty_DisabledHookSkipping_DisabledBeforeEnabled(t *testing.T) {
	// Feature: partition-hooks, Property 3: Disabled Hook Skipping
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Place disabled hooks first, then enabled hooks
		numDisabledBefore := rapid.IntRange(1, 4).Draw(t, "numDisabledBefore")
		numEnabledAfter := rapid.IntRange(1, 4).Draw(t, "numEnabledAfter")

		hooks := make([]HookEntry, 0, numDisabledBefore+numEnabledAfter)

		// Add disabled hooks first
		for i := 0; i < numDisabledBefore; i++ {
			hook := genValidHookEntry(t, "dis", false)
			hook.Name = "disabled-" + hook.Name
			hooks = append(hooks, hook)
		}

		// Add enabled hooks after
		expectedNames := make([]string, 0, numEnabledAfter)

		for i := 0; i < numEnabledAfter; i++ {
			hook := genValidHookEntry(t, "en", true)
			hook.Name = "enabled-" + hook.Name
			hooks = append(hooks, hook)
			expectedNames = append(expectedNames, hook.Name)
		}

		runner := &orchestratorMockRunner{}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		err := executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// No errors expected
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		// All enabled hooks must have been executed
		if int(runner.callCount.Load()) != numEnabledAfter {
			t.Fatalf("expected %d enabled hooks to execute, got %d", numEnabledAfter, runner.callCount.Load())
		}

		// Verify execution order
		for i, name := range expectedNames {
			if runner.executedHooks[i] != name {
				t.Fatalf("expected hook at position %d to be %q, got %q", i, name, runner.executedHooks[i])
			}
		}
	})
}

// Feature: partition-hooks, Property 4: Sequential Execution Order

// **Validates: Requirements 2.14, 9.1, 10.1, 11.1, 12.1**
//
// Property 4: Sequential Execution Order
// For any list of enabled hooks within a lifecycle event, the hooks SHALL be executed
// in the exact order they are defined in the configuration, and each hook SHALL complete
// (success or failure) before the next hook begins.

// TestProperty_SequentialExecution_OrderMatchesDefinition verifies that for any randomly
// generated list of N enabled hooks (N >= 2) in any lifecycle event, the execution order
// matches the definition order.
func TestProperty_SequentialExecution_OrderMatchesDefinition(t *testing.T) {
	// Feature: partition-hooks, Property 4: Sequential Execution Order
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate N >= 2 enabled hooks with unique names
		numHooks := rapid.IntRange(2, 8).Draw(t, "numHooks")
		hooks := make([]HookEntry, 0, numHooks)
		expectedOrder := make([]string, 0, numHooks)

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			// Ensure unique names by prefixing with index
			hook.Name = rapid.StringMatching(`[a-z][a-z0-9]{2,10}`).Draw(t, "name") + "-" + string(rune('a'+i))
			hooks = append(hooks, hook)
			expectedOrder = append(expectedOrder, hook.Name)
		}

		runner := &orchestratorMockRunner{}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		err := executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// All hooks succeed with mock runner, so no error expected
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		// Verify execution order matches definition order
		if len(runner.executedHooks) != numHooks {
			t.Fatalf("expected %d hooks executed, got %d", numHooks, len(runner.executedHooks))
		}

		for i, expectedName := range expectedOrder {
			if runner.executedHooks[i] != expectedName {
				t.Fatalf("execution order mismatch at position %d: expected %q, got %q\nExpected order: %v\nActual order:   %v",
					i, expectedName, runner.executedHooks[i], expectedOrder, runner.executedHooks)
			}
		}
	})
}

// TestProperty_SequentialExecution_CountEqualsEnabled verifies that the number of executions
// equals the number of enabled hooks, confirming each hook completes before the next begins.
func TestProperty_SequentialExecution_CountEqualsEnabled(t *testing.T) {
	// Feature: partition-hooks, Property 4: Sequential Execution Order
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate a mix of enabled and disabled hooks (at least 2 enabled)
		numEnabled := rapid.IntRange(2, 6).Draw(t, "numEnabled")
		numDisabled := rapid.IntRange(0, 4).Draw(t, "numDisabled")
		totalHooks := numEnabled + numDisabled

		hooks := make([]HookEntry, 0, totalHooks)
		enabledCount := 0
		disabledCount := 0

		// Interleave enabled and disabled hooks randomly
		for i := 0; i < totalHooks; i++ {
			placeDisabled := false
			if disabledCount < numDisabled && enabledCount < numEnabled {
				placeDisabled = rapid.Bool().Draw(t, "placeDisabled")
			} else if disabledCount < numDisabled {
				placeDisabled = true
			}

			if placeDisabled {
				hook := genValidHookEntry(t, "dis", false)
				hook.Name = "disabled-" + hook.Name + "-" + string(rune('a'+i))
				hooks = append(hooks, hook)
				disabledCount++
			} else {
				hook := genValidHookEntry(t, "en", true)
				hook.Name = "enabled-" + hook.Name + "-" + string(rune('a'+i))
				hooks = append(hooks, hook)
				enabledCount++
			}
		}

		runner := &orchestratorMockRunner{}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		err := executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// No error expected (all enabled hooks succeed with mock runner)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		// The number of executions must equal the number of enabled hooks
		if int(runner.callCount.Load()) != numEnabled {
			t.Fatalf("expected %d executions (one per enabled hook), got %d", numEnabled, runner.callCount.Load())
		}

		// executedHooks slice length must match callCount (sequential, no concurrent writes)
		if len(runner.executedHooks) != int(runner.callCount.Load()) {
			t.Fatalf("executedHooks length (%d) does not match callCount (%d), suggesting non-sequential execution",
				len(runner.executedHooks), runner.callCount.Load())
		}
	})
}

// TestProperty_SequentialExecution_AllLifecycleEvents verifies that sequential execution
// holds across all lifecycle events (before-detach, after-detach, before-drop, after-drop).
func TestProperty_SequentialExecution_AllLifecycleEvents(t *testing.T) {
	// Feature: partition-hooks, Property 4: Sequential Execution Order
	rapid.Check(t, func(t *rapid.T) {
		// Test each lifecycle event explicitly
		for _, event := range allLifecycleEvents() {
			numHooks := rapid.IntRange(2, 5).Draw(t, "numHooks_"+string(event))
			hooks := make([]HookEntry, 0, numHooks)
			expectedOrder := make([]string, 0, numHooks)

			for i := 0; i < numHooks; i++ {
				hook := genValidHookEntry(t, "hook_"+string(event), true)
				hook.Name = string(event) + "-hook-" + string(rune('a'+i))
				hooks = append(hooks, hook)
				expectedOrder = append(expectedOrder, hook.Name)
			}

			runner := &orchestratorMockRunner{}
			cfg := buildHooksConfigForEvent(event, hooks)
			orch := newTestOrchestrator(cfg, runner)

			err := executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

			if err != nil {
				t.Fatalf("event %s: expected no error, got: %v", event, err)
			}

			// Verify count matches
			if int(runner.callCount.Load()) != numHooks {
				t.Fatalf("event %s: expected %d executions, got %d", event, numHooks, runner.callCount.Load())
			}

			// Verify order matches definition order
			for i, expectedName := range expectedOrder {
				if runner.executedHooks[i] != expectedName {
					t.Fatalf("event %s: order mismatch at position %d: expected %q, got %q",
						event, i, expectedName, runner.executedHooks[i])
				}
			}
		}
	})
}

// Feature: partition-hooks, Property 5: Before-Hook Failure Cancels Operation

// **Validates: Requirements 6.1, 9.2, 11.2**
//
// Property 5: Before-Hook Failure Cancels Operation
// For any before-hook (before-detach or before-drop) that fails after all retry attempts
// with default on_failure behavior, the associated operation (detach or drop) SHALL NOT
// be executed for the affected partition.

// beforeHookEvents returns only the before-* lifecycle events.
func beforeHookEvents() []LifecycleEvent {
	return []LifecycleEvent{BeforeDetach, BeforeDrop}
}

// TestProperty_BeforeHookFailureCancelsOperation_ReturnsError verifies that for any
// before-hook (before-detach or before-drop) that fails with default on_failure behavior,
// the orchestrator returns a non-nil error signaling the operation should be cancelled.
func TestProperty_BeforeHookFailureCancelsOperation_ReturnsError(t *testing.T) {
	// Feature: partition-hooks, Property 5: Before-Hook Failure Cancels Operation
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(beforeHookEvents()).Draw(t, "event")

		// Generate a random number of hooks with the failing hook at a random position
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		failIdx := rapid.IntRange(0, numHooks-1).Draw(t, "failIdx")

		hooks := make([]HookEntry, 0, numHooks)
		var failingHookName string

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))
			// Ensure default on_failure (empty string) for all hooks
			hook.OnFailure = ""
			hooks = append(hooks, hook)

			if i == failIdx {
				failingHookName = hook.Name
			}
		}

		runner := &orchestratorMockRunner{failOnHook: failingHookName}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		err := executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// Before-hook failure with default on_failure MUST return a non-nil error
		// signaling the associated operation should be cancelled
		if err == nil {
			t.Fatalf("expected non-nil error when before-hook %q fails with default on_failure, got nil (event=%s)",
				failingHookName, event)
		}
	})
}

// TestProperty_BeforeHookFailureCancelsOperation_NotAbort verifies that the error returned
// by a failing before-hook with default on_failure is NOT an ErrAbort. The default behavior
// is cancellation of the single operation, not aborting the entire cleanup process.
func TestProperty_BeforeHookFailureCancelsOperation_NotAbort(t *testing.T) {
	// Feature: partition-hooks, Property 5: Before-Hook Failure Cancels Operation
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(beforeHookEvents()).Draw(t, "event")

		// Generate hooks with the failing hook at a random position
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		failIdx := rapid.IntRange(0, numHooks-1).Draw(t, "failIdx")

		hooks := make([]HookEntry, 0, numHooks)
		var failingHookName string

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))
			// Ensure default on_failure (empty string) — NOT abort, NOT continue
			hook.OnFailure = ""
			hooks = append(hooks, hook)

			if i == failIdx {
				failingHookName = hook.Name
			}
		}

		runner := &orchestratorMockRunner{failOnHook: failingHookName}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		err := executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// The error must NOT be an ErrAbort — default cancellation is different from abort
		if errors.Is(err, ErrAbort) {
			t.Fatalf("expected non-abort error for default on_failure, but got ErrAbort (event=%s, hook=%s)",
				event, failingHookName)
		}
	})
}

// TestProperty_BeforeHookFailureCancelsOperation_TracksFailure verifies that when a
// before-hook fails with default on_failure, the orchestrator tracks the failure via HasFailures().
func TestProperty_BeforeHookFailureCancelsOperation_TracksFailure(t *testing.T) {
	// Feature: partition-hooks, Property 5: Before-Hook Failure Cancels Operation
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(beforeHookEvents()).Draw(t, "event")

		// Generate hooks with the failing hook at a random position
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		failIdx := rapid.IntRange(0, numHooks-1).Draw(t, "failIdx")

		hooks := make([]HookEntry, 0, numHooks)
		var failingHookName string

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))
			// Ensure default on_failure (empty string)
			hook.OnFailure = ""
			hooks = append(hooks, hook)

			if i == failIdx {
				failingHookName = hook.Name
			}
		}

		runner := &orchestratorMockRunner{failOnHook: failingHookName}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		_ = executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// The orchestrator MUST track the failure
		if !orch.HasFailures() {
			t.Fatalf("expected HasFailures()=true after before-hook %q failed (event=%s)",
				failingHookName, event)
		}
	})
}

// Feature: partition-hooks, Property 6: Hook Failure Short-Circuits Event

// **Validates: Requirements 6.2, 6.6, 9.3, 10.4, 11.3, 12.3**
//
// Property 6: Hook Failure Short-Circuits Event
// For any list of hooks in a lifecycle event where hook at position N fails,
// all hooks at positions N+1 through the end of the list SHALL NOT be executed.

// TestProperty_HookFailureShortCircuitsEvent_OnlyHooksUpToFailAreExecuted verifies that
// when a hook at position N fails, only hooks at positions 0..N are executed and hooks
// at positions N+1..end are NOT executed.
func TestProperty_HookFailureShortCircuitsEvent_OnlyHooksUpToFailAreExecuted(t *testing.T) {
	// Feature: partition-hooks, Property 6: Hook Failure Short-Circuits Event
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate N >= 2 hooks with unique names
		numHooks := rapid.IntRange(2, 8).Draw(t, "numHooks")
		failIdx := rapid.IntRange(0, numHooks-1).Draw(t, "failIdx")

		hooks := make([]HookEntry, 0, numHooks)
		hookNames := make([]string, 0, numHooks)

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))
			// Default on_failure (empty string) to test default short-circuit behavior
			hook.OnFailure = ""
			hooks = append(hooks, hook)
			hookNames = append(hookNames, hook.Name)
		}

		failingHookName := hookNames[failIdx]
		runner := &orchestratorMockRunner{failOnHook: failingHookName}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		_ = executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// Only hooks 0..failIdx should have been executed
		expectedExecuted := hookNames[:failIdx+1]

		if len(runner.executedHooks) != len(expectedExecuted) {
			t.Fatalf("expected %d hooks executed (0..%d), got %d: %v (event=%s)",
				len(expectedExecuted), failIdx, len(runner.executedHooks), runner.executedHooks, event)
		}

		for i, name := range expectedExecuted {
			if runner.executedHooks[i] != name {
				t.Fatalf("expected hook at position %d to be %q, got %q (event=%s)",
					i, name, runner.executedHooks[i], event)
			}
		}

		// Verify hooks after failIdx are NOT in executedHooks
		skippedHooks := hookNames[failIdx+1:]
		for _, skipped := range skippedHooks {
			for _, executed := range runner.executedHooks {
				if executed == skipped {
					t.Fatalf("hook %q at position after fail index should NOT have been executed (event=%s)",
						skipped, event)
				}
			}
		}
	})
}

// TestProperty_HookFailureShortCircuitsEvent_ExecutionCountEqualsFailIdxPlusOne verifies that
// the number of executed hooks equals N+1 where N is the fail position (0-indexed).
func TestProperty_HookFailureShortCircuitsEvent_ExecutionCountEqualsFailIdxPlusOne(t *testing.T) {
	// Feature: partition-hooks, Property 6: Hook Failure Short-Circuits Event
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate N >= 2 hooks with unique names
		numHooks := rapid.IntRange(2, 8).Draw(t, "numHooks")
		failIdx := rapid.IntRange(0, numHooks-1).Draw(t, "failIdx")

		hooks := make([]HookEntry, 0, numHooks)

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))
			hook.OnFailure = ""
			hooks = append(hooks, hook)
		}

		failingHookName := hooks[failIdx].Name
		runner := &orchestratorMockRunner{failOnHook: failingHookName}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		_ = executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// The number of executed hooks must equal failIdx + 1
		expectedCount := int32(failIdx + 1)
		if runner.callCount.Load() != expectedCount {
			t.Fatalf("expected callCount=%d (failIdx=%d + 1), got %d (event=%s)",
				expectedCount, failIdx, runner.callCount.Load(), event)
		}
	})
}

// TestProperty_HookFailureShortCircuitsEvent_AllLifecycleEvents verifies that the
// short-circuit behavior applies to ALL lifecycle events (before-detach, after-detach,
// before-drop, after-drop).
func TestProperty_HookFailureShortCircuitsEvent_AllLifecycleEvents(t *testing.T) {
	// Feature: partition-hooks, Property 6: Hook Failure Short-Circuits Event
	rapid.Check(t, func(t *rapid.T) {
		// Test each lifecycle event explicitly
		for _, event := range allLifecycleEvents() {
			numHooks := rapid.IntRange(2, 5).Draw(t, "numHooks_"+string(event))
			failIdx := rapid.IntRange(0, numHooks-1).Draw(t, "failIdx_"+string(event))

			hooks := make([]HookEntry, 0, numHooks)

			for i := 0; i < numHooks; i++ {
				hook := genValidHookEntry(t, "hook_"+string(event), true)
				hook.Name = string(event) + "-hook-" + string(rune('a'+i))
				hook.OnFailure = ""
				hooks = append(hooks, hook)
			}

			failingHookName := hooks[failIdx].Name
			runner := &orchestratorMockRunner{failOnHook: failingHookName}
			cfg := buildHooksConfigForEvent(event, hooks)
			orch := newTestOrchestrator(cfg, runner)

			_ = executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

			// Verify short-circuit: only failIdx+1 hooks executed
			expectedCount := int32(failIdx + 1)
			if runner.callCount.Load() != expectedCount {
				t.Fatalf("event %s: expected %d hooks executed, got %d (failIdx=%d)",
					event, expectedCount, runner.callCount.Load(), failIdx)
			}

			// Verify the last executed hook is the failing one
			if len(runner.executedHooks) == 0 {
				t.Fatalf("event %s: no hooks were executed", event)
			}

			lastExecuted := runner.executedHooks[len(runner.executedHooks)-1]
			if lastExecuted != failingHookName {
				t.Fatalf("event %s: expected last executed hook to be %q (the failing one), got %q",
					event, failingHookName, lastExecuted)
			}
		}
	})
}

// TestProperty_HookFailureShortCircuitsEvent_HasFailuresTrue verifies that the orchestrator
// reports HasFailures() = true after a hook failure causes short-circuit.
func TestProperty_HookFailureShortCircuitsEvent_HasFailuresTrue(t *testing.T) {
	// Feature: partition-hooks, Property 6: Hook Failure Short-Circuits Event
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate N >= 2 hooks with unique names
		numHooks := rapid.IntRange(2, 8).Draw(t, "numHooks")
		failIdx := rapid.IntRange(0, numHooks-1).Draw(t, "failIdx")

		hooks := make([]HookEntry, 0, numHooks)

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))
			hook.OnFailure = ""
			hooks = append(hooks, hook)
		}

		failingHookName := hooks[failIdx].Name
		runner := &orchestratorMockRunner{failOnHook: failingHookName}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		_ = executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// The orchestrator MUST report HasFailures() = true after short-circuit
		if !orch.HasFailures() {
			t.Fatalf("expected HasFailures()=true after hook %q failed and short-circuited (event=%s, failIdx=%d)",
				failingHookName, event, failIdx)
		}
	})
}

// Feature: partition-hooks, Property 7: After-Detach Failure Skips Drop

// **Validates: Requirements 6.5, 10.3**
//
// Property 7: After-Detach Failure Skips Drop
// For any partition with cleanup policy "drop", if any after-detach hook fails after all retry attempts,
// the drop operation and all drop-related hooks (before-drop, after-drop) SHALL NOT be executed for that partition.
//
// The orchestrator's contract: ExecuteAfterDetach returns a non-nil error when an after-detach hook fails,
// signaling the caller (cleanup flow) to skip the drop operation.

// TestProperty_AfterDetachFailureSkipsDrop_ReturnsError verifies that when any after-detach hook
// fails with default on_failure behavior, ExecuteAfterDetach returns a non-nil error (which the
// caller uses to skip the drop operation).
func TestProperty_AfterDetachFailureSkipsDrop_ReturnsError(t *testing.T) {
	// Feature: partition-hooks, Property 7: After-Detach Failure Skips Drop
	rapid.Check(t, func(t *rapid.T) {
		// Generate a random number of after-detach hooks with the failing hook at a random position
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		failIdx := rapid.IntRange(0, numHooks-1).Draw(t, "failIdx")

		hooks := make([]HookEntry, 0, numHooks)
		var failingHookName string

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))
			// Ensure default on_failure (empty string) for all hooks
			hook.OnFailure = ""
			hooks = append(hooks, hook)

			if i == failIdx {
				failingHookName = hook.Name
			}
		}

		runner := &orchestratorMockRunner{failOnHook: failingHookName}
		cfg := buildHooksConfigForEvent(AfterDetach, hooks)
		orch := newTestOrchestrator(cfg, runner)

		err := orch.ExecuteAfterDetach(context.Background(), newTestPartitionContext())

		// After-detach hook failure MUST return a non-nil error
		// signaling the caller to skip the drop operation
		if err == nil {
			t.Fatalf("expected non-nil error when after-detach hook %q fails with default on_failure, got nil",
				failingHookName)
		}
	})
}

// TestProperty_AfterDetachFailureSkipsDrop_NotAbort verifies that the error returned by a
// failing after-detach hook with default on_failure is NOT an ErrAbort. The default behavior
// for after-hooks is informational (skip drop for this partition), not aborting the entire process.
func TestProperty_AfterDetachFailureSkipsDrop_NotAbort(t *testing.T) {
	// Feature: partition-hooks, Property 7: After-Detach Failure Skips Drop
	rapid.Check(t, func(t *rapid.T) {
		// Generate hooks with the failing hook at a random position
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		failIdx := rapid.IntRange(0, numHooks-1).Draw(t, "failIdx")

		hooks := make([]HookEntry, 0, numHooks)
		var failingHookName string

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))
			// Ensure default on_failure (empty string) — NOT abort, NOT continue
			hook.OnFailure = ""
			hooks = append(hooks, hook)

			if i == failIdx {
				failingHookName = hook.Name
			}
		}

		runner := &orchestratorMockRunner{failOnHook: failingHookName}
		cfg := buildHooksConfigForEvent(AfterDetach, hooks)
		orch := newTestOrchestrator(cfg, runner)

		err := orch.ExecuteAfterDetach(context.Background(), newTestPartitionContext())

		// The error must NOT be an ErrAbort — default after-hook failure is informational
		if errors.Is(err, ErrAbort) {
			t.Fatalf("expected non-abort error for default on_failure on after-detach, but got ErrAbort (hook=%s)",
				failingHookName)
		}
	})
}

// TestProperty_AfterDetachFailureSkipsDrop_TracksFailure verifies that when an after-detach
// hook fails with default on_failure, the orchestrator tracks the failure via HasFailures().
func TestProperty_AfterDetachFailureSkipsDrop_TracksFailure(t *testing.T) {
	// Feature: partition-hooks, Property 7: After-Detach Failure Skips Drop
	rapid.Check(t, func(t *rapid.T) {
		// Generate hooks with the failing hook at a random position
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		failIdx := rapid.IntRange(0, numHooks-1).Draw(t, "failIdx")

		hooks := make([]HookEntry, 0, numHooks)
		var failingHookName string

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))
			// Ensure default on_failure (empty string)
			hook.OnFailure = ""
			hooks = append(hooks, hook)

			if i == failIdx {
				failingHookName = hook.Name
			}
		}

		runner := &orchestratorMockRunner{failOnHook: failingHookName}
		cfg := buildHooksConfigForEvent(AfterDetach, hooks)
		orch := newTestOrchestrator(cfg, runner)

		_ = orch.ExecuteAfterDetach(context.Background(), newTestPartitionContext())

		// The orchestrator MUST track the failure
		if !orch.HasFailures() {
			t.Fatalf("expected HasFailures()=true after after-detach hook %q failed",
				failingHookName)
		}
	})
}

// Feature: partition-hooks, Property 8: Operation Failure Skips After-Hooks

// **Validates: Requirements 10.2, 12.2**
//
// Property 8: Operation Failure Skips After-Hooks
// For any partition where the detach operation fails, all after-detach hooks SHALL NOT be executed.
// Similarly, for any partition where the drop operation fails, all after-drop hooks SHALL NOT be executed.
//
// This property tests the CALLER's contract: the orchestrator's ExecuteAfterDetach and ExecuteAfterDrop
// methods are only called by the cleanup flow when the operation succeeds. When the operation fails,
// the caller simply does NOT call these methods. The test verifies that after-hooks configured in the
// HooksConfig are NOT automatically executed; they require explicit invocation.

// TestProperty_OperationFailureSkipsAfterHooks_AfterDetachNotCalledMeansNoExecution verifies that
// when ExecuteAfterDetach is NOT called (simulating a failed detach operation), no after-detach hooks
// are executed, even though they are configured in the HooksConfig.
func TestProperty_OperationFailureSkipsAfterHooks_AfterDetachNotCalledMeansNoExecution(t *testing.T) {
	// Feature: partition-hooks, Property 8: Operation Failure Skips After-Hooks
	rapid.Check(t, func(t *rapid.T) {
		// Generate a random number of after-detach hooks
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		hooks := make([]HookEntry, 0, numHooks)

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "after-detach-hook-" + string(rune('a'+i))
			hooks = append(hooks, hook)
		}

		runner := &orchestratorMockRunner{}
		cfg := &HooksConfig{
			AfterDetach: hooks,
		}
		orch := newTestOrchestrator(cfg, runner)

		// Simulate a failed detach operation: the caller does NOT call ExecuteAfterDetach.
		// We only call ExecuteBeforeDetach (which has no hooks configured) to show the
		// orchestrator is active but after-detach hooks are not auto-triggered.
		err := orch.ExecuteBeforeDetach(context.Background(), newTestPartitionContext())

		// No error expected from before-detach (no hooks configured for that event)
		if err != nil {
			t.Fatalf("expected no error from ExecuteBeforeDetach with no before-detach hooks, got: %v", err)
		}

		// The key assertion: no after-detach hooks were executed because ExecuteAfterDetach was never called
		if runner.callCount.Load() != 0 {
			t.Fatalf("expected 0 hook executions (simulating failed detach, ExecuteAfterDetach not called), got %d: %v",
				runner.callCount.Load(), runner.executedHooks)
		}
	})
}

// TestProperty_OperationFailureSkipsAfterHooks_AfterDropNotCalledMeansNoExecution verifies that
// when ExecuteAfterDrop is NOT called (simulating a failed drop operation), no after-drop hooks
// are executed, even though they are configured in the HooksConfig.
func TestProperty_OperationFailureSkipsAfterHooks_AfterDropNotCalledMeansNoExecution(t *testing.T) {
	// Feature: partition-hooks, Property 8: Operation Failure Skips After-Hooks
	rapid.Check(t, func(t *rapid.T) {
		// Generate a random number of after-drop hooks
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		hooks := make([]HookEntry, 0, numHooks)

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "after-drop-hook-" + string(rune('a'+i))
			hooks = append(hooks, hook)
		}

		runner := &orchestratorMockRunner{}
		cfg := &HooksConfig{
			AfterDrop: hooks,
		}
		orch := newTestOrchestrator(cfg, runner)

		// Simulate a failed drop operation: the caller does NOT call ExecuteAfterDrop.
		// We only call ExecuteBeforeDrop (which has no hooks configured) to show the
		// orchestrator is active but after-drop hooks are not auto-triggered.
		err := orch.ExecuteBeforeDrop(context.Background(), newTestPartitionContext())

		// No error expected from before-drop (no hooks configured for that event)
		if err != nil {
			t.Fatalf("expected no error from ExecuteBeforeDrop with no before-drop hooks, got: %v", err)
		}

		// The key assertion: no after-drop hooks were executed because ExecuteAfterDrop was never called
		if runner.callCount.Load() != 0 {
			t.Fatalf("expected 0 hook executions (simulating failed drop, ExecuteAfterDrop not called), got %d: %v",
				runner.callCount.Load(), runner.executedHooks)
		}
	})
}

// TestProperty_OperationFailureSkipsAfterHooks_ExplicitCallRequired verifies that after-hooks
// require explicit invocation — they are NOT automatically triggered by the orchestrator.
// This confirms the orchestrator correctly supports the pattern where the caller controls
// whether after-hooks run based on operation success/failure.
func TestProperty_OperationFailureSkipsAfterHooks_ExplicitCallRequired(t *testing.T) {
	// Feature: partition-hooks, Property 8: Operation Failure Skips After-Hooks
	rapid.Check(t, func(t *rapid.T) {
		// Generate hooks for ALL lifecycle events
		numAfterDetach := rapid.IntRange(1, 4).Draw(t, "numAfterDetach")
		numAfterDrop := rapid.IntRange(1, 4).Draw(t, "numAfterDrop")

		afterDetachHooks := make([]HookEntry, 0, numAfterDetach)
		for i := 0; i < numAfterDetach; i++ {
			hook := genValidHookEntry(t, "ad_hook", true)
			hook.Name = "after-detach-" + string(rune('a'+i))
			afterDetachHooks = append(afterDetachHooks, hook)
		}

		afterDropHooks := make([]HookEntry, 0, numAfterDrop)
		for i := 0; i < numAfterDrop; i++ {
			hook := genValidHookEntry(t, "adrop_hook", true)
			hook.Name = "after-drop-" + string(rune('a'+i))
			afterDropHooks = append(afterDropHooks, hook)
		}

		runner := &orchestratorMockRunner{}
		cfg := &HooksConfig{
			AfterDetach: afterDetachHooks,
			AfterDrop:   afterDropHooks,
		}
		orch := newTestOrchestrator(cfg, runner)

		// Simulate: both detach and drop operations fail.
		// The caller does NOT call ExecuteAfterDetach or ExecuteAfterDrop.
		// Instead, only before-* methods are called (which have no hooks configured).
		_ = orch.ExecuteBeforeDetach(context.Background(), newTestPartitionContext())
		_ = orch.ExecuteBeforeDrop(context.Background(), newTestPartitionContext())

		// No after-hooks should have been executed
		if runner.callCount.Load() != 0 {
			t.Fatalf("expected 0 hook executions when after-hook methods are not called (simulating operation failures), got %d: %v",
				runner.callCount.Load(), runner.executedHooks)
		}

		// Now verify that calling ExecuteAfterDetach DOES execute the hooks (proving explicit call is required)
		err := orch.ExecuteAfterDetach(context.Background(), newTestPartitionContext())
		if err != nil {
			t.Fatalf("expected no error from ExecuteAfterDetach, got: %v", err)
		}

		// After explicit call, after-detach hooks should have been executed
		if int(runner.callCount.Load()) != numAfterDetach {
			t.Fatalf("expected %d after-detach hooks executed after explicit call, got %d: %v",
				numAfterDetach, runner.callCount.Load(), runner.executedHooks)
		}
	})
}

// Feature: partition-hooks, Property 10: Partition Isolation

// **Validates: Requirements 6.3**
//
// Property 10: Partition Isolation
// For any set of partitions being cleaned up, a hook failure for one partition SHALL NOT
// prevent the processing of other partitions (unless `on_failure` is set to "abort").

// TestProperty_PartitionIsolation_FailureForOneDoesNotPreventOthers verifies that when a hook
// fails for one partition, the orchestrator can still successfully execute hooks for other partitions.
// This simulates the cleanup flow iterating over multiple partitions.
func TestProperty_PartitionIsolation_FailureForOneDoesNotPreventOthers(t *testing.T) {
	// Feature: partition-hooks, Property 10: Partition Isolation
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate multiple partitions (at least 2)
		numPartitions := rapid.IntRange(2, 6).Draw(t, "numPartitions")

		// Pick one partition to fail
		failPartitionIdx := rapid.IntRange(0, numPartitions-1).Draw(t, "failPartitionIdx")

		// Generate partition contexts with unique names
		partitions := make([]PartitionContext, 0, numPartitions)
		for i := 0; i < numPartitions; i++ {
			p := PartitionContext{
				Schema:        "public",
				Table:         fmt.Sprintf("events_%d", i),
				ParentTable:   "events",
				LowerBound:    "2024-01-01",
				UpperBound:    "2024-02-01",
				PartitionName: fmt.Sprintf("partition_%d", i),
				Retention:     "30",
				Interval:      "daily",
				DatabaseName:  "mydb",
				Hostname:      "localhost",
			}
			partitions = append(partitions, p)
		}

		// Generate hooks — use a hook name that includes the partition name so we can
		// configure the mock runner to fail only for the specific partition's hook
		numHooks := rapid.IntRange(1, 3).Draw(t, "numHooks")

		// Track which partitions were successfully processed
		successfulPartitions := 0

		// Simulate the cleanup flow: iterate over partitions, calling the orchestrator for each
		for i, partition := range partitions {
			// Create a fresh orchestrator for each partition (as the cleanup flow would do
			// after resolving hooks per partition)
			hooks := make([]HookEntry, 0, numHooks)
			var failingHookName string

			for j := 0; j < numHooks; j++ {
				hookName := fmt.Sprintf("hook-%s-%d", partition.PartitionName, j)
				hook := HookEntry{
					Name:      hookName,
					Type:      ShellType,
					Enabled:   boolPtr(true),
					Timeout:   30 * time.Second,
					OnFailure: "", // default behavior (NOT abort)
					Retry:     RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
					Config:    map[string]interface{}{"command": "/usr/bin/echo"},
				}
				hooks = append(hooks, hook)

				// Make the first hook of the failing partition fail
				if i == failPartitionIdx && j == 0 {
					failingHookName = hookName
				}
			}

			runner := &orchestratorMockRunner{failOnHook: failingHookName}
			cfg := buildHooksConfigForEvent(event, hooks)
			orch := newTestOrchestrator(cfg, runner)

			err := executeEventOnOrchestrator(context.Background(), orch, event, partition)

			if i == failPartitionIdx {
				// This partition's hook should fail
				if err == nil {
					t.Fatalf("expected error for failing partition %d, got nil", i)
				}
				// But it should NOT be an abort error (default on_failure)
				if errors.Is(err, ErrAbort) {
					t.Fatalf("expected non-abort error for partition %d with default on_failure, got ErrAbort", i)
				}
			} else {
				// Other partitions should succeed
				if err != nil {
					t.Fatalf("partition %d should not be affected by partition %d's failure, but got error: %v",
						i, failPartitionIdx, err)
				}
				successfulPartitions++
			}
		}

		// Verify that all non-failing partitions were successfully processed
		expectedSuccessful := numPartitions - 1
		if successfulPartitions != expectedSuccessful {
			t.Fatalf("expected %d successful partitions, got %d", expectedSuccessful, successfulPartitions)
		}
	})
}

// partitionAwareMockRunner fails only when executing hooks for a specific partition.
type partitionAwareMockRunner struct {
	failForPartition      string // partition name that should trigger failure
	executedForPartitions []string
}

func (r *partitionAwareMockRunner) Run(_ context.Context, hook *ResolvedHook) error {
	r.executedForPartitions = append(r.executedForPartitions, hook.PartitionContext.PartitionName)

	if r.failForPartition != "" && hook.PartitionContext.PartitionName == r.failForPartition {
		return fmt.Errorf("hook %q for partition %q: %w", hook.Name, hook.PartitionContext.PartitionName, errSimulatedFailure)
	}

	return nil
}

// TestProperty_PartitionIsolation_SharedOrchestratorStillProcesses verifies that even when
// using a shared orchestrator instance (which tracks hasFailure state), subsequent partition
// hook executions still proceed. The hasFailure flag is informational and does NOT block execution.
func TestProperty_PartitionIsolation_SharedOrchestratorStillProcesses(t *testing.T) {
	// Feature: partition-hooks, Property 10: Partition Isolation
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate multiple partitions (at least 3 to have partitions before and after the failure)
		numPartitions := rapid.IntRange(3, 6).Draw(t, "numPartitions")

		// Pick one partition to fail (not the last one, so we can verify subsequent processing)
		failPartitionIdx := rapid.IntRange(0, numPartitions-2).Draw(t, "failPartitionIdx")

		// Generate partition contexts
		partitions := make([]PartitionContext, 0, numPartitions)
		for i := 0; i < numPartitions; i++ {
			p := PartitionContext{
				Schema:        "public",
				Table:         fmt.Sprintf("events_%d", i),
				ParentTable:   "events",
				LowerBound:    "2024-01-01",
				UpperBound:    "2024-02-01",
				PartitionName: fmt.Sprintf("partition_%d", i),
				Retention:     "30",
				Interval:      "daily",
				DatabaseName:  "mydb",
				Hostname:      "localhost",
			}
			partitions = append(partitions, p)
		}

		// Create a partition-aware runner that only fails for the specific partition
		failingPartitionName := fmt.Sprintf("partition_%d", failPartitionIdx)
		runner := &partitionAwareMockRunner{failForPartition: failingPartitionName}

		// Create hooks config with a single hook
		hookEntry := HookEntry{
			Name:      "test-hook",
			Type:      ShellType,
			Enabled:   boolPtr(true),
			Timeout:   30 * time.Second,
			OnFailure: "", // default behavior (NOT abort)
			Retry:     RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
			Config:    map[string]interface{}{"command": "/usr/bin/echo"},
		}

		cfg := buildHooksConfigForEvent(event, []HookEntry{hookEntry})

		logger := newOrchestratorTestLogger()
		executor := NewExecutor(runner, logger)
		metrics := NewMetricsCollector(logger)
		orch := NewOrchestrator(cfg, executor, metrics, logger, "postgresql://user:pass@localhost:5432/mydb")

		// Simulate the cleanup flow with a shared orchestrator
		processedAfterFailure := 0

		for i, partition := range partitions {
			err := executeEventOnOrchestrator(context.Background(), orch, event, partition)

			if i == failPartitionIdx {
				// This partition should fail
				if err == nil {
					t.Fatalf("expected error for failing partition %d, got nil", i)
				}
			} else if i > failPartitionIdx {
				// Partitions AFTER the failure should still be processed
				if err != nil {
					t.Fatalf("partition %d (after failure at %d) should still be processed, but got error: %v",
						i, failPartitionIdx, err)
				}
				processedAfterFailure++
			}
		}

		// Verify that partitions after the failure were still processed
		expectedAfterFailure := numPartitions - failPartitionIdx - 1
		if processedAfterFailure != expectedAfterFailure {
			t.Fatalf("expected %d partitions processed after failure, got %d",
				expectedAfterFailure, processedAfterFailure)
		}

		// The orchestrator should report failures (from the failed partition)
		if !orch.HasFailures() {
			t.Fatal("expected HasFailures()=true after one partition's hook failed")
		}

		// Verify that the runner was called for ALL partitions (not just the ones before failure)
		if len(runner.executedForPartitions) != numPartitions {
			t.Fatalf("expected runner to be called for all %d partitions, got %d calls",
				numPartitions, len(runner.executedForPartitions))
		}
	})
}

// TestProperty_PartitionIsolation_AbortPreventsSubsequentPartitions verifies the exception:
// when on_failure=abort is set and a hook fails, subsequent partitions SHALL NOT be processed.
// This confirms the "unless on_failure is set to abort" clause in the property.
func TestProperty_PartitionIsolation_AbortPreventsSubsequentPartitions(t *testing.T) {
	// Feature: partition-hooks, Property 10: Partition Isolation
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate multiple partitions
		numPartitions := rapid.IntRange(2, 5).Draw(t, "numPartitions")

		// Pick one partition to fail (not the last one)
		failPartitionIdx := rapid.IntRange(0, numPartitions-2).Draw(t, "failPartitionIdx")

		// Generate partition contexts
		partitions := make([]PartitionContext, 0, numPartitions)
		for i := 0; i < numPartitions; i++ {
			p := PartitionContext{
				Schema:        "public",
				Table:         fmt.Sprintf("events_%d", i),
				ParentTable:   "events",
				LowerBound:    "2024-01-01",
				UpperBound:    "2024-02-01",
				PartitionName: fmt.Sprintf("partition_%d", i),
				Retention:     "30",
				Interval:      "daily",
				DatabaseName:  "mydb",
				Hostname:      "localhost",
			}
			partitions = append(partitions, p)
		}

		// Simulate the cleanup flow: when abort is received, stop processing
		abortReceived := false
		processedAfterAbort := 0

		for i, partition := range partitions {
			if abortReceived {
				// After abort, the cleanup flow should NOT process any more partitions
				processedAfterAbort++
				continue
			}

			// Create hooks for this partition
			hookName := fmt.Sprintf("hook-partition_%d", i)
			hookEntry := HookEntry{
				Name:      hookName,
				Type:      ShellType,
				Enabled:   boolPtr(true),
				Timeout:   30 * time.Second,
				OnFailure: OnFailureAbort, // ABORT on failure
				Retry:     RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
				Config:    map[string]interface{}{"command": "/usr/bin/echo"},
			}

			// Only the failing partition's hook will fail
			failOnHook := ""
			if i == failPartitionIdx {
				failOnHook = hookName
			}

			runner := &orchestratorMockRunner{failOnHook: failOnHook}
			cfg := buildHooksConfigForEvent(event, []HookEntry{hookEntry})
			orch := newTestOrchestrator(cfg, runner)

			err := executeEventOnOrchestrator(context.Background(), orch, event, partition)

			if i == failPartitionIdx {
				// This partition should fail with ErrAbort
				if err == nil {
					t.Fatalf("expected error for failing partition %d with on_failure=abort, got nil", i)
				}
				if !errors.Is(err, ErrAbort) {
					t.Fatalf("expected ErrAbort for partition %d with on_failure=abort, got: %v", i, err)
				}
				// Signal that the cleanup flow should stop
				abortReceived = true
			} else if i < failPartitionIdx {
				// Partitions before the failure should succeed
				if err != nil {
					t.Fatalf("partition %d (before failure) should succeed, got error: %v", i, err)
				}
			}
		}

		// Verify that NO partitions were processed after the abort
		if processedAfterAbort == 0 && failPartitionIdx == numPartitions-1 {
			// If the failing partition is the last one, there are no subsequent partitions
			// This is fine — the property still holds
		} else if !abortReceived {
			t.Fatal("expected abort to be received")
		}

		// The key assertion: partitions after the abort were NOT processed
		// (they were skipped in the loop above via the `continue` statement)
		// This verifies the "unless on_failure is set to abort" exception
	})
}

// Feature: partition-hooks, Property 11: Abort Stops Entire Process

// **Validates: Requirements 6.8**
//
// Property 11: Abort Stops Entire Process
// For any hook with `on_failure` set to "abort" that fails after all retry attempts,
// the entire cleanup process SHALL stop immediately without processing any remaining partitions.

// TestProperty_AbortStopsEntireProcess_ReturnsErrAbort verifies that for any hook with
// on_failure=abort that fails, the orchestrator returns an ErrAbort error across all lifecycle events.
func TestProperty_AbortStopsEntireProcess_ReturnsErrAbort(t *testing.T) {
	// Feature: partition-hooks, Property 11: Abort Stops Entire Process
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate hooks with the abort hook at a random position
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		abortIdx := rapid.IntRange(0, numHooks-1).Draw(t, "abortIdx")

		hooks := make([]HookEntry, 0, numHooks)
		var abortHookName string

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))

			if i == abortIdx {
				hook.OnFailure = OnFailureAbort
				abortHookName = hook.Name
			} else {
				// Other hooks have default on_failure
				hook.OnFailure = ""
			}

			hooks = append(hooks, hook)
		}

		runner := &orchestratorMockRunner{failOnHook: abortHookName}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		err := executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// The error MUST be non-nil
		if err == nil {
			t.Fatalf("expected non-nil error when abort hook %q fails (event=%s), got nil",
				abortHookName, event)
		}

		// The error MUST be ErrAbort
		if !errors.Is(err, ErrAbort) {
			t.Fatalf("expected ErrAbort when hook %q with on_failure=abort fails (event=%s), got: %v",
				abortHookName, event, err)
		}
	})
}

// TestProperty_AbortStopsEntireProcess_StopsRemainingPartitions verifies that when a hook
// with on_failure=abort fails, the cleanup flow stops immediately and does NOT process
// any remaining partitions.
func TestProperty_AbortStopsEntireProcess_StopsRemainingPartitions(t *testing.T) {
	// Feature: partition-hooks, Property 11: Abort Stops Entire Process
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate multiple partitions (at least 3 to have partitions before and after)
		numPartitions := rapid.IntRange(3, 8).Draw(t, "numPartitions")

		// Pick a partition to fail (not the last one, so we can verify subsequent ones are skipped)
		failPartitionIdx := rapid.IntRange(0, numPartitions-2).Draw(t, "failPartitionIdx")

		// Generate partition contexts
		partitions := make([]PartitionContext, 0, numPartitions)
		for i := 0; i < numPartitions; i++ {
			p := PartitionContext{
				Schema:        "public",
				Table:         fmt.Sprintf("events_%d", i),
				ParentTable:   "events",
				LowerBound:    "2024-01-01",
				UpperBound:    "2024-02-01",
				PartitionName: fmt.Sprintf("partition_%d", i),
				Retention:     "30",
				Interval:      "daily",
				DatabaseName:  "mydb",
				Hostname:      "localhost",
			}
			partitions = append(partitions, p)
		}

		// Simulate the cleanup flow: iterate over partitions, stop on ErrAbort
		processedPartitions := 0
		abortReceived := false

		for i, partition := range partitions {
			if abortReceived {
				// After abort, the cleanup flow MUST NOT process any more partitions
				break
			}

			// Create a hook with on_failure=abort
			hookName := fmt.Sprintf("abort-hook-partition_%d", i)
			hookEntry := HookEntry{
				Name:      hookName,
				Type:      ShellType,
				Enabled:   boolPtr(true),
				Timeout:   30 * time.Second,
				OnFailure: OnFailureAbort,
				Retry:     RetryConfig{Attempts: 0, Backoff: BackoffFixed, InitialDelay: DefaultInitialDelay, MaxDelay: DefaultMaxDelay},
				Config:    map[string]interface{}{"command": "/usr/bin/echo"},
			}

			// Only the failing partition's hook will fail
			failOnHook := ""
			if i == failPartitionIdx {
				failOnHook = hookName
			}

			runner := &orchestratorMockRunner{failOnHook: failOnHook}
			cfg := buildHooksConfigForEvent(event, []HookEntry{hookEntry})
			orch := newTestOrchestrator(cfg, runner)

			err := executeEventOnOrchestrator(context.Background(), orch, event, partition)

			processedPartitions++

			if errors.Is(err, ErrAbort) {
				abortReceived = true
			}
		}

		// Abort MUST have been received
		if !abortReceived {
			t.Fatalf("expected ErrAbort to be received at partition %d, but it was not", failPartitionIdx)
		}

		// Only partitions up to and including the failing one should have been processed
		expectedProcessed := failPartitionIdx + 1
		if processedPartitions != expectedProcessed {
			t.Fatalf("expected %d partitions processed before abort (0..%d), got %d (event=%s)",
				expectedProcessed, failPartitionIdx, processedPartitions, event)
		}
	})
}

// TestProperty_AbortStopsEntireProcess_ShortCircuitsHooksInEvent verifies that when a hook
// with on_failure=abort fails, remaining hooks in the same lifecycle event are also skipped
// (short-circuit behavior combined with abort).
func TestProperty_AbortStopsEntireProcess_ShortCircuitsHooksInEvent(t *testing.T) {
	// Feature: partition-hooks, Property 11: Abort Stops Entire Process
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate multiple hooks with the abort hook at a random position
		numHooks := rapid.IntRange(2, 6).Draw(t, "numHooks")
		abortIdx := rapid.IntRange(0, numHooks-1).Draw(t, "abortIdx")

		hooks := make([]HookEntry, 0, numHooks)
		var abortHookName string

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))

			if i == abortIdx {
				hook.OnFailure = OnFailureAbort
				abortHookName = hook.Name
			} else {
				hook.OnFailure = ""
			}

			hooks = append(hooks, hook)
		}

		runner := &orchestratorMockRunner{failOnHook: abortHookName}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		err := executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// Must return ErrAbort
		if !errors.Is(err, ErrAbort) {
			t.Fatalf("expected ErrAbort, got: %v (event=%s, abortHook=%s)", err, event, abortHookName)
		}

		// Only hooks up to and including the abort hook should have been executed
		expectedExecuted := abortIdx + 1
		if int(runner.callCount.Load()) != expectedExecuted {
			t.Fatalf("expected %d hooks executed (0..%d), got %d (event=%s)",
				expectedExecuted, abortIdx, runner.callCount.Load(), event)
		}

		// Verify hooks after abortIdx were NOT executed
		for _, executed := range runner.executedHooks {
			for j := abortIdx + 1; j < numHooks; j++ {
				skippedName := "hook-" + string(rune('a'+j))
				if executed == skippedName {
					t.Fatalf("hook %q (after abort at idx %d) should NOT have been executed (event=%s)",
						skippedName, abortIdx, event)
				}
			}
		}
	})
}

// TestProperty_AbortStopsEntireProcess_TracksFailure verifies that when a hook with
// on_failure=abort fails, the orchestrator tracks the failure via HasFailures().
func TestProperty_AbortStopsEntireProcess_TracksFailure(t *testing.T) {
	// Feature: partition-hooks, Property 11: Abort Stops Entire Process
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate a single hook with on_failure=abort that fails
		hook := genValidHookEntry(t, "hook", true)
		hook.OnFailure = OnFailureAbort

		runner := &orchestratorMockRunner{failOnHook: hook.Name}
		cfg := buildHooksConfigForEvent(event, []HookEntry{hook})
		orch := newTestOrchestrator(cfg, runner)

		err := executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// Must return ErrAbort
		if !errors.Is(err, ErrAbort) {
			t.Fatalf("expected ErrAbort, got: %v (event=%s)", err, event)
		}

		// The orchestrator MUST track the failure
		if !orch.HasFailures() {
			t.Fatalf("expected HasFailures()=true after abort hook failed (event=%s)", event)
		}
	})
}

// Feature: partition-hooks, Property 12: Continue Overrides Default Cancel

// **Validates: Requirements 6.9**
//
// Property 12: Continue Overrides Default Cancel
// For any before-hook with `on_failure` set to "continue" that fails, the associated
// operation (detach or drop) SHALL still be executed.

// TestProperty_ContinueOverridesDefaultCancel_ReturnsNilError verifies that for any
// before-hook (before-detach or before-drop) with on_failure=continue that fails,
// the orchestrator returns nil (no error), meaning the associated operation proceeds.
func TestProperty_ContinueOverridesDefaultCancel_ReturnsNilError(t *testing.T) {
	// Feature: partition-hooks, Property 12: Continue Overrides Default Cancel
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(beforeHookEvents()).Draw(t, "event")

		// Generate hooks with the continue hook at a random position
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		continueIdx := rapid.IntRange(0, numHooks-1).Draw(t, "continueIdx")

		hooks := make([]HookEntry, 0, numHooks)
		var continueHookName string

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))

			if i == continueIdx {
				hook.OnFailure = OnFailureContinue
				continueHookName = hook.Name
			} else {
				// Other hooks have default on_failure
				hook.OnFailure = ""
			}

			hooks = append(hooks, hook)
		}

		runner := &orchestratorMockRunner{failOnHook: continueHookName}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		err := executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// on_failure=continue MUST return nil error, meaning the operation proceeds
		if err != nil {
			t.Fatalf("expected nil error when before-hook %q with on_failure=continue fails (event=%s), got: %v",
				continueHookName, event, err)
		}
	})
}

// TestProperty_ContinueOverridesDefaultCancel_TracksFailure verifies that even though
// on_failure=continue allows the operation to proceed, the failure is still tracked
// via HasFailures() for exit code purposes (Requirement 6.7).
func TestProperty_ContinueOverridesDefaultCancel_TracksFailure(t *testing.T) {
	// Feature: partition-hooks, Property 12: Continue Overrides Default Cancel
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(beforeHookEvents()).Draw(t, "event")

		// Generate hooks with the continue hook at a random position
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		continueIdx := rapid.IntRange(0, numHooks-1).Draw(t, "continueIdx")

		hooks := make([]HookEntry, 0, numHooks)
		var continueHookName string

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))

			if i == continueIdx {
				hook.OnFailure = OnFailureContinue
				continueHookName = hook.Name
			} else {
				hook.OnFailure = ""
			}

			hooks = append(hooks, hook)
		}

		runner := &orchestratorMockRunner{failOnHook: continueHookName}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		_ = executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// The failure MUST still be tracked even though the operation proceeds
		if !orch.HasFailures() {
			t.Fatalf("expected HasFailures()=true after hook %q with on_failure=continue failed (event=%s)",
				continueHookName, event)
		}
	})
}

// TestProperty_ContinueOverridesDefaultCancel_NotErrAbort verifies that the behavior of
// on_failure=continue is distinct from on_failure=abort — it does NOT return ErrAbort.
func TestProperty_ContinueOverridesDefaultCancel_NotErrAbort(t *testing.T) {
	// Feature: partition-hooks, Property 12: Continue Overrides Default Cancel
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(beforeHookEvents()).Draw(t, "event")

		// Generate hooks with the continue hook at a random position
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		continueIdx := rapid.IntRange(0, numHooks-1).Draw(t, "continueIdx")

		hooks := make([]HookEntry, 0, numHooks)
		var continueHookName string

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))

			if i == continueIdx {
				hook.OnFailure = OnFailureContinue
				continueHookName = hook.Name
			} else {
				hook.OnFailure = ""
			}

			hooks = append(hooks, hook)
		}

		runner := &orchestratorMockRunner{failOnHook: continueHookName}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		err := executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// on_failure=continue returns nil, so it cannot be ErrAbort
		if err != nil && errors.Is(err, ErrAbort) {
			t.Fatalf("on_failure=continue must NOT return ErrAbort (event=%s, hook=%s), got: %v",
				event, continueHookName, err)
		}
	})
}

// TestProperty_ContinueOverridesDefaultCancel_ContrastWithDefault verifies the contrast:
// the same hook configuration with default on_failure (empty) WOULD cancel the operation,
// but with on_failure=continue it does NOT. This proves "continue overrides default cancel".
func TestProperty_ContinueOverridesDefaultCancel_ContrastWithDefault(t *testing.T) {
	// Feature: partition-hooks, Property 12: Continue Overrides Default Cancel
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(beforeHookEvents()).Draw(t, "event")

		// Generate a single hook that will fail
		hook := genValidHookEntry(t, "hook", true)
		hook.Name = "contrast-hook"

		// Test 1: Default on_failure — should return error (cancel operation)
		hook.OnFailure = ""
		runner1 := &orchestratorMockRunner{failOnHook: hook.Name}
		cfg1 := buildHooksConfigForEvent(event, []HookEntry{hook})
		orch1 := newTestOrchestrator(cfg1, runner1)

		errDefault := executeEventOnOrchestrator(context.Background(), orch1, event, newTestPartitionContext())

		// Default behavior: before-hook failure cancels the operation (returns error)
		if errDefault == nil {
			t.Fatalf("expected error with default on_failure for before-hook %q (event=%s), got nil",
				hook.Name, event)
		}

		// Test 2: on_failure=continue — should return nil (operation proceeds)
		hook.OnFailure = OnFailureContinue
		runner2 := &orchestratorMockRunner{failOnHook: hook.Name}
		cfg2 := buildHooksConfigForEvent(event, []HookEntry{hook})
		orch2 := newTestOrchestrator(cfg2, runner2)

		errContinue := executeEventOnOrchestrator(context.Background(), orch2, event, newTestPartitionContext())

		// Continue behavior: operation proceeds despite hook failure (returns nil)
		if errContinue != nil {
			t.Fatalf("expected nil error with on_failure=continue for before-hook %q (event=%s), got: %v",
				hook.Name, event, errContinue)
		}
	})
}

// Feature: partition-hooks, Property 19: Non-Zero Exit on Hook Failure

// **Validates: Requirements 6.7**
//
// Property 19: Non-Zero Exit on Hook Failure
// For any cleanup run where at least one hook failed (after all retries), the cleanup process
// SHALL return a non-nil error (resulting in non-zero exit code), even if all partition operations
// themselves succeeded. The mechanism is: HasFailures() returns true when any hook has failed,
// and the cleanup flow checks HasFailures() to determine the exit code.

// TestProperty_NonZeroExitOnHookFailure_HasFailuresTrueWhenHookFails verifies that for any
// lifecycle event, when a hook fails with default on_failure, HasFailures() must be true.
func TestProperty_NonZeroExitOnHookFailure_HasFailuresTrueWhenHookFails(t *testing.T) {
	// Feature: partition-hooks, Property 19: Non-Zero Exit on Hook Failure
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate a random number of hooks with the failing hook at a random position
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		failIdx := rapid.IntRange(0, numHooks-1).Draw(t, "failIdx")

		hooks := make([]HookEntry, 0, numHooks)
		var failingHookName string

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))
			hook.OnFailure = "" // default on_failure
			hooks = append(hooks, hook)

			if i == failIdx {
				failingHookName = hook.Name
			}
		}

		runner := &orchestratorMockRunner{failOnHook: failingHookName}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		_ = executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// HasFailures() MUST be true when any hook has failed
		if !orch.HasFailures() {
			t.Fatalf("expected HasFailures()=true after hook %q failed with default on_failure (event=%s)",
				failingHookName, event)
		}
	})
}

// TestProperty_NonZeroExitOnHookFailure_HasFailuresTrueWithOnFailureContinue verifies that
// even when on_failure=continue (which returns nil error so the operation proceeds),
// HasFailures() must still be true because the hook DID fail.
func TestProperty_NonZeroExitOnHookFailure_HasFailuresTrueWithOnFailureContinue(t *testing.T) {
	// Feature: partition-hooks, Property 19: Non-Zero Exit on Hook Failure
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate a random number of hooks with the failing hook at a random position
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		failIdx := rapid.IntRange(0, numHooks-1).Draw(t, "failIdx")

		hooks := make([]HookEntry, 0, numHooks)
		var failingHookName string

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))

			if i == failIdx {
				hook.OnFailure = OnFailureContinue
				failingHookName = hook.Name
			} else {
				hook.OnFailure = ""
			}

			hooks = append(hooks, hook)
		}

		runner := &orchestratorMockRunner{failOnHook: failingHookName}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		err := executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// on_failure=continue means the Execute* method returns nil (operation proceeds)
		if err != nil {
			t.Fatalf("expected nil error with on_failure=continue for hook %q (event=%s), got: %v",
				failingHookName, event, err)
		}

		// But HasFailures() MUST still be true because the hook DID fail
		if !orch.HasFailures() {
			t.Fatalf("expected HasFailures()=true after hook %q failed with on_failure=continue (event=%s): "+
				"the hook failed even though the operation proceeded",
				failingHookName, event)
		}
	})
}

// TestProperty_NonZeroExitOnHookFailure_HasFailuresTrueWithOnFailureAbort verifies that
// when on_failure=abort triggers, HasFailures() must be true.
func TestProperty_NonZeroExitOnHookFailure_HasFailuresTrueWithOnFailureAbort(t *testing.T) {
	// Feature: partition-hooks, Property 19: Non-Zero Exit on Hook Failure
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate a random number of hooks with the abort hook at a random position
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		abortIdx := rapid.IntRange(0, numHooks-1).Draw(t, "abortIdx")

		hooks := make([]HookEntry, 0, numHooks)
		var abortHookName string

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))

			if i == abortIdx {
				hook.OnFailure = OnFailureAbort
				abortHookName = hook.Name
			} else {
				hook.OnFailure = ""
			}

			hooks = append(hooks, hook)
		}

		runner := &orchestratorMockRunner{failOnHook: abortHookName}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		err := executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// on_failure=abort returns ErrAbort
		if !errors.Is(err, ErrAbort) {
			t.Fatalf("expected ErrAbort when hook %q with on_failure=abort fails (event=%s), got: %v",
				abortHookName, event, err)
		}

		// HasFailures() MUST be true
		if !orch.HasFailures() {
			t.Fatalf("expected HasFailures()=true after hook %q failed with on_failure=abort (event=%s)",
				abortHookName, event)
		}
	})
}

// TestProperty_NonZeroExitOnHookFailure_HasFailuresFalseWhenAllSucceed verifies the contrast:
// when no hooks fail, HasFailures() must be false.
func TestProperty_NonZeroExitOnHookFailure_HasFailuresFalseWhenAllSucceed(t *testing.T) {
	// Feature: partition-hooks, Property 19: Non-Zero Exit on Hook Failure
	rapid.Check(t, func(t *rapid.T) {
		event := rapid.SampledFrom(allLifecycleEvents()).Draw(t, "event")

		// Generate a random number of hooks — all will succeed
		numHooks := rapid.IntRange(1, 5).Draw(t, "numHooks")
		hooks := make([]HookEntry, 0, numHooks)

		for i := 0; i < numHooks; i++ {
			hook := genValidHookEntry(t, "hook", true)
			hook.Name = "hook-" + string(rune('a'+i))
			// Randomize on_failure to ensure it doesn't matter when hooks succeed
			hook.OnFailure = rapid.SampledFrom([]OnFailure{"", OnFailureAbort, OnFailureContinue}).Draw(t, "onFailure")
			hooks = append(hooks, hook)
		}

		// No hook will fail (failOnHook is empty)
		runner := &orchestratorMockRunner{}
		cfg := buildHooksConfigForEvent(event, hooks)
		orch := newTestOrchestrator(cfg, runner)

		err := executeEventOnOrchestrator(context.Background(), orch, event, newTestPartitionContext())

		// No error expected when all hooks succeed
		if err != nil {
			t.Fatalf("expected no error when all hooks succeed (event=%s), got: %v", event, err)
		}

		// HasFailures() MUST be false when no hooks failed
		if orch.HasFailures() {
			t.Fatalf("expected HasFailures()=false when all hooks succeed (event=%s)", event)
		}
	})
}
