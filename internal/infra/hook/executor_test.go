package hook

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockRunner is a configurable test runner for executor unit tests.
type mockRunner struct {
	callCount atomic.Int32
	// delay simulates how long the hook takes to execute.
	delay time.Duration
	// failUntilAttempt: fail on attempts < this value, succeed on >= this value.
	// 0 means always fail.
	failUntilAttempt int
}

func (r *mockRunner) Run(ctx context.Context, _ *ResolvedHook) error {
	current := int(r.callCount.Add(1))

	if r.delay > 0 {
		select {
		case <-ctx.Done():
			return fmt.Errorf("hook execution timed out: %w", ctx.Err())
		case <-time.After(r.delay):
		}
	}

	if r.failUntilAttempt > 0 && current >= r.failUntilAttempt {
		return nil
	}

	return fmt.Errorf("simulated failure on attempt %d", current)
}

func newTestLogger() slog.Logger {
	return *slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func newTestHook(name string) *ResolvedHook {
	return &ResolvedHook{
		Name: name,
		Type: ShellType,
		PartitionContext: PartitionContext{
			PartitionName: "test-partition",
		},
	}
}

// --- Timeout Enforcement Tests ---
// Validates: Requirements 13.1, 13.2

func TestExecutor_TimeoutEnforcement_HookExceedsTimeout(t *testing.T) {
	t.Parallel()

	// Runner takes 500ms but timeout is 50ms
	runner := &mockRunner{delay: 500 * time.Millisecond, failUntilAttempt: 0}
	logger := newTestLogger()
	executor := NewExecutor(runner, logger)

	entry := HookEntry{
		Name:    "slow-hook",
		Type:    ShellType,
		Timeout: 50 * time.Millisecond,
		Retry: RetryConfig{
			Attempts:     0,
			Backoff:      BackoffFixed,
			InitialDelay: 1 * time.Millisecond,
			MaxDelay:     10 * time.Millisecond,
		},
	}

	hook := newTestHook("slow-hook")

	err := executor.Execute(context.Background(), hook, entry)

	// Requirement 13.1: hook exceeding timeout SHALL be terminated and treated as failed
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}

func TestExecutor_TimeoutEnforcement_HookCompletesWithinTimeout(t *testing.T) {
	t.Parallel()

	// Runner takes 10ms and timeout is 1s - should succeed
	runner := &mockRunner{delay: 10 * time.Millisecond, failUntilAttempt: 1}
	logger := newTestLogger()
	executor := NewExecutor(runner, logger)

	entry := HookEntry{
		Name:    "fast-hook",
		Type:    ShellType,
		Timeout: 1 * time.Second,
		Retry: RetryConfig{
			Attempts:     0,
			Backoff:      BackoffFixed,
			InitialDelay: 1 * time.Millisecond,
			MaxDelay:     10 * time.Millisecond,
		},
	}

	hook := newTestHook("fast-hook")

	err := executor.Execute(context.Background(), hook, entry)
	require.NoError(t, err)
}

func TestExecutor_TimeoutEnforcement_TimeoutOnRetry(t *testing.T) {
	t.Parallel()

	// Runner takes 200ms, timeout is 50ms, with 2 retries
	// All attempts should time out
	runner := &mockRunner{delay: 200 * time.Millisecond, failUntilAttempt: 0}
	logger := newTestLogger()
	executor := NewExecutor(runner, logger)

	entry := HookEntry{
		Name:    "timeout-retry-hook",
		Type:    ShellType,
		Timeout: 50 * time.Millisecond,
		Retry: RetryConfig{
			Attempts:     2,
			Backoff:      BackoffFixed,
			InitialDelay: 1 * time.Millisecond,
			MaxDelay:     10 * time.Millisecond,
		},
	}

	hook := newTestHook("timeout-retry-hook")

	err := executor.Execute(context.Background(), hook, entry)

	require.Error(t, err)
	// Should have attempted 3 times (1 initial + 2 retries)
	assert.Equal(t, int32(3), runner.callCount.Load())
}

// --- Fixed Backoff Delay Tests ---
// Validates: Requirement 14.2

func TestExecutor_FixedBackoff_DelayIsConstant(t *testing.T) {
	t.Parallel()

	initialDelay := 50 * time.Millisecond

	// Verify calculateBackoff returns initial_delay for all attempts with fixed strategy
	retry := RetryConfig{
		Attempts:     5,
		Backoff:      BackoffFixed,
		InitialDelay: initialDelay,
		MaxDelay:     1 * time.Second,
	}

	for attempt := 1; attempt <= 5; attempt++ {
		delay := calculateBackoff(retry, attempt)
		assert.Equal(t, initialDelay, delay, "Fixed backoff should return initial_delay for attempt %d", attempt)
	}
}

func TestExecutor_FixedBackoff_ExecutionWithRetries(t *testing.T) {
	t.Parallel()

	// Runner fails first 2 attempts, succeeds on 3rd
	runner := &mockRunner{failUntilAttempt: 3}
	logger := newTestLogger()
	executor := NewExecutor(runner, logger)

	initialDelay := 20 * time.Millisecond

	entry := HookEntry{
		Name:    "fixed-backoff-hook",
		Type:    ShellType,
		Timeout: 1 * time.Second,
		Retry: RetryConfig{
			Attempts:     3,
			Backoff:      BackoffFixed,
			InitialDelay: initialDelay,
			MaxDelay:     1 * time.Second,
		},
	}

	hook := newTestHook("fixed-backoff-hook")

	start := time.Now()
	err := executor.Execute(context.Background(), hook, entry)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, int32(3), runner.callCount.Load())

	// With fixed backoff of 20ms and 2 waits (between attempt 1→2 and 2→3),
	// total delay should be at least 40ms
	expectedMinDelay := 2 * initialDelay
	assert.GreaterOrEqual(t, elapsed, expectedMinDelay,
		"Expected at least %v of backoff delay, got %v", expectedMinDelay, elapsed)
}

// --- Exponential Backoff with max_delay Cap Tests ---
// Validates: Requirement 14.3

func TestExecutor_ExponentialBackoff_DelayDoubles(t *testing.T) {
	t.Parallel()

	initialDelay := 10 * time.Millisecond
	maxDelay := 1 * time.Second

	retry := RetryConfig{
		Attempts:     5,
		Backoff:      BackoffExponential,
		InitialDelay: initialDelay,
		MaxDelay:     maxDelay,
	}

	// Verify delay = initial_delay × 2^(N-1) for each attempt
	expectedDelays := []time.Duration{
		10 * time.Millisecond,  // attempt 1: 10ms × 2^0 = 10ms
		20 * time.Millisecond,  // attempt 2: 10ms × 2^1 = 20ms
		40 * time.Millisecond,  // attempt 3: 10ms × 2^2 = 40ms
		80 * time.Millisecond,  // attempt 4: 10ms × 2^3 = 80ms
		160 * time.Millisecond, // attempt 5: 10ms × 2^4 = 160ms
	}

	for i, expected := range expectedDelays {
		attempt := i + 1
		delay := calculateBackoff(retry, attempt)
		assert.Equal(t, expected, delay, "Exponential backoff for attempt %d", attempt)
	}
}

func TestExecutor_ExponentialBackoff_CappedAtMaxDelay(t *testing.T) {
	t.Parallel()

	initialDelay := 10 * time.Millisecond
	maxDelay := 50 * time.Millisecond

	retry := RetryConfig{
		Attempts:     10,
		Backoff:      BackoffExponential,
		InitialDelay: initialDelay,
		MaxDelay:     maxDelay,
	}

	// attempt 1: 10ms, attempt 2: 20ms, attempt 3: 40ms, attempt 4: 80ms → capped at 50ms
	assert.Equal(t, 10*time.Millisecond, calculateBackoff(retry, 1))
	assert.Equal(t, 20*time.Millisecond, calculateBackoff(retry, 2))
	assert.Equal(t, 40*time.Millisecond, calculateBackoff(retry, 3))
	assert.Equal(t, maxDelay, calculateBackoff(retry, 4)) // 80ms capped to 50ms
	assert.Equal(t, maxDelay, calculateBackoff(retry, 5)) // 160ms capped to 50ms
	assert.Equal(t, maxDelay, calculateBackoff(retry, 10))
}

func TestExecutor_ExponentialBackoff_ExecutionWithRetries(t *testing.T) {
	t.Parallel()

	// Runner always fails - we want to verify the exponential delay behavior
	runner := &mockRunner{failUntilAttempt: 0}
	logger := newTestLogger()
	executor := NewExecutor(runner, logger)

	initialDelay := 20 * time.Millisecond
	maxDelay := 100 * time.Millisecond

	entry := HookEntry{
		Name:    "exp-backoff-hook",
		Type:    ShellType,
		Timeout: 1 * time.Second,
		Retry: RetryConfig{
			Attempts:     3,
			Backoff:      BackoffExponential,
			InitialDelay: initialDelay,
			MaxDelay:     maxDelay,
		},
	}

	hook := newTestHook("exp-backoff-hook")

	start := time.Now()
	err := executor.Execute(context.Background(), hook, entry)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Equal(t, int32(4), runner.callCount.Load()) // 1 initial + 3 retries

	// Expected delays: 20ms (attempt 1→2) + 40ms (attempt 2→3) + 80ms (attempt 3→4) = 140ms
	expectedMinDelay := 20*time.Millisecond + 40*time.Millisecond + 80*time.Millisecond
	assert.GreaterOrEqual(t, elapsed, expectedMinDelay,
		"Expected at least %v of exponential backoff delay, got %v", expectedMinDelay, elapsed)
}

func TestExecutor_ExponentialBackoff_MaxDelayCapsExecution(t *testing.T) {
	t.Parallel()

	// Runner always fails
	runner := &mockRunner{failUntilAttempt: 0}
	logger := newTestLogger()
	executor := NewExecutor(runner, logger)

	initialDelay := 20 * time.Millisecond
	maxDelay := 30 * time.Millisecond // Cap at 30ms (less than 2nd exponential step of 40ms)

	entry := HookEntry{
		Name:    "capped-exp-hook",
		Type:    ShellType,
		Timeout: 1 * time.Second,
		Retry: RetryConfig{
			Attempts:     3,
			Backoff:      BackoffExponential,
			InitialDelay: initialDelay,
			MaxDelay:     maxDelay,
		},
	}

	hook := newTestHook("capped-exp-hook")

	start := time.Now()
	err := executor.Execute(context.Background(), hook, entry)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Equal(t, int32(4), runner.callCount.Load())

	// Expected delays: 20ms (attempt 1→2) + 30ms (capped, attempt 2→3) + 30ms (capped, attempt 3→4) = 80ms
	expectedMinDelay := 20*time.Millisecond + 30*time.Millisecond + 30*time.Millisecond
	assert.GreaterOrEqual(t, elapsed, expectedMinDelay,
		"Expected at least %v with max_delay cap, got %v", expectedMinDelay, elapsed)

	// Should not take much longer than expected (allow some tolerance for scheduling)
	maxExpected := expectedMinDelay + 100*time.Millisecond
	assert.LessOrEqual(t, elapsed, maxExpected,
		"Execution took too long (%v), max_delay cap may not be working", elapsed)
}

// --- Retry Behavior Tests ---
// Validates: Requirement 14.1

func TestExecutor_Retry_SucceedsOnFirstAttempt(t *testing.T) {
	t.Parallel()

	runner := &mockRunner{failUntilAttempt: 1} // succeed immediately
	logger := newTestLogger()
	executor := NewExecutor(runner, logger)

	entry := HookEntry{
		Name:    "first-attempt-hook",
		Type:    ShellType,
		Timeout: 1 * time.Second,
		Retry: RetryConfig{
			Attempts:     3,
			Backoff:      BackoffFixed,
			InitialDelay: 50 * time.Millisecond,
			MaxDelay:     1 * time.Second,
		},
	}

	hook := newTestHook("first-attempt-hook")

	err := executor.Execute(context.Background(), hook, entry)

	require.NoError(t, err)
	assert.Equal(t, int32(1), runner.callCount.Load(), "Should only execute once when first attempt succeeds")
}

func TestExecutor_Retry_SucceedsOnLastAttempt(t *testing.T) {
	t.Parallel()

	// Succeed on attempt 4 (1 initial + 3 retries)
	runner := &mockRunner{failUntilAttempt: 4}
	logger := newTestLogger()
	executor := NewExecutor(runner, logger)

	entry := HookEntry{
		Name:    "last-attempt-hook",
		Type:    ShellType,
		Timeout: 1 * time.Second,
		Retry: RetryConfig{
			Attempts:     3,
			Backoff:      BackoffFixed,
			InitialDelay: 1 * time.Millisecond,
			MaxDelay:     10 * time.Millisecond,
		},
	}

	hook := newTestHook("last-attempt-hook")

	err := executor.Execute(context.Background(), hook, entry)

	require.NoError(t, err)
	assert.Equal(t, int32(4), runner.callCount.Load(), "Should execute 4 times (1 initial + 3 retries)")
}

func TestExecutor_Retry_NoRetriesConfigured(t *testing.T) {
	t.Parallel()

	runner := &mockRunner{failUntilAttempt: 0} // always fail
	logger := newTestLogger()
	executor := NewExecutor(runner, logger)

	entry := HookEntry{
		Name:    "no-retry-hook",
		Type:    ShellType,
		Timeout: 1 * time.Second,
		Retry: RetryConfig{
			Attempts:     0,
			Backoff:      BackoffFixed,
			InitialDelay: 1 * time.Millisecond,
			MaxDelay:     10 * time.Millisecond,
		},
	}

	hook := newTestHook("no-retry-hook")

	err := executor.Execute(context.Background(), hook, entry)

	require.Error(t, err)
	assert.Equal(t, int32(1), runner.callCount.Load(), "Should only execute once with 0 retry attempts")
}

func TestExecutor_Retry_ContextCancelledDuringBackoff(t *testing.T) {
	t.Parallel()

	runner := &mockRunner{failUntilAttempt: 0} // always fail
	logger := newTestLogger()
	executor := NewExecutor(runner, logger)

	entry := HookEntry{
		Name:    "cancel-during-backoff",
		Type:    ShellType,
		Timeout: 1 * time.Second,
		Retry: RetryConfig{
			Attempts:     5,
			Backoff:      BackoffFixed,
			InitialDelay: 5 * time.Second, // long delay so we can cancel during it
			MaxDelay:     10 * time.Second,
		},
	}

	hook := newTestHook("cancel-during-backoff")

	// Cancel context after 100ms (during the first backoff wait)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := executor.Execute(ctx, hook, entry)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "context cancelled during backoff wait")
	// Should have executed only once before the backoff wait was interrupted
	assert.Equal(t, int32(1), runner.callCount.Load())
}
