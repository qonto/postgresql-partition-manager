// Feature: partition-hooks, Property 14: Retry Execution Count
package hook

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// **Validates: Requirements 14.1, 14.4**
//
// Property 14: Retry Execution Count
// For any hook with retry.attempts = N that fails on every attempt, the hook SHALL be
// executed exactly N+1 times (1 initial + N retries). If the hook succeeds on attempt K
// (where K ≤ N+1), it SHALL be executed exactly K times.

// countingRunner is a test runner that counts invocations and can be configured
// to succeed on a specific attempt or always fail.
type countingRunner struct {
	callCount        atomic.Int32
	succeedOnAttempt int // 0 means always fail
}

func (r *countingRunner) Run(_ context.Context, _ *ResolvedHook) error {
	current := int(r.callCount.Add(1))
	if r.succeedOnAttempt > 0 && current >= r.succeedOnAttempt {
		return nil
	}

	return fmt.Errorf("simulated failure on attempt %d", current)
}

func TestProperty_RetryExecutionCount_AlwaysFails(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a random number of retry attempts (1 to 10)
		attempts := rapid.IntRange(1, 10).Draw(t, "attempts")

		runner := &countingRunner{succeedOnAttempt: 0} // always fail
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
		executor := NewExecutor(runner, *logger)

		entry := HookEntry{
			Name:    "test-hook",
			Type:    ShellType,
			Timeout: 5 * time.Second,
			Retry: RetryConfig{
				Attempts:     attempts,
				Backoff:      BackoffFixed,
				InitialDelay: 1 * time.Millisecond, // minimal delay for fast tests
				MaxDelay:     10 * time.Millisecond,
			},
		}

		hook := &ResolvedHook{
			Name: "test-hook",
			Type: ShellType,
			PartitionContext: PartitionContext{
				PartitionName: "test-partition",
			},
		}

		err := executor.Execute(context.Background(), hook, entry)

		// The hook should have failed
		if err == nil {
			t.Fatal("Expected error when hook always fails, got nil")
		}

		// Property: hook SHALL be executed exactly N+1 times (1 initial + N retries)
		expectedCalls := attempts + 1
		actualCalls := int(runner.callCount.Load())

		if actualCalls != expectedCalls {
			t.Fatalf("Hook with retry.attempts=%d that always fails: expected %d executions (1 initial + %d retries), got %d",
				attempts, expectedCalls, attempts, actualCalls)
		}
	})
}

func TestProperty_RetryExecutionCount_SucceedsOnAttemptK(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a random number of retry attempts (1 to 10)
		attempts := rapid.IntRange(1, 10).Draw(t, "attempts")

		// Generate the attempt on which the hook succeeds (1 to N+1)
		maxAttempts := attempts + 1
		succeedOn := rapid.IntRange(1, maxAttempts).Draw(t, "succeedOnAttempt")

		runner := &countingRunner{succeedOnAttempt: succeedOn}
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
		executor := NewExecutor(runner, *logger)

		entry := HookEntry{
			Name:    "test-hook",
			Type:    ShellType,
			Timeout: 5 * time.Second,
			Retry: RetryConfig{
				Attempts:     attempts,
				Backoff:      BackoffFixed,
				InitialDelay: 1 * time.Millisecond, // minimal delay for fast tests
				MaxDelay:     10 * time.Millisecond,
			},
		}

		hook := &ResolvedHook{
			Name: "test-hook",
			Type: ShellType,
			PartitionContext: PartitionContext{
				PartitionName: "test-partition",
			},
		}

		err := executor.Execute(context.Background(), hook, entry)

		// The hook should have succeeded
		if err != nil {
			t.Fatalf("Expected hook to succeed on attempt %d, got error: %v", succeedOn, err)
		}

		// Property: hook SHALL be executed exactly K times
		actualCalls := int(runner.callCount.Load())

		if actualCalls != succeedOn {
			t.Fatalf("Hook with retry.attempts=%d that succeeds on attempt %d: expected %d executions, got %d",
				attempts, succeedOn, succeedOn, actualCalls)
		}
	})
}
