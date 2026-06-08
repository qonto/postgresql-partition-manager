package hook

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"
)

// Executor wraps a Runner with retry and timeout logic.
type Executor struct {
	runner Runner
	logger slog.Logger
}

// NewExecutor creates a new Executor that wraps the given Runner with retry and timeout logic.
func NewExecutor(runner Runner, logger slog.Logger) *Executor {
	return &Executor{
		runner: runner,
		logger: logger,
	}
}

// Execute runs the hook with timeout and retry logic.
// It applies the configured timeout via context.WithTimeout and retries on failure
// up to retry.attempts times using the configured backoff strategy.
func (e *Executor) Execute(ctx context.Context, hook *ResolvedHook, entry HookEntry) error {
	var lastErr error

	maxAttempts := entry.Retry.Attempts + 1 // 1 initial attempt + N retries

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		lastErr = e.executeWithTimeout(ctx, hook, entry.Timeout)
		if lastErr == nil {
			return nil
		}

		// If this was the last attempt, don't retry
		if attempt >= maxAttempts {
			break
		}

		// Check if the parent context is already cancelled
		if ctx.Err() != nil {
			return fmt.Errorf("hook %q: context cancelled during retry: %w", hook.Name, ctx.Err())
		}

		// Calculate backoff delay for the next retry
		delay := calculateBackoff(entry.Retry, attempt)

		e.logger.Warn("Hook execution failed, retrying",
			"hook", hook.Name,
			"type", string(hook.Type),
			"partition", hook.PartitionContext.PartitionName,
			"attempt", attempt,
			"max_attempts", maxAttempts,
			"next_delay", delay.String(),
			"error", lastErr.Error(),
		)

		// Wait for the backoff delay or context cancellation
		select {
		case <-ctx.Done():
			return fmt.Errorf("hook %q: context cancelled during backoff wait: %w", hook.Name, ctx.Err())
		case <-time.After(delay):
		}
	}

	return lastErr
}

// executeWithTimeout runs the hook with a timeout derived from the hook's configured timeout.
func (e *Executor) executeWithTimeout(ctx context.Context, hook *ResolvedHook, timeout time.Duration) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return e.runner.Run(timeoutCtx, hook)
}

// calculateBackoff computes the delay for the given retry attempt (1-based).
// For fixed backoff: returns initial_delay.
// For exponential backoff (the default): returns min(initial_delay × 2^(attempt-1), max_delay).
func calculateBackoff(retry RetryConfig, attempt int) time.Duration {
	if retry.Backoff == BackoffFixed {
		return retry.InitialDelay
	}

	// Exponential backoff (also the default for unrecognized strategies).
	multiplier := math.Pow(2, float64(attempt-1))

	delay := time.Duration(float64(retry.InitialDelay) * multiplier)
	if delay > retry.MaxDelay {
		return retry.MaxDelay
	}

	return delay
}
