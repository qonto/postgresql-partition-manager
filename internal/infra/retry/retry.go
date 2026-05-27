// Package retry provides methods to retry operations
package retry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgconn"
)

func WithRetry(maxRetries int, operation func(attempt int) error) error {
	var err error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err = operation(attempt); err == nil {
			return nil
		}

		time.Sleep(time.Duration(attempt) * time.Second)
	}

	return err
}

// WithDeadlockRetry retries an operation up to 3 times when a PostgreSQL deadlock
// (error code 40P01) is detected, with a 1-second fixed delay between attempts.
// Non-deadlock errors are returned immediately without retry.
func WithDeadlockRetry(ctx context.Context, logger slog.Logger, operation func() error) error {
	return WithDeadlockRetryWithDelay(ctx, logger, 1*time.Second, operation)
}

// WithDeadlockRetryWithDelay is like WithDeadlockRetry but accepts a configurable delay
// between retry attempts. This is useful for testing with zero delay.
func WithDeadlockRetryWithDelay(ctx context.Context, logger slog.Logger, retryDelay time.Duration, operation func() error) error {
	const maxRetries = 3

	var err error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = operation()
		if err == nil {
			return nil
		}

		if !isDeadlockError(err) {
			return err
		}

		logger.Warn("Deadlock detected, retrying", "attempt", attempt, "maxRetries", maxRetries)

		if attempt < maxRetries {
			if retryDelay > 0 {
				select {
				case <-ctx.Done():
					return fmt.Errorf("context cancelled during deadlock retry: %w", ctx.Err())
				case <-time.After(retryDelay):
				}
			} else {
				select {
				case <-ctx.Done():
					return fmt.Errorf("context cancelled during deadlock retry: %w", ctx.Err())
				default:
				}
			}
		}
	}

	return fmt.Errorf("operation failed after %d deadlock retries: %w", maxRetries, err)
}

// isDeadlockError checks if the error is a PostgreSQL deadlock error (code 40P01).
func isDeadlockError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40P01"
	}

	return false
}
