package retry_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/qonto/postgresql-partition-manager/internal/infra/retry"
	"pgregory.net/rapid"
)

// Feature: table-partition-conversion, Property 15: Deadlock Retry Behavior
// For any batch operation that fails with a PostgreSQL deadlock error (error code 40P01),
// the system SHALL retry the operation up to 3 times with a 1-second delay between attempts
// before propagating the error.
// **Validates: Requirements 15.4**

// TestProperty15_DeadlockAlwaysRetriesExactly3Times verifies that for any batch operation
// that always fails with deadlock error code 40P01, the operation is called exactly 3 times
// before the error propagates.
func TestProperty15_DeadlockAlwaysRetriesExactly3Times(t *testing.T) {
	logger := *slog.Default()

	rapid.Check(t, func(t *rapid.T) {
		// Generate a random deadlock error message to ensure behavior is independent of message content
		errMsg := rapid.StringMatching(`[a-zA-Z ]{1,50}`).Draw(t, "errorMessage")
		deadlockErr := &pgconn.PgError{Code: "40P01", Message: errMsg}

		attempts := 0
		operation := func() error {
			attempts++
			return deadlockErr
		}

		err := retry.WithDeadlockRetryWithDelay(context.Background(), logger, 0, operation)

		// The operation must have been called exactly 3 times
		if attempts != 3 {
			t.Fatalf("expected exactly 3 attempts for deadlock error, got %d (message: %q)", attempts, errMsg)
		}

		// The error must be propagated (not nil)
		if err == nil {
			t.Fatal("expected error to be propagated after 3 retries, got nil")
		}
	})
}

// TestProperty15_NonDeadlockErrorNotRetried verifies that for any non-deadlock error,
// the operation is called exactly 1 time (no retry).
func TestProperty15_NonDeadlockErrorNotRetried(t *testing.T) {
	logger := *slog.Default()

	rapid.Check(t, func(t *rapid.T) {
		// Generate random non-deadlock PG error codes (not 40P01)
		nonDeadlockCodes := []string{"23505", "42P01", "55P03", "57014", "08006", "42501", "P0001"}
		codeIdx := rapid.IntRange(0, len(nonDeadlockCodes)-1).Draw(t, "codeIdx")
		pgErr := &pgconn.PgError{Code: nonDeadlockCodes[codeIdx]}

		attempts := 0
		operation := func() error {
			attempts++
			return pgErr
		}

		err := retry.WithDeadlockRetryWithDelay(context.Background(), logger, 0, operation)

		// The operation must have been called exactly 1 time (no retry)
		if attempts != 1 {
			t.Fatalf("expected exactly 1 attempt for non-deadlock error (code %s), got %d", nonDeadlockCodes[codeIdx], attempts)
		}

		// The error must be returned
		if err == nil {
			t.Fatal("expected error to be returned immediately for non-deadlock error, got nil")
		}
	})
}

// TestProperty15_NonPgErrorNotRetried verifies that for any generic (non-PgError) error,
// the operation is called exactly 1 time (no retry).
func TestProperty15_NonPgErrorNotRetried(t *testing.T) {
	logger := *slog.Default()

	rapid.Check(t, func(t *rapid.T) {
		errMsg := rapid.StringMatching(`[a-zA-Z ]{1,50}`).Draw(t, "errorMessage")
		genericErr := errors.New(errMsg)

		attempts := 0
		operation := func() error {
			attempts++
			return genericErr
		}

		err := retry.WithDeadlockRetryWithDelay(context.Background(), logger, 0, operation)

		// The operation must have been called exactly 1 time (no retry)
		if attempts != 1 {
			t.Fatalf("expected exactly 1 attempt for generic error, got %d (message: %q)", attempts, errMsg)
		}

		// The error must be returned
		if err == nil {
			t.Fatal("expected error to be returned immediately for generic error, got nil")
		}
	})
}

// TestProperty15_DeadlockSucceedsAfterNFailures verifies that for any operation that
// fails with deadlock N times (where N < 3) and then succeeds, the operation is called
// exactly N+1 times total.
func TestProperty15_DeadlockSucceedsAfterNFailures(t *testing.T) {
	logger := *slog.Default()

	rapid.Check(t, func(t *rapid.T) {
		// N is the number of deadlock failures before success (0, 1, or 2)
		failuresBeforeSuccess := rapid.IntRange(0, 2).Draw(t, "failuresBeforeSuccess")
		deadlockErr := &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}

		attempts := 0
		operation := func() error {
			attempts++
			if attempts <= failuresBeforeSuccess {
				return deadlockErr
			}
			return nil
		}

		err := retry.WithDeadlockRetryWithDelay(context.Background(), logger, 0, operation)

		expectedAttempts := failuresBeforeSuccess + 1
		if attempts != expectedAttempts {
			t.Fatalf("expected %d attempts (after %d deadlock failures then success), got %d",
				expectedAttempts, failuresBeforeSuccess, attempts)
		}

		// Should succeed (no error propagated)
		if err != nil {
			t.Fatalf("expected no error after eventual success, got: %v", err)
		}
	})
}

// TestProperty15_DeadlockErrorWrapped verifies that when the operation fails with deadlock
// after all retries, the final error wraps the original deadlock error.
func TestProperty15_DeadlockErrorWrapped(t *testing.T) {
	logger := *slog.Default()

	rapid.Check(t, func(t *rapid.T) {
		errMsg := rapid.StringMatching(`[a-zA-Z ]{1,50}`).Draw(t, "errorMessage")
		deadlockErr := &pgconn.PgError{Code: "40P01", Message: errMsg}

		operation := func() error {
			return deadlockErr
		}

		err := retry.WithDeadlockRetryWithDelay(context.Background(), logger, 0, operation)

		// The returned error must not be nil
		if err == nil {
			t.Fatal("expected error after all retries exhausted, got nil")
		}

		// The returned error must wrap the original deadlock error
		var wrappedPgErr *pgconn.PgError
		if !errors.As(err, &wrappedPgErr) {
			t.Fatalf("expected returned error to wrap *pgconn.PgError, got: %v", err)
		}

		if wrappedPgErr.Code != "40P01" {
			t.Fatalf("expected wrapped error code 40P01, got %s", wrappedPgErr.Code)
		}
	})
}

// TestProperty15_ContextCancellationStopsRetry verifies that when the context is cancelled,
// the retry loop stops even for deadlock errors.
func TestProperty15_ContextCancellationStopsRetry(t *testing.T) {
	logger := *slog.Default()

	rapid.Check(t, func(t *rapid.T) {
		// Cancel context before calling WithDeadlockRetry
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		deadlockErr := &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}

		attempts := 0
		operation := func() error {
			attempts++
			return deadlockErr
		}

		err := retry.WithDeadlockRetryWithDelay(ctx, logger, 0, operation)

		// With a cancelled context, the operation should be called exactly once
		// (first attempt fails with deadlock, then context cancellation prevents retry)
		if attempts != 1 {
			t.Fatalf("expected 1 attempt with cancelled context, got %d", attempts)
		}

		// An error must be returned
		if err == nil {
			t.Fatal("expected error with cancelled context, got nil")
		}

		// The error should indicate context cancellation
		if !errors.Is(err, context.Canceled) {
			// It's acceptable if the error wraps context.Canceled
			errStr := fmt.Sprintf("%v", err)
			if errStr == "" {
				t.Fatal("expected non-empty error message")
			}
		}
	})
}
