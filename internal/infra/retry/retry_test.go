package retry_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/qonto/postgresql-partition-manager/internal/infra/retry"
)

var ErrGeneric = errors.New("failed")

func TestWithRetry(t *testing.T) {
	t.Run("operation succeeds after 1 retry", func(t *testing.T) {
		attempts := 0
		operation := func(_ int) error {
			attempts++
			if attempts < 2 {
				return ErrGeneric
			}

			return nil
		}

		err := retry.WithRetry(2, operation)
		if err != nil {
			t.Fatalf("expected no error, but got: %v", err)
		}

		if attempts != 2 {
			t.Fatalf("expected 2 attempts, but got: %d", attempts)
		}
	})

	t.Run("operation never succeeds", func(t *testing.T) {
		maxRetries := 2
		attempts := 0
		operation := func(_ int) error {
			attempts++

			return ErrGeneric
		}

		err := retry.WithRetry(maxRetries, operation)
		if err == nil {
			t.Fatalf("expected error, but got none")
		}

		if attempts != maxRetries {
			t.Fatalf("expected %d attempts, but got: %d", maxRetries, attempts)
		}
	})
}

func TestWithDeadlockRetry(t *testing.T) {
	logger := *slog.Default()

	t.Run("operation succeeds on first attempt", func(t *testing.T) {
		attempts := 0
		operation := func() error {
			attempts++
			return nil
		}

		err := retry.WithDeadlockRetry(context.Background(), logger, operation)
		if err != nil {
			t.Fatalf("expected no error, but got: %v", err)
		}

		if attempts != 1 {
			t.Fatalf("expected 1 attempt, but got: %d", attempts)
		}
	})

	t.Run("operation succeeds after deadlock retry", func(t *testing.T) {
		attempts := 0
		deadlockErr := &pgconn.PgError{Code: "40P01"}
		operation := func() error {
			attempts++
			if attempts < 3 {
				return deadlockErr
			}
			return nil
		}

		err := retry.WithDeadlockRetry(context.Background(), logger, operation)
		if err != nil {
			t.Fatalf("expected no error, but got: %v", err)
		}

		if attempts != 3 {
			t.Fatalf("expected 3 attempts, but got: %d", attempts)
		}
	})

	t.Run("operation fails after 3 deadlock retries", func(t *testing.T) {
		attempts := 0
		deadlockErr := &pgconn.PgError{Code: "40P01"}
		operation := func() error {
			attempts++
			return deadlockErr
		}

		err := retry.WithDeadlockRetry(context.Background(), logger, operation)
		if err == nil {
			t.Fatalf("expected error, but got none")
		}

		if attempts != 3 {
			t.Fatalf("expected 3 attempts, but got: %d", attempts)
		}
	})

	t.Run("non-deadlock error is not retried", func(t *testing.T) {
		attempts := 0
		operation := func() error {
			attempts++
			return ErrGeneric
		}

		err := retry.WithDeadlockRetry(context.Background(), logger, operation)
		if err == nil {
			t.Fatalf("expected error, but got none")
		}

		if attempts != 1 {
			t.Fatalf("expected 1 attempt (no retry for non-deadlock), but got: %d", attempts)
		}
	})

	t.Run("context cancellation stops retry", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		attempts := 0
		deadlockErr := &pgconn.PgError{Code: "40P01"}
		operation := func() error {
			attempts++
			return deadlockErr
		}

		err := retry.WithDeadlockRetry(ctx, logger, operation)
		if err == nil {
			t.Fatalf("expected error, but got none")
		}

		if attempts != 1 {
			t.Fatalf("expected 1 attempt before context cancellation, but got: %d", attempts)
		}
	})
}
