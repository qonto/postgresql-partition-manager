package retry_test

import (
	"errors"
	"testing"

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
