// Feature: partition-hooks, Property 13: Backoff Delay Calculation
package hook

import (
	"math"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// **Validates: Requirements 2.13, 2.14, 14.2, 14.3**
//
// Property 13: Backoff Delay Calculation
// For any retry configuration with `backoff` = "fixed", the delay between each retry attempt
// SHALL equal `initial_delay`. For any retry configuration with `backoff` = "exponential",
// the delay for attempt N SHALL equal min(`initial_delay` × 2^(N-1), `max_delay`).

// genRetryConfigFixed generates a RetryConfig with fixed backoff strategy.
func genRetryConfigFixed(t *rapid.T) RetryConfig {
	initialDelay := time.Duration(rapid.IntRange(1, 120).Draw(t, "initialDelaySec")) * time.Second

	return RetryConfig{
		Attempts:     rapid.IntRange(1, 10).Draw(t, "attempts"),
		Backoff:      BackoffFixed,
		InitialDelay: initialDelay,
		MaxDelay:     time.Duration(rapid.IntRange(60, 300).Draw(t, "maxDelaySec")) * time.Second,
	}
}

// genRetryConfigExponential generates a RetryConfig with exponential backoff strategy
// where MaxDelay >= InitialDelay.
func genRetryConfigExponential(t *rapid.T) RetryConfig {
	initialDelaySec := rapid.IntRange(1, 30).Draw(t, "initialDelaySec")
	maxDelaySec := rapid.IntRange(initialDelaySec, 300).Draw(t, "maxDelaySec")

	return RetryConfig{
		Attempts:     rapid.IntRange(1, 10).Draw(t, "attempts"),
		Backoff:      BackoffExponential,
		InitialDelay: time.Duration(initialDelaySec) * time.Second,
		MaxDelay:     time.Duration(maxDelaySec) * time.Second,
	}
}

func TestProperty_BackoffDelayCalculation_Fixed(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		retry := genRetryConfigFixed(t)
		attempt := rapid.IntRange(1, retry.Attempts+1).Draw(t, "attempt")

		delay := calculateBackoff(retry, attempt)

		// Property: For fixed backoff, the delay SHALL always equal initial_delay
		if delay != retry.InitialDelay {
			t.Fatalf("Fixed backoff: expected delay %v for attempt %d, got %v",
				retry.InitialDelay, attempt, delay)
		}
	})
}

func TestProperty_BackoffDelayCalculation_Exponential(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		retry := genRetryConfigExponential(t)
		attempt := rapid.IntRange(1, retry.Attempts+1).Draw(t, "attempt")

		delay := calculateBackoff(retry, attempt)

		// Property: For exponential backoff, delay = min(initial_delay × 2^(attempt-1), max_delay)
		multiplier := math.Pow(2, float64(attempt-1))
		expectedDelay := time.Duration(float64(retry.InitialDelay) * multiplier)
		if expectedDelay > retry.MaxDelay {
			expectedDelay = retry.MaxDelay
		}

		if delay != expectedDelay {
			t.Fatalf("Exponential backoff: expected delay %v for attempt %d (initial=%v, max=%v), got %v",
				expectedDelay, attempt, retry.InitialDelay, retry.MaxDelay, delay)
		}

		// Property: The result SHALL never exceed MaxDelay
		if delay > retry.MaxDelay {
			t.Fatalf("Exponential backoff: delay %v exceeds max_delay %v for attempt %d",
				delay, retry.MaxDelay, attempt)
		}
	})
}

func TestProperty_BackoffDelayCalculation_ExponentialMonotonicity(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		retry := genRetryConfigExponential(t)

		// Need at least 2 attempts to test monotonicity
		if retry.Attempts < 2 {
			retry.Attempts = 2
		}

		attempt := rapid.IntRange(1, retry.Attempts).Draw(t, "attempt")

		delayN := calculateBackoff(retry, attempt)
		delayN1 := calculateBackoff(retry, attempt+1)

		// Property: For exponential backoff, delay for attempt N+1 >= delay for attempt N
		// (monotonically non-decreasing)
		if delayN1 < delayN {
			t.Fatalf("Exponential backoff: delay for attempt %d (%v) < delay for attempt %d (%v), violates monotonicity",
				attempt+1, delayN1, attempt, delayN)
		}
	})
}
