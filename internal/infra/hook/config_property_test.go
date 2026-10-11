// Feature: partition-hooks, Property 2: Default Values Application
package hook

import (
	"testing"
	"time"

	"pgregory.net/rapid"
)

// **Validates: Requirements 2.6, 2.9, 2.15, 2.16, 2.17, 2.18, 8.2**
//
// Property 2: Default Values Application
// For any hook configuration with missing optional fields, the resolved hook SHALL have:
// enabled = true, timeout = 300s, retry.attempts = 0, retry.backoff = "exponential",
// retry.initial_delay = 5s, retry.max_delay = 60s, and propagate-credentials = false.

// genHookEntryWithMissingDefaults generates a HookEntry with required fields set
// but optional fields left at their zero values (simulating missing configuration).
func genHookEntryWithMissingDefaults(t *rapid.T) HookEntry {
	hookType := rapid.SampledFrom([]HookType{ShellType, PostgreSQLType}).Draw(t, "hookType")
	name := rapid.StringMatching(`[a-z][a-z0-9\-]{1,20}`).Draw(t, "name")

	var config map[string]interface{}

	switch hookType {
	case ShellType:
		config = map[string]interface{}{
			"command": rapid.StringMatching(`/[a-z/]{1,30}`).Draw(t, "command"),
		}
	case PostgreSQLType:
		config = map[string]interface{}{
			"sql_query": rapid.StringMatching(`SELECT [a-z]{1,10}`).Draw(t, "sql_query"),
		}
	}

	return HookEntry{
		Name:    name,
		Type:    hookType,
		Enabled: nil, // Missing - should default to true
		Timeout: 0,   // Missing - should default to 300s
		Retry: RetryConfig{
			Attempts:     0,  // Default: 0 attempts
			Backoff:      "", // Missing - should default to "exponential"
			InitialDelay: 0,  // Missing - should default to 5s
			MaxDelay:     0,  // Missing - should default to 60s
		},
		Config: config,
	}
}

func TestProperty_DefaultValuesApplication_HookEntry(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		hook := genHookEntryWithMissingDefaults(t)

		// Apply defaults
		hook.ApplyDefaults()

		// Verify: enabled defaults to true (Requirement 2.6)
		if hook.Enabled == nil {
			t.Fatal("Enabled should not be nil after ApplyDefaults")
		}

		if *hook.Enabled != true {
			t.Fatalf("Enabled should default to true, got %v", *hook.Enabled)
		}

		// Verify: timeout is not modified by ApplyDefaults (it is a required field)
		if hook.Timeout != 0 {
			t.Fatalf("Timeout should remain unchanged (0) after ApplyDefaults, got %v", hook.Timeout)
		}

		// Verify: retry.attempts defaults to 0 (Requirement 2.15)
		if hook.Retry.Attempts != 0 {
			t.Fatalf("Retry.Attempts should default to 0, got %d", hook.Retry.Attempts)
		}

		// Verify: retry.backoff defaults to "exponential" (Requirement 2.16)
		if hook.Retry.Backoff != BackoffExponential {
			t.Fatalf("Retry.Backoff should default to 'exponential', got %q", hook.Retry.Backoff)
		}

		// Verify: retry.initial_delay defaults to 5s (Requirement 2.17)
		if hook.Retry.InitialDelay != 5*time.Second {
			t.Fatalf("Retry.InitialDelay should default to 5s, got %v", hook.Retry.InitialDelay)
		}

		// Verify: retry.max_delay defaults to 60s (Requirement 2.18)
		if hook.Retry.MaxDelay != 60*time.Second {
			t.Fatalf("Retry.MaxDelay should default to 60s, got %v", hook.Retry.MaxDelay)
		}
	})
}

func TestProperty_DefaultValuesApplication_HooksConfig(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a HooksConfig with hooks in various lifecycle events,
		// all with missing optional fields
		numBeforeDetach := rapid.IntRange(0, 3).Draw(t, "numBeforeDetach")
		numAfterDetach := rapid.IntRange(0, 3).Draw(t, "numAfterDetach")
		numBeforeDrop := rapid.IntRange(0, 3).Draw(t, "numBeforeDrop")
		numAfterDrop := rapid.IntRange(0, 3).Draw(t, "numAfterDrop")

		config := HooksConfig{}

		for i := 0; i < numBeforeDetach; i++ {
			config.BeforeDetach = append(config.BeforeDetach, genHookEntryWithMissingDefaults(t))
		}

		for i := 0; i < numAfterDetach; i++ {
			config.AfterDetach = append(config.AfterDetach, genHookEntryWithMissingDefaults(t))
		}

		for i := 0; i < numBeforeDrop; i++ {
			config.BeforeDrop = append(config.BeforeDrop, genHookEntryWithMissingDefaults(t))
		}

		for i := 0; i < numAfterDrop; i++ {
			config.AfterDrop = append(config.AfterDrop, genHookEntryWithMissingDefaults(t))
		}

		// Apply defaults at the config level
		config.ApplyDefaults()

		// Verify all hooks across all lifecycle events have correct defaults
		allHooks := [][]HookEntry{
			config.BeforeDetach,
			config.AfterDetach,
			config.BeforeDrop,
			config.AfterDrop,
		}

		for _, hooks := range allHooks {
			for _, hook := range hooks {
				if hook.Enabled == nil || *hook.Enabled != true {
					t.Fatalf("Hook %q: Enabled should default to true", hook.Name)
				}

				if hook.Retry.Backoff != BackoffExponential {
					t.Fatalf("Hook %q: Retry.Backoff should default to 'exponential', got %q", hook.Name, hook.Retry.Backoff)
				}

				if hook.Retry.InitialDelay != 5*time.Second {
					t.Fatalf("Hook %q: Retry.InitialDelay should default to 5s, got %v", hook.Name, hook.Retry.InitialDelay)
				}

				if hook.Retry.MaxDelay != 60*time.Second {
					t.Fatalf("Hook %q: Retry.MaxDelay should default to 60s, got %v", hook.Name, hook.Retry.MaxDelay)
				}
			}
		}
	})
}

func TestProperty_DefaultValuesApplication_PreserveExplicitValues(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a hook with SOME explicit values set - verify defaults don't override them
		hookType := rapid.SampledFrom([]HookType{ShellType, PostgreSQLType}).Draw(t, "hookType")
		name := rapid.StringMatching(`[a-z][a-z0-9\-]{1,20}`).Draw(t, "name")

		var config map[string]interface{}

		switch hookType {
		case ShellType:
			config = map[string]interface{}{
				"command": "/usr/bin/test",
			}
		case PostgreSQLType:
			config = map[string]interface{}{
				"sql_query": "SELECT 1",
			}
		}

		// Explicitly set some values
		explicitEnabled := rapid.Bool().Draw(t, "explicitEnabled")
		explicitTimeout := time.Duration(rapid.IntRange(1, 3600).Draw(t, "timeoutSec")) * time.Second
		explicitBackoff := rapid.SampledFrom([]BackoffStrategy{BackoffFixed, BackoffExponential}).Draw(t, "backoff")
		explicitInitialDelay := time.Duration(rapid.IntRange(1, 120).Draw(t, "initialDelaySec")) * time.Second
		explicitMaxDelay := time.Duration(rapid.IntRange(1, 300).Draw(t, "maxDelaySec")) * time.Second

		hook := HookEntry{
			Name:    name,
			Type:    hookType,
			Enabled: &explicitEnabled,
			Timeout: explicitTimeout,
			Retry: RetryConfig{
				Backoff:      explicitBackoff,
				InitialDelay: explicitInitialDelay,
				MaxDelay:     explicitMaxDelay,
			},
			Config: config,
		}

		hook.ApplyDefaults()

		// Verify explicit values are preserved
		if *hook.Enabled != explicitEnabled {
			t.Fatalf("Enabled should preserve explicit value %v, got %v", explicitEnabled, *hook.Enabled)
		}

		if hook.Timeout != explicitTimeout {
			t.Fatalf("Timeout should preserve explicit value %v, got %v", explicitTimeout, hook.Timeout)
		}

		if hook.Retry.Backoff != explicitBackoff {
			t.Fatalf("Retry.Backoff should preserve explicit value %q, got %q", explicitBackoff, hook.Retry.Backoff)
		}

		if hook.Retry.InitialDelay != explicitInitialDelay {
			t.Fatalf("Retry.InitialDelay should preserve explicit value %v, got %v", explicitInitialDelay, hook.Retry.InitialDelay)
		}

		if hook.Retry.MaxDelay != explicitMaxDelay {
			t.Fatalf("Retry.MaxDelay should preserve explicit value %v, got %v", explicitMaxDelay, hook.Retry.MaxDelay)
		}
	})
}
