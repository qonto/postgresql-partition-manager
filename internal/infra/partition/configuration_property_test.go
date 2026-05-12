package partition

import (
	"testing"

	"github.com/go-playground/validator/v10"
	"pgregory.net/rapid"
)

// Feature: table-partition-conversion, Property 11: Configuration Validation
// For any ConversionConfiguration struct, validation SHALL pass if and only if all required fields
// (schema, table, partitionKey, interval, retention, preProvisioned) are present and valid,
// interval is one of the allowed values, retention and preProvisioned are greater than zero,
// and optional batch sizes are within [1, 1,000,000].
// Validates: Requirements 12.1, 12.2, 12.4
func TestProperty11_ConfigurationValidation_ValidConfig(t *testing.T) {
	validate := validator.New()

	rapid.Check(t, func(t *rapid.T) {
		intervals := []Interval{Daily, Weekly, Monthly, Quarterly, Yearly}
		cleanupPolicies := []CleanupPolicy{Drop, Detach}

		cfg := Configuration{
			Schema:           rapid.StringMatching(`[a-z][a-z0-9_]{0,19}`).Draw(t, "schema"),
			Table:            rapid.StringMatching(`[a-z][a-z0-9_]{0,19}`).Draw(t, "table"),
			PartitionKey:     rapid.StringMatching(`[a-z][a-z0-9_]{0,19}`).Draw(t, "partitionKey"),
			Interval:         intervals[rapid.IntRange(0, len(intervals)-1).Draw(t, "intervalIdx")],
			Retention:        rapid.IntRange(1, 365).Draw(t, "retention"),
			PreProvisioned:   rapid.IntRange(1, 30).Draw(t, "preProvisioned"),
			CleanupPolicy:    cleanupPolicies[rapid.IntRange(0, len(cleanupPolicies)-1).Draw(t, "cleanupPolicyIdx")],
			BatchSize:        rapid.IntRange(1, 1000000).Draw(t, "batchSize"),
			ReplayBatchSize:  rapid.IntRange(1, 1000000).Draw(t, "replayBatchSize"),
			LockTimeout:      rapid.IntRange(1, 60).Draw(t, "lockTimeout"),
			StatementTimeout: rapid.IntRange(5, 120).Draw(t, "statementTimeout"),
		}

		err := validate.Struct(cfg)
		if err != nil {
			t.Fatalf("valid configuration should pass validation, got error: %v (config: %+v)", err, cfg)
		}
	})
}

func TestProperty11_ConfigurationValidation_MissingSchema(t *testing.T) {
	validate := validator.New()

	rapid.Check(t, func(t *rapid.T) {
		cfg := validConversionConfig(t)
		cfg.Schema = ""

		err := validate.Struct(cfg)
		if err == nil {
			t.Fatal("configuration with empty schema should fail validation")
		}
	})
}

func TestProperty11_ConfigurationValidation_MissingTable(t *testing.T) {
	validate := validator.New()

	rapid.Check(t, func(t *rapid.T) {
		cfg := validConversionConfig(t)
		cfg.Table = ""

		err := validate.Struct(cfg)
		if err == nil {
			t.Fatal("configuration with empty table should fail validation")
		}
	})
}

func TestProperty11_ConfigurationValidation_MissingPartitionKey(t *testing.T) {
	validate := validator.New()

	rapid.Check(t, func(t *rapid.T) {
		cfg := validConversionConfig(t)
		cfg.PartitionKey = ""

		err := validate.Struct(cfg)
		if err == nil {
			t.Fatal("configuration with empty partitionKey should fail validation")
		}
	})
}

func TestProperty11_ConfigurationValidation_InvalidInterval(t *testing.T) {
	validate := validator.New()

	rapid.Check(t, func(t *rapid.T) {
		cfg := validConversionConfig(t)
		cfg.Interval = Interval(rapid.StringMatching(`[a-z]{3,10}`).Filter(func(s string) bool {
			return s != "daily" && s != "weekly" && s != "monthly" && s != "quarterly" && s != "yearly"
		}).Draw(t, "invalidInterval"))

		err := validate.Struct(cfg)
		if err == nil {
			t.Fatalf("configuration with invalid interval %q should fail validation", cfg.Interval)
		}
	})
}

func TestProperty11_ConfigurationValidation_InvalidRetention(t *testing.T) {
	validate := validator.New()

	rapid.Check(t, func(t *rapid.T) {
		cfg := validConversionConfig(t)
		cfg.Retention = rapid.IntRange(-100, 0).Draw(t, "invalidRetention")

		err := validate.Struct(cfg)
		if err == nil {
			t.Fatalf("configuration with retention=%d should fail validation", cfg.Retention)
		}
	})
}

func TestProperty11_ConfigurationValidation_InvalidPreProvisioned(t *testing.T) {
	validate := validator.New()

	rapid.Check(t, func(t *rapid.T) {
		cfg := validConversionConfig(t)
		cfg.PreProvisioned = rapid.IntRange(-100, 0).Draw(t, "invalidPreProvisioned")

		err := validate.Struct(cfg)
		if err == nil {
			t.Fatalf("configuration with preProvisioned=%d should fail validation", cfg.PreProvisioned)
		}
	})
}

func TestProperty11_ConfigurationValidation_ZeroBatchSizeAccepted(t *testing.T) {
	validate := validator.New()

	// Zero batch sizes are accepted (omitempty means zero value skips validation)
	rapid.Check(t, func(t *rapid.T) {
		cfg := validConversionConfig(t)
		cfg.BatchSize = 0
		cfg.ReplayBatchSize = 0
		cfg.LockTimeout = 0
		cfg.StatementTimeout = 0

		err := validate.Struct(cfg)
		if err != nil {
			t.Fatalf("configuration with zero optional fields should pass validation (omitempty), got: %v", err)
		}
	})
}

// Feature: table-partition-conversion, Property 12: Batch Size Range Validation
// For any integer value provided as a batch size (backfill or replay), the system SHALL accept
// values in the range [1, 1,000,000] and reject values outside this range with a validation error.
// Validates: Requirements 4.2, 5.4
func TestProperty12_BatchSizeRangeValidation_ValidRange(t *testing.T) {
	validate := validator.New()

	rapid.Check(t, func(t *rapid.T) {
		batchSize := rapid.IntRange(1, 1000000).Draw(t, "batchSize")

		cfg := validConversionConfig(t)
		cfg.BatchSize = batchSize

		err := validate.Struct(cfg)
		if err != nil {
			t.Fatalf("batch size %d in [1, 1000000] should be accepted, got error: %v", batchSize, err)
		}
	})
}

func TestProperty12_BatchSizeRangeValidation_TooLarge(t *testing.T) {
	validate := validator.New()

	rapid.Check(t, func(t *rapid.T) {
		batchSize := rapid.IntRange(1000001, 10000000).Draw(t, "tooLargeBatchSize")

		cfg := validConversionConfig(t)
		cfg.BatchSize = batchSize

		err := validate.Struct(cfg)
		if err == nil {
			t.Fatalf("batch size %d > 1000000 should be rejected", batchSize)
		}
	})
}

func TestProperty12_BatchSizeRangeValidation_Negative(t *testing.T) {
	validate := validator.New()

	rapid.Check(t, func(t *rapid.T) {
		batchSize := rapid.IntRange(-1000000, -1).Draw(t, "negativeBatchSize")

		cfg := validConversionConfig(t)
		cfg.BatchSize = batchSize

		err := validate.Struct(cfg)
		if err == nil {
			t.Fatalf("batch size %d < 1 should be rejected", batchSize)
		}
	})
}

func TestProperty12_ReplayBatchSizeRangeValidation_ValidRange(t *testing.T) {
	validate := validator.New()

	rapid.Check(t, func(t *rapid.T) {
		replayBatchSize := rapid.IntRange(1, 1000000).Draw(t, "replayBatchSize")

		cfg := validConversionConfig(t)
		cfg.ReplayBatchSize = replayBatchSize

		err := validate.Struct(cfg)
		if err != nil {
			t.Fatalf("replay batch size %d in [1, 1000000] should be accepted, got error: %v", replayBatchSize, err)
		}
	})
}

func TestProperty12_ReplayBatchSizeRangeValidation_TooLarge(t *testing.T) {
	validate := validator.New()

	rapid.Check(t, func(t *rapid.T) {
		replayBatchSize := rapid.IntRange(1000001, 10000000).Draw(t, "tooLargeReplayBatchSize")

		cfg := validConversionConfig(t)
		cfg.ReplayBatchSize = replayBatchSize

		err := validate.Struct(cfg)
		if err == nil {
			t.Fatalf("replay batch size %d > 1000000 should be rejected", replayBatchSize)
		}
	})
}

func TestProperty12_ReplayBatchSizeRangeValidation_Negative(t *testing.T) {
	validate := validator.New()

	rapid.Check(t, func(t *rapid.T) {
		replayBatchSize := rapid.IntRange(-1000000, -1).Draw(t, "negativeReplayBatchSize")

		cfg := validConversionConfig(t)
		cfg.ReplayBatchSize = replayBatchSize

		err := validate.Struct(cfg)
		if err == nil {
			t.Fatalf("replay batch size %d < 1 should be rejected", replayBatchSize)
		}
	})
}

// validConversionConfig generates a valid Configuration struct for use in property tests.
func validConversionConfig(t *rapid.T) Configuration {
	intervals := []Interval{Daily, Weekly, Monthly, Quarterly, Yearly}
	cleanupPolicies := []CleanupPolicy{Drop, Detach}

	return Configuration{
		Schema:           rapid.StringMatching(`[a-z][a-z0-9_]{0,19}`).Draw(t, "schema"),
		Table:            rapid.StringMatching(`[a-z][a-z0-9_]{0,19}`).Draw(t, "table"),
		PartitionKey:     rapid.StringMatching(`[a-z][a-z0-9_]{0,19}`).Draw(t, "partitionKey"),
		Interval:         intervals[rapid.IntRange(0, len(intervals)-1).Draw(t, "intervalIdx")],
		Retention:        rapid.IntRange(1, 365).Draw(t, "retention"),
		PreProvisioned:   rapid.IntRange(1, 30).Draw(t, "preProvisioned"),
		CleanupPolicy:    cleanupPolicies[rapid.IntRange(0, len(cleanupPolicies)-1).Draw(t, "cleanupPolicyIdx")],
		BatchSize:        rapid.IntRange(1, 1000000).Draw(t, "batchSize"),
		ReplayBatchSize:  rapid.IntRange(1, 1000000).Draw(t, "replayBatchSize"),
		LockTimeout:      rapid.IntRange(1, 60).Draw(t, "lockTimeout"),
		StatementTimeout: rapid.IntRange(5, 120).Draw(t, "statementTimeout"),
	}
}
