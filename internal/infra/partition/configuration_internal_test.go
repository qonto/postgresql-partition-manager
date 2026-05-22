package partition

import (
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"gotest.tools/assert"
)

// --- ConvertSettings.ApplyDefaults tests ---

func TestConvertSettingsApplyDefaults_AllZeroValues(t *testing.T) {
	cs := &ConvertSettings{}
	cs.ApplyDefaults()

	assert.Equal(t, cs.BackfillBatchSize, 10000)
	assert.Equal(t, cs.ReplayBatchSize, 1000)
	assert.Equal(t, cs.LockTimeout, 5)
	assert.Equal(t, cs.StatementTimeout, 30)
}

func TestConvertSettingsApplyDefaults_PreservesExistingValues(t *testing.T) {
	cs := &ConvertSettings{
		BackfillBatchSize: 5000,
		ReplayBatchSize:   500,
		LockTimeout:       10,
		StatementTimeout:  60,
	}
	cs.ApplyDefaults()

	assert.Equal(t, cs.BackfillBatchSize, 5000)
	assert.Equal(t, cs.ReplayBatchSize, 500)
	assert.Equal(t, cs.LockTimeout, 10)
	assert.Equal(t, cs.StatementTimeout, 60)
}

func TestConvertSettingsApplyDefaults_PartialValues(t *testing.T) {
	cs := &ConvertSettings{
		BackfillBatchSize: 2000,
		// ReplayBatchSize left at 0 → should get default
		LockTimeout: 3,
		// StatementTimeout left at 0 → should get default
	}
	cs.ApplyDefaults()

	assert.Equal(t, cs.BackfillBatchSize, 2000)
	assert.Equal(t, cs.ReplayBatchSize, 1000)
	assert.Equal(t, cs.LockTimeout, 3)
	assert.Equal(t, cs.StatementTimeout, 30)
}

// --- Configuration struct with Convert field tests ---

func TestConfiguration_NilConvert_IsValid(t *testing.T) {
	cfg := Configuration{
		Schema:         "public",
		Table:          "events",
		PartitionKey:   "created_at",
		Interval:       Daily,
		Retention:      3,
		PreProvisioned: 3,
		CleanupPolicy:  Drop,
		Convert:        nil,
	}

	validate := validator.New()
	err := validate.Struct(cfg)
	assert.NilError(t, err)
}

func TestConfiguration_EmptyConvert_IsValid(t *testing.T) {
	cfg := Configuration{
		Schema:         "public",
		Table:          "events",
		PartitionKey:   "created_at",
		Interval:       Daily,
		Retention:      3,
		PreProvisioned: 3,
		CleanupPolicy:  Drop,
		Convert:        &ConvertSettings{},
	}

	validate := validator.New()
	err := validate.Struct(cfg)
	assert.NilError(t, err)
}

func TestConfiguration_WithConvert_IsValid(t *testing.T) {
	cfg := Configuration{
		Schema:         "public",
		Table:          "events",
		PartitionKey:   "created_at",
		Interval:       Daily,
		Retention:      3,
		PreProvisioned: 3,
		CleanupPolicy:  Drop,
		Convert: &ConvertSettings{
			BackfillBatchSize: 5000,
			ReplayBatchSize:   2000,
			LockTimeout:       10,
			StatementTimeout:  60,
		},
	}

	validate := validator.New()
	err := validate.Struct(cfg)
	assert.NilError(t, err)
}

func TestConfiguration_WithInvalidConvert_FailsValidation(t *testing.T) {
	cfg := Configuration{
		Schema:         "public",
		Table:          "events",
		PartitionKey:   "created_at",
		Interval:       Daily,
		Retention:      3,
		PreProvisioned: 3,
		CleanupPolicy:  Drop,
		Convert: &ConvertSettings{
			BackfillBatchSize: -1, // invalid: negative
		},
	}

	validate := validator.New()
	err := validate.Struct(cfg)
	assert.Assert(t, err != nil, "configuration with invalid convert settings should fail validation")
}

func TestConfiguration_ConvertStatementTimeoutBelowMin_FailsValidation(t *testing.T) {
	cfg := Configuration{
		Schema:         "public",
		Table:          "events",
		PartitionKey:   "created_at",
		Interval:       Daily,
		Retention:      3,
		PreProvisioned: 3,
		CleanupPolicy:  Drop,
		Convert: &ConvertSettings{
			StatementTimeout: 2, // invalid: min is 5
		},
	}

	validate := validator.New()
	err := validate.Struct(cfg)
	assert.Assert(t, err != nil, "configuration with statementTimeout below min should fail validation")
}

func configForInterval(interval Interval, retention, preProvisioned int) Configuration {
	return Configuration{
		Schema:         "public",
		Table:          "test_table",
		PartitionKey:   "created_at",
		Interval:       interval,
		Retention:      retention,
		PreProvisioned: preProvisioned,
		CleanupPolicy:  Drop,
	}
}

// --- getPrevDate tests ---

func TestGetPrevDateDaily(t *testing.T) {
	config := configForInterval(Daily, 4, 1)

	testCases := []struct {
		name     string
		forDate  time.Time
		i        int
		expected time.Time
	}{
		{
			name:     "Back 1 day",
			forDate:  time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2026, 3, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Across month boundary",
			forDate:  time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2026, 2, 28, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Across year boundary",
			forDate:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Leap day",
			forDate:  time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := config.getPrevDate(tc.forDate, tc.i)
			assert.NilError(t, err)
			assert.Equal(t, result, tc.expected)
		})
	}
}

func TestGetPrevDateWeekly(t *testing.T) {
	config := configForInterval(Weekly, 4, 1)

	testCases := []struct {
		name     string
		forDate  time.Time
		i        int
		expected time.Time
	}{
		{
			name:     "Back 1 week",
			forDate:  time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2026, 3, 8, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Back 4 weeks across month",
			forDate:  time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC),
			i:        4,
			expected: time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := config.getPrevDate(tc.forDate, tc.i)
			assert.NilError(t, err)
			assert.Equal(t, result, tc.expected)
		})
	}
}

func TestGetPrevDateMonthly(t *testing.T) {
	config := configForInterval(Monthly, 4, 1)

	testCases := []struct {
		name     string
		forDate  time.Time
		i        int
		expected time.Time
	}{
		{
			name:     "Back 1 month",
			forDate:  time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Back 1 month from January",
			forDate:  time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Back 12 months",
			forDate:  time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC),
			i:        12,
			expected: time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := config.getPrevDate(tc.forDate, tc.i)
			assert.NilError(t, err)
			assert.Equal(t, result, tc.expected)
		})
	}
}

func TestGetPrevDateQuarterly(t *testing.T) {
	config := configForInterval(Quarterly, 4, 1)

	testCases := []struct {
		name     string
		forDate  time.Time
		i        int
		expected time.Time
	}{
		{
			name:     "March 31 back 1 quarter",
			forDate:  time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2025, 10, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "March 31 back 2 quarters",
			forDate:  time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			i:        2,
			expected: time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "March 31 back 3 quarters",
			forDate:  time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			i:        3,
			expected: time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "March 31 back 4 quarters",
			forDate:  time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			i:        4,
			expected: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "May 31 back 1 quarter",
			forDate:  time.Date(2026, 5, 31, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "May 31 back 2 quarters",
			forDate:  time.Date(2026, 5, 31, 0, 0, 0, 0, time.UTC),
			i:        2,
			expected: time.Date(2025, 10, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "January 1 back 1 quarter",
			forDate:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2025, 10, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "August 15 back 1 quarter",
			forDate:  time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := config.getPrevDate(tc.forDate, tc.i)
			assert.NilError(t, err)
			assert.Equal(t, result, tc.expected)
		})
	}
}

func TestGetPrevDateYearly(t *testing.T) {
	config := configForInterval(Yearly, 4, 1)

	testCases := []struct {
		name     string
		forDate  time.Time
		i        int
		expected time.Time
	}{
		{
			name:     "Back 1 year",
			forDate:  time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Back 5 years",
			forDate:  time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
			i:        5,
			expected: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := config.getPrevDate(tc.forDate, tc.i)
			assert.NilError(t, err)
			assert.Equal(t, result, tc.expected)
		})
	}
}

// --- getNextDate tests ---

func TestGetNextDateDaily(t *testing.T) {
	config := configForInterval(Daily, 1, 4)

	testCases := []struct {
		name     string
		forDate  time.Time
		i        int
		expected time.Time
	}{
		{
			name:     "Forward 1 day",
			forDate:  time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Across year boundary",
			forDate:  time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := config.getNextDate(tc.forDate, tc.i)
			assert.NilError(t, err)
			assert.Equal(t, result, tc.expected)
		})
	}
}

func TestGetNextDateWeekly(t *testing.T) {
	config := configForInterval(Weekly, 1, 4)

	result, err := config.getNextDate(time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC), 2)
	assert.NilError(t, err)
	assert.Equal(t, result, time.Date(2026, 3, 29, 0, 0, 0, 0, time.UTC))
}

func TestGetNextDateMonthly(t *testing.T) {
	config := configForInterval(Monthly, 1, 4)

	testCases := []struct {
		name     string
		forDate  time.Time
		i        int
		expected time.Time
	}{
		{
			name:     "Forward 1 month from day 31",
			forDate:  time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "Forward across year boundary",
			forDate:  time.Date(2026, 12, 15, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := config.getNextDate(tc.forDate, tc.i)
			assert.NilError(t, err)
			assert.Equal(t, result, tc.expected)
		})
	}
}

func TestGetNextDateQuarterly(t *testing.T) {
	config := configForInterval(Quarterly, 1, 4)

	testCases := []struct {
		name     string
		forDate  time.Time
		i        int
		expected time.Time
	}{
		{
			name:     "March 31 forward 1 quarter",
			forDate:  time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "March 31 forward 2 quarters",
			forDate:  time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			i:        2,
			expected: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "March 31 forward 3 quarters",
			forDate:  time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			i:        3,
			expected: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "March 31 forward 4 quarters",
			forDate:  time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			i:        4,
			expected: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "May 31 forward 1 quarter",
			forDate:  time.Date(2026, 5, 31, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "January 1 forward 1 quarter",
			forDate:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			i:        1,
			expected: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := config.getNextDate(tc.forDate, tc.i)
			assert.NilError(t, err)
			assert.Equal(t, result, tc.expected)
		})
	}
}

func TestGetNextDateYearly(t *testing.T) {
	config := configForInterval(Yearly, 1, 4)

	result, err := config.getNextDate(time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC), 2)
	assert.NilError(t, err)
	assert.Equal(t, result, time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC))
}

// --- Retention contiguity tests for all intervals ---

func TestGetRetentionPartitionsContiguity(t *testing.T) {
	testCases := []struct {
		name      string
		interval  Interval
		forDate   time.Time
		retention int
	}{
		// Daily
		{
			name:      "Daily from March 31",
			interval:  Daily,
			forDate:   time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			retention: 30,
		},
		{
			name:      "Daily across leap year",
			interval:  Daily,
			forDate:   time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
			retention: 5,
		},
		// Weekly
		{
			name:      "Weekly from March 31",
			interval:  Weekly,
			forDate:   time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			retention: 12,
		},
		// Monthly
		{
			name:      "Monthly from day 31",
			interval:  Monthly,
			forDate:   time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			retention: 12,
		},
		{
			name:      "Monthly from day 15",
			interval:  Monthly,
			forDate:   time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC),
			retention: 24,
		},
		// Quarterly
		{
			name:      "Quarterly from March 31",
			interval:  Quarterly,
			forDate:   time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			retention: 40,
		},
		{
			name:      "Quarterly from May 31",
			interval:  Quarterly,
			forDate:   time.Date(2026, 5, 31, 0, 0, 0, 0, time.UTC),
			retention: 8,
		},
		{
			name:      "Quarterly from December 31",
			interval:  Quarterly,
			forDate:   time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
			retention: 8,
		},
		{
			name:      "Quarterly 404 quarters back to 1925",
			interval:  Quarterly,
			forDate:   time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			retention: 404,
		},
		// Yearly
		{
			name:      "Yearly from December 31",
			interval:  Yearly,
			forDate:   time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
			retention: 10,
		},
		{
			name:      "Yearly from leap day",
			interval:  Yearly,
			forDate:   time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC),
			retention: 5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := configForInterval(tc.interval, tc.retention, 1)
			partitions, err := config.GetRetentionPartitions(tc.forDate)
			assert.NilError(t, err)
			assert.Equal(t, len(partitions), tc.retention)

			for i, p := range partitions {
				assert.Assert(t, p.LowerBound.Before(p.UpperBound), "partition %d: lower bound must be before upper bound", i)
			}

			// Most recent retention partition must be contiguous with current partition
			current, err := config.GeneratePartition(tc.forDate)
			assert.NilError(t, err)
			assert.Equal(t, partitions[0].UpperBound, current.LowerBound,
				"most recent retention partition must be contiguous with current partition")

			// Each successive retention partition must be contiguous
			for i := 1; i < len(partitions); i++ {
				assert.Equal(t, partitions[i].UpperBound, partitions[i-1].LowerBound,
					"retention partition %d upper bound must equal partition %d lower bound", i, i-1)
			}
		})
	}
}

// --- PreProvisioned contiguity tests for all intervals ---

func TestGetPreProvisionedPartitionsContiguity(t *testing.T) {
	testCases := []struct {
		name           string
		interval       Interval
		forDate        time.Time
		preProvisioned int
	}{
		// Daily
		{
			name:           "Daily from March 31",
			interval:       Daily,
			forDate:        time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			preProvisioned: 10,
		},
		// Weekly
		{
			name:           "Weekly from March 31",
			interval:       Weekly,
			forDate:        time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			preProvisioned: 8,
		},
		// Monthly
		{
			name:           "Monthly from day 31",
			interval:       Monthly,
			forDate:        time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			preProvisioned: 12,
		},
		// Quarterly
		{
			name:           "Quarterly from March 31",
			interval:       Quarterly,
			forDate:        time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
			preProvisioned: 8,
		},
		{
			name:           "Quarterly from May 31",
			interval:       Quarterly,
			forDate:        time.Date(2026, 5, 31, 0, 0, 0, 0, time.UTC),
			preProvisioned: 8,
		},
		{
			name:           "Quarterly from December 31",
			interval:       Quarterly,
			forDate:        time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
			preProvisioned: 8,
		},
		// Yearly
		{
			name:           "Yearly from December 31",
			interval:       Yearly,
			forDate:        time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
			preProvisioned: 5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := configForInterval(tc.interval, 1, tc.preProvisioned)
			partitions, err := config.GetPreProvisionedPartitions(tc.forDate)
			assert.NilError(t, err)
			assert.Equal(t, len(partitions), tc.preProvisioned)

			// First pre-provisioned must be contiguous with current partition
			current, err := config.GeneratePartition(tc.forDate)
			assert.NilError(t, err)
			assert.Equal(t, partitions[0].LowerBound, current.UpperBound,
				"first pre-provisioned partition must be contiguous with current partition")

			// Each successive partition must be contiguous
			for i := 1; i < len(partitions); i++ {
				assert.Equal(t, partitions[i].LowerBound, partitions[i-1].UpperBound,
					"partition %d lower bound must equal partition %d upper bound", i, i-1)
			}
		})
	}
}
