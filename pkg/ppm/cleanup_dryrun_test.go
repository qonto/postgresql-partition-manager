package ppm_test

import (
	"context"
	"testing"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/hook"
	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/pkg/ppm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Cleanup dry-run integration tests ---
// Validates: Requirements 17.2, 17.3, 17.4, 17.5

// TestCleanupDryRun_PartitionsNotModified verifies that with dryRun=true,
// DetachPartitionConcurrently and DropTable are never called.
// Validates: Requirement 17.4
func TestCleanupDryRun_PartitionsNotModified(t *testing.T) {
	config := partition.Configuration{
		Schema:         "public",
		Table:          "my_table",
		PartitionKey:   "created_at",
		Interval:       "daily",
		Retention:      1,
		PreProvisioned: 1,
		CleanupPolicy:  partition.Drop,
	}

	partitions := map[string]partition.Configuration{
		"unittest": config,
	}

	logger, postgreSQLMock := setupMocks(t)

	// Generate partitions: dayBeforeYesterday should be cleaned in normal mode
	dayBeforeYesterdayPartition, _ := config.GeneratePartition(dayBeforeYesterday)
	yesterdayPartition, _ := config.GeneratePartition(yesterday)
	currentPartition, _ := config.GeneratePartition(today)
	tomorrowPartition, _ := config.GeneratePartition(tomorrow)

	existingPartitions := []partition.Partition{
		dayBeforeYesterdayPartition,
		yesterdayPartition,
		currentPartition,
		tomorrowPartition,
	}

	postgreSQLMock.On("ListPartitions", config.Schema, config.Table).
		Return(partitionResultToPartition(t, existingPartitions), nil).Once()

	// Create PPM with dryRun=true
	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, partitions, time.Now(), "", nil, true)
	err := checker.CleanupPartitions()

	// Should succeed without error
	assert.Nil(t, err, "CleanupPartitions in dry-run mode should succeed")
	postgreSQLMock.AssertExpectations(t)

	// Verify no partition modifications occurred
	postgreSQLMock.AssertNotCalled(t, "DetachPartitionConcurrently")
	postgreSQLMock.AssertNotCalled(t, "DropTable")
}

// TestCleanupDryRun_WithHooksNoExecution verifies that the full cleanup flow
// works correctly in dry-run mode with hooks configured (no errors, no execution).
// Validates: Requirements 17.3, 17.5
func TestCleanupDryRun_WithHooksNoExecution(t *testing.T) {
	globalHooks := &hook.HooksConfig{
		BeforeDetach: []hook.HookEntry{
			{
				Name:    "backup-before-detach",
				Type:    hook.ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config: map[string]interface{}{
					"command":               "/usr/bin/backup",
					"args":                  []interface{}{"--schema", "{{.Schema}}", "--table", "{{.Table}}"},
					"propagate-credentials": true,
				},
				Retry: hook.RetryConfig{Attempts: 0, Backoff: hook.BackoffFixed, InitialDelay: hook.DefaultInitialDelay, MaxDelay: hook.DefaultMaxDelay},
			},
		},
		AfterDetach: []hook.HookEntry{
			{
				Name:    "vacuum-after-detach",
				Type:    hook.PostgreSQLType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config: map[string]interface{}{
					"sql_query": "VACUUM ANALYZE {{.Schema}}.{{.Table}}",
				},
				Retry: hook.RetryConfig{Attempts: 0, Backoff: hook.BackoffFixed, InitialDelay: hook.DefaultInitialDelay, MaxDelay: hook.DefaultMaxDelay},
			},
		},
		BeforeDrop: []hook.HookEntry{
			{
				Name:    "notify-before-drop",
				Type:    hook.ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config: map[string]interface{}{
					"command": "/usr/bin/notify",
					"args":    []interface{}{"--partition", "{{.PartitionName}}"},
				},
				Retry: hook.RetryConfig{Attempts: 0, Backoff: hook.BackoffFixed, InitialDelay: hook.DefaultInitialDelay, MaxDelay: hook.DefaultMaxDelay},
			},
		},
		AfterDrop: []hook.HookEntry{
			{
				Name:    "cleanup-after-drop",
				Type:    hook.ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config: map[string]interface{}{
					"command": "/usr/bin/cleanup-metadata",
				},
				Retry: hook.RetryConfig{Attempts: 0, Backoff: hook.BackoffFixed, InitialDelay: hook.DefaultInitialDelay, MaxDelay: hook.DefaultMaxDelay},
			},
		},
	}

	config := partition.Configuration{
		Schema:         "public",
		Table:          "my_table",
		PartitionKey:   "created_at",
		Interval:       "daily",
		Retention:      1,
		PreProvisioned: 1,
		CleanupPolicy:  partition.Drop,
	}

	partitions := map[string]partition.Configuration{
		"unittest": config,
	}

	logger, postgreSQLMock := setupMocks(t)

	dayBeforeYesterdayPartition, _ := config.GeneratePartition(dayBeforeYesterday)
	yesterdayPartition, _ := config.GeneratePartition(yesterday)
	currentPartition, _ := config.GeneratePartition(today)
	tomorrowPartition, _ := config.GeneratePartition(tomorrow)

	existingPartitions := []partition.Partition{
		dayBeforeYesterdayPartition,
		yesterdayPartition,
		currentPartition,
		tomorrowPartition,
	}

	postgreSQLMock.On("ListPartitions", config.Schema, config.Table).
		Return(partitionResultToPartition(t, existingPartitions), nil).Once()

	connURL := "postgresql://user:pass@localhost:5432/mydb"
	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, partitions, time.Now(), connURL, globalHooks, true)
	err := checker.CleanupPartitions()

	// Should succeed without error (all templates resolve correctly)
	assert.Nil(t, err, "CleanupPartitions in dry-run mode with hooks should succeed")
	postgreSQLMock.AssertExpectations(t)

	// Verify no partition modifications occurred
	postgreSQLMock.AssertNotCalled(t, "DetachPartitionConcurrently")
	postgreSQLMock.AssertNotCalled(t, "DropTable")
}

// TestCleanupDryRun_ExitsZeroWithValidConfig verifies that CleanupPartitions returns nil
// when dry-run is enabled and the configuration is valid.
// Validates: Requirement 17.3
func TestCleanupDryRun_ExitsZeroWithValidConfig(t *testing.T) {
	config := partition.Configuration{
		Schema:         "public",
		Table:          "my_table",
		PartitionKey:   "created_at",
		Interval:       "daily",
		Retention:      1,
		PreProvisioned: 1,
		CleanupPolicy:  partition.Detach,
	}

	partitions := map[string]partition.Configuration{
		"unittest": config,
	}

	logger, postgreSQLMock := setupMocks(t)

	dayBeforeYesterdayPartition, _ := config.GeneratePartition(dayBeforeYesterday)
	yesterdayPartition, _ := config.GeneratePartition(yesterday)
	currentPartition, _ := config.GeneratePartition(today)
	tomorrowPartition, _ := config.GeneratePartition(tomorrow)

	existingPartitions := []partition.Partition{
		dayBeforeYesterdayPartition,
		yesterdayPartition,
		currentPartition,
		tomorrowPartition,
	}

	postgreSQLMock.On("ListPartitions", config.Schema, config.Table).
		Return(partitionResultToPartition(t, existingPartitions), nil).Once()

	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, partitions, time.Now(), "", nil, true)
	err := checker.CleanupPartitions()

	// Must return nil (exit code 0)
	require.NoError(t, err, "CleanupPartitions in dry-run should return nil for valid config")
	postgreSQLMock.AssertExpectations(t)

	// Verify no detach occurred (detach policy, but dry-run skips it)
	postgreSQLMock.AssertNotCalled(t, "DetachPartitionConcurrently")
}

// TestCleanupDryRun_TemplateErrorReturnsError verifies that when hooks have undefined
// template variables, dry-run still returns an error (non-zero exit).
// Validates: Requirement 17.2 (template errors are still reported)
func TestCleanupDryRun_TemplateErrorReturnsError(t *testing.T) {
	globalHooks := &hook.HooksConfig{
		BeforeDetach: []hook.HookEntry{
			{
				Name:    "bad-template-hook",
				Type:    hook.ShellType,
				Enabled: boolPtr(true),
				Timeout: 30 * time.Second,
				Config: map[string]interface{}{
					"command": "echo {{.UndefinedVariable}}",
				},
				Retry: hook.RetryConfig{Attempts: 0, Backoff: hook.BackoffFixed, InitialDelay: hook.DefaultInitialDelay, MaxDelay: hook.DefaultMaxDelay},
			},
		},
	}

	config := partition.Configuration{
		Schema:         "public",
		Table:          "my_table",
		PartitionKey:   "created_at",
		Interval:       "daily",
		Retention:      1,
		PreProvisioned: 1,
		CleanupPolicy:  partition.Drop,
	}

	partitions := map[string]partition.Configuration{
		"unittest": config,
	}

	logger, postgreSQLMock := setupMocks(t)

	dayBeforeYesterdayPartition, _ := config.GeneratePartition(dayBeforeYesterday)
	yesterdayPartition, _ := config.GeneratePartition(yesterday)
	currentPartition, _ := config.GeneratePartition(today)
	tomorrowPartition, _ := config.GeneratePartition(tomorrow)

	existingPartitions := []partition.Partition{
		dayBeforeYesterdayPartition,
		yesterdayPartition,
		currentPartition,
		tomorrowPartition,
	}

	postgreSQLMock.On("ListPartitions", config.Schema, config.Table).
		Return(partitionResultToPartition(t, existingPartitions), nil).Once()

	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, partitions, time.Now(), "", globalHooks, true)
	err := checker.CleanupPartitions()

	// Should return an error due to undefined template variable
	require.Error(t, err, "CleanupPartitions in dry-run should return error for undefined template variables")
	postgreSQLMock.AssertExpectations(t)

	// Verify no partition modifications occurred
	postgreSQLMock.AssertNotCalled(t, "DetachPartitionConcurrently")
	postgreSQLMock.AssertNotCalled(t, "DropTable")
}

// boolPtr is a helper to create a pointer to a bool value.
func boolPtr(b bool) *bool {
	return &b
}
