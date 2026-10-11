package ppm_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/hook"
	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/pkg/ppm"
	"github.com/stretchr/testify/assert"
)

// hookPartitionConfig returns a partition configuration with the given cleanup policy and hooks.
func hookPartitionConfig(cleanupPolicy partition.CleanupPolicy, hooks *hook.HooksConfig) partition.Configuration {
	return partition.Configuration{
		Schema:         "public",
		Table:          "my_table",
		PartitionKey:   "created_at",
		Interval:       "daily",
		Retention:      1,
		PreProvisioned: 1,
		CleanupPolicy:  cleanupPolicy,
		Hooks:          hooks,
	}
}

// failingShellHook returns a HookEntry with a shell command that always fails.
func failingShellHook(name string, onFailure hook.OnFailure) hook.HookEntry {
	return hook.HookEntry{
		Name:      name,
		Type:      hook.ShellType,
		Timeout:   30 * time.Second,
		OnFailure: onFailure,
		Config: map[string]interface{}{
			"command": "/usr/bin/false",
		},
	}
}

// succeedingShellHook returns a HookEntry with a shell command that always succeeds.
func succeedingShellHook(name string) hook.HookEntry {
	return hook.HookEntry{
		Name:    name,
		Type:    hook.ShellType,
		Timeout: 30 * time.Second,
		Config: map[string]interface{}{
			"command": "/usr/bin/true",
		},
	}
}

// TestCleanupWithBeforeDetachHookFailure verifies that when a before-detach hook fails,
// the detach operation is skipped for that partition but the process continues.
// Requirements: 6.1
func TestCleanupWithBeforeDetachHookFailure(t *testing.T) {
	config := hookPartitionConfig(partition.Drop, &hook.HooksConfig{
		BeforeDetach: []hook.HookEntry{
			failingShellHook("fail-before-detach", ""),
		},
	})

	partitions := map[string]partition.Configuration{
		"unittest": config,
	}

	logger, postgreSQLMock := setupMocks(t)

	// Generate partitions: dayBeforeYesterday should be cleaned
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

	// DetachPartitionConcurrently should NOT be called because before-detach hook fails
	// DropTable should NOT be called either

	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, partitions, time.Now(), "", nil, false)
	err := checker.CleanupPartitions()

	// Should return error because hook failure occurred
	assert.NotNil(t, err, "CleanupPartitions should report an error due to hook failure")
	assert.True(t, errors.Is(err, ppm.ErrPartitionCleanupFailed), "Error should be ErrPartitionCleanupFailed")
	postgreSQLMock.AssertExpectations(t)

	// Verify DetachPartitionConcurrently was never called (detach skipped)
	postgreSQLMock.AssertNotCalled(t, "DetachPartitionConcurrently")
	postgreSQLMock.AssertNotCalled(t, "DropTable")
}

// TestCleanupWithAfterDetachHookFailure verifies that when an after-detach hook fails,
// the drop operation is skipped for that partition.
// Requirements: 6.5
func TestCleanupWithAfterDetachHookFailure(t *testing.T) {
	config := hookPartitionConfig(partition.Drop, &hook.HooksConfig{
		AfterDetach: []hook.HookEntry{
			failingShellHook("fail-after-detach", ""),
		},
	})

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

	// Detach should succeed
	postgreSQLMock.On("DetachPartitionConcurrently",
		dayBeforeYesterdayPartition.Schema,
		dayBeforeYesterdayPartition.Name,
		dayBeforeYesterdayPartition.ParentTable,
	).Return(nil).Once()

	// DropTable should NOT be called because after-detach hook fails

	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, partitions, time.Now(), "", nil, false)
	err := checker.CleanupPartitions()

	// Should return error because hook failure occurred
	assert.NotNil(t, err, "CleanupPartitions should report an error due to hook failure")
	assert.True(t, errors.Is(err, ppm.ErrPartitionCleanupFailed), "Error should be ErrPartitionCleanupFailed")
	postgreSQLMock.AssertExpectations(t)

	// Verify DropTable was never called (drop skipped due to after-detach failure)
	postgreSQLMock.AssertNotCalled(t, "DropTable")
}

// TestCleanupWithOnFailureAbort verifies that when a hook with on_failure=abort fails,
// the entire cleanup process stops immediately.
// Requirements: 6.8
func TestCleanupWithOnFailureAbort(t *testing.T) {
	config := hookPartitionConfig(partition.Drop, &hook.HooksConfig{
		BeforeDetach: []hook.HookEntry{
			failingShellHook("abort-hook", hook.OnFailureAbort),
		},
	})

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

	// No detach or drop operations should be attempted

	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, partitions, time.Now(), "", nil, false)
	err := checker.CleanupPartitions()

	// Should return error containing ErrAbort
	assert.NotNil(t, err, "CleanupPartitions should report an error due to abort")
	assert.True(t, errors.Is(err, ppm.ErrPartitionCleanupFailed), "Error should be ErrPartitionCleanupFailed")
	assert.True(t, errors.Is(err, hook.ErrAbort), "Error should contain ErrAbort")
	postgreSQLMock.AssertExpectations(t)

	// Verify no partition operations were attempted
	postgreSQLMock.AssertNotCalled(t, "DetachPartitionConcurrently")
	postgreSQLMock.AssertNotCalled(t, "DropTable")
}

// TestCleanupWithOnFailureContinue verifies that when a before-hook with on_failure=continue
// fails, the operation still proceeds.
// Requirements: 6.9
func TestCleanupWithOnFailureContinue(t *testing.T) {
	config := hookPartitionConfig(partition.Drop, &hook.HooksConfig{
		BeforeDetach: []hook.HookEntry{
			failingShellHook("continue-hook", hook.OnFailureContinue),
		},
	})

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

	// Detach should still happen because on_failure=continue
	postgreSQLMock.On("DetachPartitionConcurrently",
		dayBeforeYesterdayPartition.Schema,
		dayBeforeYesterdayPartition.Name,
		dayBeforeYesterdayPartition.ParentTable,
	).Return(nil).Once()

	// Drop should also happen since detach succeeded and no after-detach hook failure
	postgreSQLMock.On("DropTable",
		dayBeforeYesterdayPartition.Schema,
		dayBeforeYesterdayPartition.Name,
	).Return(nil).Once()

	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, partitions, time.Now(), "", nil, false)
	err := checker.CleanupPartitions()

	// on_failure=continue means the hook failure is recorded but operations proceed
	// The orchestrator returns nil so hookFailureOccurred stays false for the before-detach
	// However, HasFailures() might still track it... let's check the actual behavior:
	// In the orchestrator, handleFailure with OnFailureContinue returns nil,
	// so hookErr == nil in CleanupPartitions, and hookFailureOccurred is NOT set.
	// The operation proceeds normally.
	assert.Nil(t, err, "CleanupPartitions should succeed when on_failure=continue")
	postgreSQLMock.AssertExpectations(t)
}

// TestCleanupWithDetachPolicy verifies that when cleanup policy is "detach",
// drop-related hooks (before-drop, after-drop) are never executed.
// Requirements: 11.4
func TestCleanupWithDetachPolicy(t *testing.T) {
	config := hookPartitionConfig(partition.Detach, &hook.HooksConfig{
		BeforeDetach: []hook.HookEntry{
			succeedingShellHook("pre-detach"),
		},
		AfterDetach: []hook.HookEntry{
			succeedingShellHook("post-detach"),
		},
		BeforeDrop: []hook.HookEntry{
			// This hook should cause test failure if it were actually executed
			// (since it uses /bin/false), but it should be ignored with detach policy
			failingShellHook("should-not-run-before-drop", ""),
		},
		AfterDrop: []hook.HookEntry{
			failingShellHook("should-not-run-after-drop", ""),
		},
	})

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

	// Detach should be called (detach policy still detaches)
	postgreSQLMock.On("DetachPartitionConcurrently",
		dayBeforeYesterdayPartition.Schema,
		dayBeforeYesterdayPartition.Name,
		dayBeforeYesterdayPartition.ParentTable,
	).Return(nil).Once()

	// DropTable should NOT be called (detach policy, no drop)

	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, partitions, time.Now(), "", nil, false)
	err := checker.CleanupPartitions()

	// Should succeed: before-detach and after-detach hooks succeed, drop hooks are ignored
	assert.Nil(t, err, "CleanupPartitions should succeed with detach policy")
	postgreSQLMock.AssertExpectations(t)

	// Verify drop was never attempted
	postgreSQLMock.AssertNotCalled(t, "DropTable")
}
