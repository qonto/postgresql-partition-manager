package ppm

import (
	"errors"
	"fmt"

	"github.com/qonto/postgresql-partition-manager/internal/infra/hook"
	partition_pkg "github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/internal/infra/retry"
)

var ErrPartitionCleanupFailed = errors.New("at least one partition could not be cleaned")

// cleanupState accumulates failure signals across the whole cleanup run to compute the exit code.
type cleanupState struct {
	hookFailure    bool
	partitionError bool
}

func (p PPM) CleanupPartitions() error {
	metrics := hook.NewMetricsCollector(p.logger)
	state := &cleanupState{}

	if p.dryRun {
		p.logger.Info("[DRY-RUN] Starting dry-run cleanup - no partitions will be modified, no hooks will be executed")
	}

	for name, config := range p.partitions {
		aborted, err := p.cleanupPartitionSet(name, config, metrics, state)
		if err != nil {
			return err
		}

		if aborted {
			metrics.LogSummary()

			return fmt.Errorf("%w: %w", ErrPartitionCleanupFailed, hook.ErrAbort)
		}
	}

	// Log execution summary at end of cleanup
	if metrics.Summary().TotalExecuted > 0 {
		metrics.LogSummary()
	}

	// Return error if any hook or operation failed during the run (for non-zero exit code).
	// In dry-run mode, only template/configuration errors set hookFailure (Requirement 17.6).
	if state.hookFailure || state.partitionError {
		return ErrPartitionCleanupFailed
	}

	if p.dryRun {
		p.logger.Info("[DRY-RUN] Dry-run cleanup complete - no changes were made")
	} else {
		p.logger.Info("All partitions are cleaned")
	}

	return nil
}

// cleanupPartitionSet processes one partition configuration: it computes which existing
// partitions fall outside the expected retention range and removes each of them.
// It returns true if a hook requested an abort of the entire cleanup process.
func (p PPM) cleanupPartitionSet(name string, config partition_pkg.Configuration, metrics *hook.MetricsCollector, state *cleanupState) (abort bool, err error) {
	p.logger.Info("Cleaning partition", "partition", name)

	foundPartitions, err := p.ListPartitions(config.Schema, config.Table)
	if err != nil {
		return false, fmt.Errorf("could not list partitions: %w", err)
	}

	currentRange, err := p.getGlobalRange(foundPartitions)
	if err != nil {
		return false, fmt.Errorf("could not evaluate existing ranges: %w", err)
	}

	p.logger.Info("Current ", "c_range", currentRange.String())

	expectedPartitions, err := getExpectedPartitions(config, p.workDate)
	if err != nil {
		return false, fmt.Errorf("could not generate expected partitions: %w", err)
	}

	expectedRange, err := p.getGlobalRange(expectedPartitions)
	if err != nil {
		return false, fmt.Errorf("could not evaluate ranges to create: %w", err)
	}

	p.logger.Info("Expected", "e_range", expectedRange)

	if expectedRange.IsEqual(currentRange) {
		return false, nil // nothing to do on this partition set
	}

	orchestrator := p.newHookOrchestrator(name, config, metrics)

	for _, part := range foundPartitions {
		if !isOutsideExpectedRange(part, expectedRange) {
			continue
		}

		p.logger.Info("No intersection", "remove-range", partition_pkg.Bounds(part.LowerBound, part.UpperBound))

		if p.removePartition(name, config, part, orchestrator, state) == outcomeAbort {
			return true, nil
		}
	}

	return false, nil
}

// removePartition runs the full detach/drop lifecycle (with surrounding hooks) for a single
// partition. Hooks run outside any PostgreSQL transaction.
func (p PPM) removePartition(name string, config partition_pkg.Configuration, part partition_pkg.Partition, orchestrator *hook.Orchestrator, state *cleanupState) partitionOutcome {
	partCtx := p.buildPartitionContext(name, config, part)

	if o := p.runHook(func() error { return orchestrator.ExecuteBeforeDetach(p.ctx, partCtx) }, &state.hookFailure, part, "Before-detach hook failed, skipping detach"); o != outcomeCompleted {
		return o
	}

	if !p.performDetach(part, state) {
		return outcomeSkipped
	}

	if o := p.runHook(func() error { return orchestrator.ExecuteAfterDetach(p.ctx, partCtx) }, &state.hookFailure, part, "After-detach hook failed, skipping drop"); o != outcomeCompleted {
		return o
	}

	// Drop-related operations only when cleanup policy is drop
	if config.CleanupPolicy != partition_pkg.Drop {
		return outcomeCompleted
	}

	if o := p.runHook(func() error { return orchestrator.ExecuteBeforeDrop(p.ctx, partCtx) }, &state.hookFailure, part, "Before-drop hook failed, skipping drop"); o != outcomeCompleted {
		return o
	}

	if !p.performDrop(part, state) {
		return outcomeSkipped
	}

	// After-drop failure: log warning, operation already done
	if o := p.runHook(func() error { return orchestrator.ExecuteAfterDrop(p.ctx, partCtx) }, &state.hookFailure, part, "After-drop hook failed"); o == outcomeAbort {
		return outcomeAbort
	}

	return outcomeCompleted
}

// performDetach detaches the partition, or logs the intended action in dry-run mode.
// It returns false if the detach failed and the partition should be skipped.
func (p PPM) performDetach(part partition_pkg.Partition, state *cleanupState) bool {
	if p.dryRun {
		p.logger.Info("[DRY-RUN] Would detach partition", "schema", part.Schema, "table", part.Name, "parent_table", part.ParentTable)

		return true
	}

	if err := p.DetachPartition(part); err != nil {
		state.partitionError = true

		p.logger.Error("Failed to detach partition", "schema", part.Schema, "table", part.Name, "error", err)

		return false
	}

	p.logger.Info("Partition detached", "schema", part.Schema, "table", part.Name, "parent_table", part.ParentTable)

	return true
}

// performDrop drops the partition, or logs the intended action in dry-run mode.
// It returns false if the drop failed and the partition should be skipped.
func (p PPM) performDrop(part partition_pkg.Partition, state *cleanupState) bool {
	if p.dryRun {
		p.logger.Info("[DRY-RUN] Would drop partition", "schema", part.Schema, "table", part.Name, "parent_table", part.ParentTable)

		return true
	}

	if err := p.DeletePartition(part); err != nil {
		state.partitionError = true

		p.logger.Error("Failed to delete partition", "schema", part.Schema, "table", part.Name, "error", err)

		return false
	}

	p.logger.Info("Partition deleted", "schema", part.Schema, "table", part.Name, "parent_table", part.ParentTable)

	return true
}

// isOutsideExpectedRange reports whether the partition's bounds fall entirely outside the
// expected retention range, meaning the partition can be removed.
func isOutsideExpectedRange(part partition_pkg.Partition, expectedRange partition_pkg.PartitionRange) bool {
	return !part.UpperBound.After(expectedRange.LowerBound) || !part.LowerBound.Before(expectedRange.UpperBound)
}

func (p PPM) DetachPartition(partition partition_pkg.Partition) error {
	p.logger.Debug("Detach partition", "schema", partition.Schema, "table", partition.Name)

	maxRetries := 3

	err := retry.WithRetry(maxRetries, func(attempt int) error {
		err := p.db.DetachPartitionConcurrently(partition.Schema, partition.Name, partition.ParentTable)
		if err != nil {
			// detachPartitionConcurrently() could fail if the specified partition is in pending detach status
			// It could occurred when a previous detach partition concurrently operation was canceled or interrupted
			// It prevent any other detach operations on the table
			// More info: https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-DETACH-PARTITION
			// To unblock the situation, we try to finalize the detach operation on Object Not In Prerequisite State error
			if isPostgreSQLErrorCode(err, ObjectNotInPrerequisiteStatePostgreSQLErrorCode) {
				p.logger.Warn("Table is already pending detach in partitioned, retry with finalize", "error", err, "schema", partition.Schema, "table", partition.Name)

				finalizeErr := p.db.FinalizePartitionDetach(partition.Schema, partition.Name, partition.ParentTable)
				if finalizeErr == nil {
					err = nil // Returns a success since the partition detach operation has been completed
				}
			} else {
				p.logger.Warn("Fail to detach partition", "error", err, "schema", partition.Schema, "table", partition.Name, "attempt", attempt, "max_retries", maxRetries)
			}
		}

		return err
	})
	if err != nil {
		return fmt.Errorf("failed to detach partition after retries: %w", err)
	}

	return nil
}

func (p PPM) DeletePartition(partition partition_pkg.Partition) error {
	p.logger.Debug("Deleting partition", "schema", partition.Schema, "table", partition.Name)

	maxRetries := 3

	err := retry.WithRetry(maxRetries, func(attempt int) error {
		err := p.db.DropTable(partition.Schema, partition.Name)
		if err != nil {
			p.logger.Warn("Fail to drop table", "error", err, "schema", partition.Schema, "table", partition.Name, "attempt", attempt, "max_retries", maxRetries)

			return fmt.Errorf("fail to drop table: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to drop table after retries: %w", err)
	}

	return nil
}
