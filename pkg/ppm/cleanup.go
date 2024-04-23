package ppm

import (
	"errors"
	"fmt"
	"time"

	partition_pkg "github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/internal/infra/retry"
)

var ErrPartitionCleanupFailed = errors.New("at least one partition could not be cleaned")

func (p PPM) CleanupPartitions() error {
	currentTime := time.Now()
	partitionContainAnError := false

	for name, config := range p.partitions {
		p.logger.Info("Cleaning partition", "partition", name)

		expectedPartitions, err := getExpectedPartitions(config, currentTime)
		if err != nil {
			return fmt.Errorf("could not generate expected partitions: %w", err)
		}

		foundPartitions, err := p.ListPartitions(config.Schema, config.Table)
		if err != nil {
			return fmt.Errorf("could not list partitions: %w", err)
		}

		unexpected, _, _ := p.comparePartitions(foundPartitions, expectedPartitions)

		for _, partition := range unexpected {
			err := p.DetachPartition(partition)
			if err != nil {
				partitionContainAnError = true

				p.logger.Error("Failed to detach partition", "schema", partition.Schema, "table", partition.Name, "error", err)

				continue
			}

			p.logger.Info("Partition detached", "schema", partition.Schema, "table", partition.Name, "parent_table", partition.ParentTable)

			if config.CleanupPolicy == partition_pkg.Drop {
				err := p.DeletePartition(partition)
				if err != nil {
					partitionContainAnError = true

					p.logger.Error("Failed to delete partition", "schema", partition.Schema, "table", partition.Name, "error", err)

					continue
				}

				p.logger.Info("Partition deleted", "schema", partition.Schema, "table", partition.Name, "parent_table", partition.ParentTable)
			}
		}
	}

	if partitionContainAnError {
		return ErrPartitionCleanupFailed
	}

	return nil
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
