package ppm

import (
	"errors"
	"fmt"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
)

var ErrPartitionCleanupFailed = errors.New("at least one partition contains could not be cleaned")

func (p PPM) CleanupPartitions() error {
	currentTime := time.Now()
	partitionContainAnError := false

	for name, config := range p.partitions {
		p.logger.Info("Cleaning partition", "partition", name)

		expectedPartitions, err := getExpectedPartitions(config, currentTime)
		if err != nil {
			return fmt.Errorf("could not generate expected partitions: %w", err)
		}

		parentTable := postgresql.Table{Schema: config.Schema, Name: config.Table}

		foundPartitions, err := p.db.ListPartitions(parentTable)
		if err != nil {
			return fmt.Errorf("could not list partitions: %w", err)
		}

		unexpected, _, _ := p.comparePartitions(foundPartitions, expectedPartitions)

		for _, partition := range unexpected {
			err := p.db.DetachPartition(partition)
			if err != nil {
				partitionContainAnError = true

				p.logger.Error("Failed to detach partition", "schema", partition.Schema, "table", partition.Name, "error", err)

				continue
			}

			p.logger.Info("Partition detached", "schema", partition.Schema, "table", partition.Name, "parent_table", partition.GetParentTable().Name)

			if config.CleanupPolicy == postgresql.DropCleanupPolicy {
				err := p.db.DeletePartition(partition)
				if err != nil {
					partitionContainAnError = true

					p.logger.Error("Failed to delete partition", "schema", partition.Schema, "table", partition.Name, "error", err)

					continue
				}

				p.logger.Info("Partition deleted", "schema", partition.Schema, "table", partition.Name, "parent_table", partition.GetParentTable().Name)
			}
		}
	}

	if partitionContainAnError {
		return ErrPartitionCleanupFailed
	}

	return nil
}
