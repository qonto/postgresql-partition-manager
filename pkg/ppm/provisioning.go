package ppm

import (
	"errors"
	"fmt"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"github.com/qonto/postgresql-partition-manager/internal/infra/retry"
	"github.com/qonto/postgresql-partition-manager/internal/infra/uuid7"
)

var ErrPartitionProvisioningFailed = errors.New("partition provisioning failed for one or more partition")

func (p PPM) ProvisioningPartitions() error {
	provisioningFailed := false

	for name, config := range p.partitions {
		p.logger.Info("Provisioning partition", "partition", name)

		if err := p.provisionPartitionsFor(config, p.workDate); err != nil {
			provisioningFailed = true
		}
	}

	if provisioningFailed {
		return ErrPartitionProvisioningFailed
	}

	p.logger.Info("All partitions are correctly provisioned")

	return nil
}

func (p PPM) provisionPartitionsFor(config partition.Configuration, at time.Time) error {
	foundPartitions, err := p.ListPartitions(config.Schema, config.Table)
	if err != nil {
		return fmt.Errorf("could not list partitions: %w", err)
	}

	partitions, err := getExpectedPartitions(config, at)
	if err != nil {
		return fmt.Errorf("could not generate partition to create: %w", err)
	}

	currentRange, err := p.getGlobalRange(foundPartitions)
	if err != nil {
		return fmt.Errorf("could not evaluate existing ranges: %w", err)
	}

	p.logger.Info("Current ", "c_range", currentRange.String())

	expectedRange, err := p.getGlobalRange(partitions)
	if err != nil {
		return fmt.Errorf("could not evaluate ranges to create: %w", err)
	}

	p.logger.Info("Expected", "e_range", expectedRange)

	if expectedRange.IsEqual(currentRange) {
		// If expected and current ranges are the same, there is no partition to create
		return nil
	}

	for _, candidate := range partitions {
		p.logger.Info("Candidate", "range", partition.Bounds(candidate.LowerBound, candidate.UpperBound))

		if !candidate.UpperBound.After(currentRange.LowerBound) || !candidate.LowerBound.Before(currentRange.UpperBound) {
			// no intersection between candidate and existing: create new partition
			p.logger.Info("No intersection", "create-range", partition.Bounds(candidate.LowerBound, candidate.UpperBound))

			err = p.CreatePartition(config, candidate)
		}

		if err == nil && candidate.LowerBound.Before(currentRange.LowerBound) && candidate.UpperBound.After(currentRange.LowerBound) {
			// left segment of the candidate outside, of the intersection with existing partitions
			segLeft := candidate
			segLeft.UpperBound = currentRange.LowerBound
			segLeft.Name = fmt.Sprintf("%s_%s_%s", config.Table, segLeft.LowerBound.Format("20060102"), segLeft.UpperBound.Format("20060102"))
			p.logger.Info("Left intersection", "create-range", partition.Bounds(segLeft.LowerBound, segLeft.UpperBound))
			err = p.CreatePartition(config, segLeft)
		}

		if err == nil && candidate.UpperBound.After(currentRange.UpperBound) && candidate.LowerBound.Before(currentRange.UpperBound) {
			// right segment of the candidate, outside of the intersection with existing partitions
			segRight := candidate
			segRight.LowerBound = currentRange.UpperBound
			segRight.Name = fmt.Sprintf("%s_%s_%s", config.Table, segRight.LowerBound.Format("20060102"), segRight.UpperBound.Format("20060102"))
			p.logger.Info("Right intersection", "create-range", partition.Bounds(segRight.LowerBound, segRight.UpperBound))
			err = p.CreatePartition(config, segRight)
		}

		if err != nil {
			p.logger.Error("Failed to create partition", "error", err)

			return ErrPartitionProvisioningFailed
		}
	}

	return nil
}

func (p PPM) CreatePartition(partitionConfiguration partition.Configuration, partition partition.Partition) error {
	p.logger.Debug("Creating partition", "schema", partition.Schema, "table", partition.Name)

	tableExists, err := p.db.IsTableExists(partition.Schema, partition.Name)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if !tableExists {
		err := p.db.CreateTableLikeTable(partition.Schema, partition.Name, partition.ParentTable)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}

		p.logger.Info("Table created", "schema", partition.Schema, "table", partition.Name)
	} else {
		p.logger.Info("Table already exists, skip", "schema", partition.Schema, "table", partition.Name)
	}

	partitionAttached, err := p.db.IsPartitionAttached(partition.Schema, partition.Name)
	if err != nil {
		return fmt.Errorf("failed to check partition attachment status: %w", err)
	}

	if partitionAttached {
		p.logger.Info("Table is already attached to the parent table, skip", "schema", partition.Schema, "table", partition.Name)

		return nil
	}

	_, partitionKey, err := p.db.GetPartitionSettings(partition.Schema, partition.ParentTable)
	if err != nil {
		return fmt.Errorf("failed to get partition settings: %w", err)
	}

	partitionKeyType, err := p.db.GetColumnDataType(partition.Schema, partition.ParentTable, partitionKey)
	if err != nil {
		return fmt.Errorf("failed to get partition settings: %w", err)
	}

	var lowerBound, upperBound string

	switch partitionKeyType {
	case postgresql.Date:
		lowerBound = partition.LowerBound.Format("2006-01-02")
		upperBound = partition.UpperBound.Format("2006-01-02")
	case postgresql.DateTime, postgresql.DateTimeWithTZ:
		lowerBound = partition.LowerBound.Format("2006-01-02 00:00:00")
		upperBound = partition.UpperBound.Format("2006-01-02 00:00:00")
	case postgresql.UUID:
		lowerBound = uuid7.FromTime(partition.LowerBound)
		upperBound = uuid7.FromTime(partition.UpperBound)
	default:
		return ErrUnsupportedPartitionStrategy
	}

	maxRetries := 3

	err = retry.WithRetry(maxRetries, func(attempt int) error {
		err := p.db.AttachPartition(partition.Schema, partition.Name, partition.ParentTable, lowerBound, upperBound)
		if err != nil {
			p.logger.Warn("fail to attach partition", "error", err, "schema", partition.Schema, "table", partition.Name, "attempt", attempt, "max_retries", maxRetries)

			return fmt.Errorf("fail to attach partition: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to attach partition after retries: %w", err)
	}

	p.logger.Info("Partition attached to parent table", "schema", partition.Schema, "table", partition.Name, "parent_table", partition.ParentTable)

	return nil
}
