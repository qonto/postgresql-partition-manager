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
	currentTime := time.Now()
	provisioningFailed := false

	for name, config := range p.partitions {
		p.logger.Info("Provisioning partition", "partition", name)

		if err := p.provisionPartitionsFor(config, currentTime); err != nil {
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
	provisioningFailed := false

	partitions, err := getExpectedPartitions(config, at)
	if err != nil {
		return fmt.Errorf("could not generate partition to create: %w", err)
	}

	for _, partition := range partitions {
		if err := p.CreatePartition(config, partition); err != nil {
			provisioningFailed = true

			p.logger.Error("Failed to create partition", "error", err, "schema", partition.Schema, "table", partition.Name)
		}
	}

	if provisioningFailed {
		return ErrPartitionProvisioningFailed
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
