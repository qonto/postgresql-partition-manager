package ppm

import (
	"errors"
	"fmt"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
)

var (
	ErrUnsupportedKeyDataType        = errors.New("unsupported partitioning column type on the table")
	ErrUnsupportedPartitionStrategy  = errors.New("unsupported partitioning strategy on the table")
	ErrPartitionKeyMismatch          = errors.New("mismatch of partition keys between settings and the table")
	ErrUnexpectedOrMissingPartitions = errors.New("unexpected or missing partitions")
	ErrInvalidPartitionConfiguration = errors.New("at least one partition contains an invalid configuration")
)

func (p *PPM) CheckPartitions() error {
	partitionContainsAnError := false

	for name, config := range p.partitions {
		p.logger.Info("Checking partition", "partition", name)

		err := p.checkPartition(config)
		if err != nil {
			partitionContainsAnError = true

			p.logger.Error(err.Error(), "schema", config.Schema, "table", config.Table)
		}
	}

	if partitionContainsAnError {
		return ErrInvalidPartitionConfiguration
	}

	return nil
}

func (p *PPM) checkPartition(config postgresql.PartitionConfiguration) error {
	p.logger.Debug("Checking partition", "schema", config.Schema, "table", config.Table)

	err := p.checkPartitionKey(config)
	if err != nil {
		return fmt.Errorf("failed to check partition key: %w", err)
	}

	err = p.checkPartitionsConfiguration(config)
	if err != nil {
		return fmt.Errorf("failed to check partitions configuration: %w", err)
	}

	p.logger.Debug("Partitions match the configuration", "schema", config.Schema, "table", config.Table)

	return nil
}

func (p *PPM) checkPartitionKey(config postgresql.PartitionConfiguration) error {
	partition := postgresql.Partition{
		Schema:      config.Schema,
		ParentTable: config.Table,
		Name:        config.Table,
	}

	partitionSettings, err := p.db.GetPartitionSettings(partition.GetParentTable())
	if err != nil {
		return fmt.Errorf("failed to get partition settings: %w", err)
	}

	p.logger.Debug("Partition configuration found", "schema", partition.Schema, "table", partition.Name, "partition_key", partitionSettings.Key, "partition_key_type", partitionSettings.KeyType, "partition_strategy", partitionSettings.Strategy)

	if partitionSettings.Key != config.PartitionKey {
		p.logger.Warn("Partition key mismatch", "expected", config.PartitionKey, "current", partitionSettings.Key)

		return ErrPartitionKeyMismatch
	}

	if !partitionSettings.SupportedStrategy() {
		return ErrUnsupportedPartitionStrategy
	}

	if !partitionSettings.SupportedKeyDataType() {
		return ErrUnsupportedKeyDataType
	}

	return nil
}

func (p *PPM) comparePartitions(existingTables, expectedTables []postgresql.Partition) (unexpectedTables, missingTables, incorrectBounds []postgresql.Partition) {
	// Maps for tracking presence
	existing := make(map[string]postgresql.Partition)
	expectedAndExists := make(map[string]bool)

	for _, t := range existingTables {
		existing[t.Name] = t
	}

	for _, t := range expectedTables {
		if _, found := existing[t.Name]; found {
			expectedAndExists[t.Name] = true
			incorrectBound := false

			if existing[t.Name].UpperBound != t.UpperBound {
				incorrectBound = true

				p.logger.Warn("Incorrect upper partition bound", "schema", t.Schema, "table", t.Name, "current_bound", existing[t.Name].UpperBound, "expected_bound", t.UpperBound)
			}

			if existing[t.Name].LowerBound != t.LowerBound {
				incorrectBound = true

				p.logger.Warn("Incorrect lower partition bound", "schema", t.Schema, "table", t.Name, "current_bound", existing[t.Name].LowerBound, "expected_bound", t.LowerBound)
			}

			if incorrectBound {
				incorrectBounds = append(incorrectBounds, t)
			}
		} else {
			missingTables = append(missingTables, t)
		}
	}

	for _, t := range existingTables {
		if _, found := expectedAndExists[t.Name]; !found {
			// Only in existingTables and not in both
			unexpectedTables = append(unexpectedTables, t)
		}
	}

	return unexpectedTables, missingTables, incorrectBounds
}

func (p *PPM) checkPartitionsConfiguration(partition postgresql.PartitionConfiguration) error {
	partitionContainAnError := false

	currentTime := time.Now()

	expectedPartitions, err := getExpectedPartitions(partition, currentTime)
	if err != nil {
		return fmt.Errorf("could not generate expected partitions: %w", err)
	}

	foundPartitions, err := p.db.ListPartitions(postgresql.Table{Schema: partition.Schema, Name: partition.Table})
	if err != nil {
		return fmt.Errorf("could not list partitions: %w", err)
	}

	unexpected, missing, incorrectBound := p.comparePartitions(foundPartitions, expectedPartitions)

	if len(unexpected) > 0 {
		partitionContainAnError = true

		p.logger.Warn("Found unexpected tables", "tables", unexpected)
	}

	if len(missing) > 0 {
		partitionContainAnError = true

		p.logger.Warn("Found missing tables", "tables", missing)
	}

	if len(incorrectBound) > 0 {
		partitionContainAnError = true

		p.logger.Warn("Found partitions with incorrect bound", "tables", incorrectBound)
	}

	if partitionContainAnError {
		return ErrUnexpectedOrMissingPartitions
	}

	return nil
}
