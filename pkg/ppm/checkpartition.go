package ppm

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
)

var (
	ErrUnsupportedKeyDataType        = errors.New("unsupported partitioning column type on the table")
	ErrUnsupportedPartitionStrategy  = errors.New("unsupported partitioning strategy on the table")
	ErrPartitionKeyMismatch          = errors.New("mismatch of partition keys between parameters and table")
	ErrUnexpectedOrMissingPartitions = errors.New("unexpected or missing partitions")
	ErrInvalidPartitionConfiguration = errors.New("at least one partition contains an invalid configuration")
	ErrPartitionGap                  = errors.New("gap found in partitions")
	ErrIncoherentBounds              = errors.New("lower bound greater or equal than upper bound")
)

var SupportedPartitionKeyDataType = []postgresql.ColumnType{
	postgresql.Date,
	postgresql.DateTime,
	postgresql.DateTimeWithTZ,
	postgresql.UUID,
}

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

	p.logger.Info("All partitions are correctly configured")

	return nil
}

func (p *PPM) checkPartition(config partition.Configuration) error {
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

func (p *PPM) checkPartitionKey(config partition.Configuration) error {
	keyDataType, err := p.db.GetColumnDataType(config.Schema, config.Table, config.PartitionKey)
	if err != nil {
		return fmt.Errorf("failed to get partition column type: %w", err)
	}

	partitionStrategy, partitionKey, err := p.db.GetPartitionSettings(config.Schema, config.Table)
	if err != nil {
		return fmt.Errorf("failed to get partition settings: %w", err)
	}

	p.logger.Debug("Partition configuration found", "schema", config.Schema, "table", config.Table, "partition_key", config.PartitionKey, "partition_key_type", keyDataType, "partition_strategy", partitionStrategy)

	if partitionKey != config.PartitionKey {
		p.logger.Warn("Partition key mismatch", "expected", config.PartitionKey, "current", partitionKey)

		return ErrPartitionKeyMismatch
	}

	if !IsSupportedStrategy(partitionStrategy) {
		p.logger.Warn("Unsupported partition strategy", "strategy", partitionStrategy)

		return ErrUnsupportedPartitionStrategy
	}

	if !IsSupportedKeyDataType(keyDataType) {
		p.logger.Warn("Unsupported partition key data type", "partition_key_data_type", keyDataType)

		return ErrUnsupportedKeyDataType
	}

	return nil
}

func IsSupportedStrategy(strategy string) bool {
	return strategy == string(partition.Range)
}

func IsSupportedKeyDataType(dataType postgresql.ColumnType) bool {
	return slices.Contains(SupportedPartitionKeyDataType, dataType)
}

func (p *PPM) comparePartitions(existingTables, expectedTables []partition.Partition) (unexpectedTables, missingTables, incorrectBounds []partition.Partition) {
	existing := make(map[string]partition.Partition)
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

func (p *PPM) ListPartitions(schema, table string) (partitions []partition.Partition, err error) {
	rawPartitions, err := p.db.ListPartitions(schema, table)
	if err != nil {
		return nil, fmt.Errorf("could not list partitions: %w", err)
	}

	for _, p := range rawPartitions {
		lowerBound, upperBound, err := parseBounds(p)
		if err != nil {
			return nil, fmt.Errorf("could not parse bounds: %w", err)
		}

		partitions = append(partitions, partition.Partition{
			Schema:      p.Schema,
			Name:        p.Name,
			ParentTable: p.ParentTable,
			LowerBound:  lowerBound,
			UpperBound:  upperBound,
		})
	}

	return partitions, nil
}

func (p *PPM) checkPartitionsConfiguration(config partition.Configuration) error {
	partitionContainAnError := false

	currentTime := time.Now()

	expectedPartitions, err := getExpectedPartitions(config, currentTime)
	if err != nil {
		return fmt.Errorf("could not generate expected partitions: %w", err)
	}

	foundPartitions, err := p.ListPartitions(config.Schema, config.Table)
	if err != nil {
		return fmt.Errorf("could not list partitions: %w", err)
	}

	existingRange, err := p.getGlobalRange(foundPartitions)
	if err != nil {
		return fmt.Errorf("incorrect set of existing partitions: %w", err)
	}

	p.logger.Info("Existing range", "range", existingRange)

	expectedRange, err := p.getGlobalRange(expectedPartitions)
	if err != nil {
		return fmt.Errorf("incorrect set of expected partitions: %w", err)
	}

	p.logger.Info("Expected range", "expected", expectedRange)

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

/* Return the lower/upper bound of all partitions combined. Any discontinuity is an error */
func (p *PPM) getGlobalRange(partitions []partition.Partition) (r partition.PartitionRange, err error) {
	var minBound, maxBound time.Time

	/* sort by lower bounds */
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].LowerBound.Before(partitions[j].LowerBound)
	})

	/* check continuity */
	for i, part := range partitions {
		if i == 0 {
			minBound = part.LowerBound
			maxBound = part.UpperBound
		} else {
			if part.LowerBound.Before(minBound) {
				minBound = part.LowerBound
			}

			if part.UpperBound.After(maxBound) {
				maxBound = part.UpperBound
			}
		}

		if i > 0 && (partitions[i-1].UpperBound != part.LowerBound) {
			/* a gap has been detected between the ranges of consecutive partitions */
			p.logger.Error("Partition Gap", "lower end", partitions[i-1].UpperBound, "upper end", part.LowerBound)

			return partition.PartitionRange{LowerBound: minBound, UpperBound: maxBound}, ErrPartitionGap
		}

		if part.LowerBound.After(part.UpperBound) || part.LowerBound.Equal(part.UpperBound) {
			/* the lower bound is greater or equal than
			   the upper bound: this should never happen
			   for existing partitions */
			p.logger.Error("Partition Gap", "lower end", part.LowerBound, "upper end", part.UpperBound)

			return partition.PartitionRange{LowerBound: minBound, UpperBound: maxBound}, ErrIncoherentBounds
		}
	}

	return partition.PartitionRange{LowerBound: minBound, UpperBound: maxBound}, nil
}
