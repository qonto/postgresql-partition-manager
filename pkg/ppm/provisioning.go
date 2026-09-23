package ppm

import (
	"errors"
	"fmt"
	"slices"
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
			p.logger.Error("Failed to provision partitions", "error", err, "schema", config.Schema, "table", config.Table)

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
	foundPartitions, rawPartitions, err := p.listPartitionsWithRawBounds(config.Schema, config.Table)
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

	edges := getRangeEdges(foundPartitions, rawPartitions, currentRange)

	expectedRange, err := p.getGlobalRange(partitions)
	if err != nil {
		return fmt.Errorf("could not evaluate ranges to create: %w", err)
	}

	p.logger.Info("Expected", "e_range", expectedRange)

	if expectedRange.IsEqual(currentRange) {
		// If expected and current ranges are the same, there is no partition to create
		return nil
	}

	for _, candidate := range provisioningOrder(partitions, currentRange) {
		p.logger.Info("Candidate", "range", partition.Bounds(candidate.LowerBound, candidate.UpperBound))

		if !candidate.UpperBound.After(currentRange.LowerBound) || !candidate.LowerBound.Before(currentRange.UpperBound) {
			// no intersection between candidate and existing: create new partition
			p.logger.Info("No intersection", "create-range", partition.Bounds(candidate.LowerBound, candidate.UpperBound))

			err = p.createPartition(config, candidate, edges)
		}

		if err == nil && candidate.LowerBound.Before(currentRange.LowerBound) && candidate.UpperBound.After(currentRange.LowerBound) {
			// left segment of the candidate outside, of the intersection with existing partitions
			segLeft := candidate
			segLeft.UpperBound = currentRange.LowerBound
			segLeft.Name = fmt.Sprintf("%s_%s_%s", config.Table, segLeft.LowerBound.Format("20060102"), segLeft.UpperBound.Format("20060102"))
			p.logger.Info("Left intersection", "create-range", partition.Bounds(segLeft.LowerBound, segLeft.UpperBound))
			err = p.createPartition(config, segLeft, edges)
		}

		if err == nil && candidate.UpperBound.After(currentRange.UpperBound) && candidate.LowerBound.Before(currentRange.UpperBound) {
			// right segment of the candidate, outside of the intersection with existing partitions
			segRight := candidate
			segRight.LowerBound = currentRange.UpperBound
			segRight.Name = fmt.Sprintf("%s_%s_%s", config.Table, segRight.LowerBound.Format("20060102"), segRight.UpperBound.Format("20060102"))
			p.logger.Info("Right intersection", "create-range", partition.Bounds(segRight.LowerBound, segRight.UpperBound))
			err = p.createPartition(config, segRight, edges)
		}

		if err != nil {
			p.logger.Error("Failed to create partition", "error", err)

			return ErrPartitionProvisioningFailed
		}
	}

	return nil
}

// provisioningOrder returns the candidates sorted so that each created partition
// is adjacent to the existing set: candidates below the existing range from the
// newest to the oldest, then the others from the oldest to the newest.
// This way, a failure never leaves a gap between partitions, which would block
// any further provisioning.
func provisioningOrder(candidates []partition.Partition, currentRange partition.PartitionRange) []partition.Partition {
	ordered := slices.Clone(candidates)

	slices.SortFunc(ordered, func(a, b partition.Partition) int {
		return a.LowerBound.Compare(b.LowerBound)
	})

	if currentRange.IsEmpty() {
		return ordered
	}

	below := 0
	for below < len(ordered) && ordered[below].LowerBound.Before(currentRange.LowerBound) {
		below++
	}

	slices.Reverse(ordered[:below])

	return ordered
}

// rangeEdges holds the bounds of the existing partition set as stored by PostgreSQL.
//
// A bound decoded from PostgreSQL only keeps its timestamp: a UUIDv7 bound created
// by a previous PPM version (or by hand) may differ from the one PPM would generate
// for the same timestamp (e.g. variant bits `...-7000-0000-...` vs `...-7000-8000-...`).
// New partitions adjacent to the existing set must reuse the existing bound verbatim,
// otherwise PostgreSQL detects an overlap (below the set) or leaves a gap (above it).
type rangeEdges struct {
	lowerBound    time.Time
	rawLowerBound string
	upperBound    time.Time
	rawUpperBound string
}

func getRangeEdges(partitions []partition.Partition, rawPartitions map[string]postgresql.PartitionResult, currentRange partition.PartitionRange) (edges rangeEdges) {
	if currentRange.IsEmpty() {
		return edges
	}

	edges.lowerBound = currentRange.LowerBound
	edges.upperBound = currentRange.UpperBound

	for _, part := range partitions {
		if part.LowerBound.Equal(currentRange.LowerBound) {
			edges.rawLowerBound = rawPartitions[part.Name].LowerBound
		}

		if part.UpperBound.Equal(currentRange.UpperBound) {
			edges.rawUpperBound = rawPartitions[part.Name].UpperBound
		}
	}

	return edges
}

// adjust replaces the bounds of a new partition touching the existing set with the existing raw bounds
func (e rangeEdges) adjust(part partition.Partition, lowerBound, upperBound string) (string, string) {
	if e.rawLowerBound != "" && part.UpperBound.Equal(e.lowerBound) {
		upperBound = e.rawLowerBound
	}

	if e.rawUpperBound != "" && part.LowerBound.Equal(e.upperBound) {
		lowerBound = e.rawUpperBound
	}

	return lowerBound, upperBound
}

func (p PPM) CreatePartition(partitionConfiguration partition.Configuration, partition partition.Partition) error {
	return p.createPartition(partitionConfiguration, partition, rangeEdges{})
}

func (p PPM) createPartition(_ partition.Configuration, partition partition.Partition, edges rangeEdges) error {
	p.logger.Debug("Creating partition", "schema", partition.Schema, "table", partition.Name)

	_, partitionKey, err := p.db.GetPartitionSettings(partition.Schema, partition.ParentTable)
	if err != nil {
		return fmt.Errorf("failed to get partition settings: %w", err)
	}

	partitionKeyType, err := p.db.GetColumnDataType(partition.Schema, partition.ParentTable, partitionKey)
	if err != nil {
		return fmt.Errorf("failed to get partition key details: %w", err)
	}

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

	var lowerBound, upperBound string

	switch partitionKeyType {
	case postgresql.Date:
		lowerBound = partition.LowerBound.Format("2006-01-02")
		upperBound = partition.UpperBound.Format("2006-01-02")
	case postgresql.DateTime, postgresql.DateTimeWithTZ:
		lowerBound = partition.LowerBound.Format("2006-01-02 00:00:00")
		upperBound = partition.UpperBound.Format("2006-01-02 00:00:00")
	case postgresql.UUID:
		lowerBound, upperBound = edges.adjust(partition, uuid7.FromTime(partition.LowerBound), uuid7.FromTime(partition.UpperBound))
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

		err = p.db.SetPartitionReplicaIdentity(partition.Schema, partition.Name, partition.ParentTable)
		if err != nil {
			p.logger.Warn("failed to set replica identity", "error", err, "schema", partition.Schema, "table", partition.Name, "attempt", attempt, "max_retries", maxRetries)

			return fmt.Errorf("fail to set replica identity: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to attach partition after retries: %w", err)
	}

	p.logger.Info("Partition attached to parent table", "schema", partition.Schema, "table", partition.Name, "parent_table", partition.ParentTable)

	return nil
}
