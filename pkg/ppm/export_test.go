package ppm

import "github.com/qonto/postgresql-partition-manager/internal/infra/partition"

func ComparePartitions(p *PPM,
	existingTables, expectedTables []partition.Partition,
	manuallyManagedPartitionNames []string,
) (unexpectedTables, missingTables, incorrectBounds []partition.Partition) {
	return p.comparePartitions(existingTables, expectedTables, manuallyManagedPartitionNames)
}
