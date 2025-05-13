package ppm_test

import (
	"testing"

	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
)

func partitionResultToPartition(t *testing.T, partitions []partition.Partition, dateFormat string) (result []postgresql.PartitionResult) {
	t.Helper()

	for _, p := range partitions {
		result = append(result, postgresql.PartitionResult{
			ParentTable: p.ParentTable,
			Schema:      p.Schema,
			Name:        p.Name,
			LowerBound:  p.LowerBound.Format(dateFormat),
			UpperBound:  p.UpperBound.Format(dateFormat),
		})
	}

	return
}
