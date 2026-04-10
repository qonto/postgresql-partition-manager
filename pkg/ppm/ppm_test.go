package ppm_test

import (
	"testing"

	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
)

const boundDateFormat = "2006-01-02"

func partitionResultToPartition(t *testing.T, partitions []partition.Partition) (result []postgresql.PartitionResult) {
	t.Helper()

	for _, p := range partitions {
		result = append(result, postgresql.PartitionResult{
			ParentTable: p.ParentTable,
			Schema:      p.Schema,
			Name:        p.Name,
			LowerBound:  p.LowerBound.Format(boundDateFormat),
			UpperBound:  p.UpperBound.Format(boundDateFormat),
		})
	}

	return
}
