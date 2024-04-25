package ppm_test

import (
	"testing"

	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
)

func formatLowerBound(t *testing.T, p partition.Partition, config partition.Configuration) (output string) {
	t.Helper()

	var dateFormat string

	switch config.Interval {
	case partition.Daily, partition.Weekly:
		dateFormat = "2006-01-02"
	case partition.Monthly:
		dateFormat = "2006-01"
	case partition.Yearly:
		dateFormat = "2006"
	}

	return p.LowerBound.Format(dateFormat)
}

func formatUpperBound(t *testing.T, p partition.Partition, config partition.Configuration) (output string) {
	t.Helper()

	var dateFormat string

	switch config.Interval {
	case partition.Daily, partition.Weekly:
		dateFormat = "2006-01-02"
	case partition.Monthly:
		dateFormat = "2006-01"
	case partition.Yearly:
		dateFormat = "2006"
	}

	return p.UpperBound.Format(dateFormat)
}

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
