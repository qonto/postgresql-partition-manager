package ppm_test

import (
	"testing"

	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
)

func getBoundFormat(config partition.Configuration) string {
	var dateFormat string

	switch config.Interval {
	case partition.Daily, partition.Weekly:
		dateFormat = "2006-01-02"
	case partition.Monthly:
		dateFormat = "2006-01"
	case partition.Quarterly:
		dateFormat = "2006-01"
	case partition.Yearly:
		dateFormat = "2006"
	}

	return dateFormat
}

func formatLowerBound(t *testing.T, p partition.Partition, config partition.Configuration) (output string) {
	t.Helper()

	return p.LowerBound.Format(getBoundFormat(config))
}

func formatUpperBound(t *testing.T, p partition.Partition, config partition.Configuration) (output string) {
	t.Helper()

	return p.UpperBound.Format(getBoundFormat(config))
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
