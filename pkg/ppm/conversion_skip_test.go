package ppm_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"github.com/qonto/postgresql-partition-manager/pkg/ppm"
	"github.com/stretchr/testify/assert"
)

var conversionTestConfig = partition.Configuration{
	Schema:         "public",
	Table:          "events",
	PartitionKey:   "created_at",
	Interval:       partition.Daily,
	Retention:      1,
	PreProvisioned: 1,
	CleanupPolicy:  "drop",
}

func TestCheckPartitions_SkipsTableWhenConversionInProgress(t *testing.T) {
	logger, postgreSQLMock := setupMocks(t)

	partitions := map[string]partition.Configuration{
		"my-events": conversionTestConfig,
	}

	// Mock returns true → conversion is in progress, table should be skipped
	postgreSQLMock.On("IsConversionInProgress", conversionTestConfig.Schema, conversionTestConfig.Table).Return(true, nil).Once()

	// No other mock calls should be made (table is skipped entirely)
	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, partitions, time.Now())
	err := checker.CheckPartitions()

	assert.Nil(t, err, "CheckPartitions should succeed when table is skipped")
	postgreSQLMock.AssertExpectations(t)
	// Verify that no partition-processing methods were called
	postgreSQLMock.AssertNotCalled(t, "GetColumnDataType")
	postgreSQLMock.AssertNotCalled(t, "GetPartitionSettings")
	postgreSQLMock.AssertNotCalled(t, "ListPartitions")
}

func TestCheckPartitions_ProcessesTableWhenNoConversionInProgress(t *testing.T) {
	logger, postgreSQLMock := setupMocks(t)

	partitions := map[string]partition.Configuration{
		"my-events": conversionTestConfig,
	}

	// Mock returns false → no conversion in progress, table should be processed
	postgreSQLMock.On("IsConversionInProgress", conversionTestConfig.Schema, conversionTestConfig.Table).Return(false, nil).Once()

	// Table is processed: mock the partition check calls
	postgreSQLMock.On("GetColumnDataType", conversionTestConfig.Schema, conversionTestConfig.Table, conversionTestConfig.PartitionKey).Return(postgresql.Date, nil).Once()
	postgreSQLMock.On("GetPartitionSettings", conversionTestConfig.Schema, conversionTestConfig.Table).Return(string(partition.Range), conversionTestConfig.PartitionKey, nil).Once()

	// Generate expected partitions for the mock
	forDate := time.Now()
	retentionTables, _ := conversionTestConfig.GetRetentionPartitions(forDate)
	currentPartition, _ := conversionTestConfig.GeneratePartition(forDate)
	preprovisionedTables, _ := conversionTestConfig.GetPreProvisionedPartitions(forDate)

	tables := make([]partition.Partition, 0, len(retentionTables)+1+len(preprovisionedTables))
	tables = append(tables, retentionTables...)
	tables = append(tables, currentPartition)
	tables = append(tables, preprovisionedTables...)

	postgreSQLMock.On("ListPartitions", conversionTestConfig.Schema, conversionTestConfig.Table).Return(partitionResultToPartition(t, tables), nil).Once()

	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, partitions, time.Now())
	err := checker.CheckPartitions()

	assert.Nil(t, err, "CheckPartitions should succeed when table is processed normally")
	postgreSQLMock.AssertExpectations(t)
}

func TestCheckPartitions_ProcessesTableOnErrorWithFailOpen(t *testing.T) {
	logger, postgreSQLMock := setupMocks(t)

	partitions := map[string]partition.Configuration{
		"my-events": conversionTestConfig,
	}

	// Mock returns error → fail-open, table should still be processed
	postgreSQLMock.On("IsConversionInProgress", conversionTestConfig.Schema, conversionTestConfig.Table).Return(false, errors.New("connection refused")).Once()

	// Table is processed despite the error: mock the partition check calls
	postgreSQLMock.On("GetColumnDataType", conversionTestConfig.Schema, conversionTestConfig.Table, conversionTestConfig.PartitionKey).Return(postgresql.Date, nil).Once()
	postgreSQLMock.On("GetPartitionSettings", conversionTestConfig.Schema, conversionTestConfig.Table).Return(string(partition.Range), conversionTestConfig.PartitionKey, nil).Once()

	// Generate expected partitions for the mock
	forDate := time.Now()
	retentionTables, _ := conversionTestConfig.GetRetentionPartitions(forDate)
	currentPartition, _ := conversionTestConfig.GeneratePartition(forDate)
	preprovisionedTables, _ := conversionTestConfig.GetPreProvisionedPartitions(forDate)

	tables := make([]partition.Partition, 0, len(retentionTables)+1+len(preprovisionedTables))
	tables = append(tables, retentionTables...)
	tables = append(tables, currentPartition)
	tables = append(tables, preprovisionedTables...)

	postgreSQLMock.On("ListPartitions", conversionTestConfig.Schema, conversionTestConfig.Table).Return(partitionResultToPartition(t, tables), nil).Once()

	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, partitions, time.Now())
	err := checker.CheckPartitions()

	assert.Nil(t, err, "CheckPartitions should succeed (fail-open: table processed despite error)")
	postgreSQLMock.AssertExpectations(t)
}

func TestProvisioningPartitions_SkipsTableWhenConversionInProgress(t *testing.T) {
	logger, postgreSQLMock := setupMocks(t)

	partitions := map[string]partition.Configuration{
		"my-events": conversionTestConfig,
	}

	// Mock returns true → conversion is in progress, table should be skipped
	postgreSQLMock.On("IsConversionInProgress", conversionTestConfig.Schema, conversionTestConfig.Table).Return(true, nil).Once()

	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, partitions, time.Now())
	err := checker.ProvisioningPartitions()

	assert.Nil(t, err, "ProvisioningPartitions should succeed when table is skipped")
	postgreSQLMock.AssertExpectations(t)
	postgreSQLMock.AssertNotCalled(t, "ListPartitions")
}

func TestCleanupPartitions_SkipsTableWhenConversionInProgress(t *testing.T) {
	logger, postgreSQLMock := setupMocks(t)

	partitions := map[string]partition.Configuration{
		"my-events": conversionTestConfig,
	}

	// Mock returns true → conversion is in progress, table should be skipped
	postgreSQLMock.On("IsConversionInProgress", conversionTestConfig.Schema, conversionTestConfig.Table).Return(true, nil).Once()

	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, partitions, time.Now())
	err := checker.CleanupPartitions()

	assert.Nil(t, err, "CleanupPartitions should succeed when table is skipped")
	postgreSQLMock.AssertExpectations(t)
	postgreSQLMock.AssertNotCalled(t, "ListPartitions")
}
