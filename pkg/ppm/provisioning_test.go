package ppm_test

import (
	"context"
	"testing"

	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"github.com/qonto/postgresql-partition-manager/pkg/ppm"
	"github.com/stretchr/testify/assert"
)

func TestProvisioning(t *testing.T) {
	dayBeforeYesterdayPartition, _ := OneDayPartitionConfiguration.GeneratePartition(dayBeforeYesterday)
	yesterdayPartition, _ := OneDayPartitionConfiguration.GeneratePartition(yesterday)
	currentPartition, _ := OneDayPartitionConfiguration.GeneratePartition(today)
	tomorrowPartition, _ := OneDayPartitionConfiguration.GeneratePartition(tomorrow)
	dayAfterTomorrowPartition, _ := OneDayPartitionConfiguration.GeneratePartition(dayAfterTomorrow)

	TwoDayPartitionConfiguration := OneDayPartitionConfiguration
	TwoDayPartitionConfiguration.Retention = 2
	TwoDayPartitionConfiguration.PreProvisioned = 2

	testCases := []struct {
		name                      string
		partitions                map[string]partition.Configuration
		expectedCreatedPartitions []partition.Partition
	}{
		{
			"Provisioning create preProvisioned and retention partitions",
			map[string]partition.Configuration{
				"unittest": OneDayPartitionConfiguration,
			},
			[]partition.Partition{
				yesterdayPartition,
				currentPartition,
				tomorrowPartition,
			},
		},
		{
			"Multiple provisioning",
			map[string]partition.Configuration{
				"unittest 1": TwoDayPartitionConfiguration,
				"unittest 2": TwoDayPartitionConfiguration,
			},
			[]partition.Partition{
				dayBeforeYesterdayPartition,
				yesterdayPartition,
				currentPartition,
				tomorrowPartition,
				dayAfterTomorrowPartition,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger, postgreSQLMock := setupMocks(t) // Reset mock on every test case

			for _, configuration := range tc.partitions {
				for _, p := range tc.expectedCreatedPartitions {
					postgreSQLMock.On("IsTableExists", p.Schema, p.Name).Return(false, nil).Once()
					postgreSQLMock.On("IsPartitionAttached", p.Schema, p.Name).Return(false, nil).Once()
					postgreSQLMock.On("GetPartitionSettings", p.Schema, p.ParentTable).Return(string(partition.RangePartitionStrategy), configuration.PartitionKey, nil).Once()
					postgreSQLMock.On("GetColumnDataType", p.Schema, p.ParentTable, configuration.PartitionKey).Return(postgresql.Date, nil).Once()
					postgreSQLMock.On("CreateTableLikeTable", p.Schema, p.Name, p.ParentTable).Return(nil).Once()
					postgreSQLMock.On("AttachPartition", p.Schema, p.Name, p.ParentTable, formatLowerBound(t, p, configuration), formatUpperBound(t, p, configuration)).Return(nil).Once()
				}
			}

			provisionner := ppm.New(context.TODO(), *logger, postgreSQLMock, tc.partitions)
			err := provisionner.ProvisioningPartitions()

			assert.Nil(t, err, "ProvisioningPartitions should succeed")
			postgreSQLMock.AssertExpectations(t)
		})
	}
}

// Test provisioning continue even if a partition could not be created
func TestProvisioningFailover(t *testing.T) {
	successPartitionConfiguration := OneDayPartitionConfiguration

	failedPartitionConfiguration := OneDayPartitionConfiguration
	failedPartitionConfiguration.Table = "failed"

	testCases := []struct {
		name                 string
		config               partition.Configuration
		createPartitionError error
	}{
		{
			"failed provisioning",
			failedPartitionConfiguration,
			ErrFake,
		},
		{
			"success provisioning",
			successPartitionConfiguration,
			nil,
		},
	}

	logger, postgreSQLMock := setupMocks(t)

	configuration := map[string]partition.Configuration{}

	for _, tc := range testCases {
		previous, _ := tc.config.GetRetentionPartitions(today)
		current, _ := tc.config.GeneratePartition(today)
		future, _ := tc.config.GetPreProvisionedPartitions(today)

		partitions := []partition.Partition{current}
		partitions = append(partitions, previous...)
		partitions = append(partitions, future...)

		for _, p := range partitions {
			postgreSQLMock.On("IsTableExists", p.Schema, p.Name).Return(false, nil).Once()
			postgreSQLMock.On("CreateTableLikeTable", p.Schema, p.Name, p.ParentTable).Return(tc.createPartitionError).Once()

			if tc.createPartitionError == nil {
				postgreSQLMock.On("IsPartitionAttached", p.Schema, p.Name).Return(false, nil)
				postgreSQLMock.On("GetPartitionSettings", p.Schema, p.ParentTable).Return(string(partition.RangePartitionStrategy), tc.config.PartitionKey, nil).Once()
				postgreSQLMock.On("GetColumnDataType", p.Schema, p.ParentTable, tc.config.PartitionKey).Return(postgresql.Date, nil).Once()
				postgreSQLMock.On("AttachPartition", p.Schema, p.Name, p.ParentTable, formatLowerBound(t, p, tc.config), formatUpperBound(t, p, tc.config)).Return(nil).Once()
			}
		}

		configuration[tc.name] = tc.config
	}

	provisionner := ppm.New(context.TODO(), *logger, postgreSQLMock, configuration)
	err := provisionner.ProvisioningPartitions()

	assert.NotNil(t, err, "ProvisioningPartitions should report an error")
	postgreSQLMock.AssertExpectations(t)
}
