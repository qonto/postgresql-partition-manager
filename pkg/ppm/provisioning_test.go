package ppm_test

import (
	"context"
	"testing"

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
		partitions                map[string]postgresql.PartitionConfiguration
		expectedCreatedPartitions []postgresql.Partition
	}{
		{
			"Provisioning create preProvisioned and retention partitions",
			map[string]postgresql.PartitionConfiguration{
				"unittest": OneDayPartitionConfiguration,
			},
			[]postgresql.Partition{
				yesterdayPartition,
				currentPartition,
				tomorrowPartition,
			},
		},
		{
			"Multiple provisioning",
			map[string]postgresql.PartitionConfiguration{
				"unittest 1": TwoDayPartitionConfiguration,
				"unittest 2": TwoDayPartitionConfiguration,
			},
			[]postgresql.Partition{
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
			logger, postgreSQLMock := getTestMocks(t) // Reset mock on every test case

			for _, partitionConfiguration := range tc.partitions {
				for _, partition := range tc.expectedCreatedPartitions {
					postgreSQLMock.On("CreatePartition", partitionConfiguration, partition).Return(nil).Once()
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
	failedPartitionConfiguration := OneDayPartitionConfiguration
	failedPartitionConfiguration.Table = "failed"

	testCases := []struct {
		name                 string
		config               postgresql.PartitionConfiguration
		createPartitionError error
	}{
		{
			"failed provisioning",
			failedPartitionConfiguration,
			ErrFake,
		},
		{
			"success provisioning",
			OneDayPartitionConfiguration,
			nil,
		},
	}

	logger, postgreSQLMock := getTestMocks(t)

	configuration := map[string]postgresql.PartitionConfiguration{}

	for _, tc := range testCases {
		previous, _ := tc.config.GetRetentionPartitions(today)
		current, _ := tc.config.GeneratePartition(today)
		future, _ := tc.config.GetPreProvisionedPartitions(today)

		partitions := []postgresql.Partition{current}
		partitions = append(partitions, previous...)
		partitions = append(partitions, future...)

		for _, partition := range partitions {
			postgreSQLMock.On("CreatePartition", tc.config, partition).Return(tc.createPartitionError).Once()
		}

		configuration[tc.name] = tc.config
	}

	provisionner := ppm.New(context.TODO(), *logger, postgreSQLMock, configuration)
	err := provisionner.ProvisioningPartitions()

	assert.NotNil(t, err, "ProvisioningPartitions should report an error")
	postgreSQLMock.AssertExpectations(t)
}
