package ppm_test

import (
	"context"
	"errors"
	"testing"

	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"github.com/qonto/postgresql-partition-manager/pkg/ppm"
	"github.com/stretchr/testify/assert"
)

var OneDayPartitionConfiguration = postgresql.PartitionConfiguration{
	Schema:         "public",
	Table:          "my_table",
	PartitionKey:   "created_at",
	Interval:       "daily",
	Retention:      1,
	PreProvisioned: 1,
	CleanupPolicy:  "drop",
}

var ErrFake = errors.New("fake error")

func TestCleanupPartitions(t *testing.T) {
	yearBeforePartition, _ := OneDayPartitionConfiguration.GeneratePartition(dayBeforeYesterday.AddDate(-1, 0, 0))
	dayBeforeYesterdayPartition, _ := OneDayPartitionConfiguration.GeneratePartition(dayBeforeYesterday)
	yesterdayPartition, _ := OneDayPartitionConfiguration.GeneratePartition(yesterday)
	currentPartition, _ := OneDayPartitionConfiguration.GeneratePartition(today)
	tomorrowPartition, _ := OneDayPartitionConfiguration.GeneratePartition(tomorrow)
	dayAfterTomorrowPartition, _ := OneDayPartitionConfiguration.GeneratePartition(dayAfterTomorrow)

	detachPartitionConfiguration := OneDayPartitionConfiguration
	detachPartitionConfiguration.CleanupPolicy = "detach"

	dropPartitionConfiguration := OneDayPartitionConfiguration
	OneDayPartitionConfiguration.CleanupPolicy = "drop"

	testCases := []struct {
		name                      string
		partitions                map[string]postgresql.PartitionConfiguration
		existingPartitions        []postgresql.Partition
		expectedRemovedPartitions []postgresql.Partition
	}{
		{
			"Drop useless partitions",
			map[string]postgresql.PartitionConfiguration{
				"unittest": dropPartitionConfiguration,
			},
			[]postgresql.Partition{yearBeforePartition, dayBeforeYesterdayPartition, yesterdayPartition, currentPartition, tomorrowPartition, dayAfterTomorrowPartition},
			[]postgresql.Partition{yearBeforePartition, dayBeforeYesterdayPartition, dayAfterTomorrowPartition},
		},
		{
			"Detach useless partitions",
			map[string]postgresql.PartitionConfiguration{
				"unittest": detachPartitionConfiguration,
			},
			[]postgresql.Partition{yearBeforePartition, dayBeforeYesterdayPartition, yesterdayPartition, currentPartition},
			[]postgresql.Partition{yearBeforePartition, dayBeforeYesterdayPartition},
		},
		{
			"No cleanup",
			map[string]postgresql.PartitionConfiguration{
				"unittest": dropPartitionConfiguration,
			},
			[]postgresql.Partition{yesterdayPartition, currentPartition, tomorrowPartition},
			[]postgresql.Partition{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger, postgreSQLMock := getTestMocks(t) // Reset mock on every test case

			for _, partitionConfiguration := range tc.partitions {
				table := postgresql.Table{Schema: partitionConfiguration.Schema, Name: partitionConfiguration.Table}
				postgreSQLMock.On("ListPartitions", table).Return(tc.existingPartitions, nil).Once()

				for _, partition := range tc.expectedRemovedPartitions {
					postgreSQLMock.On("DetachPartition", partition).Return(nil).Once()

					if partitionConfiguration.CleanupPolicy == postgresql.DropCleanupPolicy {
						postgreSQLMock.On("DeletePartition", partition).Return(nil).Once()
					}
				}
			}

			checker := ppm.New(context.TODO(), *logger, postgreSQLMock, tc.partitions)
			err := checker.CleanupPartitions()

			assert.Nil(t, err, "CleanupPartitions should succeed")
			postgreSQLMock.AssertExpectations(t)
		})
	}
}

// Test cleanup continue even if a partition could not be dropped or deleted
func TestCleanupPartitionsFailover(t *testing.T) {
	successPartitionConfiguration := OneDayPartitionConfiguration

	undetachablePartitionConfiguration := OneDayPartitionConfiguration
	undetachablePartitionConfiguration.Table = "undetachable"

	undropablePartitionConfiguration := OneDayPartitionConfiguration
	undropablePartitionConfiguration.Table = "undropable"

	configuration := map[string]postgresql.PartitionConfiguration{
		"undetachable": undetachablePartitionConfiguration,
		"undropable":   undropablePartitionConfiguration,
		"success":      successPartitionConfiguration,
	}

	logger, postgreSQLMock := getTestMocks(t)

	for _, config := range configuration {
		table := postgresql.Table{Schema: config.Schema, Name: config.Table}

		dayBeforeYesterdayPartition, _ := config.GeneratePartition(dayBeforeYesterday)
		yesterdayPartitionPartition, _ := config.GeneratePartition(yesterday)
		todayPartitionPartition, _ := config.GeneratePartition(today)
		tomorrowPartitionPartition, _ := config.GeneratePartition(tomorrow)

		partitions := []postgresql.Partition{
			dayBeforeYesterdayPartition, // This partition should be removed by the cleanup
			yesterdayPartitionPartition,
			todayPartitionPartition,
			tomorrowPartitionPartition,
		}

		postgreSQLMock.On("ListPartitions", table).Return(partitions, nil).Once()
	}

	// Undetachable partition will return an error on detach operation
	undetachablePartition, _ := undetachablePartitionConfiguration.GeneratePartition(dayBeforeYesterday)
	postgreSQLMock.On("DetachPartition", undetachablePartition).Return(ErrFake).Once()

	// Undropable partition will return an error on drop operation
	undropablePartition, _ := undropablePartitionConfiguration.GeneratePartition(dayBeforeYesterday)
	postgreSQLMock.On("DetachPartition", undropablePartition).Return(nil).Once()
	postgreSQLMock.On("DeletePartition", undropablePartition).Return(ErrFake).Once()

	// Detach and drop will succeed
	successPartition, _ := successPartitionConfiguration.GeneratePartition(dayBeforeYesterday)
	postgreSQLMock.On("DetachPartition", successPartition).Return(nil).Once()
	postgreSQLMock.On("DeletePartition", successPartition).Return(nil).Once()

	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, configuration)
	err := checker.CleanupPartitions()

	assert.NotNil(t, err, "CleanupPartitions should report an error")
	postgreSQLMock.AssertExpectations(t)
}
