package ppm_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/pkg/ppm"
	"github.com/stretchr/testify/assert"
)

var OneDayPartitionConfiguration = partition.Configuration{
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

	boundDateFormat := "2006-01-02"

	testCases := []struct {
		name                      string
		partitions                map[string]partition.Configuration
		existingPartitions        []partition.Partition
		expectedRemovedPartitions []partition.Partition
	}{
		{
			"Drop useless partitions",
			map[string]partition.Configuration{
				"unittest": dropPartitionConfiguration,
			},
			[]partition.Partition{yearBeforePartition, dayBeforeYesterdayPartition, yesterdayPartition, currentPartition, tomorrowPartition, dayAfterTomorrowPartition},
			[]partition.Partition{yearBeforePartition, dayBeforeYesterdayPartition, dayAfterTomorrowPartition},
		},
		{
			"Detach useless partitions",
			map[string]partition.Configuration{
				"unittest": detachPartitionConfiguration,
			},
			[]partition.Partition{yearBeforePartition, dayBeforeYesterdayPartition, yesterdayPartition, currentPartition},
			[]partition.Partition{yearBeforePartition, dayBeforeYesterdayPartition},
		},
		{
			"No cleanup",
			map[string]partition.Configuration{
				"unittest": dropPartitionConfiguration,
			},
			[]partition.Partition{yesterdayPartition, currentPartition, tomorrowPartition},
			[]partition.Partition{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger, postgreSQLMock := setupMocks(t) // Reset mock on every test case

			for _, partitionConfiguration := range tc.partitions {
				postgreSQLMock.On("ListPartitions", partitionConfiguration.Schema, partitionConfiguration.Table).Return(partitionResultToPartition(t, tc.existingPartitions, boundDateFormat), nil).Once()

				for _, p := range tc.expectedRemovedPartitions {
					postgreSQLMock.On("DetachPartitionConcurrently", p.Schema, p.Name, p.ParentTable).Return(nil).Once()

					if partitionConfiguration.CleanupPolicy == partition.Drop {
						postgreSQLMock.On("DropTable", p.Schema, p.Name).Return(nil).Once()
					}
				}
			}

			checker := ppm.New(context.TODO(), *logger, postgreSQLMock, tc.partitions, time.Now())
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

	pendingFinalizePartitionConfiguration := OneDayPartitionConfiguration
	pendingFinalizePartitionConfiguration.Table = "pendingFinalize"

	configuration := map[string]partition.Configuration{
		"undetachable":    undetachablePartitionConfiguration,
		"undropable":      undropablePartitionConfiguration,
		"pendingFinalize": pendingFinalizePartitionConfiguration,
		"success":         successPartitionConfiguration,
	}

	boundDateFormat := "2006-01-02"

	logger, postgreSQLMock := setupMocks(t)

	for _, config := range configuration {
		dayBeforeYesterdayPartition, _ := config.GeneratePartition(dayBeforeYesterday)
		yesterdayPartitionPartition, _ := config.GeneratePartition(yesterday)
		todayPartitionPartition, _ := config.GeneratePartition(today)
		tomorrowPartitionPartition, _ := config.GeneratePartition(tomorrow)

		partitions := []partition.Partition{
			dayBeforeYesterdayPartition, // This partition should be removed by the cleanup
			yesterdayPartitionPartition,
			todayPartitionPartition,
			tomorrowPartitionPartition,
		}

		postgreSQLMock.On("ListPartitions", config.Schema, config.Table).Return(partitionResultToPartition(t, partitions, boundDateFormat), nil).Once()
	}

	// Undetachable partition will return an error on detach operation
	undetachablePartition, _ := undetachablePartitionConfiguration.GeneratePartition(dayBeforeYesterday)
	postgreSQLMock.On("DetachPartitionConcurrently", undetachablePartition.Schema, undetachablePartition.Name, undetachablePartition.ParentTable).Return(ErrFake)

	// Undropable partition will return an error on drop operation
	undropablePartition, _ := undropablePartitionConfiguration.GeneratePartition(dayBeforeYesterday)
	postgreSQLMock.On("DetachPartitionConcurrently", undropablePartition.Schema, undropablePartition.Name, undropablePartition.ParentTable).Return(nil)
	postgreSQLMock.On("DropTable", undropablePartition.Schema, undropablePartition.Name).Return(ErrFake)

	// Pending finalize partition will return an error on detach operation
	pendingFinalizePartition, _ := pendingFinalizePartitionConfiguration.GeneratePartition(dayBeforeYesterday)
	ErrObjectNotInPrerequisiteState := &pgconn.PgError{
		Code:       ppm.ObjectNotInPrerequisiteStatePostgreSQLErrorCode,
		SchemaName: pendingFinalizePartition.Schema,
		TableName:  pendingFinalizePartition.Name,
	}
	postgreSQLMock.On("DetachPartitionConcurrently", pendingFinalizePartition.Schema, pendingFinalizePartition.Name, pendingFinalizePartition.ParentTable).Return(ErrObjectNotInPrerequisiteState)
	postgreSQLMock.On("FinalizePartitionDetach", pendingFinalizePartition.Schema, pendingFinalizePartition.Name, pendingFinalizePartition.ParentTable).Return(nil)
	postgreSQLMock.On("DropTable", pendingFinalizePartition.Schema, pendingFinalizePartition.Name).Return(ErrFake)

	// Detach and drop will succeed
	successPartition, _ := successPartitionConfiguration.GeneratePartition(dayBeforeYesterday)
	postgreSQLMock.On("DetachPartitionConcurrently", successPartition.Schema, successPartition.Name, successPartition.ParentTable).Return(nil).Once()
	postgreSQLMock.On("DropTable", successPartition.Schema, successPartition.Name).Return(nil).Once()

	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, configuration, time.Now())
	err := checker.CleanupPartitions()

	assert.NotNil(t, err, "CleanupPartitions should report an error")
	postgreSQLMock.AssertExpectations(t)
}
