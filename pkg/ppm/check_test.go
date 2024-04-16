package ppm_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/logger"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"github.com/qonto/postgresql-partition-manager/pkg/ppm"
	"github.com/qonto/postgresql-partition-manager/pkg/ppm/mocks"
	"gotest.tools/assert"
)

var (
	dayBeforeYesterday = yesterday.AddDate(0, 0, -1)
	yesterday          = time.Now().AddDate(0, 0, -1)
	today              = time.Now()
	tomorrow           = time.Now().AddDate(0, 0, +1)
	dayAfterTomorrow   = tomorrow.AddDate(0, 0, +1)
)

func getTestMocks(t *testing.T) (*slog.Logger, *mocks.PostgreSQLClient) {
	t.Helper()

	logger, err := logger.New(false, "text")
	if err != nil {
		fmt.Println("ERROR: Fail to initialize logger: %w", err)
		panic(err)
	}

	postgreSQLMock := mocks.PostgreSQLClient{}

	return logger, &postgreSQLMock
}

func TestCheckPartitions(t *testing.T) {
	logger, postgreSQLMock := getTestMocks(t)

	partitions := map[string]postgresql.PartitionConfiguration{}
	partitions["daily partition"] = postgresql.PartitionConfiguration{Schema: "app", Table: "daily_table1", PartitionKey: "column", Interval: postgresql.DailyInterval, Retention: 2, PreProvisioned: 2}
	partitions["daily partition without retention"] = postgresql.PartitionConfiguration{Schema: "public", Table: "daily_table2", PartitionKey: "created_at", Interval: postgresql.DailyInterval, Retention: 0, PreProvisioned: 1}
	partitions["daily partition without preprovisioned"] = postgresql.PartitionConfiguration{Schema: "public", Table: "daily_table3", PartitionKey: "column", Interval: postgresql.DailyInterval, Retention: 4, PreProvisioned: 0}
	partitions["weekly partition"] = postgresql.PartitionConfiguration{Schema: "public", Table: "weekly_table", PartitionKey: "weekly", Interval: postgresql.WeeklyInterval, Retention: 2, PreProvisioned: 2}
	partitions["monthly partition"] = postgresql.PartitionConfiguration{Schema: "public", Table: "monthly_table", PartitionKey: "month", Interval: postgresql.MonthlyInterval, Retention: 2, PreProvisioned: 2}
	partitions["yearly partition"] = postgresql.PartitionConfiguration{Schema: "public", Table: "yearly_table", PartitionKey: "year", Interval: postgresql.YearlyInterval, Retention: 4, PreProvisioned: 4}

	// Build mock for each partitions
	for _, p := range partitions {
		settings := postgresql.PartitionSettings{
			KeyType:  postgresql.DateColumnType,
			Strategy: postgresql.RangePartitionStrategy,
			Key:      p.PartitionKey,
		}
		postgreSQLMock.On("GetPartitionSettings", postgresql.Table{Schema: p.Schema, Name: p.Table}).Return(settings, nil).Once()

		var tables []postgresql.Partition

		var partition postgresql.Partition

		for i := 0; i <= p.Retention; i++ {
			switch p.Interval {
			case postgresql.DailyInterval:
				partition, _ = p.GeneratePartition(time.Now().AddDate(0, 0, -i))
			case postgresql.WeeklyInterval:
				partition, _ = p.GeneratePartition(time.Now().AddDate(0, 0, -i*7))
			case postgresql.MonthlyInterval:
				partition, _ = p.GeneratePartition(time.Now().AddDate(0, -i, 0))
			case postgresql.YearlyInterval:
				partition, _ = p.GeneratePartition(time.Now().AddDate(-i, 0, 0))
			default:
				t.Errorf("unuspported partition interval in retention table mock")
			}

			tables = append(tables, partition)
		}

		for i := 0; i <= p.PreProvisioned; i++ {
			switch p.Interval {
			case postgresql.DailyInterval:
				partition, _ = p.GeneratePartition(time.Now().AddDate(0, 0, i))
			case postgresql.WeeklyInterval:
				partition, _ = p.GeneratePartition(time.Now().AddDate(0, 0, i*7))
			case postgresql.MonthlyInterval:
				partition, _ = p.GeneratePartition(time.Now().AddDate(0, i, 0))
			case postgresql.YearlyInterval:
				partition, _ = p.GeneratePartition(time.Now().AddDate(i, 0, 0))
			default:
				t.Errorf("unuspported partition interval in preprovisonned table mock")
			}

			tables = append(tables, partition)
		}
		postgreSQLMock.On("ListPartitions", postgresql.Table{Schema: p.Schema, Name: p.Table}).Return(tables, nil).Once()
	}

	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, partitions)
	assert.NilError(t, checker.CheckPartitions(), "Partitions should succeed")
}

func TestCheckMissingPartitions(t *testing.T) {
	logger, postgreSQLMock := getTestMocks(t)

	partition := postgresql.PartitionConfiguration{
		Schema:         "public",
		Table:          "my_table",
		PartitionKey:   "created_at",
		Interval:       postgresql.DailyInterval,
		Retention:      2,
		PreProvisioned: 2,
	}

	todayPartition, _ := partition.GeneratePartition(today)
	yesterdayPartition, _ := partition.GeneratePartition(yesterday)
	tomorrowPartition, _ := partition.GeneratePartition(tomorrow)

	testCases := []struct {
		name   string
		tables []postgresql.Partition
	}{
		{
			"Missing Yesterday retention partition",
			[]postgresql.Partition{
				todayPartition,
				yesterdayPartition,
			},
		},
		{
			"Missing Tomorrow partition",
			[]postgresql.Partition{
				todayPartition,
				tomorrowPartition,
			},
		},
		{
			"Missing Today partition",
			[]postgresql.Partition{
				yesterdayPartition,
				tomorrowPartition,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			settings := postgresql.PartitionSettings{
				KeyType:  postgresql.DateColumnType,
				Strategy: postgresql.RangePartitionStrategy,
				Key:      partition.PartitionKey,
			}
			postgreSQLMock.On("GetPartitionSettings", postgresql.Table{Schema: partition.Schema, Name: partition.Table}).Return(settings, nil).Once()

			fmt.Println("tc.tables", tc.tables)
			postgreSQLMock.On("ListPartitions", postgresql.Table{Schema: partition.Schema, Name: partition.Table}).Return(tc.tables, nil).Once()

			checker := ppm.New(context.TODO(), *logger, postgreSQLMock, map[string]postgresql.PartitionConfiguration{"test": partition})
			assert.Error(t, checker.CheckPartitions(), "at least one partition contains an invalid configuration")
		})
	}
}

func TestUnsupportedPartitionsStrategy(t *testing.T) {
	logger, postgreSQLMock := getTestMocks(t)

	partition := postgresql.PartitionConfiguration{
		Schema:         "public",
		Table:          "my_table",
		PartitionKey:   "created_at",
		Interval:       postgresql.DailyInterval,
		Retention:      2,
		PreProvisioned: 2,
	}

	testCases := []struct {
		name     string
		settings postgresql.PartitionSettings
	}{
		{
			"Unsupported list partition strategy",
			postgresql.PartitionSettings{Strategy: postgresql.ListPartitionStrategy, Key: "created_at", KeyType: postgresql.DateColumnType},
		},
		{
			"Unsupported hash partition strategy",
			postgresql.PartitionSettings{Strategy: postgresql.HashPartitionStrategy, Key: "created_at", KeyType: postgresql.DateColumnType},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			postgreSQLMock.On("GetPartitionSettings", postgresql.Table{Schema: partition.Schema, Name: partition.Table}).Return(tc.settings, nil).Once()

			checker := ppm.New(context.TODO(), *logger, postgreSQLMock, map[string]postgresql.PartitionConfiguration{"test": partition})
			assert.Error(t, checker.CheckPartitions(), "at least one partition contains an invalid configuration")
		})
	}
}
