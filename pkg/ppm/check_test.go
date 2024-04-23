package ppm_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/logger"
	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
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

func setupMocks(t *testing.T) (*slog.Logger, *mocks.PostgreSQLClient) {
	t.Helper()

	logger, err := logger.New(true, "text")
	if err != nil {
		t.Fatalf("ERROR: Fail to initialize logger: %s", err)
	}

	postgreSQLMock := mocks.PostgreSQLClient{}

	return logger, &postgreSQLMock
}

func TestCheckPartitions(t *testing.T) {
	logger, postgreSQLMock := setupMocks(t)
	boundDateFormat := "2006-01-02" //nolint:goconst

	partitions := map[string]partition.Configuration{}
	partitions["daily partition"] = partition.Configuration{Schema: "app", Table: "daily_table1", PartitionKey: "column", Interval: partition.Daily, Retention: 2, PreProvisioned: 2}
	partitions["daily partition without retention"] = partition.Configuration{Schema: "public", Table: "daily_table2", PartitionKey: "created_at", Interval: partition.Daily, Retention: 0, PreProvisioned: 1}
	partitions["daily partition without preprovisioned"] = partition.Configuration{Schema: "public", Table: "daily_table3", PartitionKey: "column", Interval: partition.Daily, Retention: 4, PreProvisioned: 0}
	partitions["weekly partition"] = partition.Configuration{Schema: "public", Table: "weekly_table", PartitionKey: "weekly", Interval: partition.Weekly, Retention: 2, PreProvisioned: 2}
	partitions["monthly partition"] = partition.Configuration{Schema: "public", Table: "monthly_table", PartitionKey: "month", Interval: partition.Monthly, Retention: 2, PreProvisioned: 2}
	partitions["yearly partition"] = partition.Configuration{Schema: "public", Table: "yearly_table", PartitionKey: "year", Interval: partition.Yearly, Retention: 4, PreProvisioned: 4}

	// Build mock for each partitions
	for _, p := range partitions {
		var tables []partition.Partition

		var table partition.Partition

		for i := 0; i <= p.Retention; i++ {
			switch p.Interval {
			case partition.Daily:
				table, _ = p.GeneratePartition(time.Now().AddDate(0, 0, -i))
			case partition.Weekly:
				table, _ = p.GeneratePartition(time.Now().AddDate(0, 0, -i*7))
			case partition.Monthly:
				table, _ = p.GeneratePartition(time.Now().AddDate(0, -i, 0))
			case partition.Yearly:
				table, _ = p.GeneratePartition(time.Now().AddDate(-i, 0, 0))
			default:
				t.Errorf("unuspported partition interval in retention table mock")
			}

			postgreSQLMock.On("GetColumnDataType", table.Schema, table.ParentTable, p.PartitionKey).Return(postgresql.Date, nil).Once()
			tables = append(tables, table)
		}

		for i := 0; i <= p.PreProvisioned; i++ {
			switch p.Interval {
			case partition.Daily:
				table, _ = p.GeneratePartition(time.Now().AddDate(0, 0, i))
			case partition.Weekly:
				table, _ = p.GeneratePartition(time.Now().AddDate(0, 0, i*7))
			case partition.Monthly:
				table, _ = p.GeneratePartition(time.Now().AddDate(0, i, 0))
			case partition.Yearly:
				table, _ = p.GeneratePartition(time.Now().AddDate(i, 0, 0))
			default:
				t.Errorf("unuspported partition interval in preprovisonned table mock")
			}

			postgreSQLMock.On("GetColumnDataType", table.Schema, table.ParentTable, p.PartitionKey).Return(postgresql.Date, nil).Once()
			tables = append(tables, table)
		}

		postgreSQLMock.On("GetPartitionSettings", p.Schema, p.Table).Return(string(partition.RangePartitionStrategy), p.PartitionKey, nil).Once()

		convertedTables := partitionResultToPartition(t, tables, boundDateFormat)
		postgreSQLMock.On("ListPartitions", p.Schema, p.Table).Return(convertedTables, nil).Once()
	}

	checker := ppm.New(context.TODO(), *logger, postgreSQLMock, partitions)
	assert.NilError(t, checker.CheckPartitions(), "Partitions should succeed")
}

func TestCheckMissingPartitions(t *testing.T) {
	logger, postgreSQLMock := setupMocks(t)
	boundDateFormat := "2006-01-02"

	config := partition.Configuration{
		Schema:         "public",
		Table:          "my_table",
		PartitionKey:   "created_at",
		Interval:       partition.Daily,
		Retention:      2,
		PreProvisioned: 2,
	}

	todayPartition, _ := config.GeneratePartition(today)
	yesterdayPartition, _ := config.GeneratePartition(yesterday)
	tomorrowPartition, _ := config.GeneratePartition(tomorrow)

	testCases := []struct {
		name   string
		tables []partition.Partition
	}{
		{
			"Missing Yesterday retention partition",
			[]partition.Partition{
				todayPartition,
				yesterdayPartition,
			},
		},
		{
			"Missing Tomorrow partition",
			[]partition.Partition{
				todayPartition,
				tomorrowPartition,
			},
		},
		{
			"Missing Today partition",
			[]partition.Partition{
				yesterdayPartition,
				tomorrowPartition,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fmt.Println("tc.tables", tc.tables)
			postgreSQLMock.On("GetPartitionSettings", config.Schema, config.Table).Return(string(partition.RangePartitionStrategy), config.PartitionKey, nil).Once()
			postgreSQLMock.On("GetColumnDataType", config.Schema, config.Table, config.PartitionKey).Return(postgresql.Date, nil).Once()

			tables := partitionResultToPartition(t, tc.tables, boundDateFormat)
			postgreSQLMock.On("ListPartitions", config.Schema, config.Table).Return(tables, nil).Once()

			checker := ppm.New(context.TODO(), *logger, postgreSQLMock, map[string]partition.Configuration{"test": config})
			assert.Error(t, checker.CheckPartitions(), "at least one partition contains an invalid configuration")
		})
	}
}

func TestUnsupportedPartitionsStrategy(t *testing.T) {
	logger, postgreSQLMock := setupMocks(t)

	config := partition.Configuration{
		Schema:         "public",
		Table:          "my_table",
		PartitionKey:   "created_at",
		Interval:       partition.Daily,
		Retention:      2,
		PreProvisioned: 2,
	}

	testCases := []struct {
		name     string
		strategy partition.PartitionStrategy
		key      string
	}{
		{
			"Unsupported list partition strategy",
			partition.ListPartitionStrategy,
			"created_at",
		},
		{
			"Unsupported hash partition strategy",
			partition.HashPartitionStrategy,
			"created_at",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			postgreSQLMock.On("GetColumnDataType", config.Schema, config.Table, config.PartitionKey).Return(postgresql.Date, nil).Once()
			postgreSQLMock.On("GetPartitionSettings", config.Schema, config.Table).Return(string(tc.strategy), tc.key, nil).Once()

			checker := ppm.New(context.TODO(), *logger, postgreSQLMock, map[string]partition.Configuration{"test": config})
			assert.Error(t, checker.CheckPartitions(), "at least one partition contains an invalid configuration")
		})
	}
}
