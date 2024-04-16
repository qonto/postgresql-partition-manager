package postgresql_test

import (
	"testing"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"gotest.tools/assert"
)

func TestPartitionAttributes(t *testing.T) {
	testCases := []struct {
		name                string
		partition           postgresql.Partition
		expectedName        string
		expectedTable       postgresql.Table
		expectedParentTable postgresql.Table
	}{
		{
			name: "Public schema",
			partition: postgresql.Partition{
				ParentTable: "my_table",
				Schema:      "public",
				Name:        "my_table_2024_12_25",
			},
			expectedName: "public.my_table_2024_12_25",
			expectedTable: postgresql.Table{
				Schema: "public",
				Name:   "my_table_2024_12_25",
			},
			expectedParentTable: postgresql.Table{
				Schema: "public",
				Name:   "my_table",
			},
		},
		{
			name: "Dashed table",
			partition: postgresql.Partition{
				ParentTable: "my-table",
				Schema:      "api",
				Name:        "my-table_2024_w01",
			},
			expectedName: "api.my-table_2024_w01",
			expectedTable: postgresql.Table{
				Schema: "api",
				Name:   "my-table_2024_w01",
			},
			expectedParentTable: postgresql.Table{
				Schema: "api",
				Name:   "my-table",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.partition.QualifiedName(), tc.expectedName, "Qualified name don't match")
			assert.Equal(t, tc.partition.String(), tc.expectedName, "Partition name don't match")
			assert.Equal(t, tc.partition.ToTable(), tc.expectedTable, "Table don't match")
			assert.Equal(t, tc.partition.GetParentTable(), tc.expectedParentTable, "Parent table don't match")
		})
	}
}

func TestPartitionName(t *testing.T) {
	testCases := []struct {
		name      string
		partition postgresql.PartitionConfiguration
		time      time.Time
		expected  postgresql.Partition
	}{
		{
			name: "Daily partition",
			partition: postgresql.PartitionConfiguration{
				Schema:         "public",
				Table:          "my_table",
				PartitionKey:   "created_at",
				Interval:       postgresql.DailyInterval,
				Retention:      7,
				PreProvisioned: 3,
				CleanupPolicy:  postgresql.DropCleanupPolicy,
			},
			time: time.Date(2024, 0o1, 30, 12, 53, 45, 100, time.UTC),
			expected: postgresql.Partition{
				Schema:     "public",
				Name:       "my_table_2024_01_30",
				LowerBound: time.Date(2024, 0o1, 30, 0, 0, 0, 0, time.UTC),
				UpperBound: time.Date(2024, 0o1, 31, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "Monthly partition",
			partition: postgresql.PartitionConfiguration{
				Schema:         "public",
				Table:          "my_table",
				PartitionKey:   "created_at",
				Interval:       postgresql.MonthlyInterval,
				Retention:      7,
				PreProvisioned: 3,
				CleanupPolicy:  postgresql.DropCleanupPolicy,
			},
			time: time.Date(2024, 0o1, 30, 12, 53, 45, 100, time.UTC),
			expected: postgresql.Partition{
				Schema:     "public",
				Name:       "my_table_2024_01",
				LowerBound: time.Date(2024, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
				UpperBound: time.Date(2024, 0o2, 0o1, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "Weekly partition",
			partition: postgresql.PartitionConfiguration{
				Schema:         "public",
				Table:          "my_table",
				PartitionKey:   "created_at",
				Interval:       postgresql.WeeklyInterval,
				Retention:      7,
				PreProvisioned: 3,
				CleanupPolicy:  postgresql.DropCleanupPolicy,
			},
			time: time.Date(2024, 0o1, 30, 12, 53, 45, 100, time.UTC),
			expected: postgresql.Partition{
				Schema:     "public",
				Name:       "my_table_2024_w05",
				LowerBound: time.Date(2024, 0o1, 29, 0, 0, 0, 0, time.UTC),
				UpperBound: time.Date(2024, 0o2, 0o5, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "Yearly partition",
			partition: postgresql.PartitionConfiguration{
				Schema:         "public",
				Table:          "my_table",
				PartitionKey:   "created_at",
				Interval:       postgresql.YearlyInterval,
				Retention:      7,
				PreProvisioned: 3,
				CleanupPolicy:  postgresql.DropCleanupPolicy,
			},
			time: time.Date(2024, 0o1, 30, 12, 53, 45, 100, time.UTC),
			expected: postgresql.Partition{
				Schema:     "public",
				Name:       "my_table_2024",
				LowerBound: time.Date(2024, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
				UpperBound: time.Date(2025, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, _ := tc.partition.GeneratePartition(tc.time)
			assert.Equal(t, tc.expected.Schema, result.Schema, "Schema don't match")
			assert.Equal(t, tc.expected.Name, result.Name, "Table name don't match")
			assert.Equal(t, tc.expected.LowerBound, result.LowerBound, "Lower bound don't match")
			assert.Equal(t, tc.expected.UpperBound, result.UpperBound, "Upper bound don't match")
		})
	}
}

func TestRetentionTableNames(t *testing.T) {
	testCases := []struct {
		name      string
		partition postgresql.PartitionConfiguration
		time      time.Time
		expected  []postgresql.Partition
	}{
		{
			name: "Daily partition",
			partition: postgresql.PartitionConfiguration{
				Schema:         "public",
				Table:          "my_table",
				PartitionKey:   "created_at",
				Interval:       postgresql.DailyInterval,
				Retention:      4,
				PreProvisioned: 3,
				CleanupPolicy:  postgresql.DropCleanupPolicy,
			},
			time: time.Date(2024, 0o1, 0o3, 12, 53, 45, 100, time.UTC),
			expected: []postgresql.Partition{
				{
					Schema:      "public",
					Name:        "my_table_2024_01_02",
					ParentTable: "my_table",
					LowerBound:  time.Date(2024, 0o1, 0o2, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2024, 0o1, 0o3, 0, 0, 0, 0, time.UTC),
				}, {
					Schema:      "public",
					Name:        "my_table_2024_01_01",
					ParentTable: "my_table",
					LowerBound:  time.Date(2024, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2024, 0o1, 0o2, 0, 0, 0, 0, time.UTC),
				}, {
					Schema:      "public",
					Name:        "my_table_2023_12_31",
					ParentTable: "my_table",
					LowerBound:  time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2024, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
				}, {
					Schema:      "public",
					Name:        "my_table_2023_12_30",
					ParentTable: "my_table",
					LowerBound:  time.Date(2023, 12, 30, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{
			name: "Monthly partition",
			partition: postgresql.PartitionConfiguration{
				Schema:         "public",
				Table:          "my_table",
				PartitionKey:   "created_at",
				Interval:       postgresql.MonthlyInterval,
				Retention:      3,
				PreProvisioned: 3,
				CleanupPolicy:  postgresql.DropCleanupPolicy,
			},
			time: time.Date(2024, 0o2, 25, 12, 53, 45, 100, time.UTC),
			expected: []postgresql.Partition{
				{
					Schema:      "public",
					Name:        "my_table_2024_01",
					ParentTable: "my_table",
					LowerBound:  time.Date(2024, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2024, 0o2, 0o1, 0, 0, 0, 0, time.UTC),
				}, {
					Schema:      "public",
					Name:        "my_table_2023_12",
					ParentTable: "my_table",
					LowerBound:  time.Date(2023, 12, 0o1, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2024, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
				}, {
					Schema:      "public",
					Name:        "my_table_2023_11",
					ParentTable: "my_table",
					LowerBound:  time.Date(2023, 11, 0o1, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2023, 12, 0o1, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{
			name: "Weekly partition",
			partition: postgresql.PartitionConfiguration{
				Schema:         "public",
				Table:          "my_table",
				PartitionKey:   "created_at",
				Interval:       postgresql.WeeklyInterval,
				Retention:      2,
				PreProvisioned: 3,
				CleanupPolicy:  postgresql.DropCleanupPolicy,
			},
			time: time.Date(2024, 0o1, 9, 12, 53, 45, 100, time.UTC),
			expected: []postgresql.Partition{
				{
					Schema:      "public",
					Name:        "my_table_2024_w01",
					ParentTable: "my_table",
					LowerBound:  time.Date(2024, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2024, 0o1, 8, 0, 0, 0, 0, time.UTC),
				}, {
					Schema:      "public",
					Name:        "my_table_2023_w52",
					ParentTable: "my_table",
					LowerBound:  time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2024, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{
			name: "Yearly partition",
			partition: postgresql.PartitionConfiguration{
				Schema:         "public",
				Table:          "my_table",
				PartitionKey:   "created_at",
				Interval:       postgresql.YearlyInterval,
				Retention:      2,
				PreProvisioned: 3,
				CleanupPolicy:  postgresql.DropCleanupPolicy,
			},
			time: time.Date(2024, 0o1, 9, 12, 53, 45, 100, time.UTC),
			expected: []postgresql.Partition{
				{
					Schema:      "public",
					Name:        "my_table_2023",
					ParentTable: "my_table",
					LowerBound:  time.Date(2023, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2024, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
				}, {
					Schema:      "public",
					Name:        "my_table_2022",
					ParentTable: "my_table",
					LowerBound:  time.Date(2022, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2023, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{
			name: "No retention",
			partition: postgresql.PartitionConfiguration{
				Schema:         "public",
				Table:          "my_table",
				PartitionKey:   "created_at",
				Interval:       postgresql.WeeklyInterval,
				Retention:      0,
				PreProvisioned: 3,
				CleanupPolicy:  postgresql.DropCleanupPolicy,
			},
			time:     time.Date(2024, 0o1, 9, 12, 53, 45, 100, time.UTC),
			expected: []postgresql.Partition{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tables, _ := tc.partition.GetRetentionPartitions(tc.time)

			assert.DeepEqual(t, tables, tc.expected)
		})
	}
}

func TestPreProvisionedTableNames(t *testing.T) {
	testCases := []struct {
		name      string
		partition postgresql.PartitionConfiguration
		time      time.Time
		expected  []postgresql.Partition
	}{
		{
			name: "Daily partition",
			partition: postgresql.PartitionConfiguration{
				Schema:         "public",
				Table:          "my_table",
				PartitionKey:   "created_at",
				Interval:       postgresql.DailyInterval,
				Retention:      4,
				PreProvisioned: 4,
				CleanupPolicy:  postgresql.DropCleanupPolicy,
			},
			time: time.Date(2024, 0o1, 29, 12, 53, 45, 100, time.UTC),
			expected: []postgresql.Partition{
				{
					Schema:      "public",
					Name:        "my_table_2024_01_30",
					ParentTable: "my_table",
					LowerBound:  time.Date(2024, 0o1, 30, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2024, 0o1, 31, 0, 0, 0, 0, time.UTC),
				}, {
					Schema:      "public",
					Name:        "my_table_2024_01_31",
					ParentTable: "my_table",
					LowerBound:  time.Date(2024, 0o1, 31, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2024, 0o2, 0o1, 0, 0, 0, 0, time.UTC),
				}, {
					Schema:      "public",
					Name:        "my_table_2024_02_01",
					ParentTable: "my_table",
					LowerBound:  time.Date(2024, 0o2, 0o1, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2024, 0o2, 0o2, 0, 0, 0, 0, time.UTC),
				}, {
					Schema:      "public",
					Name:        "my_table_2024_02_02",
					ParentTable: "my_table",
					LowerBound:  time.Date(2024, 0o2, 0o2, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2024, 0o2, 0o3, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{
			name: "Monthly partition",
			partition: postgresql.PartitionConfiguration{
				Schema:         "public",
				Table:          "my_table",
				PartitionKey:   "created_at",
				Interval:       postgresql.MonthlyInterval,
				Retention:      3,
				PreProvisioned: 3,
				CleanupPolicy:  postgresql.DropCleanupPolicy,
			},
			time: time.Date(2023, 11, 29, 12, 53, 45, 100, time.UTC),
			expected: []postgresql.Partition{
				{
					Schema:      "public",
					Name:        "my_table_2023_12",
					ParentTable: "my_table",
					LowerBound:  time.Date(2023, 12, 0o1, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2024, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
				}, {
					Schema:      "public",
					Name:        "my_table_2024_01",
					ParentTable: "my_table",
					LowerBound:  time.Date(2024, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2024, 0o2, 0o1, 0, 0, 0, 0, time.UTC),
				}, {
					Schema:      "public",
					Name:        "my_table_2024_02",
					ParentTable: "my_table",
					LowerBound:  time.Date(2024, 0o2, 0o1, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2024, 0o3, 0o1, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{
			name: "Weekly partition",
			partition: postgresql.PartitionConfiguration{
				Schema:         "public",
				Table:          "my_table",
				PartitionKey:   "created_at",
				Interval:       postgresql.WeeklyInterval,
				Retention:      2,
				PreProvisioned: 2,
				CleanupPolicy:  postgresql.DropCleanupPolicy,
			},
			time: time.Date(2023, 12, 20, 12, 53, 45, 100, time.UTC),
			expected: []postgresql.Partition{
				{
					Schema:      "public",
					Name:        "my_table_2023_w52",
					ParentTable: "my_table",
					LowerBound:  time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2024, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
				}, {
					Schema:      "public",
					Name:        "my_table_2024_w01",
					ParentTable: "my_table",
					LowerBound:  time.Date(2024, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2024, 0o1, 8, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{
			name: "Yearly partition",
			partition: postgresql.PartitionConfiguration{
				Schema:         "public",
				Table:          "my_table",
				PartitionKey:   "created_at",
				Interval:       postgresql.YearlyInterval,
				Retention:      2,
				PreProvisioned: 2,
				CleanupPolicy:  postgresql.DropCleanupPolicy,
			},
			time: time.Date(2023, 12, 20, 12, 53, 45, 100, time.UTC),
			expected: []postgresql.Partition{
				{
					Schema:      "public",
					Name:        "my_table_2024",
					ParentTable: "my_table",
					LowerBound:  time.Date(2024, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2025, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
				}, {
					Schema:      "public",
					Name:        "my_table_2025",
					ParentTable: "my_table",
					LowerBound:  time.Date(2025, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
					UpperBound:  time.Date(2026, 0o1, 0o1, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{
			name: "No PreProvisioned",
			partition: postgresql.PartitionConfiguration{
				Schema:         "public",
				Table:          "my_table",
				PartitionKey:   "created_at",
				Interval:       postgresql.WeeklyInterval,
				Retention:      2,
				PreProvisioned: 0,
				CleanupPolicy:  postgresql.DropCleanupPolicy,
			},
			time:     time.Date(2023, 12, 20, 12, 53, 45, 100, time.UTC),
			expected: []postgresql.Partition{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tables, _ := tc.partition.GetPreProvisionedPartitions(tc.time)

			assert.DeepEqual(t, tables, tc.expected)
		})
	}
}
