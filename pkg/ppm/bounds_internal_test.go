package ppm

import (
	"testing"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"gotest.tools/assert"
)

func TestParseBounds(t *testing.T) {
	testCases := []struct {
		name       string
		partition  postgresql.PartitionResult
		lowerbound string
		upperBound string
	}{
		{
			"Date bounds",
			postgresql.PartitionResult{
				Schema:     "public",
				Name:       "my_table",
				LowerBound: "2024-01-01",
				UpperBound: "2025-03-02",
			},
			"2024-01-01T00:00:00Z",
			"2025-03-02T00:00:00Z",
		},
		{
			"Datetime bounds",
			postgresql.PartitionResult{
				Schema:     "public",
				Name:       "my_table",
				LowerBound: "2024-01-01 10:00:00",
				UpperBound: "2025-02-03 12:53:00",
			},
			"2024-01-01T10:00:00Z",
			"2025-02-03T12:53:00Z",
		},
		{
			"Datetime with timezone bounds",
			postgresql.PartitionResult{
				Schema:     "public",
				Name:       "my_table",
				LowerBound: "2024-01-01 23:30:00-01",
				UpperBound: "2025-02-03 00:30:00+01",
			},
			"2024-01-01T23:30:00Z",
			"2025-02-03T00:30:00Z",
		},
		{
			"UUIDv7 bounds",
			postgresql.PartitionResult{
				Schema:     "public",
				Name:       "my_table",
				LowerBound: "018cc251-f400-7100-0000-000000000000", // UUIDv7: 2024-01-01
				UpperBound: "018cc778-5000-7100-0000-000000000000", // UUIDv7: 2024-01-02
			},
			"2024-01-01T00:00:00Z",
			"2024-01-02T00:00:00Z",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expectedLowerbound, err := time.Parse(time.RFC3339, tc.lowerbound)
			assert.NilError(t, err, "LowerBound parsing failed")

			expectedUpperBound, err := time.Parse(time.RFC3339, tc.upperBound)
			assert.NilError(t, err, "Upperbound parsing failed")

			lowerBound, upperBound, err := parseBounds(tc.partition)

			assert.NilError(t, err, "Bounds parsing should succeed")
			assert.Equal(t, lowerBound, expectedLowerbound, "LowerBound mismatch")
			assert.Equal(t, upperBound, expectedUpperBound, "UpperBound mismatch")
		})
	}
}

func TestParseInvalidBounds(t *testing.T) {
	testCases := []struct {
		name      string
		partition postgresql.PartitionResult
	}{
		{
			"UUID v1 upper bound",
			postgresql.PartitionResult{
				Schema:     "public",
				Name:       "my_table",
				LowerBound: "018cc251-f400-7100-0000-000000000000", // UUIDv7: 2024-01-01
				UpperBound: "47568e76-fb49-11ee-b9c7-325096b39f47", // UUIDv1
			},
		},
		{
			"UUID v1 lower bound",
			postgresql.PartitionResult{
				Schema:     "public",
				Name:       "my_table",
				LowerBound: "ad5dac7a-fb46-11ee-be67-325096b39f47", // UUIDv1
				UpperBound: "018cc778-5000-7100-0000-000000000000", // UUIDv7: 2024-01-02
			},
		},
		{
			"Mix date format",
			postgresql.PartitionResult{
				Schema:     "public",
				Name:       "my_table",
				LowerBound: "2024-01-01",
				UpperBound: "2024-01-02 00:00:00",
			},
		},
		{
			"Mix date and UUIDv7",
			postgresql.PartitionResult{
				Schema:     "public",
				Name:       "my_table",
				LowerBound: "2024-01-01",
				UpperBound: "018cc778-5000-7100-0000-000000000000", // UUIDv7: 2024-01-02
			},
		},
		{
			"Mix date and UUIDv7",
			postgresql.PartitionResult{
				Schema:     "public",
				Name:       "my_table",
				LowerBound: "2024-01-01",
				UpperBound: "018cc778-5000-7100-0000-000000000000", // UUIDv7: 2024-01-02
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := parseBounds(tc.partition)

			assert.ErrorContains(t, err, "partition bounds cannot be decoded")
		})
	}
}
