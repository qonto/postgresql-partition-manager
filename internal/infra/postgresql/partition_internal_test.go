package postgresql

import (
	"testing"
	"time"

	"gotest.tools/assert"
)

func TestParseBounds(t *testing.T) {
	testCases := []struct {
		name       string
		partition  Partition
		lowerbound string
		upperBound string
	}{
		{
			"Date bounds",
			Partition{
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
			Partition{
				Schema:     "public",
				Name:       "my_table",
				LowerBound: "2024-01-01 10:00:00",
				UpperBound: "2025-02-03 12:53:00",
			},
			"2024-01-01T10:00:00Z",
			"2025-02-03T12:53:00Z",
		},
		{
			"UUIDv7 bounds",
			Partition{
				Schema:     "public",
				Name:       "my_table",
				LowerBound: "018cc251-f400-7100-0000-000000000000", // UUIDv7: 2024-01-01
				UpperBound: "018cc778-5000-7100-0000-000000000000", // UUIDv7: 2024-01-02
			},
			"2024-01-01T00:00:00Z",
			"2024-01-02T00:00:00Z",
		},
		{
			"Native Time.time bounds",
			Partition{
				Schema:     "public",
				Name:       "my_table",
				LowerBound: time.Date(2024, 1, 1, 10, 12, 5, 0, time.Now().UTC().Location()),
				UpperBound: time.Date(2025, 2, 3, 12, 53, 35, 0, time.Now().UTC().Location()),
			},
			"2024-01-01T10:12:05Z",
			"2025-02-03T12:53:35Z",
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
		partition Partition
	}{
		{
			"UUID v1 upper bound",
			Partition{
				Schema:     "public",
				Name:       "my_table",
				LowerBound: "018cc251-f400-7100-0000-000000000000", // UUIDv7: 2024-01-01
				UpperBound: "47568e76-fb49-11ee-b9c7-325096b39f47", // UUIDv1
			},
		},
		{
			"UUID v1 lower bound",
			Partition{
				Schema:     "public",
				Name:       "my_table",
				LowerBound: "ad5dac7a-fb46-11ee-be67-325096b39f47", // UUIDv1
				UpperBound: "018cc778-5000-7100-0000-000000000000", // UUIDv7: 2024-01-02
			},
		},
		{
			"Mix date format",
			Partition{
				Schema:     "public",
				Name:       "my_table",
				LowerBound: "2024-01-01",
				UpperBound: "2024-01-02 00:00:00",
			},
		},
		{
			"Mix date and UUIDv7",
			Partition{
				Schema:     "public",
				Name:       "my_table",
				LowerBound: "2024-01-01",
				UpperBound: "018cc778-5000-7100-0000-000000000000", // UUIDv7: 2024-01-02
			},
		},
		{
			"Mix date and UUIDv7",
			Partition{
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

func TestDebug(t *testing.T) {
	partition := Partition{
		Schema:     "public",
		Name:       "my_table",
		LowerBound: "2024-01-01 10:00:00",
		UpperBound: "2024-01-02 14:00:00",
	}
	_, _, err := parseBoundAsDateTime(partition)
	assert.NilError(t, err)
}
