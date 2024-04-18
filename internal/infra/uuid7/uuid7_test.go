package uuid7_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/qonto/postgresql-partition-manager/internal/infra/uuid7"
	"github.com/stretchr/testify/assert"
)

const UUIDv7Version uuid.Version = 7

func TestFromTime(t *testing.T) {
	testCases := []struct {
		timestamp string
		expected  string
	}{
		{"2023-12-31T23:59:59Z", "018cc251-f018-7000-0000-000000000000"},
		{"2024-01-01T00:00:00Z", "018cc251-f400-7000-0000-000000000000"},
		{"2024-01-02T12:45:35Z", "018cca35-3998-7000-0000-000000000000"},
		{"2020-02-29T01:00:00Z", "01708e75-0a80-7000-0000-000000000000"},      // leap year
		{"1996-12-19T16:39:57-08:00", "00c62614-6b48-7000-0000-000000000000"}, // 20 minutes and 50.52 seconds after the 23rd hour of April 12th, 1985 in UTC.
	}

	for _, tc := range testCases {
		t.Run(tc.timestamp, func(t *testing.T) {
			timestamp, err := time.Parse(time.RFC3339, tc.timestamp)
			assert.Nil(t, err, "Time parse failed")

			generated := uuid7.FromTime(timestamp)

			assert.Equal(t, generated, tc.expected, "Should match expected ")

			decoded, err := uuid.Parse(generated)
			decodedTimestamp, _ := decoded.Time().UnixTime()

			assert.Nil(t, err, "UUID should be parsable")
			assert.Equal(t, decoded.Version(), UUIDv7Version, "Should be an UUIDv7")
			assert.Equal(t, timestamp.Unix(), decodedTimestamp, "Timestamp from generated UUID should match")
		})
	}
}
