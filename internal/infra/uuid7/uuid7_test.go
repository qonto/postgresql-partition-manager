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
		timestamp time.Time
		expected  string
	}{
		{
			time.Date(2024, 1, 20, 0, 0, 0, 0, time.UTC),
			"018d242a-c800-7000-0000-000000000000",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.timestamp.String(), func(t *testing.T) {
			generated := uuid7.FromTime(tc.timestamp)

			assert.Equal(t, generated, tc.expected, "Should match expected ")

			decoded, err := uuid.Parse(generated)
			timestamp, _ := decoded.Time().UnixTime()

			assert.Nil(t, err, "UUID should be parsable")
			assert.Equal(t, decoded.Version(), UUIDv7Version, "Should be an UUIDv7")
			assert.Equal(t, timestamp, tc.timestamp.Unix(), "Timestamp from generated UUID should match")
		})
	}
}
