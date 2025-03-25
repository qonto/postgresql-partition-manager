package partition

import (
	"testing"
	"time"

	"gotest.tools/assert"
)

func TestGetWeeklyBounds(t *testing.T) {
	testCases := []struct {
		name          string
		date          time.Time
		expectedLower time.Time
		expectedUpper time.Time
	}{
		{
			name:          "Monday",
			date:          time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC), // Monday
			expectedLower: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "Wednesday",
			date:          time.Date(2024, 1, 3, 15, 45, 0, 0, time.UTC), // Wednesday
			expectedLower: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "Sunday",
			date:          time.Date(2024, 1, 7, 23, 59, 59, 0, time.UTC), // Sunday
			expectedLower: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "Saturday",
			date:          time.Date(2024, 1, 6, 0, 0, 0, 0, time.UTC), // Saturday
			expectedLower: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "Month boundary",
			date:          time.Date(2024, 1, 31, 12, 0, 0, 0, time.UTC), // Wednesday
			expectedLower: time.Date(2024, 1, 29, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2024, 2, 5, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "Year boundary",
			date:          time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC), // Sunday
			expectedLower: time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lowerBound, upperBound := getWeeklyBounds(tc.date)

			assert.Equal(t, lowerBound, tc.expectedLower, "Lower bound mismatch")
			assert.Equal(t, upperBound, tc.expectedUpper, "Upper bound mismatch")

			// Verify the bounds span exactly 7 days
			daysDiff := upperBound.Sub(lowerBound).Hours() / 24
			assert.Equal(t, float64(nbDaysInAWeek), daysDiff, "Bounds should span exactly 7 days")
		})
	}
}
