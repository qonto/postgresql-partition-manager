package partition

import (
	"testing"
	"time"

	"gotest.tools/assert"
)

func TestGetDailyBounds(t *testing.T) {
	testCases := []struct {
		name          string
		date          time.Time
		expectedLower time.Time
		expectedUpper time.Time
	}{
		{
			name:          "Regular day",
			date:          time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC),
			expectedLower: time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "Month boundary",
			date:          time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC),
			expectedLower: time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "Leap day",
			date:          time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),
			expectedLower: time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "Year boundary",
			date:          time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
			expectedLower: time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lowerBound, upperBound := getDailyBounds(tc.date)

			assert.Equal(t, lowerBound, tc.expectedLower, "Lower bound mismatch")
			assert.Equal(t, upperBound, tc.expectedUpper, "Upper bound mismatch")

			daysDiff := upperBound.Sub(lowerBound).Hours() / 24
			assert.Equal(t, float64(1), daysDiff, "Bounds should span exactly 1 day")
		})
	}
}

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

func TestGetMonthlyBounds(t *testing.T) {
	testCases := []struct {
		name          string
		date          time.Time
		expectedLower time.Time
		expectedUpper time.Time
	}{
		{
			name:          "Regular month",
			date:          time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
			expectedLower: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "January",
			date:          time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC),
			expectedLower: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "February leap year",
			date:          time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC),
			expectedLower: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "December year boundary",
			date:          time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
			expectedLower: time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "First day of month",
			date:          time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
			expectedLower: time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lowerBound, upperBound := getMonthlyBounds(tc.date)

			assert.Equal(t, lowerBound, tc.expectedLower, "Lower bound mismatch")
			assert.Equal(t, upperBound, tc.expectedUpper, "Upper bound mismatch")
		})
	}
}

func TestGetQuarterlyBounds(t *testing.T) {
	testCases := []struct {
		name          string
		date          time.Time
		expectedLower time.Time
		expectedUpper time.Time
	}{
		{
			name:          "Q1 start",
			date:          time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedLower: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "Q1 end",
			date:          time.Date(2024, 3, 31, 0, 0, 0, 0, time.UTC),
			expectedLower: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "Q2 mid",
			date:          time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC),
			expectedLower: time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "Q3 start",
			date:          time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC),
			expectedLower: time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "Q4 end",
			date:          time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
			expectedLower: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lowerBound, upperBound := getQuarterlyBounds(tc.date)

			assert.Equal(t, lowerBound, tc.expectedLower, "Lower bound mismatch")
			assert.Equal(t, upperBound, tc.expectedUpper, "Upper bound mismatch")
		})
	}
}

func TestGetYearlyBounds(t *testing.T) {
	testCases := []struct {
		name          string
		date          time.Time
		expectedLower time.Time
		expectedUpper time.Time
	}{
		{
			name:          "Start of year",
			date:          time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedLower: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "End of year",
			date:          time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			expectedLower: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:          "Leap year",
			date:          time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC),
			expectedLower: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedUpper: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lowerBound, upperBound := getYearlyBounds(tc.date)

			assert.Equal(t, lowerBound, tc.expectedLower, "Lower bound mismatch")
			assert.Equal(t, upperBound, tc.expectedUpper, "Upper bound mismatch")
		})
	}
}
