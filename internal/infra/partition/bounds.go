package partition

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

const (
	UUIDv7Version      uuid.Version = 7
	nbDaysInAWeek      int          = 7
	nbMonthsInAQuarter int          = 3
)

var (
	ErrLowerBoundAfterUpperBound = errors.New("lowerbound is after upperbound")

	// ErrCantDecodePartitionBounds represents an error indicating that the partition bounds cannot be decoded.
	ErrCantDecodePartitionBounds = errors.New("partition bounds cannot be decoded")

	ErrUnsupportedUUIDVersion = errors.New("unsupported UUID version")
)

func getDailyBounds(date time.Time) (lowerBound, upperBound time.Time) {
	lowerBound = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.UTC().Location())
	upperBound = lowerBound.AddDate(0, 0, 1)

	return
}

func getWeeklyBounds(date time.Time) (lowerBound, upperBound time.Time) {
	lowerBound = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.UTC().Location()).AddDate(0, 0, -int(date.Weekday()-time.Monday))
	upperBound = lowerBound.AddDate(0, 0, nbDaysInAWeek)

	return
}

func getMonthlyBounds(date time.Time) (lowerBound, upperBound time.Time) {
	lowerBound = time.Date(date.Year(), date.Month(), 1, 0, 0, 0, 0, date.UTC().Location())
	upperBound = lowerBound.AddDate(0, 1, 0)

	return
}

func getQuarterlyBounds(date time.Time) (lowerBound, upperBound time.Time) {
	year, month, _ := date.Date()

	var firstMonthOfTheQuarter time.Month

	switch {
	case month >= 1 && month <= 3:
		firstMonthOfTheQuarter = time.January
	case month >= 4 && month <= 6:
		firstMonthOfTheQuarter = time.April
	case month >= 7 && month <= 9:
		firstMonthOfTheQuarter = time.July
	case month >= 10 && month <= 12:
		firstMonthOfTheQuarter = time.October
	}

	lowerBound = time.Date(year, firstMonthOfTheQuarter, 1, 0, 0, 0, 0, date.UTC().Location())
	upperBound = lowerBound.AddDate(0, nbMonthsInAQuarter, 0)

	return
}

func getYearlyBounds(date time.Time) (lowerBound, upperBound time.Time) {
	lowerBound = time.Date(date.Year(), 1, 1, 0, 0, 0, 0, date.UTC().Location())
	upperBound = lowerBound.AddDate(1, 0, 0)

	return
}
