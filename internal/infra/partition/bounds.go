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
	offset := int(date.Weekday() - time.Monday)
	if offset < 0 {
		offset = 6 // adjust Sunday to be 6 days after Monday instead of 1 day before
	}

	lowerBound = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.UTC().Location()).AddDate(0, 0, -1*offset)
	upperBound = lowerBound.AddDate(0, 0, nbDaysInAWeek)

	return
}

func getMonthlyBounds(date time.Time) (lowerBound, upperBound time.Time) {
	lowerBound = time.Date(date.Year(), date.Month(), 1, 0, 0, 0, 0, date.UTC().Location())
	upperBound = lowerBound.AddDate(0, 1, 0)

	return
}

func getQuarterlyBounds(date time.Time) (lowerBound, upperBound time.Time) {
	year, _, _ := date.Date()

	quarter := (int(date.Month()) - 1) / nbMonthsInAQuarter
	firstMonthOfTheQuarter := time.Month(quarter*nbMonthsInAQuarter + 1)

	lowerBound = time.Date(year, firstMonthOfTheQuarter, 1, 0, 0, 0, 0, date.UTC().Location())
	upperBound = lowerBound.AddDate(0, nbMonthsInAQuarter, 0)

	return
}

func getYearlyBounds(date time.Time) (lowerBound, upperBound time.Time) {
	lowerBound = time.Date(date.Year(), 1, 1, 0, 0, 0, 0, date.UTC().Location())
	upperBound = lowerBound.AddDate(1, 0, 0)

	return
}
