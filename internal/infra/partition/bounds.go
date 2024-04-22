package partition

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

const (
	UUIDv7Version uuid.Version = 7
	daysInAweek   int          = 7
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
	upperBound = lowerBound.AddDate(0, 0, daysInAweek)

	return
}

func getMonthlyBounds(date time.Time) (lowerBound, upperBound time.Time) {
	lowerBound = time.Date(date.Year(), date.Month(), 1, 0, 0, 0, 0, date.UTC().Location())
	upperBound = lowerBound.AddDate(0, 1, 0)

	return
}

func getYearlyBounds(date time.Time) (lowerBound, upperBound time.Time) {
	lowerBound = time.Date(date.Year(), 1, 1, 0, 0, 0, 0, date.UTC().Location())
	upperBound = lowerBound.AddDate(1, 0, 0)

	return
}
