package partition

import (
	"errors"
	"fmt"
	"log/slog"
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

type PartitionRange struct {
	LowerBound time.Time
	UpperBound time.Time
}

// Bounds provides a concise way to create a PartitionRange
func Bounds(lBound, uBound time.Time) PartitionRange {
	return PartitionRange{LowerBound: lBound, UpperBound: uBound}
}

func (r PartitionRange) String() string {
	return fmt.Sprintf("[ %s , %s ]", r.LowerBound.Format("02-01-2006"), r.UpperBound.Format("02-01-2006"))
}

func (r PartitionRange) LogValue() slog.Value {
	return slog.StringValue(r.String())
}

func (r PartitionRange) IsEmpty() bool {
	/* IsEmpty() is true when
	- either LowerBound.IsZero() is true and UpperBound.IsZero() is true
	- either the bounds are set (non-zero) but equal
	*/
	return r.LowerBound.Equal(r.UpperBound)
}

func (r PartitionRange) IsEqual(r2 PartitionRange) bool {
	return r.LowerBound.Equal(r2.LowerBound) && r.UpperBound.Equal(r2.UpperBound)
}

// Intersection returns the intersection between the intervals r and r2
func (r PartitionRange) Intersection(r2 PartitionRange) PartitionRange {
	var res PartitionRange // initialized with {time.Time{}, time.Time{}}

	if !(r2.LowerBound.After(r.UpperBound) || r.LowerBound.After(r2.UpperBound)) { // !empty intersection
		if r.LowerBound.After(r2.LowerBound) {
			res.LowerBound = r.LowerBound
		} else {
			res.LowerBound = r2.LowerBound
		}

		if r.UpperBound.Before(r2.UpperBound) {
			res.UpperBound = r.UpperBound
		} else {
			res.UpperBound = r2.UpperBound
		}
	}

	return res
}

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
