package postgresql

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	UUIDv7Version uuid.Version = 7
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

func parseBounds(partition Partition) (lowerBound time.Time, upperBound time.Time, err error) {
	lowerBound, upperBound, err = parseBoundAsTime(partition)
	if err == nil {
		return lowerBound, upperBound, nil
	}

	lowerBound, upperBound, err = parseBoundAsDate(partition)
	if err == nil {
		return lowerBound, upperBound, nil
	}

	lowerBound, upperBound, err = parseBoundAsDateTime(partition)
	if err == nil {
		return lowerBound, upperBound, nil
	}

	lowerBound, upperBound, err = parseBoundAsUUIDv7(partition)
	if err == nil {
		return lowerBound, upperBound, nil
	}

	if lowerBound.After(lowerBound) {
		return time.Time{}, time.Time{}, ErrLowerBoundAfterUpperBound
	}

	return time.Time{}, time.Time{}, ErrCantDecodePartitionBounds
}

func parseBoundAsTime(partition Partition) (lowerBound, upperBound time.Time, err error) {
	lowerBound, ok := partition.LowerBound.(time.Time)
	if !ok {
		return time.Time{}, time.Time{}, fmt.Errorf("can't parse lowerbound as time: %w", err)
	}

	upperBound, ok = partition.UpperBound.(time.Time)
	if !ok {
		return time.Time{}, time.Time{}, fmt.Errorf("can't parse upperbound as time: %w", err)
	}

	return lowerBound, upperBound, nil
}

func parseBoundAsDate(partition Partition) (lowerBound, upperBound time.Time, err error) {
	lowerBound, err = time.Parse("2006-01-02", partition.LowerBound.(string))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("can't parse lowerbound as date: %w", err)
	}

	upperBound, err = time.Parse("2006-01-02", partition.UpperBound.(string))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("can't parse upperbound as date: %w", err)
	}

	return lowerBound, upperBound, nil
}

func parseBoundAsDateTime(partition Partition) (lowerBound, upperBound time.Time, err error) {
	lowerBound, err = time.Parse("2006-01-02 15:04:05", partition.LowerBound.(string))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("can't parse lowerbound as datetime: %w", err)
	}

	upperBound, err = time.Parse("2006-01-02 15:04:05", partition.UpperBound.(string))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("can't parse upperbound as datetime: %w", err)
	}

	return lowerBound, upperBound, nil
}

func parseBoundAsUUIDv7(partition Partition) (lowerBound, upperBound time.Time, err error) {
	lowerBoundUUID, err := uuid.Parse(partition.LowerBound.(string))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("can't parse lowerbound as UUID: %w", err)
	}

	upperBoundUUID, err := uuid.Parse(partition.UpperBound.(string))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("can't parse upperbound as UUID: %w", err)
	}

	if upperBoundUUID.Version() != UUIDv7Version || lowerBoundUUID.Version() != UUIDv7Version {
		return time.Time{}, time.Time{}, ErrUnsupportedUUIDVersion
	}

	upperBound = time.Unix(upperBoundUUID.Time().UnixTime()).UTC()
	lowerBound = time.Unix(lowerBoundUUID.Time().UnixTime()).UTC()

	return lowerBound, upperBound, nil
}
