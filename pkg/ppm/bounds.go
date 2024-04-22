package ppm

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
)

const (
	UUIDv7Version uuid.Version = 7
)

var (
	ErrLowerBoundAfterUpperBound = errors.New("lowerbound is after upperbound")
	ErrCantDecodePartitionBounds = errors.New("partition bounds cannot be decoded")
	ErrUnsupportedUUIDVersion    = errors.New("unsupported UUID version")
)

func parseBounds(partition postgresql.PartitionResult) (lowerBound time.Time, upperBound time.Time, err error) {
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

func parseBoundAsDate(partition postgresql.PartitionResult) (lowerBound, upperBound time.Time, err error) {
	lowerBound, err = time.Parse("2006-01-02", partition.LowerBound)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("can't parse lowerbound as date: %w", err)
	}

	upperBound, err = time.Parse("2006-01-02", partition.UpperBound)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("can't parse upperbound as date: %w", err)
	}

	return lowerBound, upperBound, nil
}

func parseBoundAsDateTime(partition postgresql.PartitionResult) (lowerBound, upperBound time.Time, err error) {
	lowerBound, err = time.Parse("2006-01-02 15:04:05", partition.LowerBound)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("can't parse lowerbound as datetime: %w", err)
	}

	upperBound, err = time.Parse("2006-01-02 15:04:05", partition.UpperBound)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("can't parse upperbound as datetime: %w", err)
	}

	return lowerBound, upperBound, nil
}

func parseBoundAsUUIDv7(partition postgresql.PartitionResult) (lowerBound, upperBound time.Time, err error) {
	lowerBoundUUID, err := uuid.Parse(partition.LowerBound)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("can't parse lowerbound as UUID: %w", err)
	}

	upperBoundUUID, err := uuid.Parse(partition.UpperBound)
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
