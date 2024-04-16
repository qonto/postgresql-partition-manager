package postgresql

import (
	"errors"
	"fmt"
	"time"
)

type (
	Interval      string
	CleanupPolicy string
)

const (
	DailyInterval       Interval      = "daily"
	WeeklyInterval      Interval      = "weekly"
	MonthlyInterval     Interval      = "monthly"
	YearlyInterval      Interval      = "yearly"
	DropCleanupPolicy   CleanupPolicy = "drop"
	DetachCleanupPolicy CleanupPolicy = "detach"
	daysInAweek         int           = 7
)

var ErrUnsupportedInterval = errors.New("unsupported partition interval")

type PartitionConfiguration struct {
	Schema         string        `mapstructure:"schema" validate:"required"`
	Table          string        `mapstructure:"table" validate:"required"`
	PartitionKey   string        `mapstructure:"partitionKey" validate:"required"`
	Interval       Interval      `mapstructure:"interval" validate:"required,oneof=daily weekly monthly yearly"`
	Retention      int           `mapstructure:"retention" validate:"required,gt=0"`
	PreProvisioned int           `mapstructure:"preProvisioned" validate:"required,gt=0"`
	CleanupPolicy  CleanupPolicy `mapstructure:"cleanupPolicy" validate:"required,oneof=drop detach"`
}

func (p PartitionConfiguration) GeneratePartition(forDate time.Time) (Partition, error) {
	var suffix string

	var lowerBound, upperBound any

	switch p.Interval {
	case DailyInterval:
		suffix = forDate.Format("2006_01_02")
		lowerBound, upperBound = getDailyBounds(forDate)
	case WeeklyInterval:
		year, week := forDate.ISOWeek()
		suffix = fmt.Sprintf("%d_w%02d", year, week)
		lowerBound, upperBound = getWeeklyBounds(forDate)
	case MonthlyInterval:
		suffix = forDate.Format("2006_01")
		lowerBound, upperBound = getMonthlyBounds(forDate)
	case YearlyInterval:
		suffix = forDate.Format("2006")
		lowerBound, upperBound = getYearlyBounds(forDate)
	default:
		return Partition{}, ErrUnsupportedInterval
	}

	partition := Partition{
		Schema:      p.Schema,
		ParentTable: p.Table,
		Name:        fmt.Sprintf("%s_%s", p.Table, suffix),
		LowerBound:  lowerBound,
		UpperBound:  upperBound,
	}

	return partition, nil
}

func (p PartitionConfiguration) GetRetentionPartitions(forDate time.Time) ([]Partition, error) {
	partitions := make([]Partition, p.Retention)

	for i := 1; i <= p.Retention; i++ {
		prevDate, err := p.getPrevDate(forDate, i)
		if err != nil {
			return nil, fmt.Errorf("could not compute previous date: %w", err)
		}

		partition, err := p.GeneratePartition(prevDate)
		if err != nil {
			return nil, fmt.Errorf("could not generate partition: %w", err)
		}

		partitions[i-1] = partition
	}

	return partitions, nil
}

func (p PartitionConfiguration) GetPreProvisionedPartitions(forDate time.Time) ([]Partition, error) {
	partitions := make([]Partition, p.PreProvisioned)

	for i := 1; i <= p.PreProvisioned; i++ {
		nextDate, err := p.getNextDate(forDate, i)
		if err != nil {
			return nil, fmt.Errorf("could not compute next date: %w", err)
		}

		partition, err := p.GeneratePartition(nextDate)
		if err != nil {
			return nil, fmt.Errorf("could not generate partition: %w", err)
		}

		partitions[i-1] = partition
	}

	return partitions, nil
}

func (p PartitionConfiguration) getPrevDate(forDate time.Time, i int) (t time.Time, err error) {
	switch p.Interval {
	case DailyInterval:
		t = forDate.AddDate(0, 0, -i)
	case WeeklyInterval:
		t = forDate.AddDate(0, 0, -i*daysInAweek)
	case MonthlyInterval:
		year, month, _ := forDate.Date()

		t = time.Date(year, month-time.Month(i), 1, 0, 0, 0, 0, forDate.Location())
	case YearlyInterval:
		year, _, _ := forDate.Date()

		t = time.Date(year-i, 1, 1, 0, 0, 0, 0, forDate.Location())
	default:
		return time.Time{}, ErrUnsupportedInterval
	}

	return t, nil
}

func (p PartitionConfiguration) getNextDate(forDate time.Time, i int) (t time.Time, err error) {
	switch p.Interval {
	case DailyInterval:
		t = forDate.AddDate(0, 0, i)
	case WeeklyInterval:
		t = forDate.AddDate(0, 0, i*daysInAweek)
	case MonthlyInterval:
		year, month, _ := forDate.Date()

		t = time.Date(year, month+time.Month(i), 1, 0, 0, 0, 0, forDate.Location())
	case YearlyInterval:
		year, _, _ := forDate.Date()

		t = time.Date(year+i, 1, 1, 0, 0, 0, 0, forDate.Location())
	default:
		return time.Time{}, ErrUnsupportedInterval
	}

	return t, nil
}
