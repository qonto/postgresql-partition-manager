package partition

import (
	"fmt"
	"time"
)

type (
	CleanupPolicy string
)

const (
	Drop   CleanupPolicy = "drop"
	Detach CleanupPolicy = "detach"
)

type Configuration struct {
	Schema         string        `mapstructure:"schema" validate:"required"`
	Table          string        `mapstructure:"table" validate:"required"`
	PartitionKey   string        `mapstructure:"partitionKey" validate:"required"`
	Interval       Interval      `mapstructure:"interval" validate:"required,oneof=daily weekly monthly yearly"`
	Retention      int           `mapstructure:"retention" validate:"required,gt=0"`
	PreProvisioned int           `mapstructure:"preProvisioned" validate:"required,gt=0"`
	CleanupPolicy  CleanupPolicy `mapstructure:"cleanupPolicy" validate:"required,oneof=drop detach"`
}

func (p Configuration) GeneratePartition(forDate time.Time) (Partition, error) {
	var suffix string

	var lowerBound, upperBound time.Time

	switch p.Interval {
	case Daily:
		suffix = forDate.Format("2006_01_02")
		lowerBound, upperBound = getDailyBounds(forDate)
	case Weekly:
		year, week := forDate.ISOWeek()
		suffix = fmt.Sprintf("%d_w%02d", year, week)
		lowerBound, upperBound = getWeeklyBounds(forDate)
	case Monthly:
		suffix = forDate.Format("2006_01")
		lowerBound, upperBound = getMonthlyBounds(forDate)
	case Yearly:
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

func (p Configuration) GetRetentionPartitions(forDate time.Time) ([]Partition, error) {
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

func (p Configuration) GetPreProvisionedPartitions(forDate time.Time) ([]Partition, error) {
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

func (p Configuration) getPrevDate(forDate time.Time, i int) (t time.Time, err error) {
	switch p.Interval {
	case Daily:
		t = forDate.AddDate(0, 0, -i)
	case Weekly:
		t = forDate.AddDate(0, 0, -i*nbDaysInAWeek)
	case Monthly:
		year, month, _ := forDate.Date()

		t = time.Date(year, month-time.Month(i), 1, 0, 0, 0, 0, forDate.Location())
	case Yearly:
		year, _, _ := forDate.Date()

		t = time.Date(year-i, 1, 1, 0, 0, 0, 0, forDate.Location())
	default:
		return time.Time{}, ErrUnsupportedInterval
	}

	return t, nil
}

func (p Configuration) getNextDate(forDate time.Time, i int) (t time.Time, err error) {
	switch p.Interval {
	case Daily:
		t = forDate.AddDate(0, 0, i)
	case Weekly:
		t = forDate.AddDate(0, 0, i*nbDaysInAWeek)
	case Monthly:
		year, month, _ := forDate.Date()

		t = time.Date(year, month+time.Month(i), 1, 0, 0, 0, 0, forDate.Location())
	case Yearly:
		year, _, _ := forDate.Date()

		t = time.Date(year+i, 1, 1, 0, 0, 0, 0, forDate.Location())
	default:
		return time.Time{}, ErrUnsupportedInterval
	}

	return t, nil
}
