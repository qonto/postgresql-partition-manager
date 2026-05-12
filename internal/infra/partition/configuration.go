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
	Interval       Interval      `mapstructure:"interval" validate:"required,oneof=daily weekly monthly quarterly yearly"`
	Retention      int           `mapstructure:"retention" validate:"required,gt=0"`
	PreProvisioned int           `mapstructure:"preProvisioned" validate:"required,gt=0"`
	CleanupPolicy  CleanupPolicy `mapstructure:"cleanupPolicy" validate:"required,oneof=drop detach"`

	// Conversion-specific fields (optional, only used by convert commands)
	BatchSize        int `mapstructure:"batchSize" validate:"omitempty,min=1,max=1000000"`
	ReplayBatchSize  int `mapstructure:"replayBatchSize" validate:"omitempty,min=1,max=1000000"`
	LockTimeout      int `mapstructure:"lockTimeout" validate:"omitempty,min=1,max=60"`
	StatementTimeout int `mapstructure:"statementTimeout" validate:"omitempty,min=5,max=120"`
}

// ApplyConvertDefaults sets default values for conversion-specific fields.
func (c *Configuration) ApplyConvertDefaults() {
	if c.BatchSize == 0 {
		c.BatchSize = 10000
	}

	if c.ReplayBatchSize == 0 {
		c.ReplayBatchSize = 1000
	}

	if c.LockTimeout == 0 {
		c.LockTimeout = 5
	}

	if c.StatementTimeout == 0 {
		c.StatementTimeout = 30
	}
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
	case Quarterly:
		year, month, _ := forDate.Date()

		var quarter int

		switch {
		case month >= 1 && month <= 3:
			quarter = 1
		case month >= 4 && month <= 6:
			quarter = 2
		case month >= 7 && month <= 9:
			quarter = 3
		case month >= 10 && month <= 12:
			quarter = 4
		}

		suffix = fmt.Sprintf("%d_q%d", year, quarter)
		lowerBound, upperBound = getQuarterlyBounds(forDate)
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
	case Quarterly:
		year, month, _ := forDate.Date()
		quarter := (int(month) - 1) / nbMonthsInAQuarter
		quarterStartMonth := time.Month(quarter*nbMonthsInAQuarter + 1)
		t = time.Date(year, quarterStartMonth-time.Month(i*nbMonthsInAQuarter), 1, 0, 0, 0, 0, forDate.Location())
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
	case Quarterly:
		year, month, _ := forDate.Date()
		quarter := (int(month) - 1) / nbMonthsInAQuarter
		quarterStartMonth := time.Month(quarter*nbMonthsInAQuarter + 1)
		t = time.Date(year, quarterStartMonth+time.Month(i*nbMonthsInAQuarter), 1, 0, 0, 0, 0, forDate.Location())
	case Yearly:
		year, _, _ := forDate.Date()

		t = time.Date(year+i, 1, 1, 0, 0, 0, 0, forDate.Location())
	default:
		return time.Time{}, ErrUnsupportedInterval
	}

	return t, nil
}
