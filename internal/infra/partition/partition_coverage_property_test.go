package partition

import (
	"testing"
	"time"

	"pgregory.net/rapid"
)

// Feature: table-partition-conversion, Property 5: Partition Coverage Calculation
// For any valid min/max date range, partition interval, and pre-provisioned count,
// the generated set of partitions SHALL cover every date from the minimum value to
// the maximum value without gaps, plus exactly the configured number of future
// partitions beyond the maximum value.
// Validates: Requirements 3.2

func TestProperty5_PartitionCoverageCalculation_CoversFullRangeWithoutGaps(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		intervals := []Interval{Daily, Weekly, Monthly, Quarterly, Yearly}
		interval := intervals[rapid.IntRange(0, len(intervals)-1).Draw(t, "intervalIdx")]
		preProvisioned := rapid.IntRange(1, 10).Draw(t, "preProvisioned")

		// Generate a min date within a reasonable range (2020-2025)
		minYear := rapid.IntRange(2020, 2024).Draw(t, "minYear")
		minMonth := rapid.IntRange(1, 12).Draw(t, "minMonth")
		minDay := rapid.IntRange(1, 28).Draw(t, "minDay") // Use 28 to avoid invalid dates
		minDate := time.Date(minYear, time.Month(minMonth), minDay, 0, 0, 0, 0, time.UTC)

		// Generate a max date that is after min date, constrained by interval to keep test fast
		var maxDate time.Time
		switch interval {
		case Daily:
			daysAhead := rapid.IntRange(1, 30).Draw(t, "daysAhead")
			maxDate = minDate.AddDate(0, 0, daysAhead)
		case Weekly:
			weeksAhead := rapid.IntRange(1, 12).Draw(t, "weeksAhead")
			maxDate = minDate.AddDate(0, 0, weeksAhead*7)
		case Monthly:
			monthsAhead := rapid.IntRange(1, 12).Draw(t, "monthsAhead")
			maxDate = minDate.AddDate(0, monthsAhead, 0)
		case Quarterly:
			quartersAhead := rapid.IntRange(1, 8).Draw(t, "quartersAhead")
			maxDate = minDate.AddDate(0, quartersAhead*3, 0)
		case Yearly:
			yearsAhead := rapid.IntRange(1, 5).Draw(t, "yearsAhead")
			maxDate = minDate.AddDate(yearsAhead, 0, 0)
		}

		config := Configuration{
			Schema:         "public",
			Table:          "test_table",
			PartitionKey:   "created_at",
			Interval:       interval,
			Retention:      1,
			PreProvisioned: preProvisioned,
			CleanupPolicy:  Drop,
		}

		// Generate partitions covering min to max (same logic as converter.go)
		var partitions []Partition
		currentDate := minDate

		for !currentDate.After(maxDate) {
			p, err := config.GeneratePartition(currentDate)
			if err != nil {
				t.Fatalf("failed to generate partition for date %s: %v", currentDate, err)
			}

			partitions = append(partitions, p)
			currentDate = p.UpperBound
		}

		// Generate pre-provisioned future partitions
		futurePartitions, err := config.GetPreProvisionedPartitions(maxDate)
		if err != nil {
			t.Fatalf("failed to generate pre-provisioned partitions: %v", err)
		}

		partitions = append(partitions, futurePartitions...)

		// Property: partitions must not be empty
		if len(partitions) == 0 {
			t.Fatal("partition set must not be empty for a valid date range")
		}

		// Property: first partition must contain minDate
		if minDate.Before(partitions[0].LowerBound) {
			t.Fatalf("first partition lower bound %s is after min date %s — gap at start",
				partitions[0].LowerBound, minDate)
		}

		// Property: partitions are contiguous (no gaps) — each partition's lower bound
		// equals the previous partition's upper bound
		for i := 1; i < len(partitions); i++ {
			if !partitions[i].LowerBound.Equal(partitions[i-1].UpperBound) {
				t.Fatalf("gap between partition %d (upper=%s) and partition %d (lower=%s)",
					i-1, partitions[i-1].UpperBound, i, partitions[i].LowerBound)
			}
		}

		// Property: the data range partitions cover maxDate
		// Find the last data partition (before future partitions)
		dataPartitionCount := len(partitions) - len(futurePartitions)
		lastDataPartition := partitions[dataPartitionCount-1]

		if maxDate.After(lastDataPartition.UpperBound) || maxDate.Before(lastDataPartition.LowerBound) {
			// maxDate should be within or before the upper bound of the last data partition
			if maxDate.After(lastDataPartition.UpperBound) {
				t.Fatalf("max date %s is not covered by data partitions (last data partition upper bound: %s)",
					maxDate, lastDataPartition.UpperBound)
			}
		}

		// Property: exactly preProvisioned future partitions beyond maxDate
		if len(futurePartitions) != preProvisioned {
			t.Fatalf("expected %d pre-provisioned partitions, got %d",
				preProvisioned, len(futurePartitions))
		}
	})
}

func TestProperty5_PartitionCoverageCalculation_NoGapsForAnyDateInRange(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		intervals := []Interval{Daily, Weekly, Monthly, Quarterly, Yearly}
		interval := intervals[rapid.IntRange(0, len(intervals)-1).Draw(t, "intervalIdx")]
		preProvisioned := rapid.IntRange(1, 5).Draw(t, "preProvisioned")

		// Generate a min date
		minYear := rapid.IntRange(2020, 2024).Draw(t, "minYear")
		minMonth := rapid.IntRange(1, 12).Draw(t, "minMonth")
		minDay := rapid.IntRange(1, 28).Draw(t, "minDay")
		minDate := time.Date(minYear, time.Month(minMonth), minDay, 0, 0, 0, 0, time.UTC)

		// Generate a max date constrained by interval
		var maxDate time.Time
		switch interval {
		case Daily:
			daysAhead := rapid.IntRange(1, 14).Draw(t, "daysAhead")
			maxDate = minDate.AddDate(0, 0, daysAhead)
		case Weekly:
			weeksAhead := rapid.IntRange(1, 8).Draw(t, "weeksAhead")
			maxDate = minDate.AddDate(0, 0, weeksAhead*7)
		case Monthly:
			monthsAhead := rapid.IntRange(1, 6).Draw(t, "monthsAhead")
			maxDate = minDate.AddDate(0, monthsAhead, 0)
		case Quarterly:
			quartersAhead := rapid.IntRange(1, 4).Draw(t, "quartersAhead")
			maxDate = minDate.AddDate(0, quartersAhead*3, 0)
		case Yearly:
			yearsAhead := rapid.IntRange(1, 3).Draw(t, "yearsAhead")
			maxDate = minDate.AddDate(yearsAhead, 0, 0)
		}

		config := Configuration{
			Schema:         "public",
			Table:          "test_table",
			PartitionKey:   "created_at",
			Interval:       interval,
			Retention:      1,
			PreProvisioned: preProvisioned,
			CleanupPolicy:  Drop,
		}

		// Generate partitions covering min to max
		var partitions []Partition
		currentDate := minDate

		for !currentDate.After(maxDate) {
			p, err := config.GeneratePartition(currentDate)
			if err != nil {
				t.Fatalf("failed to generate partition for date %s: %v", currentDate, err)
			}

			partitions = append(partitions, p)
			currentDate = p.UpperBound
		}

		// Generate pre-provisioned future partitions
		futurePartitions, err := config.GetPreProvisionedPartitions(maxDate)
		if err != nil {
			t.Fatalf("failed to generate pre-provisioned partitions: %v", err)
		}

		partitions = append(partitions, futurePartitions...)

		// Property: any arbitrary date between minDate and maxDate must fall within
		// exactly one partition (no gaps)
		totalRange := maxDate.Sub(minDate)
		if totalRange <= 0 {
			return // degenerate case
		}

		// Pick a random point in the range
		offsetNanos := rapid.Int64Range(0, int64(totalRange)).Draw(t, "offsetNanos")
		testDate := minDate.Add(time.Duration(offsetNanos))

		found := false

		for _, p := range partitions {
			if !testDate.Before(p.LowerBound) && testDate.Before(p.UpperBound) {
				found = true

				break
			}
		}

		if !found {
			t.Fatalf("date %s in range [%s, %s] is not covered by any partition",
				testDate, minDate, maxDate)
		}
	})
}

func TestProperty5_PartitionCoverageCalculation_ExactPreProvisionedCount(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		intervals := []Interval{Daily, Weekly, Monthly, Quarterly, Yearly}
		interval := intervals[rapid.IntRange(0, len(intervals)-1).Draw(t, "intervalIdx")]
		preProvisioned := rapid.IntRange(1, 15).Draw(t, "preProvisioned")

		// Generate a reference date
		year := rapid.IntRange(2020, 2025).Draw(t, "year")
		month := rapid.IntRange(1, 12).Draw(t, "month")
		day := rapid.IntRange(1, 28).Draw(t, "day")
		maxDate := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)

		config := Configuration{
			Schema:         "public",
			Table:          "test_table",
			PartitionKey:   "created_at",
			Interval:       interval,
			Retention:      1,
			PreProvisioned: preProvisioned,
			CleanupPolicy:  Drop,
		}

		// Generate pre-provisioned future partitions
		futurePartitions, err := config.GetPreProvisionedPartitions(maxDate)
		if err != nil {
			t.Fatalf("failed to generate pre-provisioned partitions: %v", err)
		}

		// Property: exactly preProvisioned partitions are generated
		if len(futurePartitions) != preProvisioned {
			t.Fatalf("expected exactly %d pre-provisioned partitions, got %d",
				preProvisioned, len(futurePartitions))
		}

		// Property: all future partitions are beyond the partition containing maxDate
		maxPartition, err := config.GeneratePartition(maxDate)
		if err != nil {
			t.Fatalf("failed to generate partition for max date: %v", err)
		}

		for i, fp := range futurePartitions {
			if fp.LowerBound.Before(maxPartition.UpperBound) {
				t.Fatalf("future partition %d (lower=%s) starts before max partition upper bound %s",
					i, fp.LowerBound, maxPartition.UpperBound)
			}
		}

		// Property: future partitions are contiguous
		for i := 1; i < len(futurePartitions); i++ {
			if !futurePartitions[i].LowerBound.Equal(futurePartitions[i-1].UpperBound) {
				t.Fatalf("gap between future partition %d (upper=%s) and %d (lower=%s)",
					i-1, futurePartitions[i-1].UpperBound, i, futurePartitions[i].LowerBound)
			}
		}
	})
}

func TestProperty5_PartitionCoverageCalculation_SingleDateRange(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		intervals := []Interval{Daily, Weekly, Monthly, Quarterly, Yearly}
		interval := intervals[rapid.IntRange(0, len(intervals)-1).Draw(t, "intervalIdx")]
		preProvisioned := rapid.IntRange(1, 10).Draw(t, "preProvisioned")

		// Generate a single date (min == max scenario)
		year := rapid.IntRange(2020, 2025).Draw(t, "year")
		month := rapid.IntRange(1, 12).Draw(t, "month")
		day := rapid.IntRange(1, 28).Draw(t, "day")
		singleDate := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)

		config := Configuration{
			Schema:         "public",
			Table:          "test_table",
			PartitionKey:   "created_at",
			Interval:       interval,
			Retention:      1,
			PreProvisioned: preProvisioned,
			CleanupPolicy:  Drop,
		}

		// When min == max, we still need at least one partition covering that date
		p, err := config.GeneratePartition(singleDate)
		if err != nil {
			t.Fatalf("failed to generate partition for single date: %v", err)
		}

		// Property: the single date is within the generated partition bounds
		if singleDate.Before(p.LowerBound) || !singleDate.Before(p.UpperBound) {
			t.Fatalf("single date %s not within partition bounds [%s, %s)",
				singleDate, p.LowerBound, p.UpperBound)
		}

		// Generate pre-provisioned partitions from that date
		futurePartitions, err := config.GetPreProvisionedPartitions(singleDate)
		if err != nil {
			t.Fatalf("failed to generate pre-provisioned partitions: %v", err)
		}

		// Property: exactly preProvisioned future partitions
		if len(futurePartitions) != preProvisioned {
			t.Fatalf("expected %d pre-provisioned partitions, got %d",
				preProvisioned, len(futurePartitions))
		}

		// Property: first future partition starts at or after the current partition's upper bound
		if futurePartitions[0].LowerBound.Before(p.UpperBound) {
			t.Fatalf("first future partition (lower=%s) starts before current partition upper bound %s",
				futurePartitions[0].LowerBound, p.UpperBound)
		}
	})
}
