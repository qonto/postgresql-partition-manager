package ppm

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"gotest.tools/assert"
)

func newTestPPM() *PPM {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	return &PPM{
		ctx:    context.Background(),
		logger: *logger,
	}
}

func TestGetGlobalRangeSinglePartition(t *testing.T) {
	p := newTestPPM()

	partitions := []partition.Partition{
		{
			LowerBound: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			UpperBound: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	r, err := p.getGlobalRange(partitions)
	assert.NilError(t, err)
	assert.Equal(t, r.LowerBound, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, r.UpperBound, time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC))
}

func TestGetGlobalRangeContiguousPartitions(t *testing.T) {
	p := newTestPPM()

	partitions := []partition.Partition{
		{
			LowerBound: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			UpperBound: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			LowerBound: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
			UpperBound: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			LowerBound: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
			UpperBound: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	r, err := p.getGlobalRange(partitions)
	assert.NilError(t, err)
	assert.Equal(t, r.LowerBound, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, r.UpperBound, time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
}

func TestGetGlobalRangeUnsortedPartitions(t *testing.T) {
	p := newTestPPM()

	// Provide partitions out of order — getGlobalRange should sort them
	partitions := []partition.Partition{
		{
			LowerBound: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
			UpperBound: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			LowerBound: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			UpperBound: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			LowerBound: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
			UpperBound: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	r, err := p.getGlobalRange(partitions)
	assert.NilError(t, err)
	assert.Equal(t, r.LowerBound, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, r.UpperBound, time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
}

func TestGetGlobalRangeWithGap(t *testing.T) {
	p := newTestPPM()

	partitions := []partition.Partition{
		{
			LowerBound: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			UpperBound: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		},
		// Gap: Q2 (Apr-Jul) is missing
		{
			LowerBound: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
			UpperBound: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	_, err := p.getGlobalRange(partitions)
	assert.Assert(t, errors.Is(err, ErrPartitionGap), "expected ErrPartitionGap")
}

func TestGetGlobalRangeIncoherentBounds(t *testing.T) {
	p := newTestPPM()

	partitions := []partition.Partition{
		{
			LowerBound: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
			UpperBound: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), // lower > upper
		},
	}

	_, err := p.getGlobalRange(partitions)
	assert.Assert(t, errors.Is(err, ErrIncoherentBounds), "expected ErrIncoherentBounds")
}

func TestGetGlobalRangeEqualBounds(t *testing.T) {
	p := newTestPPM()

	partitions := []partition.Partition{
		{
			LowerBound: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			UpperBound: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), // lower == upper
		},
	}

	_, err := p.getGlobalRange(partitions)
	assert.Assert(t, errors.Is(err, ErrIncoherentBounds), "expected ErrIncoherentBounds")
}

func TestGetExpectedPartitionsContiguity(t *testing.T) {
	testCases := []struct {
		name     string
		config   partition.Configuration
		workDate time.Time
	}{
		{
			name: "Daily March 31",
			config: partition.Configuration{
				Schema: "public", Table: "t", PartitionKey: "c",
				Interval: partition.Daily, Retention: 7, PreProvisioned: 3, CleanupPolicy: partition.Drop,
			},
			workDate: time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Weekly March 31",
			config: partition.Configuration{
				Schema: "public", Table: "t", PartitionKey: "c",
				Interval: partition.Weekly, Retention: 4, PreProvisioned: 2, CleanupPolicy: partition.Drop,
			},
			workDate: time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Monthly March 31",
			config: partition.Configuration{
				Schema: "public", Table: "t", PartitionKey: "c",
				Interval: partition.Monthly, Retention: 12, PreProvisioned: 3, CleanupPolicy: partition.Drop,
			},
			workDate: time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Quarterly March 31",
			config: partition.Configuration{
				Schema: "public", Table: "t", PartitionKey: "c",
				Interval: partition.Quarterly, Retention: 40, PreProvisioned: 2, CleanupPolicy: partition.Drop,
			},
			workDate: time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Quarterly May 31",
			config: partition.Configuration{
				Schema: "public", Table: "t", PartitionKey: "c",
				Interval: partition.Quarterly, Retention: 8, PreProvisioned: 2, CleanupPolicy: partition.Drop,
			},
			workDate: time.Date(2026, 5, 31, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "Yearly December 31",
			config: partition.Configuration{
				Schema: "public", Table: "t", PartitionKey: "c",
				Interval: partition.Yearly, Retention: 10, PreProvisioned: 2, CleanupPolicy: partition.Drop,
			},
			workDate: time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
		},
	}

	p := newTestPPM()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			partitions, err := getExpectedPartitions(tc.config, tc.workDate)
			assert.NilError(t, err)

			expectedCount := tc.config.Retention + 1 + tc.config.PreProvisioned
			assert.Equal(t, len(partitions), expectedCount)

			// getGlobalRange validates contiguity — no gaps, no incoherent bounds
			_, err = p.getGlobalRange(partitions)
			assert.NilError(t, err, "expected partitions must be contiguous with no gaps")
		})
	}
}
