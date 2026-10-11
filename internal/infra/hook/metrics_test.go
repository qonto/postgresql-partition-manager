package hook

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewMetricsCollector(t *testing.T) {
	logger := newTestLogger()
	collector := NewMetricsCollector(logger)

	assert.NotNil(t, collector)
	assert.Empty(t, collector.Metrics())

	summary := collector.Summary()
	assert.Equal(t, 0, summary.TotalExecuted)
	assert.Equal(t, 0, summary.TotalSuccess)
	assert.Equal(t, 0, summary.TotalFailures)
	assert.Equal(t, 0, summary.TotalRetries)
}

func TestMetricsCollector_Record(t *testing.T) {
	logger := newTestLogger()
	collector := NewMetricsCollector(logger)

	metric := HookMetric{
		HookName:       "notify-detach",
		HookType:       ShellType,
		LifecycleEvent: BeforeDetach,
		PartitionName:  "events_2024_01",
		Duration:       150 * time.Millisecond,
		Outcome:        "success",
		RetryAttempts:  0,
	}

	collector.Record(metric)

	metrics := collector.Metrics()
	assert.Len(t, metrics, 1)
	assert.Equal(t, metric, metrics[0])
}

func TestMetricsCollector_Summary_MixedOutcomes(t *testing.T) {
	logger := newTestLogger()
	collector := NewMetricsCollector(logger)

	collector.Record(HookMetric{
		HookName:       "hook-1",
		HookType:       ShellType,
		LifecycleEvent: BeforeDetach,
		PartitionName:  "part_1",
		Duration:       100 * time.Millisecond,
		Outcome:        "success",
		RetryAttempts:  0,
	})

	collector.Record(HookMetric{
		HookName:       "hook-2",
		HookType:       PostgreSQLType,
		LifecycleEvent: AfterDetach,
		PartitionName:  "part_1",
		Duration:       200 * time.Millisecond,
		Outcome:        "failure",
		RetryAttempts:  3,
	})

	collector.Record(HookMetric{
		HookName:       "hook-3",
		HookType:       ShellType,
		LifecycleEvent: BeforeDrop,
		PartitionName:  "part_2",
		Duration:       50 * time.Millisecond,
		Outcome:        "success",
		RetryAttempts:  1,
	})

	summary := collector.Summary()
	assert.Equal(t, 3, summary.TotalExecuted)
	assert.Equal(t, 2, summary.TotalSuccess)
	assert.Equal(t, 1, summary.TotalFailures)
	assert.Equal(t, 4, summary.TotalRetries)
}

func TestMetricsCollector_Summary_AllSuccess(t *testing.T) {
	logger := newTestLogger()
	collector := NewMetricsCollector(logger)

	for i := 0; i < 5; i++ {
		collector.Record(HookMetric{
			HookName:       "hook",
			HookType:       ShellType,
			LifecycleEvent: AfterDrop,
			PartitionName:  "part",
			Duration:       10 * time.Millisecond,
			Outcome:        "success",
			RetryAttempts:  0,
		})
	}

	summary := collector.Summary()
	assert.Equal(t, 5, summary.TotalExecuted)
	assert.Equal(t, 5, summary.TotalSuccess)
	assert.Equal(t, 0, summary.TotalFailures)
	assert.Equal(t, 0, summary.TotalRetries)
}

func TestMetricsCollector_Summary_AllFailures(t *testing.T) {
	logger := newTestLogger()
	collector := NewMetricsCollector(logger)

	for i := 0; i < 3; i++ {
		collector.Record(HookMetric{
			HookName:       "failing-hook",
			HookType:       PostgreSQLType,
			LifecycleEvent: BeforeDetach,
			PartitionName:  "part",
			Duration:       500 * time.Millisecond,
			Outcome:        "failure",
			RetryAttempts:  2,
		})
	}

	summary := collector.Summary()
	assert.Equal(t, 3, summary.TotalExecuted)
	assert.Equal(t, 0, summary.TotalSuccess)
	assert.Equal(t, 3, summary.TotalFailures)
	assert.Equal(t, 6, summary.TotalRetries)
}

func TestMetricsCollector_Metrics_ReturnsCopy(t *testing.T) {
	logger := newTestLogger()
	collector := NewMetricsCollector(logger)

	collector.Record(HookMetric{
		HookName:       "hook-1",
		HookType:       ShellType,
		LifecycleEvent: BeforeDetach,
		PartitionName:  "part_1",
		Duration:       100 * time.Millisecond,
		Outcome:        "success",
		RetryAttempts:  0,
	})

	metrics := collector.Metrics()
	metrics[0].Outcome = "modified"

	// Original should be unchanged
	original := collector.Metrics()
	assert.Equal(t, "success", original[0].Outcome)
}
