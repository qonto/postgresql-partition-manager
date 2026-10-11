package hook

import (
	"log/slog"
	"sync"
	"time"
)

// ExecutionSummary holds aggregate metrics for the cleanup run.
type ExecutionSummary struct {
	TotalExecuted int
	TotalSuccess  int
	TotalFailures int
	TotalRetries  int
}

// HookMetric records a single hook execution.
type HookMetric struct {
	HookName       string
	HookType       HookType
	LifecycleEvent LifecycleEvent
	PartitionName  string
	Duration       time.Duration
	Outcome        string // "success" or "failure"
	RetryAttempts  int
}

// MetricsCollector records hook execution metrics and provides a summary.
type MetricsCollector struct {
	mu      sync.Mutex
	metrics []HookMetric
	logger  slog.Logger
}

// NewMetricsCollector creates a new MetricsCollector with the given logger.
func NewMetricsCollector(logger slog.Logger) *MetricsCollector {
	return &MetricsCollector{
		metrics: make([]HookMetric, 0),
		logger:  logger,
	}
}

// Record adds a hook execution metric to the collector and logs it as structured JSON output.
func (m *MetricsCollector) Record(metric HookMetric) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.metrics = append(m.metrics, metric)

	m.logger.Info("Hook execution metric",
		"hook", metric.HookName,
		"type", string(metric.HookType),
		"lifecycle_event", string(metric.LifecycleEvent),
		"partition", metric.PartitionName,
		"duration_ms", metric.Duration.Milliseconds(),
		"outcome", metric.Outcome,
		"retry_attempts", metric.RetryAttempts,
	)
}

// Summary returns the aggregate execution summary across all recorded metrics.
func (m *MetricsCollector) Summary() ExecutionSummary {
	m.mu.Lock()
	defer m.mu.Unlock()

	summary := ExecutionSummary{
		TotalExecuted: len(m.metrics),
	}

	for _, metric := range m.metrics {
		switch metric.Outcome {
		case "success":
			summary.TotalSuccess++
		case "failure":
			summary.TotalFailures++
		}

		summary.TotalRetries += metric.RetryAttempts
	}

	return summary
}

// LogSummary logs the execution summary at info level for end-of-cleanup reporting.
func (m *MetricsCollector) LogSummary() {
	summary := m.Summary()

	m.logger.Info("Hook execution summary",
		"total_executed", summary.TotalExecuted,
		"total_success", summary.TotalSuccess,
		"total_failures", summary.TotalFailures,
		"total_retries", summary.TotalRetries,
	)
}

// Metrics returns a copy of all recorded metrics.
func (m *MetricsCollector) Metrics() []HookMetric {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]HookMetric, len(m.metrics))
	copy(result, m.metrics)

	return result
}
