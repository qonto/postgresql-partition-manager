package convert

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
	"github.com/qonto/postgresql-partition-manager/internal/infra/retry"
)

const (
	defaultReplayBatchSize   = 1000
	minReplayBatchSize       = 1
	maxReplayBatchSize       = 50000
	defaultReplayLogInterval = 10 * time.Second
)

// ErrSourceRowNotFound is returned by ApplyUpsert when the source row no longer exists.
// The ReplayEngine catches this error and skips the event with a warning log.
var ErrSourceRowNotFound = errors.New("source row not found")

// ReplayEngine dequeues CDC events from the queue and applies them to the target
// partitioned table, handling INSERT/UPDATE via upsert and DELETE via delete.
type ReplayEngine struct {
	db          ConvertDBClient
	logger      slog.Logger
	schema      string
	sourceTable string
	targetTable string
	pkColumns   []string
	batchSize   int
	logInterval time.Duration
}

// ReplayEngineConfig holds the configuration for creating a ReplayEngine.
type ReplayEngineConfig struct {
	Schema      string
	SourceTable string
	TargetTable string
	PKColumns   []string
	BatchSize   int
	LogInterval time.Duration
}

// NewReplayEngine creates a new ReplayEngine with the given configuration.
func NewReplayEngine(logger slog.Logger, db ConvertDBClient, cfg ReplayEngineConfig) *ReplayEngine {
	batchSize := cfg.BatchSize
	if batchSize < minReplayBatchSize || batchSize > maxReplayBatchSize {
		batchSize = defaultReplayBatchSize
	}

	logInterval := cfg.LogInterval
	if logInterval <= 0 {
		logInterval = defaultReplayLogInterval
	}

	return &ReplayEngine{
		db:          db,
		logger:      logger,
		schema:      cfg.Schema,
		sourceTable: cfg.SourceTable,
		targetTable: cfg.TargetTable,
		pkColumns:   cfg.PKColumns,
		batchSize:   batchSize,
		logInterval: logInterval,
	}
}

// Run executes the replay process: dequeues CDC events in batches, dispatches each
// event by operation type, and exits when the queue is empty (convergence).
func (e *ReplayEngine) Run(ctx context.Context) error {
	startTime := time.Now()
	lastLogTime := startTime

	var totalEventsProcessed int64

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return fmt.Errorf("replay cancelled: %w", ctx.Err())
		default:
		}

		// Check if queue is empty and replay lag is zero (Requirement 5.8)
		empty, err := e.db.IsCDCQueueEmpty(e.schema, e.sourceTable)
		if err != nil {
			return fmt.Errorf("failed to check CDC queue empty status: %w", err)
		}

		if empty {
			lag, err := e.db.GetReplayLag(e.schema, e.sourceTable)
			if err != nil {
				return fmt.Errorf("failed to get replay lag: %w", err)
			}

			if lag == 0 {
				// Convergence reached (Requirement 5.5, 5.8)
				elapsed := time.Since(startTime)

				e.logger.Info("Replay converged, CDC queue is empty",
					"schema", e.schema,
					"table", e.sourceTable,
					"totalEventsProcessed", totalEventsProcessed,
					"elapsed", elapsed.String(),
				)

				return nil
			}
		}

		// Dequeue and process a batch with deadlock retry (Requirement 15.4)
		eventsProcessed, batchErr := e.executeBatchWithRetry(ctx)
		if batchErr != nil {
			return batchErr
		}

		totalEventsProcessed += int64(eventsProcessed)

		// Log replay lag and processing rate at configurable interval (Requirement 11.2)
		if time.Since(lastLogTime) >= e.logInterval {
			e.logProgress(totalEventsProcessed, startTime)
			lastLogTime = time.Now()
		}
	}
}

// executeBatchWithRetry dequeues a batch of events and processes them, wrapped in deadlock retry.
func (e *ReplayEngine) executeBatchWithRetry(ctx context.Context) (int, error) {
	var eventsProcessed int

	err := retry.WithDeadlockRetry(ctx, e.logger, func() error {
		var batchErr error
		eventsProcessed, batchErr = e.processBatch()

		return batchErr
	})
	if err != nil {
		return 0, fmt.Errorf("replay batch failed: %w", err)
	}

	return eventsProcessed, nil
}

// processBatch dequeues events from the CDC queue and dispatches them by operation type.
func (e *ReplayEngine) processBatch() (int, error) {
	events, err := e.db.DequeueEvents(e.schema, e.sourceTable, e.batchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to dequeue events: %w", err)
	}

	if len(events) == 0 {
		return 0, nil
	}

	for _, event := range events {
		if err := e.dispatchEvent(event); err != nil {
			// Log error with seq ID range of the failed batch (Requirement 5.7)
			e.logger.Error("Replay event dispatch failed",
				"schema", e.schema,
				"table", e.sourceTable,
				"seqID", event.SeqID,
				"operation", event.Operation,
				"firstSeqID", events[0].SeqID,
				"lastSeqID", events[len(events)-1].SeqID,
				"error", err,
			)

			return 0, fmt.Errorf("replay batch failed at seq_id %d (batch range %d-%d): %w",
				event.SeqID, events[0].SeqID, events[len(events)-1].SeqID, err)
		}
	}

	// Update last replay seq in migration state
	lastSeqID := events[len(events)-1].SeqID
	if err := e.updateLastReplaySeq(lastSeqID); err != nil {
		return 0, err
	}

	return len(events), nil
}

// dispatchEvent applies a single CDC event to the target table based on operation type.
func (e *ReplayEngine) dispatchEvent(event postgresql.CDCEvent) error {
	switch event.Operation {
	case "INSERT", "UPDATE":
		// Apply upsert by fetching current row from source (Requirement 5.2)
		err := e.db.ApplyUpsert(e.schema, e.targetTable, e.sourceTable, e.pkColumns, event.PKValues)
		if err != nil {
			// If source row no longer exists, skip and log warning (Requirement 5.6)
			if errors.Is(err, ErrSourceRowNotFound) {
				e.logger.Warn("Source row not found for replay event, skipping",
					"schema", e.schema,
					"table", e.sourceTable,
					"operation", event.Operation,
					"seqID", event.SeqID,
					"pkValues", event.PKValues,
				)

				return nil
			}

			return fmt.Errorf("apply upsert failed for seq_id %d: %w", event.SeqID, err)
		}

	case "DELETE":
		// Delete from target by PK (Requirement 5.3)
		err := e.db.ApplyDelete(e.schema, e.targetTable, e.pkColumns, event.PKValues)
		if err != nil {
			return fmt.Errorf("apply delete failed for seq_id %d: %w", event.SeqID, err)
		}

	default:
		e.logger.Warn("Unknown CDC operation type, skipping",
			"schema", e.schema,
			"table", e.sourceTable,
			"operation", event.Operation,
			"seqID", event.SeqID,
		)
	}

	return nil
}

// updateLastReplaySeq persists the last successfully processed sequence ID to migration metadata.
func (e *ReplayEngine) updateLastReplaySeq(lastSeqID int64) error {
	state, err := e.db.GetMigrationState(e.schema, e.sourceTable)
	if err != nil {
		return fmt.Errorf("failed to get migration state for replay progress update: %w", err)
	}

	if state == nil {
		return fmt.Errorf("%w for %s.%s", ErrMigrationStateNotFound, e.schema, e.sourceTable)
	}

	state.LastReplaySeq = lastSeqID
	state.UpdatedAt = time.Now()

	if err := e.db.UpdateMigrationState(e.schema, e.sourceTable, state); err != nil {
		return fmt.Errorf("failed to persist replay progress: %w", err)
	}

	return nil
}

// logProgress logs the current replay lag and processing rate (Requirement 11.2).
func (e *ReplayEngine) logProgress(totalEventsProcessed int64, startTime time.Time) {
	elapsed := time.Since(startTime)

	var eventsPerSecond float64
	if elapsed.Seconds() > 0 {
		eventsPerSecond = float64(totalEventsProcessed) / elapsed.Seconds()
	}

	lag, err := e.db.GetReplayLag(e.schema, e.sourceTable)
	if err != nil {
		e.logger.Warn("Could not get replay lag for progress logging",
			"schema", e.schema,
			"table", e.sourceTable,
			"error", err,
		)

		return
	}

	e.logger.Info("Replay progress",
		"schema", e.schema,
		"table", e.sourceTable,
		"totalEventsProcessed", totalEventsProcessed,
		"replayLag", lag,
		"eventsPerSecond", fmt.Sprintf("%.1f", eventsPerSecond),
		"elapsed", elapsed.String(),
	)
}
