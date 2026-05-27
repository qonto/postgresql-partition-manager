package convert

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/retry"
)

const (
	defaultBatchSize        = 10000
	defaultProgressInterval = 10
	maxPercentage           = 100
)

// ErrMigrationStateNotFound is returned when the migration state record does not exist.
var ErrMigrationStateNotFound = errors.New("migration state not found")

// BackfillEngine copies data from the source table to the target partitioned table
// in PK-ordered batches with progress tracking and resumability.
type BackfillEngine struct {
	db               ConvertDBClient
	logger           slog.Logger
	schema           string
	sourceTable      string
	targetTable      string
	pkColumns        []string
	batchSize        int
	progressInterval int // Log progress every N batches (default 10)
}

// BackfillEngineConfig holds the configuration for creating a BackfillEngine.
type BackfillEngineConfig struct {
	Schema           string
	SourceTable      string
	TargetTable      string
	PKColumns        []string
	BatchSize        int
	ProgressInterval int
}

// NewBackfillEngine creates a new BackfillEngine with the given configuration.
func NewBackfillEngine(logger slog.Logger, db ConvertDBClient, cfg BackfillEngineConfig) *BackfillEngine {
	progressInterval := cfg.ProgressInterval
	if progressInterval <= 0 {
		progressInterval = defaultProgressInterval
	}

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	return &BackfillEngine{
		db:               db,
		logger:           logger,
		schema:           cfg.Schema,
		sourceTable:      cfg.SourceTable,
		targetTable:      cfg.TargetTable,
		pkColumns:        cfg.PKColumns,
		batchSize:        batchSize,
		progressInterval: progressInterval,
	}
}

// Run executes the backfill process: copies rows from source to target in PK-ordered
// batches, tracking progress and updating metadata after each batch.
// It resumes from the last successfully processed PK if a previous run was interrupted.
func (e *BackfillEngine) Run(ctx context.Context) error {
	startTime := time.Now()

	// Get estimated total row count for progress reporting
	estimatedTotal, err := e.db.GetTableRowCount(e.schema, e.sourceTable)
	if err != nil {
		e.logger.Warn("Could not estimate total row count, progress percentage will be omitted",
			"schema", e.schema,
			"table", e.sourceTable,
			"error", err,
		)

		estimatedTotal = -1
	}

	// Handle empty table case (Requirement 4.7)
	if estimatedTotal == 0 {
		e.logger.Info("No rows to backfill, source table is empty",
			"schema", e.schema,
			"table", e.sourceTable,
		)

		return nil
	}

	// Read last backfill PK from metadata for resumability (Requirement 4.4)
	afterPK, err := e.loadResumePoint()
	if err != nil {
		return err
	}

	var totalRowsCopied int64

	var batchCount int

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return fmt.Errorf("backfill cancelled: %w", ctx.Err())
		default:
		}

		// Execute batch with deadlock retry (Requirement 15.4)
		lastPK, rowsCopied, batchErr := e.executeBatchWithRetry(ctx, afterPK)
		if batchErr != nil {
			// Log error with PK range of the failed batch (Requirement 4.6)
			e.logger.Error("Backfill batch failed",
				"schema", e.schema,
				"table", e.sourceTable,
				"afterPK", afterPK,
				"batchSize", e.batchSize,
				"error", batchErr,
			)

			return fmt.Errorf("backfill batch failed after PK %v: %w", afterPK, batchErr)
		}

		// No more rows to process — backfill complete
		if lastPK == nil {
			break
		}

		batchCount++
		totalRowsCopied += rowsCopied

		// Update metadata with last processed PK (Requirement 4.4)
		lastPKStrings := anySliceToStringSlice(lastPK)

		if err := e.updateProgress(lastPKStrings); err != nil {
			return fmt.Errorf("failed to update backfill progress: %w", err)
		}

		// Move cursor forward
		afterPK = lastPK

		// Log progress every N batches (Requirement 11.1)
		if batchCount%e.progressInterval == 0 {
			e.logProgress(batchCount, totalRowsCopied, estimatedTotal, startTime)
		}
	}

	// Log completion message (Requirement 4.5)
	elapsed := time.Since(startTime)

	e.logger.Info("Backfill completed",
		"schema", e.schema,
		"table", e.sourceTable,
		"totalRowsCopied", totalRowsCopied,
		"totalBatches", batchCount,
		"elapsed", elapsed.String(),
	)

	return nil
}

// loadResumePoint reads the last backfill PK from metadata for resumability.
func (e *BackfillEngine) loadResumePoint() ([]any, error) {
	state, err := e.db.GetMigrationState(e.schema, e.sourceTable)
	if err != nil {
		return nil, fmt.Errorf("failed to get migration state: %w", err)
	}

	if state == nil || len(state.LastBackfillPK) == 0 {
		return nil, nil
	}

	afterPK := make([]any, len(state.LastBackfillPK))
	for i, v := range state.LastBackfillPK {
		afterPK[i] = v
	}

	e.logger.Info("Resuming backfill from last checkpoint",
		"schema", e.schema,
		"table", e.sourceTable,
		"lastPK", state.LastBackfillPK,
	)

	return afterPK, nil
}

// executeBatchWithRetry executes a single backfill batch wrapped in deadlock retry logic.
func (e *BackfillEngine) executeBatchWithRetry(ctx context.Context, afterPK []any) ([]any, int64, error) {
	var (
		lastPK     []any
		rowsCopied int64
	)

	err := retry.WithDeadlockRetry(ctx, e.logger, func() error {
		var batchErr error

		lastPK, rowsCopied, batchErr = e.db.BackfillBatch(
			e.schema,
			e.sourceTable,
			e.targetTable,
			e.pkColumns,
			afterPK,
			e.batchSize,
		)
		if batchErr != nil {
			return fmt.Errorf("backfill batch execution failed: %w", batchErr)
		}

		return nil
	})
	if err != nil {
		return nil, 0, fmt.Errorf("backfill batch with deadlock retry failed: %w", err)
	}

	return lastPK, rowsCopied, nil
}

// updateProgress persists the last successfully processed PK to the migration metadata.
func (e *BackfillEngine) updateProgress(lastPK []string) error {
	state, err := e.db.GetMigrationState(e.schema, e.sourceTable)
	if err != nil {
		return fmt.Errorf("failed to get migration state for progress update: %w", err)
	}

	if state == nil {
		return fmt.Errorf("%w for %s.%s", ErrMigrationStateNotFound, e.schema, e.sourceTable)
	}

	state.LastBackfillPK = lastPK
	state.UpdatedAt = time.Now()

	if err := e.db.UpdateMigrationState(e.schema, e.sourceTable, state); err != nil {
		return fmt.Errorf("failed to persist backfill progress: %w", err)
	}

	return nil
}

// logProgress logs backfill progress including rows processed, percentage, and ETA.
// If estimatedTotal is negative (could not be estimated), percentage and ETA are omitted
// per Requirement 11.4.
func (e *BackfillEngine) logProgress(batchCount int, rowsProcessed, estimatedTotal int64, startTime time.Time) {
	elapsed := time.Since(startTime)

	if estimatedTotal <= 0 {
		// Cannot estimate percentage or ETA (Requirement 11.4)
		e.logger.Info("Backfill progress",
			"schema", e.schema,
			"table", e.sourceTable,
			"rowsProcessed", rowsProcessed,
			"batchCount", batchCount,
			"elapsed", elapsed.String(),
		)

		return
	}

	percentage := float64(rowsProcessed) / float64(estimatedTotal) * maxPercentage

	if percentage > maxPercentage {
		percentage = maxPercentage
	}

	eta := e.calculateETA(rowsProcessed, estimatedTotal, elapsed)

	e.logger.Info("Backfill progress",
		"schema", e.schema,
		"table", e.sourceTable,
		"rowsProcessed", rowsProcessed,
		"batchCount", batchCount,
		"percentage", fmt.Sprintf("%.1f%%", percentage),
		"eta", eta,
		"elapsed", elapsed.String(),
	)
}

// calculateETA computes the estimated time remaining based on current progress.
func (e *BackfillEngine) calculateETA(rowsProcessed, estimatedTotal int64, elapsed time.Duration) string {
	if rowsProcessed <= 0 {
		return "unknown"
	}

	rowsPerSecond := float64(rowsProcessed) / elapsed.Seconds()

	remainingRows := estimatedTotal - rowsProcessed

	if remainingRows > 0 && rowsPerSecond > 0 {
		remainingSeconds := float64(remainingRows) / rowsPerSecond
		etaDuration := time.Duration(remainingSeconds * float64(time.Second))

		return etaDuration.Truncate(time.Second).String()
	}

	return "0s"
}

// anySliceToStringSlice converts a slice of any values to a slice of strings.
func anySliceToStringSlice(values []any) []string {
	result := make([]string, len(values))
	for i, v := range values {
		result[i] = fmt.Sprintf("%v", v)
	}

	return result
}
