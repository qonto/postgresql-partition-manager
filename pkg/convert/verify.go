package convert

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
)

// VerifyEngine performs convergence verification between the source and target tables.
// It compares row counts, checks the replay lag, and determines whether the migration
// is ready for cutover.
type VerifyEngine struct {
	db          ConvertDBClient
	logger      slog.Logger
	schema      string
	sourceTable string
	targetTable string
}

// VerifyEngineConfig holds the configuration for creating a VerifyEngine.
type VerifyEngineConfig struct {
	Schema      string
	SourceTable string
	TargetTable string
}

// NewVerifyEngine creates a new VerifyEngine with the given configuration.
func NewVerifyEngine(logger slog.Logger, db ConvertDBClient, cfg VerifyEngineConfig) *VerifyEngine {
	return &VerifyEngine{
		db:          db,
		logger:      logger,
		schema:      cfg.Schema,
		sourceTable: cfg.SourceTable,
		targetTable: cfg.TargetTable,
	}
}

// Verify performs convergence verification by comparing row counts between the source
// and target tables, checking the replay lag, and determining readiness for cutover.
//
// The migration is considered ready for cutover when:
//   - The replay lag is zero (no unprocessed CDC events)
//   - The row count difference is zero (source and target have the same number of rows)
//
// Requirements: 6.1, 6.2, 6.3, 6.4
func (e *VerifyEngine) Verify(_ context.Context) (*postgresql.VerifyResult, error) {
	e.logger.Info("Starting convergence verification",
		"schema", e.schema,
		"sourceTable", e.sourceTable,
		"targetTable", e.targetTable,
	)

	// Step 1: Get source table row count (Requirement 6.1)
	sourceRowCount, err := e.db.GetTableRowCount(e.schema, e.sourceTable)
	if err != nil {
		return nil, fmt.Errorf("failed to get source table row count: %w", err)
	}

	// Step 2: Get target table row count (Requirement 6.1)
	targetRowCount, err := e.db.GetTableRowCount(e.schema, e.targetTable)
	if err != nil {
		return nil, fmt.Errorf("failed to get target table row count: %w", err)
	}

	// Step 3: Calculate absolute row difference (Requirement 6.1)
	rowDifference := sourceRowCount - targetRowCount
	if rowDifference < 0 {
		rowDifference = -rowDifference
	}

	// Step 4: Get replay lag (Requirement 6.2)
	replayLag, err := e.db.GetReplayLag(e.schema, e.sourceTable)
	if err != nil {
		return nil, fmt.Errorf("failed to get replay lag: %w", err)
	}

	// Step 5: Determine readiness (Requirement 6.3, 6.4)
	readyForCutover := replayLag == 0 && rowDifference == 0

	result := &postgresql.VerifyResult{
		SourceRowCount:  sourceRowCount,
		TargetRowCount:  targetRowCount,
		RowDifference:   rowDifference,
		ReplayLag:       replayLag,
		ReadyForCutover: readyForCutover,
	}

	if readyForCutover {
		e.logger.Info("Migration is ready for cutover",
			"schema", e.schema,
			"sourceTable", e.sourceTable,
			"targetTable", e.targetTable,
			"sourceRowCount", sourceRowCount,
			"targetRowCount", targetRowCount,
		)
	} else {
		e.logger.Warn("Migration is NOT ready for cutover",
			"schema", e.schema,
			"sourceTable", e.sourceTable,
			"targetTable", e.targetTable,
			"sourceRowCount", sourceRowCount,
			"targetRowCount", targetRowCount,
			"rowDifference", rowDifference,
			"replayLag", replayLag,
		)
	}

	return result, nil
}
