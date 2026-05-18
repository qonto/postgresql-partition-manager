package convert

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// ErrConfirmRequired is returned when cleanup is attempted without --confirm or --force.
var ErrConfirmRequired = fmt.Errorf("cleanup requires --confirm flag (or --force to bypass)")

// ErrCleanupPhaseInvalid is returned when cleanup is attempted in a phase that doesn't allow it.
var ErrCleanupPhaseInvalid = fmt.Errorf("cleanup requires a completed cutover (phase must be 'cutover')")

// CleanupEngine handles the removal of migration artifacts after a successful cutover.
// It drops the CDC trigger, CDC queue, source_old table, and migration metadata entry
// in that specific order.
type CleanupEngine struct {
	db          ConvertDBClient
	logger      slog.Logger
	schema      string
	sourceTable string
}

// CleanupEngineConfig holds the configuration for creating a CleanupEngine.
type CleanupEngineConfig struct {
	Schema      string
	SourceTable string
}

// NewCleanupEngine creates a new CleanupEngine with the given configuration.
func NewCleanupEngine(logger slog.Logger, db ConvertDBClient, cfg CleanupEngineConfig) *CleanupEngine {
	return &CleanupEngine{
		db:          db,
		logger:      logger,
		schema:      cfg.Schema,
		sourceTable: cfg.SourceTable,
	}
}

// Cleanup removes all migration artifacts in the correct order.
// The confirm and force flags control the behavior:
//   - If neither confirm nor force is provided: display what would be removed and return ErrConfirmRequired
//   - If force is provided: remove all artifacts regardless of current migration phase
//   - If confirm is provided (without force): proceed only if migration state is in post-cutover phase
//
// Cleanup order:
//  1. Drop CDC trigger from source_old
//  2. Drop CDC queue table
//  3. Drop source_old table
//  4. Remove migration metadata entry
//
// Requirements: 9.1, 9.2, 9.3, 9.4, 9.5
func (e *CleanupEngine) Cleanup(_ context.Context, confirm, force bool) error {
	startTime := time.Now()
	sourceOldName := e.sourceTable + "_old"
	triggerName := fmt.Sprintf("ppm_cdc_%s", e.sourceTable)
	triggerFunctionName := fmt.Sprintf("ppm_cdc_trigger_%s", e.sourceTable)

	e.logger.Info("Starting cleanup",
		"schema", e.schema,
		"sourceTable", e.sourceTable,
		"confirm", confirm,
		"force", force,
	)

	// If neither confirm nor force: display artifacts and exit (Requirement 9.3)
	if !confirm && !force {
		e.logArtifactsToRemove(sourceOldName, triggerName, triggerFunctionName)

		return ErrConfirmRequired
	}

	// If confirm without force: validate migration phase (Requirement 9.4)
	if confirm && !force {
		if err := e.validateCleanupPhase(); err != nil {
			return err
		}
	}

	// Force mode: skip phase validation entirely (Requirement 9.5)

	// Step 1: Drop CDC trigger from source_old (Requirement 9.1)
	if err := e.dropCDCTrigger(sourceOldName, triggerName, triggerFunctionName); err != nil {
		return fmt.Errorf("failed to drop CDC trigger: %w", err)
	}

	// Step 2: Drop CDC queue table (Requirement 9.1)
	if err := e.dropCDCQueue(); err != nil {
		return fmt.Errorf("failed to drop CDC queue: %w", err)
	}

	// Step 3: Drop source_old table (Requirement 9.1)
	if err := e.dropSourceOldTable(sourceOldName); err != nil {
		return fmt.Errorf("failed to drop source_old table: %w", err)
	}

	// Step 4: Remove migration metadata entry (Requirement 9.1)
	if err := e.deleteMigrationMetadata(); err != nil {
		return fmt.Errorf("failed to remove migration metadata: %w", err)
	}

	elapsed := time.Since(startTime)

	e.logger.Info("Cleanup completed successfully",
		"schema", e.schema,
		"table", e.sourceTable,
		"elapsed", elapsed.String(),
	)

	return nil
}

// logArtifactsToRemove logs the list of artifacts that would be removed during cleanup.
func (e *CleanupEngine) logArtifactsToRemove(sourceOldName, triggerName, triggerFunctionName string) {
	e.logger.Info("The following artifacts would be removed during cleanup:",
		"schema", e.schema,
		"table", e.sourceTable,
	)
	e.logger.Info("  1. CDC trigger",
		"trigger", triggerName,
		"on_table", fmt.Sprintf("%s.%s", e.schema, sourceOldName),
	)
	e.logger.Info("  2. CDC trigger function",
		"function", fmt.Sprintf("%s.%s", e.schema, triggerFunctionName),
	)
	e.logger.Info("  3. CDC queue table",
		"table", fmt.Sprintf("%s.%s_cdc_queue", e.schema, e.sourceTable),
	)
	e.logger.Info("  4. Source old table",
		"table", fmt.Sprintf("%s.%s", e.schema, sourceOldName),
	)
	e.logger.Info("  5. Migration metadata entry",
		"schema", e.schema,
		"table", e.sourceTable,
	)
	e.logger.Info("Use --confirm flag to proceed with cleanup, or --force to bypass phase validation")
}

// validateCleanupPhase checks that the migration is in the post-cutover phase.
// Cleanup is only allowed when the current phase is PhaseCutover (meaning cutover completed).
func (e *CleanupEngine) validateCleanupPhase() error {
	state, err := e.db.GetMigrationState(e.schema, e.sourceTable)
	if err != nil {
		return fmt.Errorf("failed to get migration state: %w", err)
	}

	if state == nil {
		// No migration state means nothing to clean up from a phase perspective,
		// but we still allow cleanup to remove any orphaned artifacts
		e.logger.Warn("No migration state found, proceeding with cleanup of any existing artifacts",
			"schema", e.schema,
			"table", e.sourceTable,
		)

		return nil
	}

	currentPhase := Phase(state.Phase)
	if currentPhase != PhaseCutover {
		e.logger.Error("Cleanup requires a completed cutover",
			"schema", e.schema,
			"table", e.sourceTable,
			"currentPhase", currentPhase,
			"requiredPhase", PhaseCutover,
		)

		return fmt.Errorf("%w: current phase is %q", ErrCleanupPhaseInvalid, currentPhase)
	}

	return nil
}

// dropCDCTrigger drops the CDC trigger and trigger function from source_old.
// Skips gracefully if the trigger or table does not exist (Requirement 9.2).
func (e *CleanupEngine) dropCDCTrigger(sourceOldName, triggerName, triggerFunctionName string) error {
	// Check if source_old table exists before attempting to drop trigger
	exists, err := e.db.IsTableExists(e.schema, sourceOldName)
	if err != nil {
		return fmt.Errorf("failed to check if %s exists: %w", sourceOldName, err)
	}

	if !exists {
		e.logger.Info("Source old table does not exist, skipping trigger drop",
			"schema", e.schema,
			"table", sourceOldName,
		)

		return nil
	}

	// Drop the trigger from source_old
	if err := e.db.DropTrigger(e.schema, sourceOldName, triggerName); err != nil {
		e.logger.Warn("Could not drop CDC trigger (may already be removed)",
			"schema", e.schema,
			"table", sourceOldName,
			"trigger", triggerName,
			"error", err,
		)
	} else {
		e.logger.Info("Dropped CDC trigger",
			"schema", e.schema,
			"table", sourceOldName,
			"trigger", triggerName,
		)
	}

	// Drop the trigger function
	if err := e.db.DropTriggerFunction(e.schema, triggerFunctionName); err != nil {
		e.logger.Warn("Could not drop CDC trigger function (may already be removed)",
			"schema", e.schema,
			"function", triggerFunctionName,
			"error", err,
		)
	} else {
		e.logger.Info("Dropped CDC trigger function",
			"schema", e.schema,
			"function", triggerFunctionName,
		)
	}

	return nil
}

// dropCDCQueue drops the CDC queue table.
// Skips gracefully if the queue does not exist (Requirement 9.2).
func (e *CleanupEngine) dropCDCQueue() error {
	exists, err := e.db.IsCDCQueueExists(e.schema, e.sourceTable)
	if err != nil {
		return fmt.Errorf("failed to check if CDC queue exists: %w", err)
	}

	if !exists {
		e.logger.Info("CDC queue does not exist, skipping",
			"schema", e.schema,
			"table", fmt.Sprintf("%s_cdc_queue", e.sourceTable),
		)

		return nil
	}

	if err := e.db.DropCDCQueue(e.schema, e.sourceTable); err != nil {
		return fmt.Errorf("failed to drop CDC queue %s.%s_cdc_queue: %w", e.schema, e.sourceTable, err)
	}

	e.logger.Info("Dropped CDC queue table",
		"schema", e.schema,
		"table", fmt.Sprintf("%s_cdc_queue", e.sourceTable),
	)

	return nil
}

// dropSourceOldTable drops the source_old table.
// Skips gracefully if the table does not exist (Requirement 9.2).
func (e *CleanupEngine) dropSourceOldTable(sourceOldName string) error {
	exists, err := e.db.IsTableExists(e.schema, sourceOldName)
	if err != nil {
		return fmt.Errorf("failed to check if %s exists: %w", sourceOldName, err)
	}

	if !exists {
		e.logger.Info("Source old table does not exist, skipping",
			"schema", e.schema,
			"table", sourceOldName,
		)

		return nil
	}

	if err := e.db.DropTable(e.schema, sourceOldName); err != nil {
		return fmt.Errorf("failed to drop table %s.%s: %w", e.schema, sourceOldName, err)
	}

	e.logger.Info("Dropped source old table",
		"schema", e.schema,
		"table", sourceOldName,
	)

	return nil
}

// deleteMigrationMetadata removes the migration metadata entry.
// Skips gracefully if no metadata entry exists (Requirement 9.2).
func (e *CleanupEngine) deleteMigrationMetadata() error {
	state, err := e.db.GetMigrationState(e.schema, e.sourceTable)
	if err != nil {
		return fmt.Errorf("failed to get migration state: %w", err)
	}

	if state == nil {
		e.logger.Info("Migration metadata entry does not exist, skipping",
			"schema", e.schema,
			"table", e.sourceTable,
		)

		return nil
	}

	if err := e.db.DeleteMigrationState(e.schema, e.sourceTable); err != nil {
		return fmt.Errorf("failed to delete migration state for %s.%s: %w", e.schema, e.sourceTable, err)
	}

	e.logger.Info("Removed migration metadata entry",
		"schema", e.schema,
		"table", e.sourceTable,
	)

	return nil
}
