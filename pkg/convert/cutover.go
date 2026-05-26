package convert

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
)

// ErrQueueNotEmpty is returned when the CDC queue is not empty after the final replay drain.
var ErrQueueNotEmpty = errors.New("CDC queue not empty after final replay - aborting cutover")

// ErrLockTimeout is returned when a lock acquisition times out during cutover or rollback.
var ErrLockTimeout = errors.New("lock timeout exceeded - could not acquire required lock")

// ErrSourceOldNotFound is returned when rollback is attempted but source_old table does not exist.
var ErrSourceOldNotFound = errors.New("source_old table does not exist - rollback is not applicable")

// ErrRollbackNotApplicable is returned when rollback cannot be performed (alias for ErrSourceOldNotFound).
var ErrRollbackNotApplicable = ErrSourceOldNotFound

// CutoverEngine handles the atomic cutover swap and rollback operations.
// It coordinates the pre-cutover FK drop, advisory lock, trigger disable,
// final replay drain, empty queue assertion, rename swap, FK recreation,
// and post-cutover operations (ANALYZE, index rename, FK validation).
type CutoverEngine struct {
	db           ConvertDBClient
	logger       slog.Logger
	schema       string
	sourceTable  string
	targetTable  string
	pkColumns    []string
	partitionKey string
	batchSize    int // replay batch size for final drain
}

// CutoverEngineConfig holds the configuration for creating a CutoverEngine.
type CutoverEngineConfig struct {
	Schema       string
	SourceTable  string
	TargetTable  string
	PKColumns    []string
	PartitionKey string
	BatchSize    int // replay batch size for final drain
}

// NewCutoverEngine creates a new CutoverEngine with the given configuration.
func NewCutoverEngine(logger slog.Logger, db ConvertDBClient, cfg CutoverEngineConfig) *CutoverEngine {
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultReplayBatchSize
	}

	return &CutoverEngine{
		db:           db,
		logger:       logger,
		schema:       cfg.Schema,
		sourceTable:  cfg.SourceTable,
		targetTable:  cfg.TargetTable,
		pkColumns:    cfg.PKColumns,
		partitionKey: cfg.PartitionKey,
		batchSize:    batchSize,
	}
}

// Cutover performs the atomic cutover swap from source to target table.
// The operation follows this sequence:
//  1. Record referencing FK definitions in metadata (for rollback)
//  2. Begin transaction with lock_timeout and statement_timeout
//  3. Acquire advisory lock
//  4. Acquire ACCESS EXCLUSIVE on child tables, source, SHARE ROW EXCLUSIVE on target
//  5. Disable CDC trigger
//  6. Final replay loop (drain queue completely)
//  7. Assert queue is empty (abort if not)
//  8. Drop referencing FKs from child tables (already locked)
//  9. Rename swap (source → source_old, target → source)
//  10. Recreate FKs with NOT VALID pointing to new partitioned table
//  11. Commit
//  12. Post-cutover: ANALYZE, rename indexes, VALIDATE CONSTRAINT
func (e *CutoverEngine) Cutover(ctx context.Context) error {
	startTime := time.Now()

	e.logger.Info("Starting cutover",
		"schema", e.schema,
		"sourceTable", e.sourceTable,
		"targetTable", e.targetTable,
	)

	// Step 1: Record referencing FK definitions in metadata (for rollback)
	referencingFKs, err := e.db.GetReferencingForeignKeys(e.schema, e.sourceTable)
	if err != nil {
		return fmt.Errorf("failed to get referencing foreign keys: %w", err)
	}

	if len(referencingFKs) > 0 {
		if err := e.recordDroppedFKs(referencingFKs); err != nil {
			return fmt.Errorf("failed to record FK definitions in metadata: %w", err)
		}

		e.logger.Info("Recorded referencing FK definitions for rollback",
			"schema", e.schema,
			"table", e.sourceTable,
			"fkCount", len(referencingFKs),
		)
	}

	// Step 2: Begin transaction with lock_timeout and statement_timeout
	tx, err := e.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin cutover transaction: %w", err)
	}

	committed := false

	defer func() {
		if !committed {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				e.logger.Error("Failed to rollback cutover transaction", "error", rbErr)
			}
		}
	}()

	// Step 3: Acquire advisory lock
	if err := e.db.AcquireAdvisoryLock(e.schema, e.sourceTable); err != nil {
		return fmt.Errorf("failed to acquire advisory lock: %w", err)
	}

	// Step 4: Acquire locks in deterministic order to prevent deadlocks.
	// Child tables (referencing FK) MUST be locked BEFORE the source table.
	// Otherwise, a concurrent transaction may hold a lock on a child table (e.g., INSERT
	// into event_comments with FK check) and wait for the source table (blocked by our
	// ACCESS EXCLUSIVE). When we then try to lock the child table, we deadlock.
	// Lock order: child tables → source → target (alphabetical within each group).
	for _, fk := range referencingFKs {
		childTable := e.getChildTableFromFK(fk)

		e.logger.Info("Acquiring ACCESS EXCLUSIVE lock on child table",
			"schema", e.schema,
			"table", childTable,
			"constraint", fk.Name,
		)

		if err := e.db.AcquireExclusiveLock(e.schema, childTable); err != nil {
			return fmt.Errorf("failed to acquire ACCESS EXCLUSIVE lock on child table %s: %w", childTable, err)
		}
	}

	if err := e.db.AcquireExclusiveLock(e.schema, e.sourceTable); err != nil {
		return fmt.Errorf("failed to acquire ACCESS EXCLUSIVE lock on source: %w", err)
	}

	if err := e.db.AcquireShareRowExclusiveLock(e.schema, e.targetTable); err != nil {
		return fmt.Errorf("failed to acquire SHARE ROW EXCLUSIVE lock on target: %w", err)
	}

	lockAcquiredAt := time.Now()

	// Step 5: Disable CDC trigger
	triggerName := fmt.Sprintf("ppm_cdc_%s", e.sourceTable)

	if err := e.db.DisableTrigger(e.schema, e.sourceTable, triggerName); err != nil {
		return fmt.Errorf("failed to disable CDC trigger: %w", err)
	}

	// Step 6: Final replay loop (drain queue completely)
	drainedEvents, err := e.drainQueue(ctx)
	if err != nil {
		return fmt.Errorf("final replay drain failed: %w", err)
	}

	e.logger.Info("Final replay drain completed",
		"schema", e.schema,
		"table", e.sourceTable,
		"eventsDrained", drainedEvents,
	)

	// Step 7: Assert queue is empty (abort if not)
	empty, err := e.db.IsCDCQueueEmpty(e.schema, e.sourceTable)
	if err != nil {
		return fmt.Errorf("failed to check CDC queue empty status: %w", err)
	}

	if !empty {
		return ErrQueueNotEmpty
	}

	// Step 8: Drop referencing FKs from child tables (already locked in step 4)
	if len(referencingFKs) > 0 {
		e.logger.Info("Dropping referencing foreign keys",
			"schema", e.schema,
			"table", e.sourceTable,
			"fkCount", len(referencingFKs),
		)

		for _, fk := range referencingFKs {
			childTable := e.getChildTableFromFK(fk)

			e.logger.Info("Dropping foreign key",
				"constraint", fk.Name,
				"childTable", childTable,
				"referencedTable", e.sourceTable,
				"columns", fk.Columns,
				"referencedColumns", fk.ReferencedColumns,
			)

			if err := e.db.DropForeignKey(e.schema, childTable, fk.Name); err != nil {
				return fmt.Errorf("failed to drop referencing FK %s on %s: %w", fk.Name, childTable, err)
			}
		}
	}

	// Step 9: Rename swap (source → source_old, target → source)
	sourceOldName := e.sourceTable + "_old"

	if err := e.db.RenameTable(e.schema, e.sourceTable, sourceOldName); err != nil {
		return fmt.Errorf("failed to rename source to source_old: %w", err)
	}

	if err := e.db.RenameTable(e.schema, e.targetTable, e.sourceTable); err != nil {
		return fmt.Errorf("failed to rename target to source: %w", err)
	}

	// Step 10: Recreate FKs with NOT VALID pointing to new partitioned table.
	// PostgreSQL requires a unique constraint matching the referenced columns for FK creation.
	// On partitioned tables, unique constraints must include all partition columns.
	// If the referenced columns don't match any existing unique constraint, we skip
	// FK recreation and log a warning — the user must handle referential integrity
	// at the application level or restructure the FK.
	recreatedFKs := e.recreateReferencingFKs(referencingFKs)

	// Step 11: Commit
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit cutover transaction: %w", err)
	}

	committed = true
	lockDuration := time.Since(lockAcquiredAt)

	e.logger.Info("Cutover transaction committed",
		"schema", e.schema,
		"sourceTable", e.sourceTable,
		"oldTableName", sourceOldName,
		"lockDurationMs", lockDuration.Milliseconds(),
	)

	// Step 12: Post-cutover operations (separate transactions, non-blocking)
	if err := e.postCutover(ctx, recreatedFKs); err != nil {
		e.logger.Error("Post-cutover operations had errors (cutover itself succeeded)",
			"schema", e.schema,
			"table", e.sourceTable,
			"error", err,
		)

		return fmt.Errorf("post-cutover operations failed: %w", err)
	}

	elapsed := time.Since(startTime)

	e.logger.Info("Cutover completed successfully",
		"schema", e.schema,
		"table", e.sourceTable,
		"elapsed", elapsed.String(),
	)

	return nil
}

// Rollback reverses the cutover by swapping the tables back to their original positions.
// The operation follows this sequence:
//  1. Verify source_old exists
//  2. Drop post-cutover FKs
//  3. Begin transaction with lock_timeout and statement_timeout
//  4. Acquire advisory lock
//  5. Acquire ACCESS EXCLUSIVE on source, SHARE ROW EXCLUSIVE on source_old
//  6. Reverse rename (source → target, source_old → source)
//  7. Commit
//  8. Post-rollback: restore original FKs, re-enable trigger, rename indexes back, ANALYZE
func (e *CutoverEngine) Rollback(ctx context.Context) error {
	startTime := time.Now()
	sourceOldName := e.sourceTable + "_old"

	e.logger.Info("Starting rollback",
		"schema", e.schema,
		"sourceTable", e.sourceTable,
		"sourceOldTable", sourceOldName,
	)

	// Step 1: Verify source_old exists
	exists, err := e.db.IsTableExists(e.schema, sourceOldName)
	if err != nil {
		return fmt.Errorf("failed to check if %s exists: %w", sourceOldName, err)
	}

	if !exists {
		return ErrSourceOldNotFound
	}

	// Step 2: Drop post-cutover FKs (FKs recreated during cutover pointing to partitioned table)
	referencingFKs, err := e.db.GetReferencingForeignKeys(e.schema, e.sourceTable)
	if err != nil {
		return fmt.Errorf("failed to get referencing foreign keys for rollback: %w", err)
	}

	for _, fk := range referencingFKs {
		childTable := e.getChildTableFromFK(fk)

		if err := e.db.DropForeignKey(e.schema, childTable, fk.Name); err != nil {
			return fmt.Errorf("failed to drop post-cutover FK %s: %w", fk.Name, err)
		}
	}

	// Step 3: Begin transaction with lock_timeout and statement_timeout
	tx, err := e.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin rollback transaction: %w", err)
	}

	committed := false

	defer func() {
		if !committed {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				e.logger.Error("Failed to rollback rollback transaction", "error", rbErr)
			}
		}
	}()

	// Step 4: Acquire advisory lock
	if err := e.db.AcquireAdvisoryLock(e.schema, e.sourceTable); err != nil {
		return fmt.Errorf("failed to acquire advisory lock for rollback: %w", err)
	}

	// Step 5: Acquire ACCESS EXCLUSIVE on source, SHARE ROW EXCLUSIVE on source_old (deterministic order)
	if err := e.db.AcquireExclusiveLock(e.schema, e.sourceTable); err != nil {
		return fmt.Errorf("failed to acquire ACCESS EXCLUSIVE lock on source for rollback: %w", err)
	}

	if err := e.db.AcquireShareRowExclusiveLock(e.schema, sourceOldName); err != nil {
		return fmt.Errorf("failed to acquire SHARE ROW EXCLUSIVE lock on source_old for rollback: %w", err)
	}

	// Step 6: Reverse rename (source → target, source_old → source)
	if err := e.db.RenameTable(e.schema, e.sourceTable, e.targetTable); err != nil {
		return fmt.Errorf("failed to rename source to target during rollback: %w", err)
	}

	if err := e.db.RenameTable(e.schema, sourceOldName, e.sourceTable); err != nil {
		return fmt.Errorf("failed to rename source_old to source during rollback: %w", err)
	}

	// Step 7: Commit
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit rollback transaction: %w", err)
	}

	committed = true

	e.logger.Info("Rollback transaction committed",
		"schema", e.schema,
		"sourceTable", e.sourceTable,
		"targetTable", e.targetTable,
	)

	// Step 8: Post-rollback operations
	if err := e.postRollback(ctx); err != nil {
		e.logger.Error("Post-rollback operations had errors (rollback itself succeeded)",
			"schema", e.schema,
			"table", e.sourceTable,
			"error", err,
		)

		return fmt.Errorf("post-rollback operations failed: %w", err)
	}

	elapsed := time.Since(startTime)

	e.logger.Info("Rollback completed successfully",
		"schema", e.schema,
		"table", e.sourceTable,
		"elapsed", elapsed.String(),
	)

	return nil
}

// postCutover performs post-cutover operations in separate transactions:
// ANALYZE, rename indexes, VALIDATE CONSTRAINT.
func (e *CutoverEngine) postCutover(ctx context.Context, referencingFKs []postgresql.ForeignKeyDef) error {
	// ANALYZE the new partitioned table (Requirement 7.9)
	e.logger.Info("Running ANALYZE on new partitioned table", "schema", e.schema, "table", e.sourceTable)

	if err := e.db.AnalyzeTable(e.schema, e.sourceTable); err != nil {
		return fmt.Errorf("failed to ANALYZE table: %w", err)
	}

	// Rename indexes: replace target table prefix with source table prefix (Requirement 7.10)
	if err := e.renameIndexesPostCutover(ctx); err != nil {
		return fmt.Errorf("failed to rename indexes post-cutover: %w", err)
	}

	// Validate FK constraints (Requirement 7.11)
	for _, fk := range referencingFKs {
		childTable := e.getChildTableFromFK(fk)

		e.logger.Info("Validating foreign key constraint",
			"schema", e.schema,
			"childTable", childTable,
			"constraint", fk.Name,
		)

		if err := e.db.ValidateForeignKey(e.schema, childTable, fk.Name); err != nil {
			return fmt.Errorf("failed to validate FK %s on %s: %w", fk.Name, childTable, err)
		}
	}

	return nil
}

// renameIndexesPostCutover renames indexes on the new source table (formerly target)
// by replacing the target table name prefix with the source table name prefix.
func (e *CutoverEngine) renameIndexesPostCutover(_ context.Context) error {
	// First, rename indexes on the old table (events_old) to avoid name conflicts.
	// After the rename swap, events_old still owns indexes with the original source table prefix
	// (e.g., "events_pkey"). We need to move them out of the way before renaming the new table's indexes.
	oldTableName := e.sourceTable + "_old"

	oldIndexes, err := e.db.GetTableIndexes(e.schema, oldTableName)
	if err != nil {
		e.logger.Warn("Could not get indexes on old table for rename (may not exist)", "error", err)
	} else {
		for _, idx := range oldIndexes {
			if strings.HasPrefix(idx.Name, e.sourceTable) {
				newName := strings.Replace(idx.Name, e.sourceTable, oldTableName, 1)

				e.logger.Info("Renaming old table index to avoid conflict",
					"schema", e.schema,
					"oldName", idx.Name,
					"newName", newName,
				)

				if err := e.db.RenameIndex(e.schema, idx.Name, newName); err != nil {
					return fmt.Errorf("failed to rename old index %s to %s: %w", idx.Name, newName, err)
				}
			}
		}
	}

	// Now rename indexes on the new source table (formerly target) to use the source table prefix
	indexes, err := e.db.GetTableIndexes(e.schema, e.sourceTable)
	if err != nil {
		return fmt.Errorf("failed to get indexes for rename: %w", err)
	}

	for _, idx := range indexes {
		// Replace target table prefix with source table prefix in index name
		if strings.HasPrefix(idx.Name, e.targetTable) {
			newName := strings.Replace(idx.Name, e.targetTable, e.sourceTable, 1)

			e.logger.Info("Renaming index",
				"schema", e.schema,
				"oldName", idx.Name,
				"newName", newName,
			)

			if err := e.db.RenameIndex(e.schema, idx.Name, newName); err != nil {
				return fmt.Errorf("failed to rename index %s to %s: %w", idx.Name, newName, err)
			}
		}
	}

	return nil
}

// postRollback performs post-rollback operations:
// restore original FKs, re-enable trigger, rename indexes back, ANALYZE.
func (e *CutoverEngine) postRollback(ctx context.Context) error {
	// Restore original FKs from migration metadata
	state, err := e.db.GetMigrationState(e.schema, e.sourceTable)
	if err != nil {
		return fmt.Errorf("failed to get migration state for post-rollback: %w", err)
	}

	if state != nil && len(state.DroppedForeignKeys) > 0 {
		for _, fk := range state.DroppedForeignKeys {
			childTable := e.getChildTableFromFK(fk)

			// Restore original FK pointing to the restored source table
			restoredFK := postgresql.ForeignKeyDef{
				Name:              fk.Name,
				Columns:           fk.Columns,
				ReferencedSchema:  e.schema,
				ReferencedTable:   e.sourceTable,
				ReferencedColumns: fk.ReferencedColumns,
				OnDelete:          fk.OnDelete,
				OnUpdate:          fk.OnUpdate,
			}

			e.logger.Info("Restoring original foreign key",
				"schema", e.schema,
				"childTable", childTable,
				"constraint", fk.Name,
			)

			if err := e.db.AddForeignKeyNotValid(e.schema, childTable, restoredFK); err != nil {
				return fmt.Errorf("failed to restore FK %s: %w", fk.Name, err)
			}
		}
	}

	// Re-enable CDC trigger on restored source table (Requirement 8.7)
	triggerName := fmt.Sprintf("ppm_cdc_%s", e.sourceTable)

	e.logger.Info("Re-enabling CDC trigger", "schema", e.schema, "table", e.sourceTable, "trigger", triggerName)

	if err := e.db.EnableTrigger(e.schema, e.sourceTable, triggerName); err != nil {
		return fmt.Errorf("failed to re-enable CDC trigger: %w", err)
	}

	// Rename indexes back: replace source table prefix with target table prefix on the target table,
	// and restore original names on the source table
	if err := e.renameIndexesPostRollback(ctx); err != nil {
		return fmt.Errorf("failed to rename indexes post-rollback: %w", err)
	}

	// ANALYZE restored source table (Requirement 8.8)
	e.logger.Info("Running ANALYZE on restored source table", "schema", e.schema, "table", e.sourceTable)

	if err := e.db.AnalyzeTable(e.schema, e.sourceTable); err != nil {
		return fmt.Errorf("failed to ANALYZE restored table: %w", err)
	}

	return nil
}

// renameIndexesPostRollback renames indexes on the restored source table
// by replacing the source table prefix with the original naming convention.
// After rollback, the source table (formerly source_old) has its original indexes.
// The target table (formerly source/partitioned) may have renamed indexes that need reverting.
func (e *CutoverEngine) renameIndexesPostRollback(_ context.Context) error {
	// Get indexes on the target table (the partitioned table that was renamed back)
	indexes, err := e.db.GetTableIndexes(e.schema, e.targetTable)
	if err != nil {
		return fmt.Errorf("failed to get indexes for rollback rename: %w", err)
	}

	for _, idx := range indexes {
		// If indexes were renamed during cutover (source prefix applied), revert to target prefix
		if strings.HasPrefix(idx.Name, e.sourceTable) {
			newName := strings.Replace(idx.Name, e.sourceTable, e.targetTable, 1)

			e.logger.Info("Reverting index name post-rollback",
				"schema", e.schema,
				"oldName", idx.Name,
				"newName", newName,
			)

			if err := e.db.RenameIndex(e.schema, idx.Name, newName); err != nil {
				return fmt.Errorf("failed to revert index %s to %s: %w", idx.Name, newName, err)
			}
		}
	}

	return nil
}

// drainQueue performs the final replay loop within the cutover transaction,
// draining the CDC queue completely by dequeuing and applying events in batches.
func (e *CutoverEngine) drainQueue(ctx context.Context) (int64, error) {
	var totalDrained int64

	for {
		select {
		case <-ctx.Done():
			return totalDrained, fmt.Errorf("context cancelled during queue drain: %w", ctx.Err())
		default:
		}

		events, err := e.db.DequeueEvents(e.schema, e.sourceTable, e.batchSize)
		if err != nil {
			return totalDrained, fmt.Errorf("failed to dequeue events during final drain: %w", err)
		}

		if len(events) == 0 {
			return totalDrained, nil
		}

		for _, event := range events {
			if err := e.applyEvent(event); err != nil {
				return totalDrained, fmt.Errorf("failed to apply event seq_id %d during final drain: %w", event.SeqID, err)
			}
		}

		totalDrained += int64(len(events))
	}
}

// applyEvent applies a single CDC event to the target table during the final drain.
func (e *CutoverEngine) applyEvent(event postgresql.CDCEvent) error {
	switch event.Operation {
	case "INSERT", "UPDATE":
		err := e.db.ApplyUpsert(e.schema, e.targetTable, e.sourceTable, e.pkColumns, event.PKValues)
		if err != nil {
			if errors.Is(err, ErrSourceRowNotFound) {
				e.logger.Warn("Source row not found during final drain, skipping",
					"schema", e.schema,
					"table", e.sourceTable,
					"operation", event.Operation,
					"seqID", event.SeqID,
					"pkValues", event.PKValues,
				)

				return nil
			}

			return fmt.Errorf("apply upsert failed: %w", err)
		}

	case "DELETE":
		if err := e.db.ApplyDelete(e.schema, e.targetTable, e.pkColumns, event.PKValues); err != nil {
			return fmt.Errorf("apply delete failed: %w", err)
		}

	default:
		e.logger.Warn("Unknown CDC operation during final drain, skipping",
			"operation", event.Operation,
			"seqID", event.SeqID,
		)
	}

	return nil
}

// recordDroppedFKs persists the FK definitions in migration metadata for rollback purposes.
func (e *CutoverEngine) recordDroppedFKs(fks []postgresql.ForeignKeyDef) error {
	state, err := e.db.GetMigrationState(e.schema, e.sourceTable)
	if err != nil {
		return fmt.Errorf("failed to get migration state: %w", err)
	}

	if state == nil {
		return fmt.Errorf("%w for %s.%s", ErrMigrationStateNotFound, e.schema, e.sourceTable)
	}

	state.DroppedForeignKeys = fks
	state.UpdatedAt = time.Now()

	if err := e.db.UpdateMigrationState(e.schema, e.sourceTable, state); err != nil {
		return fmt.Errorf("failed to persist dropped FK definitions: %w", err)
	}

	return nil
}

// recreateReferencingFKs attempts to recreate referencing FKs with NOT VALID on the new
// partitioned table.
//
// PostgreSQL requires that any unique constraint on a partitioned table includes all
// partition key columns. As a consequence, FKs that reference a partitioned table can only
// target column sets that match an existing unique constraint on the parent table.
//
// Currently, only one FK shape is supported for automatic recreation: when the FK
// referenced columns are exactly the partition key column. In that case the partitioned
// table always exposes a unique constraint matching those columns (the partition key
// is required to be part of the PK / any unique index, so a single-column partition key
// PK trivially provides it).
//
// Other shapes (composite PK including the partition key, FK referencing a non-partition-
// key column, etc.) are skipped with a warning. Support for those will come later.
//
// Returns the list of FKs that were successfully recreated (for post-cutover validation).
func (e *CutoverEngine) recreateReferencingFKs(referencingFKs []postgresql.ForeignKeyDef) []postgresql.ForeignKeyDef {
	var recreated []postgresql.ForeignKeyDef

	for _, fk := range referencingFKs {
		childTable := e.getChildTableFromFK(fk)

		if !e.isFKReferencingPartitionKey(fk) {
			e.logger.Warn("Skipping FK recreation — only FKs referencing exactly the partition key column "+
				"are supported today (other FK shapes will be supported in a future release)",
				"schema", e.schema,
				"childTable", childTable,
				"constraint", fk.Name,
				"referencedColumns", fk.ReferencedColumns,
				"partitionKey", e.partitionKey,
			)

			continue
		}

		recreatedFK := postgresql.ForeignKeyDef{
			Name:              fk.Name,
			Columns:           fk.Columns,
			ReferencedSchema:  e.schema,
			ReferencedTable:   e.sourceTable,
			ReferencedColumns: fk.ReferencedColumns,
			OnDelete:          fk.OnDelete,
			OnUpdate:          fk.OnUpdate,
		}

		e.logger.Info("Recreating referencing FK on partitioned table (FK matches partition key)",
			"schema", e.schema,
			"childTable", childTable,
			"constraint", fk.Name,
			"referencedColumns", fk.ReferencedColumns,
		)

		if err := e.db.AddForeignKeyNotValid(e.schema, childTable, recreatedFK); err != nil {
			e.logger.Warn("Failed to recreate referencing FK",
				"schema", e.schema,
				"childTable", childTable,
				"constraint", fk.Name,
				"error", err,
			)

			continue
		}

		recreated = append(recreated, fk)
	}

	return recreated
}

// isFKReferencingPartitionKey reports whether the FK referenced columns are exactly
// the partition key column (a single column matching e.partitionKey).
//
// This is the only FK shape currently supported for automatic recreation on the new
// partitioned table — see recreateReferencingFKs for context.
func (e *CutoverEngine) isFKReferencingPartitionKey(fk postgresql.ForeignKeyDef) bool {
	if e.partitionKey == "" {
		return false
	}

	if len(fk.ReferencedColumns) != 1 {
		return false
	}

	return fk.ReferencedColumns[0] == e.partitionKey
}

// getChildTableFromFK extracts the child table name from a ForeignKeyDef.
// For referencing FKs returned by GetReferencingForeignKeys, the struct stores
// the child table (the table that owns the FK constraint) in ReferencedTable,
// because the query returns the source table of the constraint in that field.
func (e *CutoverEngine) getChildTableFromFK(fk postgresql.ForeignKeyDef) string {
	return fk.ReferencedTable
}
