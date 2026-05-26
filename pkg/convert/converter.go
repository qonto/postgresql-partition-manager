package convert

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
)

// RedactConnectionURL redacts credentials (password) from a PostgreSQL connection URL.
// It replaces the password with "REDACTED" in the output, ensuring sensitive data
// is never logged. If the URL cannot be parsed, it returns a generic redacted string.
func RedactConnectionURL(connectionURL string) string {
	parsed, err := url.Parse(connectionURL)
	if err != nil {
		return "postgres://***:REDACTED@<unparseable-url>"
	}

	if parsed.User != nil {
		if _, hasPassword := parsed.User.Password(); hasPassword {
			parsed.User = url.UserPassword(parsed.User.Username(), "REDACTED")
		}
	}

	return parsed.String()
}

// ConvertDBClient defines the database operations needed by the conversion engine.
type ConvertDBClient interface {
	// Schema introspection
	GetTableColumns(schema, table string) ([]postgresql.ColumnDef, error)
	GetTablePrimaryKey(schema, table string) ([]string, error)
	GetTableIndexes(schema, table string) ([]postgresql.IndexDef, error)
	GetTableForeignKeys(schema, table string) ([]postgresql.ForeignKeyDef, error)
	GetReferencingForeignKeys(schema, table string) ([]postgresql.ForeignKeyDef, error)
	GetTableCheckConstraints(schema, table string) ([]postgresql.CheckConstraintDef, error)
	GetPartitionKeyRange(schema, table, partitionKey string) (min, max time.Time, err error)
	GetTableRowCount(schema, table string) (int64, error)
	IsTableExists(schema, table string) (bool, error)
	HasPrimaryKey(schema, table string) (bool, error)

	// Setup operations
	CreateCDCQueue(schema, table string, pkColumns []string) error
	CreateCDCTriggerFunction(schema, table string, pkColumns []string) error
	InstallCDCTrigger(schema, table string) error
	CreatePartitionedTable(schema, table string, columns []postgresql.ColumnDef, partitionKey string) error
	CreatePartition(schema, parentTable, partitionName, lowerBound, upperBound string) error
	CreateIndex(schema, table string, idx postgresql.IndexDef) error
	CreateForeignKey(schema, table string, fk postgresql.ForeignKeyDef) error
	IsCDCQueueExists(schema, table string) (bool, error)
	IsCDCTriggerExists(schema, table string) (bool, error)

	// Backfill operations
	BackfillBatch(schema, sourceTable, targetTable string, pkColumns []string, afterPK []any, batchSize int) (lastPK []any, rowsCopied int64, err error)

	// Replay operations
	DequeueEvents(schema, table string, batchSize int) ([]postgresql.CDCEvent, error)
	ApplyUpsert(schema, targetTable, sourceTable string, pkColumns []string, pkValues []string) error
	ApplyDelete(schema, targetTable string, pkColumns []string, pkValues []string) error
	GetReplayLag(schema, table string) (int64, error)
	IsCDCQueueEmpty(schema, table string) (bool, error)

	// Cutover operations
	AcquireAdvisoryLock(schema, table string) error
	AcquireExclusiveLock(schema, table string) error
	AcquireShareRowExclusiveLock(schema, table string) error
	DisableTrigger(schema, table, triggerName string) error
	EnableTrigger(schema, table, triggerName string) error
	RenameTable(schema, oldName, newName string) error
	RenameIndex(schema, oldName, newName string) error
	DropTrigger(schema, table, triggerName string) error
	DropTriggerFunction(schema, functionName string) error
	AddForeignKeyNotValid(schema, table string, fk postgresql.ForeignKeyDef) error
	ValidateForeignKey(schema, table, constraintName string) error
	DropForeignKey(schema, table, constraintName string) error
	AnalyzeTable(schema, table string) error

	// Metadata operations
	EnsureMetadataTable() error
	GetMigrationState(schema, table string) (*postgresql.MigrationState, error)
	UpdateMigrationState(schema, table string, state *postgresql.MigrationState) error
	DeleteMigrationState(schema, table string) error

	// Cleanup operations
	DropTable(schema, table string) error
	DropCDCQueue(schema, table string) error
	ReassignSequences(schema, oldTable, newTable string) error

	// Transaction management
	BeginTx(ctx context.Context) (postgresql.Tx, error)
}

// ErrSourceTableNotFound is returned when the source table does not exist.
var ErrSourceTableNotFound = fmt.Errorf("source table does not exist")

// ErrNoPrimaryKey is returned when the source table has no primary key.
var ErrNoPrimaryKey = fmt.Errorf("source table has no primary key (required for CDC tracking)")

// Converter orchestrates the migration phases, validates state transitions,
// coordinates engines, and provides audit logging and dry-run support.
type Converter struct {
	db          ConvertDBClient
	config      partition.Configuration
	state       *StateMachine
	logger      slog.Logger
	dryRun      bool
	operationID string
}

// New creates a new Converter with a unique operation ID for audit tracing.
func New(logger slog.Logger, db ConvertDBClient, config partition.Configuration, dryRun bool) *Converter {
	return &Converter{
		db:          db,
		config:      config,
		state:       NewStateMachine(logger, db),
		logger:      logger,
		dryRun:      dryRun,
		operationID: uuid.New().String(),
	}
}

// Setup executes the setup phase: creates CDC queue, trigger, and target partitioned table.
func (c *Converter) Setup(ctx context.Context) error {
	return c.executePhase(ctx, "setup", PhaseSetup, func(ctx context.Context) error {
		return c.runSetup(ctx)
	})
}

// Backfill executes the backfill phase: copies data from source to target in batches.
func (c *Converter) Backfill(ctx context.Context) error {
	return c.executePhase(ctx, "backfill", PhaseBackfill, func(ctx context.Context) error {
		return c.runBackfill(ctx)
	})
}

// Replay executes the replay phase: applies CDC events to the target table.
func (c *Converter) Replay(ctx context.Context) error {
	return c.executePhase(ctx, "replay", PhaseReplay, func(ctx context.Context) error {
		return c.runReplay(ctx)
	})
}

// VerifyOptions holds options for the verify phase.
type VerifyOptions struct {
	WithAnalyze bool
}

// Verify executes the verify phase: checks convergence between source and target.
// Verify is purely informational and does NOT modify the migration state.
// It can be called at any time regardless of the current phase.
func (c *Converter) Verify(ctx context.Context, opts VerifyOptions) (*postgresql.VerifyResult, error) {
	startTime := time.Now()

	c.logger.Info("Operation started",
		"operationID", c.operationID,
		"phase", "verify",
		"schema", c.config.Schema,
		"table", c.config.Table,
		"startTime", startTime.Format(time.RFC3339),
		"dryRun", c.dryRun,
	)

	result, err := c.runVerify(ctx, opts)
	if err != nil {
		c.logger.Error("Operation failed",
			"operationID", c.operationID,
			"phase", "verify",
			"schema", c.config.Schema,
			"table", c.config.Table,
			"error", err.Error(),
		)

		return nil, err
	}

	c.logger.Info("Operation completed",
		"operationID", c.operationID,
		"phase", "verify",
		"schema", c.config.Schema,
		"table", c.config.Table,
		"startTime", startTime.Format(time.RFC3339),
		"endTime", time.Now().Format(time.RFC3339),
		"elapsed", time.Since(startTime),
	)

	return result, nil
}

// Cutover executes the cutover phase: atomically swaps source and target tables.
func (c *Converter) Cutover(ctx context.Context) error {
	return c.executePhase(ctx, "cutover", PhaseCutoverComplete, func(ctx context.Context) error {
		return c.runCutover(ctx)
	})
}

// Rollback reverses a completed cutover, restoring the original table.
func (c *Converter) Rollback(ctx context.Context) error {
	return c.executePhase(ctx, "rollback", PhaseRollbackComplete, func(ctx context.Context) error {
		return c.runRollback(ctx)
	})
}

// Cleanup removes migration artifacts after a successful cutover.
// When force is true, the state machine transition validation is bypassed,
// allowing cleanup from any phase (e.g., to recover from a failed setup).
func (c *Converter) Cleanup(ctx context.Context, confirm, force bool) error {
	return c.executePhase(ctx, "cleanup", PhaseCleanup, func(ctx context.Context) error {
		return c.runCleanup(ctx, confirm, force)
	}, force)
}

// executePhase is the common orchestration logic for all phases.
// It validates the state transition, executes the engine logic, transitions state,
// and logs audit information (start/end timestamps, operation ID).
// The optional skipValidation parameter (first variadic bool) bypasses state machine
// transition validation when true — used by cleanup --force to allow cleanup from any phase.
func (c *Converter) executePhase(ctx context.Context, phaseName string, targetPhase Phase, fn func(ctx context.Context) error, skipValidation ...bool) error {
	startTime := time.Now()

	c.logger.Info("Operation started",
		"operationID", c.operationID,
		"phase", phaseName,
		"schema", c.config.Schema,
		"table", c.config.Table,
		"startTime", startTime.Format(time.RFC3339),
		"dryRun", c.dryRun,
	)

	// Validate state transition
	currentState, err := c.state.GetState(c.config.Schema, c.config.Table)
	if err != nil {
		c.logError(phaseName, err)

		return fmt.Errorf("failed to get migration state: %w", err)
	}

	currentPhase := Phase(currentState.Phase)

	// Determine if validation should be skipped (cleanup --force)
	shouldSkipValidation := len(skipValidation) > 0 && skipValidation[0]

	// For setup phase, if we're already in setup (initial state), allow it
	// For cleanup with force, skip phase validation entirely
	if !shouldSkipValidation && (targetPhase != PhaseSetup || currentPhase != PhaseSetup) {
		if err := c.state.ValidateTransition(currentPhase, targetPhase); err != nil {
			if c.dryRun {
				c.logDryRun("Phase transition validation failed: current=%s, requested=%s", currentPhase, targetPhase)
			}

			c.logError(phaseName, err)

			return fmt.Errorf("invalid phase transition: %w", err)
		}
	}

	// Dry-run mode: output what would be done without executing
	if c.dryRun {
		c.logDryRun("Would execute phase %q for %s.%s", phaseName, c.config.Schema, c.config.Table)
		c.logDryRun("Current phase: %s, target phase: %s", currentPhase, targetPhase)

		return c.executeDryRun(ctx, phaseName)
	}

	// Execute the engine logic
	if err := fn(ctx); err != nil {
		c.logError(phaseName, err)

		return err
	}

	// Transition state (skip for setup when already in setup, and for cleanup which removes metadata)
	if targetPhase != PhaseSetup && targetPhase != PhaseCleanup {
		if err := c.state.TransitionTo(c.config.Schema, c.config.Table, targetPhase); err != nil {
			c.logError(phaseName, err)

			return fmt.Errorf("failed to transition state to %s: %w", targetPhase, err)
		}
	}

	endTime := time.Now()
	elapsed := endTime.Sub(startTime)

	c.logger.Info("Operation completed",
		"operationID", c.operationID,
		"phase", phaseName,
		"schema", c.config.Schema,
		"table", c.config.Table,
		"startTime", startTime.Format(time.RFC3339),
		"endTime", endTime.Format(time.RFC3339),
		"elapsed", elapsed.String(),
	)

	return nil
}

// executeDryRun outputs what each phase would do without making changes.
func (c *Converter) executeDryRun(_ context.Context, phaseName string) error {
	schema := c.config.Schema
	table := c.config.Table
	targetTable := table + "_partitioned"

	switch phaseName {
	case "setup":
		c.logDryRun("CREATE TABLE %s.%s_cdc_queue (...)", schema, table)
		c.logDryRun("CREATE FUNCTION %s.ppm_cdc_trigger_%s() ...", schema, table)
		c.logDryRun("CREATE TRIGGER ppm_cdc_%s ON %s.%s ...", table, schema, table)
		c.logDryRun("CREATE TABLE %s.%s PARTITION BY RANGE (%s)", schema, targetTable, c.config.PartitionKey)
		c.logDryRun("Would create partitions covering existing data range + %d pre-provisioned", c.config.PreProvisioned)
		c.logDryRun("Would replicate indexes, unique constraints, and foreign keys")

	case "backfill":
		rowCount, err := c.db.GetTableRowCount(schema, table)
		if err != nil {
			c.logDryRun("Could not estimate row count: %v", err)
		} else {
			batches := rowCount / int64(c.config.Convert.BackfillBatchSize)
			if rowCount%int64(c.config.Convert.BackfillBatchSize) != 0 {
				batches++
			}

			c.logDryRun("Estimated rows to process: %d", rowCount)
			c.logDryRun("Backfill batch size: %d", c.config.Convert.BackfillBatchSize)
			c.logDryRun("Estimated batches: %d", batches)
		}

		c.logDryRun("INSERT INTO %s.%s SELECT * FROM %s.%s WHERE (pk) > (last_pk) ORDER BY pk LIMIT %d ON CONFLICT DO NOTHING",
			schema, targetTable, schema, table, c.config.Convert.BackfillBatchSize)

	case "replay":
		lag, err := c.db.GetReplayLag(schema, table)
		if err != nil {
			c.logDryRun("Could not get replay lag: %v", err)
		} else {
			c.logDryRun("Current replay lag: %d events", lag)
		}

		c.logDryRun("DELETE FROM %s.%s_cdc_queue ... RETURNING (replay batch of %d)", schema, table, c.config.Convert.ReplayBatchSize)
		c.logDryRun("For INSERT/UPDATE: INSERT INTO target ... ON CONFLICT DO UPDATE")
		c.logDryRun("For DELETE: DELETE FROM target WHERE pk = values")

	case "verify":
		c.logDryRun("Would compare row counts between %s.%s and %s.%s", schema, table, schema, targetTable)
		c.logDryRun("Would check replay lag in %s.%s_cdc_queue", schema, table)
		c.logDryRun("Ready for cutover when: replay_lag == 0 AND row_count_difference == 0")

	case "cutover":
		c.logDryRun("BEGIN transaction with lock_timeout=%ds, statement_timeout=%ds", c.config.Convert.LockTimeout, c.config.Convert.StatementTimeout)
		c.logDryRun("SELECT pg_advisory_xact_lock(hashtext('ppm_migration_%s.%s'))", schema, table)
		c.logDryRun("LOCK TABLE %s.%s IN ACCESS EXCLUSIVE MODE", schema, table)
		c.logDryRun("LOCK TABLE %s.%s IN SHARE ROW EXCLUSIVE MODE", schema, targetTable)
		c.logDryRun("ALTER TABLE %s.%s DISABLE TRIGGER ppm_cdc_%s", schema, table, table)
		c.logDryRun("Final replay drain loop until queue is empty")
		c.logDryRun("Assert CDC queue is empty")
		c.logDryRun("DROP referencing foreign keys from child tables")
		c.logDryRun("ALTER TABLE %s.%s RENAME TO %s_old", schema, table, table)
		c.logDryRun("ALTER TABLE %s.%s RENAME TO %s", schema, targetTable, table)
		c.logDryRun("Recreate foreign keys with NOT VALID")
		c.logDryRun("COMMIT")
		c.logDryRun("ANALYZE %s.%s", schema, table)
		c.logDryRun("Rename indexes (target prefix → source prefix)")
		c.logDryRun("VALIDATE CONSTRAINT on recreated foreign keys")

	case "rollback":
		c.logDryRun("BEGIN transaction with lock_timeout=%ds, statement_timeout=%ds", c.config.Convert.LockTimeout, c.config.Convert.StatementTimeout)
		c.logDryRun("SELECT pg_advisory_xact_lock(hashtext('ppm_migration_%s.%s'))", schema, table)
		c.logDryRun("LOCK TABLE %s.%s IN ACCESS EXCLUSIVE MODE", schema, table)
		c.logDryRun("LOCK TABLE %s.%s_old IN SHARE ROW EXCLUSIVE MODE", schema, table)
		c.logDryRun("ALTER TABLE %s.%s RENAME TO %s", schema, table, targetTable)
		c.logDryRun("ALTER TABLE %s.%s_old RENAME TO %s", schema, table, table)
		c.logDryRun("COMMIT")
		c.logDryRun("Restore original foreign keys")
		c.logDryRun("ALTER TABLE %s.%s ENABLE TRIGGER ppm_cdc_%s", schema, table, table)
		c.logDryRun("ANALYZE %s.%s", schema, table)

	case "cleanup":
		c.logDryRun("DROP TRIGGER ppm_cdc_%s ON %s.%s_old", table, schema, table)
		c.logDryRun("DROP FUNCTION %s.ppm_cdc_trigger_%s()", schema, table)
		c.logDryRun("DROP TABLE %s.%s_cdc_queue", schema, table)
		c.logDryRun("DROP TABLE %s.%s_old", schema, table)
		c.logDryRun("DELETE FROM ppm_migration_metadata WHERE schema_name='%s' AND table_name='%s'", schema, table)
	}

	c.logDryRun("Phase %q completed (no changes made)", phaseName)

	return nil
}

// runSetup executes the setup phase logic.
func (c *Converter) runSetup(ctx context.Context) error {
	schema := c.config.Schema
	table := c.config.Table
	targetTable := table + "_partitioned"

	// Verify source table exists (Requirement 1.4)
	exists, err := c.db.IsTableExists(schema, table)
	if err != nil {
		return fmt.Errorf("failed to check if source table exists: %w", err)
	}

	if !exists {
		return fmt.Errorf("%w: %s.%s", ErrSourceTableNotFound, schema, table)
	}

	// Verify source table has a primary key (Requirement 1.5)
	hasPK, err := c.db.HasPrimaryKey(schema, table)
	if err != nil {
		return fmt.Errorf("failed to check primary key: %w", err)
	}

	if !hasPK {
		return fmt.Errorf("%w: %s.%s", ErrNoPrimaryKey, schema, table)
	}

	// Get PK columns
	pkColumns, err := c.db.GetTablePrimaryKey(schema, table)
	if err != nil {
		return fmt.Errorf("failed to get primary key columns: %w", err)
	}

	// Create CDC queue (skip if exists) (Requirement 1.3)
	queueExists, err := c.db.IsCDCQueueExists(schema, table)
	if err != nil {
		return fmt.Errorf("failed to check CDC queue existence: %w", err)
	}

	if queueExists {
		c.logger.Info("CDC queue already exists, skipping creation",
			"operationID", c.operationID,
			"schema", schema,
			"table", table+"_cdc_queue",
		)
	} else {
		c.logger.Info("Creating CDC queue",
			"operationID", c.operationID,
			"schema", schema,
			"table", table+"_cdc_queue",
		)

		if err := c.db.CreateCDCQueue(schema, table, pkColumns); err != nil {
			return fmt.Errorf("failed to create CDC queue: %w", err)
		}
	}

	// Create CDC trigger function and install trigger (skip if exists) (Requirement 2.3)
	triggerExists, err := c.db.IsCDCTriggerExists(schema, table)
	if err != nil {
		return fmt.Errorf("failed to check CDC trigger existence: %w", err)
	}

	if triggerExists {
		c.logger.Info("CDC trigger already exists, skipping installation",
			"operationID", c.operationID,
			"schema", schema,
			"table", table,
		)
	} else {
		c.logger.Info("Creating CDC trigger function",
			"operationID", c.operationID,
			"schema", schema,
			"table", table,
		)

		if err := c.db.CreateCDCTriggerFunction(schema, table, pkColumns); err != nil {
			return fmt.Errorf("failed to create CDC trigger function: %w", err)
		}

		c.logger.Info("Installing CDC trigger",
			"operationID", c.operationID,
			"schema", schema,
			"table", table,
		)

		if err := c.db.InstallCDCTrigger(schema, table); err != nil {
			return fmt.Errorf("failed to install CDC trigger: %w", err)
		}
	}

	// Create target partitioned table (skip if exists) (Requirement 3.3)
	targetExists, err := c.db.IsTableExists(schema, targetTable)
	if err != nil {
		return fmt.Errorf("failed to check target table existence: %w", err)
	}

	if targetExists {
		c.logger.Info("Target partitioned table already exists, skipping creation",
			"operationID", c.operationID,
			"schema", schema,
			"table", targetTable,
		)
	} else {
		if err := c.createTargetTable(ctx, schema, table, targetTable, pkColumns); err != nil {
			return err
		}
	}

	// Transition to setup phase (initialize metadata)
	if err := c.state.TransitionTo(schema, table, PhaseBackfill); err != nil {
		// If transition fails because we're already past setup, that's fine for idempotency
		c.logger.Warn("Could not transition to backfill (may already be past setup)",
			"operationID", c.operationID,
			"error", err,
		)
	}

	return nil
}

// createTargetTable creates the target partitioned table with columns, partitions, indexes, and FKs.
func (c *Converter) createTargetTable(_ context.Context, schema, sourceTable, targetTable string, pkColumns []string) error {
	// Get column definitions
	columns, err := c.db.GetTableColumns(schema, sourceTable)
	if err != nil {
		return fmt.Errorf("failed to get table columns: %w", err)
	}

	c.logger.Info("Creating target partitioned table",
		"operationID", c.operationID,
		"schema", schema,
		"table", targetTable,
		"partitionKey", c.config.PartitionKey,
	)

	if err := c.db.CreatePartitionedTable(schema, targetTable, columns, c.config.PartitionKey); err != nil {
		return fmt.Errorf("failed to create partitioned table: %w", err)
	}

	// Create partitions covering existing data range + pre-provisioned
	if err := c.createPartitions(schema, sourceTable, targetTable); err != nil {
		return fmt.Errorf("failed to create partitions: %w", err)
	}

	// Replicate indexes (Requirement 3.4)
	if err := c.replicateIndexes(schema, sourceTable, targetTable, pkColumns); err != nil {
		return fmt.Errorf("failed to replicate indexes: %w", err)
	}

	// Replicate foreign keys (Requirement 3.6)
	if err := c.replicateForeignKeys(schema, sourceTable, targetTable); err != nil {
		return fmt.Errorf("failed to replicate foreign keys: %w", err)
	}

	return nil
}

// createPartitions creates partitions covering the data range plus pre-provisioned future partitions.
func (c *Converter) createPartitions(schema, sourceTable, targetTable string) error {
	minDate, maxDate, err := c.db.GetPartitionKeyRange(schema, sourceTable, c.config.PartitionKey)
	if err != nil {
		c.logger.Warn("Could not get partition key range, creating only pre-provisioned partitions",
			"operationID", c.operationID,
			"error", err,
		)

		maxDate = time.Now()
		minDate = maxDate
	}

	// Generate partitions from min to max, advancing by one interval each iteration
	currentDate := minDate

	for !currentDate.After(maxDate) {
		p, err := c.config.GeneratePartition(currentDate)
		if err != nil {
			return fmt.Errorf("failed to generate partition for date %s: %w", currentDate, err)
		}

		partitionName := targetTable + "_" + p.Name[len(sourceTable)+1:]

		c.logger.Info("Creating partition",
			"operationID", c.operationID,
			"schema", schema,
			"partition", partitionName,
		)

		if err := c.db.CreatePartition(schema, targetTable, partitionName,
			p.LowerBound.Format(time.RFC3339),
			p.UpperBound.Format(time.RFC3339)); err != nil {
			return fmt.Errorf("failed to create partition %s: %w", partitionName, err)
		}

		// Advance to the next interval by using the current partition's upper bound
		currentDate = p.UpperBound
	}

	// Create pre-provisioned future partitions
	futurePartitions, err := c.config.GetPreProvisionedPartitions(maxDate)
	if err != nil {
		return fmt.Errorf("failed to generate pre-provisioned partitions: %w", err)
	}

	for _, p := range futurePartitions {
		partitionName := targetTable + "_" + p.Name[len(sourceTable)+1:]

		c.logger.Info("Creating pre-provisioned partition",
			"operationID", c.operationID,
			"schema", schema,
			"partition", partitionName,
		)

		if err := c.db.CreatePartition(schema, targetTable, partitionName,
			p.LowerBound.Format(time.RFC3339),
			p.UpperBound.Format(time.RFC3339)); err != nil {
			return fmt.Errorf("failed to create pre-provisioned partition %s: %w", partitionName, err)
		}
	}

	return nil
}

// replicateIndexes replicates non-PK indexes from source to target table.
func (c *Converter) replicateIndexes(schema, sourceTable, targetTable string, pkColumns []string) error {
	indexes, err := c.db.GetTableIndexes(schema, sourceTable)
	if err != nil {
		return fmt.Errorf("failed to get source table indexes: %w", err)
	}

	for _, idx := range indexes {
		// Create PK with partition key included (Requirement 3.7)
		if idx.IsPrimary {
			newPKColumns := appendPartitionKeyIfNeeded(pkColumns, c.config.PartitionKey)
			pkIdx := postgresql.IndexDef{
				Name:      targetTable + "_pkey",
				Columns:   newPKColumns,
				IsUnique:  true,
				IsPrimary: true,
				Method:    "btree",
			}

			c.logger.Info("Creating primary key on target table",
				"operationID", c.operationID,
				"schema", schema,
				"table", targetTable,
				"columns", newPKColumns,
			)

			if err := c.db.CreateIndex(schema, targetTable, pkIdx); err != nil {
				return fmt.Errorf("failed to create primary key on target: %w", err)
			}

			continue
		}

		// Replicate non-PK indexes with target table name prefix
		targetIdx := idx
		targetIdx.Name = targetTable + idx.Name[len(sourceTable):]

		// For unique indexes, append partition key (Requirement 3.5)
		if idx.IsUnique {
			targetIdx.Columns = appendPartitionKeyIfNeeded(idx.Columns, c.config.PartitionKey)
		}

		c.logger.Info("Replicating index",
			"operationID", c.operationID,
			"schema", schema,
			"sourceIndex", idx.Name,
			"targetIndex", targetIdx.Name,
		)

		if err := c.db.CreateIndex(schema, targetTable, targetIdx); err != nil {
			return fmt.Errorf("failed to replicate index %s: %w", idx.Name, err)
		}
	}

	return nil
}

// replicateForeignKeys replicates foreign keys from source to target table.
func (c *Converter) replicateForeignKeys(schema, sourceTable, targetTable string) error {
	fks, err := c.db.GetTableForeignKeys(schema, sourceTable)
	if err != nil {
		return fmt.Errorf("failed to get source table foreign keys: %w", err)
	}

	for _, fk := range fks {
		targetFK := fk
		targetFK.Name = targetTable + fk.Name[len(sourceTable):]

		c.logger.Info("Replicating foreign key",
			"operationID", c.operationID,
			"schema", schema,
			"sourceFK", fk.Name,
			"targetFK", targetFK.Name,
		)

		if err := c.db.CreateForeignKey(schema, targetTable, targetFK); err != nil {
			return fmt.Errorf("failed to replicate foreign key %s: %w", fk.Name, err)
		}
	}

	return nil
}

// runBackfill executes the backfill engine.
func (c *Converter) runBackfill(ctx context.Context) error {
	pkColumns, err := c.db.GetTablePrimaryKey(c.config.Schema, c.config.Table)
	if err != nil {
		return fmt.Errorf("failed to get primary key columns: %w", err)
	}

	targetTable := c.config.Table + "_partitioned"

	engine := NewBackfillEngine(c.logger, c.db, BackfillEngineConfig{
		Schema:      c.config.Schema,
		SourceTable: c.config.Table,
		TargetTable: targetTable,
		PKColumns:   pkColumns,
		BatchSize:   c.config.Convert.BackfillBatchSize,
	})

	return engine.Run(ctx)
}

// runReplay executes the replay engine.
func (c *Converter) runReplay(ctx context.Context) error {
	pkColumns, err := c.db.GetTablePrimaryKey(c.config.Schema, c.config.Table)
	if err != nil {
		return fmt.Errorf("failed to get primary key columns: %w", err)
	}

	targetTable := c.config.Table + "_partitioned"

	engine := NewReplayEngine(c.logger, c.db, ReplayEngineConfig{
		Schema:      c.config.Schema,
		SourceTable: c.config.Table,
		TargetTable: targetTable,
		PKColumns:   pkColumns,
		BatchSize:   c.config.Convert.ReplayBatchSize,
	})

	return engine.Run(ctx)
}

// runVerify executes the verification engine.
func (c *Converter) runVerify(ctx context.Context, opts VerifyOptions) (*postgresql.VerifyResult, error) {
	targetTable := c.config.Table + "_partitioned"

	engine := NewVerifyEngine(c.logger, c.db, VerifyEngineConfig{
		Schema:      c.config.Schema,
		SourceTable: c.config.Table,
		TargetTable: targetTable,
		WithAnalyze: opts.WithAnalyze,
	})

	return engine.Verify(ctx)
}

// runCutover executes the cutover engine.
func (c *Converter) runCutover(ctx context.Context) error {
	pkColumns, err := c.db.GetTablePrimaryKey(c.config.Schema, c.config.Table)
	if err != nil {
		return fmt.Errorf("failed to get primary key columns: %w", err)
	}

	targetTable := c.config.Table + "_partitioned"

	engine := NewCutoverEngine(c.logger, c.db, CutoverEngineConfig{
		Schema:       c.config.Schema,
		SourceTable:  c.config.Table,
		TargetTable:  targetTable,
		PKColumns:    pkColumns,
		PartitionKey: c.config.PartitionKey,
		BatchSize:    c.config.Convert.ReplayBatchSize,
	})

	return engine.Cutover(ctx)
}

// runRollback executes the rollback via the cutover engine.
func (c *Converter) runRollback(ctx context.Context) error {
	pkColumns, err := c.db.GetTablePrimaryKey(c.config.Schema, c.config.Table)
	if err != nil {
		return fmt.Errorf("failed to get primary key columns: %w", err)
	}

	targetTable := c.config.Table + "_partitioned"

	engine := NewCutoverEngine(c.logger, c.db, CutoverEngineConfig{
		Schema:       c.config.Schema,
		SourceTable:  c.config.Table,
		TargetTable:  targetTable,
		PKColumns:    pkColumns,
		PartitionKey: c.config.PartitionKey,
		BatchSize:    c.config.Convert.ReplayBatchSize,
	})

	return engine.Rollback(ctx)
}

// runCleanup delegates to the CleanupEngine.
func (c *Converter) runCleanup(ctx context.Context, confirm, force bool) error {
	engine := NewCleanupEngine(c.logger, c.db, CleanupEngineConfig{
		Schema:      c.config.Schema,
		SourceTable: c.config.Table,
	})

	return engine.Cleanup(ctx, confirm, force)
}

// logDryRun outputs a message with the [DRY-RUN] prefix.
func (c *Converter) logDryRun(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)

	c.logger.Info("[DRY-RUN] "+msg,
		"operationID", c.operationID,
		"schema", c.config.Schema,
		"table", c.config.Table,
	)
}

// logError logs an error with the current phase and operation ID.
func (c *Converter) logError(phase string, err error) {
	c.logger.Error("Operation failed",
		"operationID", c.operationID,
		"phase", phase,
		"schema", c.config.Schema,
		"table", c.config.Table,
		"error", err,
	)
}

// appendPartitionKeyIfNeeded appends the partition key to a column list if not already present.
func appendPartitionKeyIfNeeded(columns []string, partitionKey string) []string {
	for _, col := range columns {
		if col == partitionKey {
			return columns
		}
	}

	result := make([]string, len(columns), len(columns)+1)
	copy(result, columns)

	return append(result, partitionKey)
}
