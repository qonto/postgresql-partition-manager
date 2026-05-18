package postgresql

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// PgxTx wraps a pgx.Tx to satisfy the Tx interface.
// It provides Commit, Rollback, Exec, QueryRow, and Query methods.
type PgxTx struct {
	tx pgx.Tx
}

func (t *PgxTx) Commit(ctx context.Context) error {
	err := t.tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	return nil
}

func (t *PgxTx) Rollback(ctx context.Context) error {
	err := t.tx.Rollback(ctx)
	if err != nil {
		return fmt.Errorf("rollback failed: %w", err)
	}

	return nil
}

func (t *PgxTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tag, err := t.tx.Exec(ctx, sql, args...)
	if err != nil {
		return tag, fmt.Errorf("exec failed: %w", err)
	}

	return tag, nil
}

func (t *PgxTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.tx.QueryRow(ctx, sql, args...)
}

func (t *PgxTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := t.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	return rows, nil
}

// BeginTx starts a new database transaction with configurable lock_timeout and
// statement_timeout set via SET LOCAL. These settings apply only within the
// transaction and are automatically reset when the transaction ends.
// The returned *PgxTx satisfies the Tx interface.
func (c *Client) BeginTx(ctx context.Context) (Tx, error) {
	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Set lock_timeout via SET LOCAL (transaction-scoped)
	lockTimeoutSQL := fmt.Sprintf("SET LOCAL lock_timeout = '%ds'", c.lockTimeout)

	_, err = tx.Exec(ctx, lockTimeoutSQL)
	if err != nil {
		_ = tx.Rollback(ctx)

		return nil, fmt.Errorf("failed to set lock_timeout: %w", err)
	}

	// Set statement_timeout via SET LOCAL (transaction-scoped)
	statementTimeoutSQL := fmt.Sprintf("SET LOCAL statement_timeout = '%ds'", c.statementTimeout)

	_, err = tx.Exec(ctx, statementTimeoutSQL)
	if err != nil {
		_ = tx.Rollback(ctx)

		return nil, fmt.Errorf("failed to set statement_timeout: %w", err)
	}

	c.logger.Info("Transaction started",
		"lockTimeout", fmt.Sprintf("%ds", c.lockTimeout),
		"statementTimeout", fmt.Sprintf("%ds", c.statementTimeout),
	)

	return &PgxTx{tx: tx}, nil
}

// AcquireAdvisoryLock acquires a transaction-scoped advisory lock using
// pg_advisory_xact_lock(hashtext('ppm_migration_<schema>.<table>')).
// This prevents concurrent migration execution against the same table.
// The lock is automatically released when the transaction ends.
func (c *Client) AcquireAdvisoryLock(schema, table string) error {
	lockKey := fmt.Sprintf("ppm_migration_%s.%s", schema, table)

	query := fmt.Sprintf("SELECT pg_advisory_xact_lock(hashtext('%s'))", lockKey)

	c.logger.Info("Acquiring advisory lock", "schema", schema, "table", table, "lockKey", lockKey)

	_, err := c.conn.Exec(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to acquire advisory lock for %s.%s: %w", schema, table, err)
	}

	return nil
}

// AcquireExclusiveLock acquires an ACCESS EXCLUSIVE lock on the specified table.
// This is the strongest lock mode and blocks all concurrent access.
// Used during cutover to prevent any reads or writes to the source table.
func (c *Client) AcquireExclusiveLock(schema, table string) error {
	qualifiedTable := pgx.Identifier{schema, table}.Sanitize()

	query := fmt.Sprintf("LOCK TABLE %s IN ACCESS EXCLUSIVE MODE", qualifiedTable)

	c.logger.Info("Acquiring ACCESS EXCLUSIVE lock", "schema", schema, "table", table)

	_, err := c.conn.Exec(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to acquire ACCESS EXCLUSIVE lock on %s.%s: %w", schema, table, err)
	}

	return nil
}

// AcquireShareRowExclusiveLock acquires a SHARE ROW EXCLUSIVE lock on the specified table.
// This lock mode conflicts with ROW EXCLUSIVE, SHARE UPDATE EXCLUSIVE, SHARE, SHARE ROW EXCLUSIVE,
// EXCLUSIVE, and ACCESS EXCLUSIVE locks. Used on the target table during cutover to prevent
// concurrent writes while allowing reads.
func (c *Client) AcquireShareRowExclusiveLock(schema, table string) error {
	qualifiedTable := pgx.Identifier{schema, table}.Sanitize()

	query := fmt.Sprintf("LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE", qualifiedTable)

	c.logger.Info("Acquiring SHARE ROW EXCLUSIVE lock", "schema", schema, "table", table)

	_, err := c.conn.Exec(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to acquire SHARE ROW EXCLUSIVE lock on %s.%s: %w", schema, table, err)
	}

	return nil
}

// DisableTrigger disables the specified trigger on the table.
// During cutover, this is done BEFORE the final replay to prevent new CDC events
// from being created after the replay starts draining.
func (c *Client) DisableTrigger(schema, table, triggerName string) error {
	qualifiedTable := pgx.Identifier{schema, table}.Sanitize()
	sanitizedTrigger := pgx.Identifier{triggerName}.Sanitize()

	query := fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER %s", qualifiedTable, sanitizedTrigger)

	c.logger.Info("Disabling trigger", "schema", schema, "table", table, "trigger", triggerName, "sql", query)

	_, err := c.conn.Exec(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to disable trigger %s on %s.%s: %w", triggerName, schema, table, err)
	}

	return nil
}

// EnableTrigger enables the specified trigger on the table.
// Used during rollback to re-enable the CDC trigger on the restored source table.
func (c *Client) EnableTrigger(schema, table, triggerName string) error {
	qualifiedTable := pgx.Identifier{schema, table}.Sanitize()
	sanitizedTrigger := pgx.Identifier{triggerName}.Sanitize()

	query := fmt.Sprintf("ALTER TABLE %s ENABLE TRIGGER %s", qualifiedTable, sanitizedTrigger)

	c.logger.Info("Enabling trigger", "schema", schema, "table", table, "trigger", triggerName, "sql", query)

	_, err := c.conn.Exec(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to enable trigger %s on %s.%s: %w", triggerName, schema, table, err)
	}

	return nil
}

// RenameTable renames a table from oldName to newName within the same schema.
// PostgreSQL's ALTER TABLE RENAME is a metadata-only operation that completes
// instantly once the required lock is held.
func (c *Client) RenameTable(schema, oldName, newName string) error {
	qualifiedTable := pgx.Identifier{schema, oldName}.Sanitize()
	sanitizedNewName := pgx.Identifier{newName}.Sanitize()

	query := fmt.Sprintf("ALTER TABLE %s RENAME TO %s", qualifiedTable, sanitizedNewName)

	c.logger.Info("Renaming table", "schema", schema, "oldName", oldName, "newName", newName, "sql", query)

	_, err := c.conn.Exec(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to rename table %s.%s to %s: %w", schema, oldName, newName, err)
	}

	return nil
}

// RenameIndex renames an index from oldName to newName within the same schema.
// Used post-cutover to update index names to match the original naming convention.
func (c *Client) RenameIndex(schema, oldName, newName string) error {
	qualifiedIndex := pgx.Identifier{schema, oldName}.Sanitize()
	sanitizedNewName := pgx.Identifier{newName}.Sanitize()

	query := fmt.Sprintf("ALTER INDEX %s RENAME TO %s", qualifiedIndex, sanitizedNewName)

	c.logger.Info("Renaming index", "schema", schema, "oldName", oldName, "newName", newName, "sql", query)

	_, err := c.conn.Exec(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to rename index %s.%s to %s: %w", schema, oldName, newName, err)
	}

	return nil
}

// DropTrigger drops the specified trigger from the table.
// Used during cleanup to remove the CDC trigger.
func (c *Client) DropTrigger(schema, table, triggerName string) error {
	qualifiedTable := pgx.Identifier{schema, table}.Sanitize()
	sanitizedTrigger := pgx.Identifier{triggerName}.Sanitize()

	query := fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", sanitizedTrigger, qualifiedTable)

	c.logger.Info("Dropping trigger", "schema", schema, "table", table, "trigger", triggerName, "sql", query)

	_, err := c.conn.Exec(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop trigger %s on %s.%s: %w", triggerName, schema, table, err)
	}

	return nil
}

// DropTriggerFunction drops the specified trigger function from the schema.
// Used during cleanup to remove the CDC trigger function.
func (c *Client) DropTriggerFunction(schema, functionName string) error {
	qualifiedFunction := pgx.Identifier{schema, functionName}.Sanitize()

	query := fmt.Sprintf("DROP FUNCTION IF EXISTS %s()", qualifiedFunction)

	c.logger.Info("Dropping trigger function", "schema", schema, "function", functionName, "sql", query)

	_, err := c.conn.Exec(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop trigger function %s.%s: %w", schema, functionName, err)
	}

	return nil
}

// AddForeignKeyNotValid adds a foreign key constraint with NOT VALID option.
// NOT VALID skips the validation scan on existing rows, making the operation instant.
// The constraint will be enforced for new rows immediately, but existing rows are
// not checked until ValidateForeignKey is called separately.
// This is used during cutover to recreate FKs pointing to the new partitioned table.
func (c *Client) AddForeignKeyNotValid(schema, table string, fk ForeignKeyDef) error {
	qualifiedTable := pgx.Identifier{schema, table}.Sanitize()
	constraintName := pgx.Identifier{fk.Name}.Sanitize()
	qualifiedRefTable := pgx.Identifier{fk.ReferencedSchema, fk.ReferencedTable}.Sanitize()

	// Build column lists
	fkCols := make([]string, len(fk.Columns))
	for i, col := range fk.Columns {
		fkCols[i] = pgx.Identifier{col}.Sanitize()
	}

	refCols := make([]string, len(fk.ReferencedColumns))
	for i, col := range fk.ReferencedColumns {
		refCols[i] = pgx.Identifier{col}.Sanitize()
	}

	alterSQL := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
		qualifiedTable,
		constraintName,
		strings.Join(fkCols, ", "),
		qualifiedRefTable,
		strings.Join(refCols, ", "),
	)

	// Add ON DELETE action if not default
	if fk.OnDelete != "" && fk.OnDelete != "NO ACTION" {
		alterSQL += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
	}

	// Add ON UPDATE action if not default
	if fk.OnUpdate != "" && fk.OnUpdate != "NO ACTION" {
		alterSQL += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
	}

	// NOT VALID skips validation scan — instant operation
	alterSQL += " NOT VALID"

	c.logger.Info("Adding foreign key NOT VALID", "schema", schema, "table", table, "constraint", fk.Name, "sql", alterSQL)

	_, err := c.conn.Exec(c.ctx, alterSQL)
	if err != nil {
		return fmt.Errorf("failed to add foreign key %s NOT VALID on %s.%s: %w", fk.Name, schema, table, err)
	}

	return nil
}

// ValidateForeignKey validates a previously added NOT VALID foreign key constraint.
// This performs a sequential scan to verify existing rows satisfy the constraint.
// It does NOT require ACCESS EXCLUSIVE lock — safe to run under load.
// Used post-cutover to validate FKs that were recreated with NOT VALID.
func (c *Client) ValidateForeignKey(schema, table, constraintName string) error {
	qualifiedTable := pgx.Identifier{schema, table}.Sanitize()
	sanitizedConstraint := pgx.Identifier{constraintName}.Sanitize()

	query := fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s", qualifiedTable, sanitizedConstraint)

	c.logger.Info("Validating foreign key", "schema", schema, "table", table, "constraint", constraintName, "sql", query)

	_, err := c.conn.Exec(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to validate foreign key %s on %s.%s: %w", constraintName, schema, table, err)
	}

	return nil
}

// DropForeignKey drops a foreign key constraint from the specified table.
// Used during cutover to drop referencing FKs before the rename swap
// (PostgreSQL FKs reference table OIDs, so they would still point to the old
// physical table after RENAME without this step).
func (c *Client) DropForeignKey(schema, table, constraintName string) error {
	qualifiedTable := pgx.Identifier{schema, table}.Sanitize()
	sanitizedConstraint := pgx.Identifier{constraintName}.Sanitize()

	query := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", qualifiedTable, sanitizedConstraint)

	c.logger.Info("Dropping foreign key", "schema", schema, "table", table, "constraint", constraintName, "sql", query)

	_, err := c.conn.Exec(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop foreign key %s on %s.%s: %w", constraintName, schema, table, err)
	}

	return nil
}

// AnalyzeTable runs ANALYZE on the specified table to refresh statistics
// for the query planner. Partitioned tables need fresh statistics after cutover
// to ensure optimal query plans.
func (c *Client) AnalyzeTable(schema, table string) error {
	qualifiedTable := pgx.Identifier{schema, table}.Sanitize()

	query := fmt.Sprintf("ANALYZE %s", qualifiedTable)

	c.logger.Info("Analyzing table", "schema", schema, "table", table, "sql", query)

	_, err := c.conn.Exec(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to analyze table %s.%s: %w", schema, table, err)
	}

	return nil
}
