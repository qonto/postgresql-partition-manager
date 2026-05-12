package postgresql

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// CreateCDCQueue creates the CDC queue table for the specified source table.
// The queue table is named <source_table>_cdc_queue and resides in the same schema.
// It also creates an index on seq_id for ordered dequeue operations.
// The pkColumns parameter is accepted for interface compatibility but is not used
// in the queue table creation (the queue schema is fixed regardless of PK structure).
// If the table already exists, this method should not be called (check with IsCDCQueueExists first).
func (c *Client) CreateCDCQueue(schema, table string, pkColumns []string) error {
	queueTable := table + "_cdc_queue"
	qualifiedQueue := pgx.Identifier{schema, queueTable}.Sanitize()
	indexName := fmt.Sprintf("idx_%s_cdc_queue_seq", table)
	qualifiedIndex := pgx.Identifier{schema, indexName}.Sanitize()

	createSQL := fmt.Sprintf(`CREATE TABLE %s (
    seq_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operation TEXT NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    pk_values TEXT[] NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`, qualifiedQueue)

	c.logger.Info("Creating CDC queue table", "schema", schema, "table", queueTable, "sql", createSQL)

	_, err := c.conn.Exec(c.ctx, createSQL)
	if err != nil {
		return fmt.Errorf("failed to create CDC queue table %s.%s: %w", schema, queueTable, err)
	}

	// Create index for ordered dequeue
	indexSQL := fmt.Sprintf("CREATE INDEX %s ON %s (seq_id)", qualifiedIndex, qualifiedQueue)

	c.logger.Info("Creating CDC queue index", "schema", schema, "index", indexName, "sql", indexSQL)

	_, err = c.conn.Exec(c.ctx, indexSQL)
	if err != nil {
		return fmt.Errorf("failed to create CDC queue index %s: %w", indexName, err)
	}

	return nil
}

// CreateCDCTriggerFunction creates the trigger function that captures PK columns
// for INSERT, UPDATE, and DELETE operations and enqueues them to the CDC queue.
func (c *Client) CreateCDCTriggerFunction(schema, table string, pkColumns []string) error {
	functionName := fmt.Sprintf("ppm_cdc_trigger_%s", table)
	qualifiedFunction := pgx.Identifier{schema, functionName}.Sanitize()
	qualifiedQueue := pgx.Identifier{schema, table + "_cdc_queue"}.Sanitize()

	// Build the array expression for OLD record (DELETE path)
	oldArrayParts := make([]string, len(pkColumns))
	for i, col := range pkColumns {
		oldArrayParts[i] = fmt.Sprintf("OLD.%s::TEXT", pgx.Identifier{col}.Sanitize())
	}
	oldArrayExpr := "ARRAY[" + strings.Join(oldArrayParts, ", ") + "]"

	// Build the array expression for NEW record (INSERT/UPDATE path)
	newArrayParts := make([]string, len(pkColumns))
	for i, col := range pkColumns {
		newArrayParts[i] = fmt.Sprintf("NEW.%s::TEXT", pgx.Identifier{col}.Sanitize())
	}
	newArrayExpr := "ARRAY[" + strings.Join(newArrayParts, ", ") + "]"

	createSQL := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO %s (operation, pk_values)
        VALUES ('DELETE', %s);
        RETURN OLD;
    ELSE
        INSERT INTO %s (operation, pk_values)
        VALUES (TG_OP, %s);
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql`, qualifiedFunction, qualifiedQueue, oldArrayExpr, qualifiedQueue, newArrayExpr)

	c.logger.Info("Creating CDC trigger function", "schema", schema, "function", functionName, "sql", createSQL)

	_, err := c.conn.Exec(c.ctx, createSQL)
	if err != nil {
		return fmt.Errorf("failed to create CDC trigger function %s.%s: %w", schema, functionName, err)
	}

	return nil
}

// InstallCDCTrigger installs the AFTER INSERT OR UPDATE OR DELETE trigger on the source table.
// The trigger executes the CDC trigger function for each row.
func (c *Client) InstallCDCTrigger(schema, table string) error {
	triggerName := fmt.Sprintf("ppm_cdc_%s", table)
	qualifiedTable := pgx.Identifier{schema, table}.Sanitize()
	qualifiedFunction := pgx.Identifier{schema, fmt.Sprintf("ppm_cdc_trigger_%s", table)}.Sanitize()

	createSQL := fmt.Sprintf(`CREATE TRIGGER %s
    AFTER INSERT OR UPDATE OR DELETE ON %s
    FOR EACH ROW EXECUTE FUNCTION %s()`,
		pgx.Identifier{triggerName}.Sanitize(),
		qualifiedTable,
		qualifiedFunction)

	c.logger.Info("Installing CDC trigger", "schema", schema, "table", table, "trigger", triggerName, "sql", createSQL)

	_, err := c.conn.Exec(c.ctx, createSQL)
	if err != nil {
		return fmt.Errorf("failed to install CDC trigger %s on %s.%s: %w", triggerName, schema, table, err)
	}

	return nil
}

// IsCDCQueueExists checks whether the CDC queue table exists for the specified source table.
func (c *Client) IsCDCQueueExists(schema, table string) (bool, error) {
	queueTable := table + "_cdc_queue"

	query := `SELECT EXISTS(
		SELECT 1
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2
	)`

	var exists bool

	err := c.conn.QueryRow(c.ctx, query, schema, queueTable).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check if CDC queue exists for %s.%s: %w", schema, table, err)
	}

	return exists, nil
}

// IsCDCTriggerExists checks whether the CDC trigger exists on the specified source table.
func (c *Client) IsCDCTriggerExists(schema, table string) (bool, error) {
	triggerName := fmt.Sprintf("ppm_cdc_%s", table)

	query := `SELECT EXISTS(
		SELECT 1
		FROM pg_catalog.pg_trigger t
		JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2 AND t.tgname = $3
	)`

	var exists bool

	err := c.conn.QueryRow(c.ctx, query, schema, table, triggerName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check if CDC trigger exists for %s.%s: %w", schema, table, err)
	}

	return exists, nil
}
