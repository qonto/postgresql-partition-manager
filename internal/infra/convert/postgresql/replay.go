package postgresql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ErrNoColumnsFound is returned when no columns are found for a table during replay operations.
var ErrNoColumnsFound = errors.New("no columns found for table")

// DequeueEvents atomically dequeues a batch of CDC events from the queue table
// using DELETE...RETURNING with FOR UPDATE SKIP LOCKED for exactly-once processing.
// Events are dequeued in sequence ID order to maintain causal ordering.
func (c *Client) DequeueEvents(schema, table string, batchSize int) ([]CDCEvent, error) {
	queueTable := table + "_cdc_queue"
	qualifiedQueue := pgx.Identifier{schema, queueTable}.Sanitize()

	// Atomic dequeue: DELETE...RETURNING ensures exactly-once processing.
	// The inner SELECT with FOR UPDATE SKIP LOCKED allows concurrent consumers
	// without blocking, while DELETE...RETURNING atomically removes and returns rows.
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE seq_id IN (
			SELECT seq_id FROM %s
			ORDER BY seq_id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING seq_id, operation, pk_values, created_at`,
		qualifiedQueue,
		qualifiedQueue,
	)

	c.logger.Info("Dequeuing CDC events",
		"schema", schema,
		"table", table,
		"batchSize", batchSize,
	)

	rows, err := c.conn.Query(c.ctx, query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to dequeue events from %s: %w", qualifiedQueue, err)
	}
	defer rows.Close()

	var events []CDCEvent

	for rows.Next() {
		var event CDCEvent

		err := rows.Scan(&event.SeqID, &event.Operation, &event.PKValues, &event.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan CDC event: %w", err)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading CDC events: %w", err)
	}

	c.logger.Info("Dequeued CDC events",
		"schema", schema,
		"table", table,
		"count", len(events),
	)

	return events, nil
}

// ApplyUpsert applies an INSERT or UPDATE event by fetching the current row state
// from the source table and upserting it into the target table.
// This implements the INSERT...ON CONFLICT DO UPDATE pattern described in the design.
// By fetching the current state from the source, out-of-order events are handled correctly
// since the target always reflects the latest source state.
//
// pkColumns are the source table's PK columns (used for the WHERE lookup on the source).
// The ON CONFLICT clause uses the target table's actual PK (which may include the partition key).
func (c *Client) ApplyUpsert(schema, targetTable, sourceTable string, pkColumns []string, pkValues []string) error {
	qualifiedTarget := pgx.Identifier{schema, targetTable}.Sanitize()
	qualifiedSource := pgx.Identifier{schema, sourceTable}.Sanitize()

	// Build sanitized PK column identifiers (source PK for WHERE lookup)
	pkCols := make([]string, len(pkColumns))
	for i, col := range pkColumns {
		pkCols[i] = pgx.Identifier{col}.Sanitize()
	}

	// Build WHERE clause for PK lookup: (col1, col2) = ($1, $2)
	placeholders := make([]string, len(pkValues))
	args := make([]any, len(pkValues))

	for i, val := range pkValues {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = val
	}

	whereClause := fmt.Sprintf("(%s) = (%s)",
		strings.Join(pkCols, ", "),
		strings.Join(placeholders, ", "),
	)

	// Get all column names from the source table to build the upsert.
	// We query information_schema to get the column list dynamically.
	columns, err := c.getTableColumnNames(schema, sourceTable)
	if err != nil {
		return fmt.Errorf("failed to get columns for upsert: %w", err)
	}

	// Build sanitized column list
	sanitizedCols := make([]string, len(columns))
	for i, col := range columns {
		sanitizedCols[i] = pgx.Identifier{col}.Sanitize()
	}

	colList := strings.Join(sanitizedCols, ", ")

	// Get the target table's PK columns for ON CONFLICT.
	// The target table's PK may include the partition key (e.g., (id, created_at))
	// which differs from the source PK (e.g., (id)).
	targetPKColumns, err := c.getTargetPKColumns(schema, targetTable)
	if err != nil || len(targetPKColumns) == 0 {
		// Fallback to source PK columns if we can't determine target PK
		targetPKColumns = pkColumns
	}

	targetPKCols := make([]string, len(targetPKColumns))
	for i, col := range targetPKColumns {
		targetPKCols[i] = pgx.Identifier{col}.Sanitize()
	}

	// Build the DO UPDATE SET clause for non-PK columns (using target PK for exclusion)
	targetPKSet := make(map[string]bool, len(targetPKColumns))
	for _, col := range targetPKColumns {
		targetPKSet[col] = true
	}

	var updateParts []string

	for _, col := range columns {
		if !targetPKSet[col] {
			sanitized := pgx.Identifier{col}.Sanitize()
			updateParts = append(updateParts, fmt.Sprintf("%s = EXCLUDED.%s", sanitized, sanitized))
		}
	}

	// Build the full upsert query
	var query string

	if len(updateParts) > 0 {
		query = fmt.Sprintf(`
			INSERT INTO %s (%s)
			SELECT %s FROM %s
			WHERE %s
			ON CONFLICT (%s) DO UPDATE SET
				%s`,
			qualifiedTarget,
			colList,
			colList,
			qualifiedSource,
			whereClause,
			strings.Join(targetPKCols, ", "),
			strings.Join(updateParts, ",\n\t\t\t\t"),
		)
	} else {
		// Edge case: all columns are PK columns (no non-PK columns to update)
		query = fmt.Sprintf(`
			INSERT INTO %s (%s)
			SELECT %s FROM %s
			WHERE %s
			ON CONFLICT (%s) DO NOTHING`,
			qualifiedTarget,
			colList,
			colList,
			qualifiedSource,
			whereClause,
			strings.Join(targetPKCols, ", "),
		)
	}

	c.logger.Debug("Applying upsert",
		"schema", schema,
		"target", targetTable,
		"source", sourceTable,
		"pkValues", pkValues,
	)

	_, err = c.conn.Exec(c.ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to apply upsert to %s for pk %v: %w", qualifiedTarget, pkValues, err)
	}

	return nil
}

// getTargetPKColumns retrieves the primary key columns of the target table.
func (c *Client) getTargetPKColumns(schema, table string) ([]string, error) {
	query := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE i.indisprimary
		  AND n.nspname = $1
		  AND c.relname = $2
		ORDER BY array_position(i.indkey, a.attnum)`

	rows, err := c.conn.Query(c.ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get PK columns for %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	var columns []string

	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("failed to scan PK column: %w", err)
		}

		columns = append(columns, col)
	}

	return columns, rows.Err()
}

// ApplyDelete removes a row from the target table by primary key values.
// This handles DELETE events from the CDC queue.
func (c *Client) ApplyDelete(schema, targetTable string, pkColumns []string, pkValues []string) error {
	qualifiedTarget := pgx.Identifier{schema, targetTable}.Sanitize()

	// Build sanitized PK column identifiers
	pkCols := make([]string, len(pkColumns))
	for i, col := range pkColumns {
		pkCols[i] = pgx.Identifier{col}.Sanitize()
	}

	// Build WHERE clause: (col1, col2) = ($1, $2)
	placeholders := make([]string, len(pkValues))
	args := make([]any, len(pkValues))

	for i, val := range pkValues {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = val
	}

	whereClause := fmt.Sprintf("(%s) = (%s)",
		strings.Join(pkCols, ", "),
		strings.Join(placeholders, ", "),
	)

	query := fmt.Sprintf("DELETE FROM %s WHERE %s", qualifiedTarget, whereClause)

	c.logger.Debug("Applying delete",
		"schema", schema,
		"target", targetTable,
		"pkValues", pkValues,
	)

	_, err := c.conn.Exec(c.ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to apply delete to %s for pk %v: %w", qualifiedTarget, pkValues, err)
	}

	return nil
}

// GetReplayLag returns the number of unprocessed events remaining in the CDC queue.
// This is used to monitor convergence progress during the replay phase.
func (c *Client) GetReplayLag(schema, table string) (int64, error) {
	queueTable := table + "_cdc_queue"
	qualifiedQueue := pgx.Identifier{schema, queueTable}.Sanitize()

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", qualifiedQueue)

	var count int64

	err := c.conn.QueryRow(c.ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get replay lag for %s.%s: %w", schema, queueTable, err)
	}

	return count, nil
}

// IsCDCQueueEmpty checks whether the CDC queue has no remaining events.
// This is used as a safety assertion before cutover to ensure all events have been replayed.
func (c *Client) IsCDCQueueEmpty(schema, table string) (bool, error) {
	queueTable := table + "_cdc_queue"
	qualifiedQueue := pgx.Identifier{schema, queueTable}.Sanitize()

	query := fmt.Sprintf("SELECT NOT EXISTS(SELECT 1 FROM %s)", qualifiedQueue)

	var empty bool

	err := c.conn.QueryRow(c.ctx, query).Scan(&empty)
	if err != nil {
		return false, fmt.Errorf("failed to check if CDC queue is empty for %s.%s: %w", schema, queueTable, err)
	}

	return empty, nil
}

// getTableColumnNames retrieves the ordered list of column names for a table.
// This is used by ApplyUpsert to dynamically build the INSERT column list.
func (c *Client) getTableColumnNames(schema, table string) ([]string, error) {
	query := `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`

	rows, err := c.conn.Query(c.ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns for %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	var columns []string

	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("failed to scan column name: %w", err)
		}

		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading columns: %w", err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("%w: %s.%s", ErrNoColumnsFound, schema, table)
	}

	return columns, nil
}
