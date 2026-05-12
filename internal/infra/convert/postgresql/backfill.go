package postgresql

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// BackfillBatch copies a batch of rows from the source table to the target table,
// ordered by primary key, starting after the given afterPK values.
// It uses INSERT...SELECT with ON CONFLICT DO NOTHING to skip rows that have been
// concurrently modified (those will be captured by the CDC trigger for replay).
//
// Parameters:
//   - schema: the database schema containing both tables
//   - sourceTable: the original non-partitioned table
//   - targetTable: the new partitioned table
//   - pkColumns: the primary key column names (supports composite PKs)
//   - afterPK: the last processed PK values (nil for the first batch)
//   - batchSize: the number of rows to process in this batch
//
// Returns:
//   - lastPK: the PK values of the last row in the batch (nil if no rows selected)
//   - rowsCopied: the number of rows actually inserted (may be less than batch due to conflicts)
//   - err: any error encountered during execution
func (c *Client) BackfillBatch(schema, sourceTable, targetTable string, pkColumns []string, afterPK []any, batchSize int) (lastPK []any, rowsCopied int64, err error) {
	qualifiedSource := pgx.Identifier{schema, sourceTable}.Sanitize()
	qualifiedTarget := pgx.Identifier{schema, targetTable}.Sanitize()

	// Build the ORDER BY clause using sanitized PK column identifiers
	orderCols := make([]string, len(pkColumns))
	for i, col := range pkColumns {
		orderCols[i] = pgx.Identifier{col}.Sanitize()
	}
	orderByClause := strings.Join(orderCols, ", ")

	// Build the WHERE clause for row-value comparison (composite PK support)
	var whereClause string
	var queryArgs []any
	argIdx := 1

	if len(afterPK) > 0 && len(afterPK) == len(pkColumns) {
		// Use row-value comparison: (col1, col2, ...) > ($1, $2, ...)
		pkColList := strings.Join(orderCols, ", ")

		placeholders := make([]string, len(afterPK))
		for i, val := range afterPK {
			placeholders[i] = fmt.Sprintf("$%d", argIdx)
			queryArgs = append(queryArgs, val)
			argIdx++
		}

		whereClause = fmt.Sprintf(" WHERE (%s) > (%s)", pkColList, strings.Join(placeholders, ", "))
	}

	// Build and execute the INSERT...SELECT...ON CONFLICT DO NOTHING query
	// We use a CTE to select the batch, then insert from it.
	// This allows us to also retrieve the last PK from the selected batch.
	batchPlaceholder := fmt.Sprintf("$%d", argIdx)
	queryArgs = append(queryArgs, batchSize)

	selectSQL := fmt.Sprintf(
		"SELECT * FROM %s%s ORDER BY %s LIMIT %s",
		qualifiedSource,
		whereClause,
		orderByClause,
		batchPlaceholder,
	)

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s SELECT * FROM batch_rows ON CONFLICT DO NOTHING",
		qualifiedTarget,
	)

	fullSQL := fmt.Sprintf(
		"WITH batch_rows AS (%s) %s",
		selectSQL,
		insertSQL,
	)

	c.logger.Info("Executing backfill batch",
		"schema", schema,
		"source", sourceTable,
		"target", targetTable,
		"batchSize", batchSize,
	)

	// Execute the INSERT and get rows affected
	tag, err := c.conn.Exec(c.ctx, fullSQL, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to execute backfill batch from %s to %s: %w", qualifiedSource, qualifiedTarget, err)
	}

	rowsCopied = tag.RowsAffected()

	// Now query the last PK from the source table for the same batch range.
	// We need this to know where to resume from, regardless of how many rows
	// were actually inserted (some may have been skipped due to conflicts).
	lastPK, err = c.getLastPKInBatch(schema, sourceTable, pkColumns, afterPK, batchSize)
	if err != nil {
		return nil, rowsCopied, fmt.Errorf("failed to get last PK in batch: %w", err)
	}

	return lastPK, rowsCopied, nil
}

// getLastPKInBatch retrieves the PK values of the last row that would be selected
// in the batch. This is needed to track progress for resumability.
func (c *Client) getLastPKInBatch(schema, sourceTable string, pkColumns []string, afterPK []any, batchSize int) ([]any, error) {
	qualifiedSource := pgx.Identifier{schema, sourceTable}.Sanitize()

	// Build ORDER BY clause
	orderCols := make([]string, len(pkColumns))
	for i, col := range pkColumns {
		orderCols[i] = pgx.Identifier{col}.Sanitize()
	}
	orderByClause := strings.Join(orderCols, ", ")

	// Build SELECT list for PK columns only
	selectCols := strings.Join(orderCols, ", ")

	// Build WHERE clause
	var whereClause string
	var queryArgs []any
	argIdx := 1

	if len(afterPK) > 0 && len(afterPK) == len(pkColumns) {
		pkColList := strings.Join(orderCols, ", ")

		placeholders := make([]string, len(afterPK))
		for i, val := range afterPK {
			placeholders[i] = fmt.Sprintf("$%d", argIdx)
			queryArgs = append(queryArgs, val)
			argIdx++
		}

		whereClause = fmt.Sprintf(" WHERE (%s) > (%s)", pkColList, strings.Join(placeholders, ", "))
	}

	batchPlaceholder := fmt.Sprintf("$%d", argIdx)
	queryArgs = append(queryArgs, batchSize)

	// Use a subquery with LIMIT to get the last row in the batch
	query := fmt.Sprintf(
		"SELECT %s FROM (SELECT %s FROM %s%s ORDER BY %s LIMIT %s) AS batch ORDER BY %s DESC LIMIT 1",
		selectCols,
		selectCols,
		qualifiedSource,
		whereClause,
		orderByClause,
		batchPlaceholder,
		orderByClause,
	)

	rows, err := c.conn.Query(c.ctx, query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to query last PK in batch from %s: %w", qualifiedSource, err)
	}
	defer rows.Close()

	if !rows.Next() {
		// No rows in the batch — backfill is complete
		return nil, nil
	}

	// Scan the PK values
	values, err := rows.Values()
	if err != nil {
		return nil, fmt.Errorf("failed to scan last PK values: %w", err)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading last PK row: %w", err)
	}

	return values, nil
}
