package postgresql

import (
	"fmt"

	"github.com/jackc/pgx/v5"
)

// DropTable drops the specified table using DROP TABLE IF EXISTS.
// The IF EXISTS clause ensures the operation is idempotent — if the table
// has already been removed, the operation succeeds without error.
// Used during cleanup to remove the Source_Table_old after a successful migration.
func (c *Client) DropTable(schema, table string) error {
	qualifiedTable := pgx.Identifier{schema, table}.Sanitize()

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", qualifiedTable)

	c.logger.Info("Dropping table", "schema", schema, "table", table, "sql", query)

	_, err := c.conn.Exec(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop table %s.%s: %w", schema, table, err)
	}

	return nil
}

// ReassignSequences reassigns ownership of all sequences owned by oldTable to newTable.
// This is necessary before dropping the old table after a cutover, because BIGSERIAL columns
// create sequences owned by the original table. After RENAME, the sequence ownership still
// points to the old table (now _old). Without reassignment, DROP TABLE on the old table
// would cascade-drop the sequence, breaking the new table's DEFAULT.
func (c *Client) ReassignSequences(schema, oldTable, newTable string) error {
	// Find all sequences owned by the old table
	query := `
		SELECT s.relname AS seq_name, a.attname AS col_name
		FROM pg_class s
		JOIN pg_depend d ON d.objid = s.oid
		JOIN pg_class t ON t.oid = d.refobjid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
		WHERE s.relkind = 'S'
		  AND d.deptype = 'a'
		  AND n.nspname = $1
		  AND t.relname = $2`

	rows, err := c.conn.Query(c.ctx, query, schema, oldTable)
	if err != nil {
		return fmt.Errorf("failed to find sequences owned by %s.%s: %w", schema, oldTable, err)
	}
	defer rows.Close()

	type seqInfo struct {
		seqName string
		colName string
	}

	var sequences []seqInfo

	for rows.Next() {
		var si seqInfo
		if err := rows.Scan(&si.seqName, &si.colName); err != nil {
			return fmt.Errorf("failed to scan sequence info: %w", err)
		}

		sequences = append(sequences, si)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating sequences: %w", err)
	}

	// Reassign each sequence to the new table
	for _, si := range sequences {
		qualifiedSeq := pgx.Identifier{schema, si.seqName}.Sanitize()
		qualifiedNewTable := pgx.Identifier{schema, newTable}.Sanitize()
		qualifiedCol := pgx.Identifier{si.colName}.Sanitize()

		alterSQL := fmt.Sprintf("ALTER SEQUENCE %s OWNED BY %s.%s",
			qualifiedSeq, qualifiedNewTable, qualifiedCol)

		c.logger.Info("Reassigning sequence ownership",
			"schema", schema,
			"sequence", si.seqName,
			"from", oldTable,
			"to", newTable,
			"column", si.colName,
			"sql", alterSQL,
		)

		if _, err := c.conn.Exec(c.ctx, alterSQL); err != nil {
			return fmt.Errorf("failed to reassign sequence %s to %s.%s: %w",
				si.seqName, newTable, si.colName, err)
		}
	}

	return nil
}

// DropCDCQueue drops the CDC queue table for the specified source table.
// The CDC queue table follows the naming convention <source_table>_cdc_queue.
// Uses DROP TABLE IF EXISTS to handle the case where the queue has already been
// cleaned up (requirement 9.2).
func (c *Client) DropCDCQueue(schema, table string) error {
	queueTable := table + "_cdc_queue"
	qualifiedQueue := pgx.Identifier{schema, queueTable}.Sanitize()

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", qualifiedQueue)

	c.logger.Info("Dropping CDC queue", "schema", schema, "table", table, "queueTable", queueTable, "sql", query)

	_, err := c.conn.Exec(c.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop CDC queue %s.%s: %w", schema, queueTable, err)
	}

	return nil
}
