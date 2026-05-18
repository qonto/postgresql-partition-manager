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
