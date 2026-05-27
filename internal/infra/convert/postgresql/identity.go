package postgresql

import (
	"fmt"

	"github.com/jackc/pgx/v5"
)

// IdentityColumnInfo holds the identity generation metadata for a single column.
type IdentityColumnInfo struct {
	ColumnName         string
	IdentityGeneration string // 'ALWAYS' or 'BY DEFAULT'
}

// GetIdentityColumns retrieves columns with an identity attribute (ALWAYS or BY DEFAULT) for the specified table.
// This is used to capture the original identity generation strategy before the rename swap,
// so it can be restored during post-cutover.
func (c *Client) GetIdentityColumns(schema, table string) ([]IdentityColumnInfo, error) {
	query := `
		SELECT column_name, identity_generation
		FROM information_schema.columns
		WHERE table_schema = $1
			AND table_name = $2
			AND identity_generation IS NOT NULL
			AND identity_generation != ''
		ORDER BY ordinal_position`

	rows, err := c.conn.Query(c.ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query identity columns: %w", err)
	}
	defer rows.Close()

	var columns []IdentityColumnInfo

	for rows.Next() {
		var col IdentityColumnInfo
		if err := rows.Scan(&col.ColumnName, &col.IdentityGeneration); err != nil {
			return nil, fmt.Errorf("failed to scan identity column row: %w", err)
		}

		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating identity column rows: %w", err)
	}

	return columns, nil
}

// RestoreIdentityGeneration restores the identity generation strategy for the specified column.
// It first drops any existing identity attribute, then adds the appropriate strategy (ALWAYS or BY DEFAULT).
// This is used during post-cutover to restore the original identity generation after the rename swap.
func (c *Client) RestoreIdentityGeneration(schema, table, column string) error {
	return c.RestoreIdentityGenerationWithStrategy(schema, table, column, "ALWAYS")
}

// RestoreIdentityGenerationWithStrategy restores the identity generation strategy for the specified column
// using the given strategy ('ALWAYS' or 'BY DEFAULT').
// It first drops any existing identity attribute, then adds the specified strategy.
func (c *Client) RestoreIdentityGenerationWithStrategy(schema, table, column, strategy string) error {
	qualifiedTable := pgx.Identifier{schema, table}.Sanitize()
	sanitizedColumn := pgx.Identifier{column}.Sanitize()

	dropSQL := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP IDENTITY IF EXISTS", qualifiedTable, sanitizedColumn)

	_, err := c.conn.Exec(c.ctx, dropSQL)
	if err != nil {
		return fmt.Errorf("failed to drop identity for column %s: %w", column, err)
	}

	addSQL := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s ADD GENERATED %s AS IDENTITY", qualifiedTable, sanitizedColumn, strategy)

	_, err = c.conn.Exec(c.ctx, addSQL)
	if err != nil {
		return fmt.Errorf("failed to add GENERATED %s AS IDENTITY for column %s: %w", strategy, column, err)
	}

	return nil
}
