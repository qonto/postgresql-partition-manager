package postgresql

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// GetTableColumns retrieves the column definitions for the specified table.
// It queries information_schema.columns to get column names, data types,
// nullability, default values, and whether the column is generated.
func (c *Client) GetTableColumns(schema, table string) ([]ColumnDef, error) {
	query := `
		SELECT column_name, data_type, is_nullable, column_default, is_generated
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`

	rows, err := c.conn.Query(c.ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query table columns: %w", err)
	}
	defer rows.Close()

	var columns []ColumnDef

	for rows.Next() {
		var col ColumnDef

		var isNullable string

		var isGenerated string

		err := rows.Scan(&col.Name, &col.DataType, &isNullable, &col.DefaultValue, &isGenerated)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column row: %w", err)
		}

		col.IsNullable = isNullable == "YES"
		col.IsGenerated = isGenerated == "ALWAYS"

		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating column rows: %w", err)
	}

	return columns, nil
}

// GetTablePrimaryKey retrieves the primary key column names for the specified table.
func (c *Client) GetTablePrimaryKey(schema, table string) ([]string, error) {
	query := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2 AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)`

	rows, err := c.conn.Query(c.ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary key: %w", err)
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

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating PK rows: %w", err)
	}

	return columns, nil
}

// GetTableIndexes retrieves all index definitions for the specified table.
func (c *Client) GetTableIndexes(schema, table string) ([]IndexDef, error) {
	query := `
		SELECT
			i.relname AS index_name,
			ix.indisunique,
			ix.indisprimary,
			am.amname AS method,
			pg_get_expr(ix.indpred, ix.indrelid) AS predicate,
			pg_get_expr(ix.indexprs, ix.indrelid) AS expression,
			array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) AS columns
		FROM pg_index ix
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN pg_am am ON am.oid = i.relam
		LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) AND a.attnum > 0
		WHERE n.nspname = $1 AND t.relname = $2
		GROUP BY i.relname, ix.indisunique, ix.indisprimary, am.amname, ix.indpred, ix.indexprs, ix.indrelid
		ORDER BY i.relname`

	rows, err := c.conn.Query(c.ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query indexes: %w", err)
	}
	defer rows.Close()

	var indexes []IndexDef

	for rows.Next() {
		var idx IndexDef

		err := rows.Scan(&idx.Name, &idx.IsUnique, &idx.IsPrimary, &idx.Method, &idx.Predicate, &idx.Expression, &idx.Columns)
		if err != nil {
			return nil, fmt.Errorf("failed to scan index row: %w", err)
		}

		indexes = append(indexes, idx)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating index rows: %w", err)
	}

	return indexes, nil
}

// GetTableForeignKeys retrieves foreign key constraints originating from the specified table.
func (c *Client) GetTableForeignKeys(schema, table string) ([]ForeignKeyDef, error) {
	query := `
		SELECT
			con.conname AS constraint_name,
			array_agg(a.attname ORDER BY array_position(con.conkey, a.attnum)) AS columns,
			nf.nspname AS referenced_schema,
			cf.relname AS referenced_table,
			array_agg(af.attname ORDER BY array_position(con.confkey, af.attnum)) AS referenced_columns,
			CASE con.confdeltype
				WHEN 'a' THEN 'NO ACTION'
				WHEN 'r' THEN 'RESTRICT'
				WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET NULL'
				WHEN 'd' THEN 'SET DEFAULT'
			END AS on_delete,
			CASE con.confupdtype
				WHEN 'a' THEN 'NO ACTION'
				WHEN 'r' THEN 'RESTRICT'
				WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET NULL'
				WHEN 'd' THEN 'SET DEFAULT'
			END AS on_update
		FROM pg_constraint con
		JOIN pg_class c ON c.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_class cf ON cf.oid = con.confrelid
		JOIN pg_namespace nf ON nf.oid = cf.relnamespace
		JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
		JOIN pg_attribute af ON af.attrelid = con.confrelid AND af.attnum = ANY(con.confkey)
		WHERE n.nspname = $1 AND c.relname = $2 AND con.contype = 'f'
		GROUP BY con.conname, nf.nspname, cf.relname, con.confdeltype, con.confupdtype
		ORDER BY con.conname`

	rows, err := c.conn.Query(c.ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query foreign keys: %w", err)
	}
	defer rows.Close()

	var fks []ForeignKeyDef

	for rows.Next() {
		var fk ForeignKeyDef

		err := rows.Scan(&fk.Name, &fk.Columns, &fk.ReferencedSchema, &fk.ReferencedTable, &fk.ReferencedColumns, &fk.OnDelete, &fk.OnUpdate)
		if err != nil {
			return nil, fmt.Errorf("failed to scan FK row: %w", err)
		}

		fks = append(fks, fk)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating FK rows: %w", err)
	}

	return fks, nil
}

// GetReferencingForeignKeys retrieves foreign key constraints from other tables that reference the specified table.
func (c *Client) GetReferencingForeignKeys(schema, table string) ([]ForeignKeyDef, error) {
	query := `
		SELECT
			con.conname AS constraint_name,
			array_agg(a.attname ORDER BY array_position(con.conkey, a.attnum)) AS columns,
			n.nspname AS referencing_schema,
			c.relname AS referencing_table,
			array_agg(af.attname ORDER BY array_position(con.confkey, af.attnum)) AS referenced_columns,
			CASE con.confdeltype
				WHEN 'a' THEN 'NO ACTION'
				WHEN 'r' THEN 'RESTRICT'
				WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET NULL'
				WHEN 'd' THEN 'SET DEFAULT'
			END AS on_delete,
			CASE con.confupdtype
				WHEN 'a' THEN 'NO ACTION'
				WHEN 'r' THEN 'RESTRICT'
				WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET NULL'
				WHEN 'd' THEN 'SET DEFAULT'
			END AS on_update
		FROM pg_constraint con
		JOIN pg_class c ON c.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_class cf ON cf.oid = con.confrelid
		JOIN pg_namespace nf ON nf.oid = cf.relnamespace
		JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
		JOIN pg_attribute af ON af.attrelid = con.confrelid AND af.attnum = ANY(con.confkey)
		WHERE nf.nspname = $1 AND cf.relname = $2 AND con.contype = 'f'
		GROUP BY con.conname, n.nspname, c.relname, con.confdeltype, con.confupdtype
		ORDER BY con.conname`

	rows, err := c.conn.Query(c.ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query referencing foreign keys: %w", err)
	}
	defer rows.Close()

	var fks []ForeignKeyDef

	for rows.Next() {
		var fk ForeignKeyDef

		err := rows.Scan(&fk.Name, &fk.Columns, &fk.ReferencedSchema, &fk.ReferencedTable, &fk.ReferencedColumns, &fk.OnDelete, &fk.OnUpdate)
		if err != nil {
			return nil, fmt.Errorf("failed to scan referencing FK row: %w", err)
		}

		fks = append(fks, fk)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating referencing FK rows: %w", err)
	}

	return fks, nil
}

// GetTableCheckConstraints retrieves CHECK constraints for the specified table.
func (c *Client) GetTableCheckConstraints(schema, table string) ([]CheckConstraintDef, error) {
	query := `
		SELECT con.conname, pg_get_constraintdef(con.oid)
		FROM pg_constraint con
		JOIN pg_class c ON c.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2 AND con.contype = 'c'
		ORDER BY con.conname`

	rows, err := c.conn.Query(c.ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query check constraints: %w", err)
	}
	defer rows.Close()

	var constraints []CheckConstraintDef

	for rows.Next() {
		var cc CheckConstraintDef
		if err := rows.Scan(&cc.Name, &cc.Expression); err != nil {
			return nil, fmt.Errorf("failed to scan check constraint row: %w", err)
		}

		constraints = append(constraints, cc)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating check constraint rows: %w", err)
	}

	return constraints, nil
}

// GetPartitionKeyRange retrieves the minimum and maximum values of the partition key column.
// This is used to determine the range of partitions to create on the target table.
func (c *Client) GetPartitionKeyRange(schema, table, partitionKey string) (min, max time.Time, err error) {
	qualifiedTable := pgx.Identifier{schema, table}.Sanitize()
	sanitizedKey := pgx.Identifier{partitionKey}.Sanitize()

	query := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", sanitizedKey, sanitizedKey, qualifiedTable)

	var minVal, maxVal *time.Time

	err = c.conn.QueryRow(c.ctx, query).Scan(&minVal, &maxVal)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("failed to get partition key range: %w", err)
	}

	if minVal == nil || maxVal == nil {
		return time.Time{}, time.Time{}, fmt.Errorf("partition key column %s has no data", partitionKey)
	}

	return *minVal, *maxVal, nil
}

// GetTableRowCount returns the estimated row count for the specified table.
// Uses pg_class.reltuples for a fast estimate without a full table scan.
func (c *Client) GetTableRowCount(schema, table string) (int64, error) {
	query := `
		SELECT COALESCE(c.reltuples, 0)::bigint
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2`

	var count int64

	err := c.conn.QueryRow(c.ctx, query, schema, table).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get table row count: %w", err)
	}

	return count, nil
}

// IsTableExists checks whether the specified table exists in the given schema.
func (c *Client) IsTableExists(schema, table string) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = $2
		)`

	var exists bool

	err := c.conn.QueryRow(c.ctx, query, schema, table).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}

	return exists, nil
}

// HasPrimaryKey checks whether the specified table has a primary key defined.
func (c *Client) HasPrimaryKey(schema, table string) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT 1 FROM pg_index i
			JOIN pg_class c ON c.oid = i.indrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2 AND i.indisprimary
		)`

	var hasPK bool

	err := c.conn.QueryRow(c.ctx, query, schema, table).Scan(&hasPK)
	if err != nil {
		return false, fmt.Errorf("failed to check primary key existence: %w", err)
	}

	return hasPK, nil
}
