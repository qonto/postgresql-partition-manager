package postgresql

import (
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ColumnDef represents a column definition from a table schema.
type ColumnDef struct {
	Name         string
	DataType     string
	IsNullable   bool
	DefaultValue *string
	IsGenerated  bool
}

// IndexDef represents an index definition from a table.
type IndexDef struct {
	Name       string
	Columns    []string
	IsUnique   bool
	IsPrimary  bool
	Predicate  *string // For partial indexes
	Expression *string // For expression indexes
	Method     string  // btree, hash, gin, gist, etc.
}

// ForeignKeyDef represents a foreign key constraint definition.
type ForeignKeyDef struct {
	Name              string
	Columns           []string
	ReferencedSchema  string
	ReferencedTable   string
	ReferencedColumns []string
	OnDelete          string
	OnUpdate          string
}

// CheckConstraintDef represents a CHECK constraint definition.
type CheckConstraintDef struct {
	Name       string
	Expression string
}

func (p Postgres) CreateTableLikeTable(schema, table, parent string) error {
	query := fmt.Sprintf("CREATE TABLE %s (LIKE %s INCLUDING ALL)",
		pgx.Identifier{schema, table}.Sanitize(),
		pgx.Identifier{schema, parent}.Sanitize())
	p.logger.Debug("Create table", "schema", schema, "table", table, "query", query)

	_, err := p.conn.Exec(p.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func (p Postgres) DropTable(schema, table string) error {
	query := fmt.Sprintf("DROP TABLE %s.%s", schema, table)
	p.logger.Debug("Drop table", "schema", schema, "table", table, "query", query)

	_, err := p.conn.Exec(p.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	return nil
}

func (p Postgres) IsTableExists(schema, table string) (exists bool, err error) {
	query := `SELECT EXISTS(
		        SELECT c.oid
		        FROM pg_class c
		        JOIN pg_namespace n ON n.oid = c.relnamespace
		        WHERE n.nspname = $1 AND c.relname = $2
			);`

	err = p.conn.QueryRow(p.ctx, query, schema, table).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check if table exists: %w", err)
	}

	return exists, nil
}

// GetTableColumns returns the column definitions for the specified table.
// It queries information_schema.columns and pg_catalog for generated column info.
func (p Postgres) GetTableColumns(schema, table string) ([]ColumnDef, error) {
	query := `
		SELECT
			col.column_name,
			col.data_type,
			col.is_nullable = 'YES' AS is_nullable,
			col.column_default,
			COALESCE(col.is_generated = 'ALWAYS', false) OR COALESCE(col.generation_expression IS NOT NULL, false) AS is_generated
		FROM information_schema.columns col
		WHERE col.table_schema = $1
			AND col.table_name = $2
		ORDER BY col.ordinal_position`

	rows, err := p.conn.Query(p.ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get table columns: %w", err)
	}
	defer rows.Close()

	var columns []ColumnDef
	for rows.Next() {
		var col ColumnDef
		if err := rows.Scan(&col.Name, &col.DataType, &col.IsNullable, &col.DefaultValue, &col.IsGenerated); err != nil {
			return nil, fmt.Errorf("failed to scan column definition: %w", err)
		}
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate column rows: %w", err)
	}

	return columns, nil
}

// GetTablePrimaryKey returns the column names that form the primary key of the specified table.
func (p Postgres) GetTablePrimaryKey(schema, table string) ([]string, error) {
	query := `
		SELECT a.attname
		FROM pg_catalog.pg_index i
		JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		JOIN pg_catalog.pg_class cls ON cls.oid = i.indrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = cls.relnamespace
		WHERE n.nspname = $1
			AND cls.relname = $2
			AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)`

	rows, err := p.conn.Query(p.ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get table primary key: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("failed to scan primary key column: %w", err)
		}
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate primary key rows: %w", err)
	}

	return columns, nil
}

// GetTableIndexes returns all index definitions for the specified table.
func (p Postgres) GetTableIndexes(schema, table string) ([]IndexDef, error) {
	query := `
		SELECT
			ic.relname AS index_name,
			i.indisunique AS is_unique,
			i.indisprimary AS is_primary,
			am.amname AS method,
			pg_get_expr(i.indpred, i.indrelid) AS predicate,
			pg_get_expr(i.indexprs, i.indrelid) AS expression,
			ARRAY(
				SELECT a.attname
				FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
				JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
				WHERE k.attnum > 0
				ORDER BY k.ord
			) AS columns
		FROM pg_catalog.pg_index i
		JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
		JOIN pg_catalog.pg_class tc ON tc.oid = i.indrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = tc.relnamespace
		JOIN pg_catalog.pg_am am ON am.oid = ic.relam
		WHERE n.nspname = $1
			AND tc.relname = $2
		ORDER BY ic.relname`

	rows, err := p.conn.Query(p.ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get table indexes: %w", err)
	}
	defer rows.Close()

	var indexes []IndexDef
	for rows.Next() {
		var idx IndexDef
		if err := rows.Scan(
			&idx.Name,
			&idx.IsUnique,
			&idx.IsPrimary,
			&idx.Method,
			&idx.Predicate,
			&idx.Expression,
			&idx.Columns,
		); err != nil {
			return nil, fmt.Errorf("failed to scan index definition: %w", err)
		}
		indexes = append(indexes, idx)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate index rows: %w", err)
	}

	return indexes, nil
}

// GetTableForeignKeys returns foreign key constraints originating from the specified table
// (i.e., this table references other tables).
func (p Postgres) GetTableForeignKeys(schema, table string) ([]ForeignKeyDef, error) {
	query := `
		SELECT
			con.conname AS name,
			ARRAY(
				SELECT a.attname
				FROM unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)
				JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
				ORDER BY k.ord
			) AS columns,
			rn.nspname AS referenced_schema,
			rc.relname AS referenced_table,
			ARRAY(
				SELECT a.attname
				FROM unnest(con.confkey) WITH ORDINALITY AS k(attnum, ord)
				JOIN pg_catalog.pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.attnum
				ORDER BY k.ord
			) AS referenced_columns,
			CASE con.confdeltype
				WHEN 'a' THEN 'NO ACTION'
				WHEN 'r' THEN 'RESTRICT'
				WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET NULL'
				WHEN 'd' THEN 'SET DEFAULT'
				ELSE 'NO ACTION'
			END AS on_delete,
			CASE con.confupdtype
				WHEN 'a' THEN 'NO ACTION'
				WHEN 'r' THEN 'RESTRICT'
				WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET NULL'
				WHEN 'd' THEN 'SET DEFAULT'
				ELSE 'NO ACTION'
			END AS on_update
		FROM pg_catalog.pg_constraint con
		JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = cls.relnamespace
		JOIN pg_catalog.pg_class rc ON rc.oid = con.confrelid
		JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace
		WHERE n.nspname = $1
			AND cls.relname = $2
			AND con.contype = 'f'
		ORDER BY con.conname`

	rows, err := p.conn.Query(p.ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get table foreign keys: %w", err)
	}
	defer rows.Close()

	var fks []ForeignKeyDef
	for rows.Next() {
		var fk ForeignKeyDef
		if err := rows.Scan(
			&fk.Name,
			&fk.Columns,
			&fk.ReferencedSchema,
			&fk.ReferencedTable,
			&fk.ReferencedColumns,
			&fk.OnDelete,
			&fk.OnUpdate,
		); err != nil {
			return nil, fmt.Errorf("failed to scan foreign key definition: %w", err)
		}
		fks = append(fks, fk)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate foreign key rows: %w", err)
	}

	return fks, nil
}

// GetReferencingForeignKeys returns foreign key constraints from other tables that reference
// the specified table (i.e., child tables pointing to this table).
func (p Postgres) GetReferencingForeignKeys(schema, table string) ([]ForeignKeyDef, error) {
	query := `
		SELECT
			con.conname AS name,
			ARRAY(
				SELECT a.attname
				FROM unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)
				JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
				ORDER BY k.ord
			) AS columns,
			sn.nspname AS source_schema,
			sc.relname AS source_table,
			ARRAY(
				SELECT a.attname
				FROM unnest(con.confkey) WITH ORDINALITY AS k(attnum, ord)
				JOIN pg_catalog.pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.attnum
				ORDER BY k.ord
			) AS referenced_columns,
			CASE con.confdeltype
				WHEN 'a' THEN 'NO ACTION'
				WHEN 'r' THEN 'RESTRICT'
				WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET NULL'
				WHEN 'd' THEN 'SET DEFAULT'
				ELSE 'NO ACTION'
			END AS on_delete,
			CASE con.confupdtype
				WHEN 'a' THEN 'NO ACTION'
				WHEN 'r' THEN 'RESTRICT'
				WHEN 'c' THEN 'CASCADE'
				WHEN 'n' THEN 'SET NULL'
				WHEN 'd' THEN 'SET DEFAULT'
				ELSE 'NO ACTION'
			END AS on_update
		FROM pg_catalog.pg_constraint con
		JOIN pg_catalog.pg_class sc ON sc.oid = con.conrelid
		JOIN pg_catalog.pg_namespace sn ON sn.oid = sc.relnamespace
		JOIN pg_catalog.pg_class rc ON rc.oid = con.confrelid
		JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace
		WHERE rn.nspname = $1
			AND rc.relname = $2
			AND con.contype = 'f'
		ORDER BY con.conname`

	rows, err := p.conn.Query(p.ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get referencing foreign keys: %w", err)
	}
	defer rows.Close()

	var fks []ForeignKeyDef
	for rows.Next() {
		var fk ForeignKeyDef
		// For referencing FKs, the "source" is the child table that holds the FK,
		// and the "referenced" is our target table. We store the child table info
		// in ReferencedSchema/ReferencedTable to represent where the FK comes from.
		if err := rows.Scan(
			&fk.Name,
			&fk.Columns,
			&fk.ReferencedSchema,
			&fk.ReferencedTable,
			&fk.ReferencedColumns,
			&fk.OnDelete,
			&fk.OnUpdate,
		); err != nil {
			return nil, fmt.Errorf("failed to scan referencing foreign key definition: %w", err)
		}
		fks = append(fks, fk)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate referencing foreign key rows: %w", err)
	}

	return fks, nil
}

// GetTableCheckConstraints returns CHECK constraints defined on the specified table,
// excluding any constraints inherited from parent tables or system-generated constraints.
func (p Postgres) GetTableCheckConstraints(schema, table string) ([]CheckConstraintDef, error) {
	query := `
		SELECT
			con.conname AS name,
			pg_get_constraintdef(con.oid) AS expression
		FROM pg_catalog.pg_constraint con
		JOIN pg_catalog.pg_class cls ON cls.oid = con.conrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = cls.relnamespace
		WHERE n.nspname = $1
			AND cls.relname = $2
			AND con.contype = 'c'
			AND NOT con.conislocal = false
		ORDER BY con.conname`

	rows, err := p.conn.Query(p.ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get table check constraints: %w", err)
	}
	defer rows.Close()

	var constraints []CheckConstraintDef
	for rows.Next() {
		var cc CheckConstraintDef
		if err := rows.Scan(&cc.Name, &cc.Expression); err != nil {
			return nil, fmt.Errorf("failed to scan check constraint definition: %w", err)
		}
		constraints = append(constraints, cc)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate check constraint rows: %w", err)
	}

	return constraints, nil
}

// GetTableRowCount returns the estimated row count for the specified table
// using pg_class.reltuples. This is fast but approximate (updated by ANALYZE).
func (p Postgres) GetTableRowCount(schema, table string) (int64, error) {
	query := `
		SELECT COALESCE(c.reltuples, 0)::bigint
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
			AND c.relname = $2`

	var count int64
	err := p.conn.QueryRow(p.ctx, query, schema, table).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get table row count: %w", err)
	}

	return count, nil
}

// HasPrimaryKey checks whether the specified table has a primary key defined.
func (p Postgres) HasPrimaryKey(schema, table string) (bool, error) {
	query := `
		SELECT EXISTS(
			SELECT 1
			FROM pg_catalog.pg_index i
			JOIN pg_catalog.pg_class cls ON cls.oid = i.indrelid
			JOIN pg_catalog.pg_namespace n ON n.oid = cls.relnamespace
			WHERE n.nspname = $1
				AND cls.relname = $2
				AND i.indisprimary
		)`

	var hasPK bool
	err := p.conn.QueryRow(p.ctx, query, schema, table).Scan(&hasPK)
	if err != nil {
		return false, fmt.Errorf("failed to check if table has primary key: %w", err)
	}

	return hasPK, nil
}
