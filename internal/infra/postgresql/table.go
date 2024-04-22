package postgresql

import "fmt"

func (p Postgres) CreateTableLikeTable(schema, table, parent string) error {
	query := fmt.Sprintf("CREATE TABLE %s.%s (LIKE %s.%s)", schema, table, schema, parent)
	p.logger.Debug("Create table", "query", schema, "table", table, "query", query)

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
