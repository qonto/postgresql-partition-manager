package postgresql

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

var (
	// ErrUnsupportedPartitionStrategy represents an error indicating that the partitioning strategy on the table is not supported.
	ErrUnsupportedPartitionStrategy = errors.New("unsupported partitioning strategy")

	// ErrTableIsNotPartioned represents an error indicating the specified table don't have partitioning
	ErrTableIsNotPartioned = errors.New("table is not partioned")
)

type PartitionResult struct {
	ParentTable string
	Schema      string
	Name        string
	LowerBound  string
	UpperBound  string
}

func (p Postgres) IsPartitionAttached(schema, table string) (exists bool, err error) {
	query := `SELECT EXISTS(
		SELECT 1 FROM pg_inherits WHERE inhrelid = $1::regclass
	)`

	err = p.conn.QueryRow(p.ctx, query, fmt.Sprintf("%s.%s", schema, table)).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check partition attachment: %w", err)
	}

	return exists, nil
}

func (p Postgres) AttachPartition(schema, table, parent, lowerBound, upperBound string) error {
	query := fmt.Sprintf("ALTER TABLE %s.%s ATTACH PARTITION %s.%s FOR VALUES FROM ('%s') TO ('%s')", schema, parent, schema, table, lowerBound, upperBound)
	p.logger.Debug("Attach partition", "query", query, "schema", schema, "table", table)

	_, err := p.conn.Exec(p.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to attach partition: %w", err)
	}

	return nil
}

// DetachPartitionConcurrently detaches specified partition from the parent table.
// The partition still exists as standalone table after detaching
// More info: https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-DETACH-PARTITION
func (p Postgres) DetachPartitionConcurrently(schema, table, parent string) error {
	query := fmt.Sprintf(`ALTER TABLE %s.%s DETACH PARTITION %s.%s CONCURRENTLY`, schema, parent, schema, table)
	p.logger.Debug("Detach partition", "schema", schema, "table", table, "query", query, "parent_table", parent)

	_, err := p.conn.Exec(p.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to detach partition from the parent table: %w", err)
	}

	return nil
}

// FinalizePartitionDetach finalizes a partition detach operation.
// It's required when a partition is in "detach pending" status.
// More info: https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-DETACH-PARTITION
func (p Postgres) FinalizePartitionDetach(schema, table, parent string) error {
	query := fmt.Sprintf(`ALTER TABLE %s.%s DETACH PARTITION %s.%s FINALIZE`, schema, parent, schema, table)
	p.logger.Debug("finialize detach partition", "schema", schema, "table", table, "query", query, "parent_table", parent)

	_, err := p.conn.Exec(p.ctx, query)
	if err != nil {
		return fmt.Errorf("failed to finalize partition detach: %w", err)
	}

	return nil
}

func (p Postgres) ListPartitions(schema, table string) (partitions []PartitionResult, err error) {
	query := fmt.Sprintf(`
	WITH parts as (
		SELECT
		   relnamespace::regnamespace as schema,
		   c.oid::pg_catalog.regclass AS part_name,
		   regexp_match(pg_get_expr(c.relpartbound, c.oid),
					  'FOR VALUES FROM \(''(.*)''\) TO \(''(.*)''\)') AS bounds
		 FROM
		   pg_catalog.pg_class c JOIN pg_catalog.pg_inherits i ON (c.oid = i.inhrelid)
		 WHERE i.inhparent = '%s.%s'::regclass
		   AND c.relkind='r'
	)
	SELECT
		schema,
		part_name as name,
		'%s' as parentTable,
		bounds[1]::text AS lowerBound,
		bounds[2]::text AS upperBound
	FROM parts
	ORDER BY part_name;`, schema, table, table)

	rows, err := p.conn.Query(p.ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list partitions: %w", err)
	}

	partitions, err = pgx.CollectRows(rows, pgx.RowToStructByName[PartitionResult])
	if err != nil {
		return nil, fmt.Errorf("failed to cast list: %w", err)
	}

	return partitions, nil
}

func (p Postgres) GetPartitionSettings(schema, table string) (strategy, key string, err error) {
	var partkeydef []string

	// pg_get_partkeydef() is a system function returning the definition of a partitioning key
	// It return a text string: <partitioningStrategy> (<partitioning key definition>)
	// Example for RANGE (created_at)
	query := fmt.Sprintf(`
	SELECT regexp_match(partkeydef, '^(.*) \((.*)\)$')
	FROM pg_catalog.pg_get_partkeydef('%s.%s'::regclass) as partkeydef
	`, schema, table)

	err = p.conn.QueryRow(p.ctx, query).Scan(&partkeydef)
	if err != nil {
		p.logger.Warn("failed to get partitioning key", "error", err, "schema", schema, "table", table)

		return "", "", fmt.Errorf("failed to get partition key: %w", err)
	}

	if len(partkeydef) == 0 {
		return "", "", ErrTableIsNotPartioned
	}

	strategy = partkeydef[0]
	key = partkeydef[1]

	return strategy, key, nil
}
