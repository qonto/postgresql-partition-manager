package postgresql

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

var (
	// ErrUnsupportedPartitionStrategy represents an error indicating that the partitioning strategy on the table is not supported.
	ErrUnsupportedPartitionStrategy = errors.New("unsupported partitioning strategy")

	// ErrTableIsNotPartitioned represents an error indicating the specified table don't have partitioning
	ErrTableIsNotPartitioned = errors.New("table is not partitioned")
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
		return "", "", ErrTableIsNotPartitioned
	}

	strategy = partkeydef[0]
	key = partkeydef[1]

	return strategy, key, nil
}

func (p Postgres) SetPartitionReplicaIdentity(schema, table, parent string) error {
	var replIdent string

	// Get the replica identity of the parent
	queryRi := "SELECT relreplident::text from pg_class c JOIN pg_namespace n ON (n.oid=c.relnamespace) WHERE n.nspname=$1 AND c.relname=$2"

	err := p.conn.QueryRow(p.ctx, queryRi, schema, parent).Scan(&replIdent)
	if err != nil {
		return fmt.Errorf("failed to check pg_class.relreplident for parent table: %w", err)
	}

	if replIdent == "f" { // replica identity = full
		queryFull := fmt.Sprintf("ALTER TABLE %s.%s REPLICA IDENTITY FULL", schema, table)
		p.logger.Debug("Set identity full", "query", queryFull)

		_, err = p.conn.Exec(p.ctx, queryFull)
		if err != nil {
			return fmt.Errorf("failed to set replica identity: %w", err)
		}
	} else if replIdent == "i" { // replica identity = specific index
		var indexName string
		/* This query finds the index that is a child of the (only) index
		   in the parent table having "indisreplident"=true.
		   "pg_inherits" holds the (parent-index, child-index) relationship. */
		queryIdx := `
SELECT c_idx_child.relname
  FROM pg_index i_parent JOIN pg_inherits inh ON (inh.inhparent=i_parent.indexrelid)
  JOIN pg_index i_child ON (i_child.indexrelid=inh.inhrelid)
  JOIN pg_class c_parent ON (c_parent.oid=i_parent.indrelid)
  JOIN pg_namespace ON (pg_namespace.oid=c_parent.relnamespace)
  JOIN pg_class c_idx_child ON (c_idx_child.oid=inh.inhrelid)
  JOIN pg_class c_child ON (c_child.oid=i_child.indrelid)
 WHERE pg_namespace.nspname= $1
 AND c_parent.relname = $2
 AND i_parent.indisreplident=true
 AND c_child.relname = $3
`

		err = p.conn.QueryRow(p.ctx, queryIdx, schema, parent, table).Scan(&indexName)
		if err != nil {
			return fmt.Errorf("failed to find the child index for the new partition: %w", err)
		}

		queryAlter := fmt.Sprintf("ALTER TABLE %s.%s REPLICA IDENTITY USING INDEX %s", schema, table, indexName)
		p.logger.Debug("Set replica identity", "schema", schema, "table", table, "index", indexName, "query", queryAlter)

		_, err := p.conn.Exec(p.ctx, queryAlter)
		if err != nil {
			return fmt.Errorf("failed to set replica identity on the new partition: %w", err)
		}
	}

	return nil
}
