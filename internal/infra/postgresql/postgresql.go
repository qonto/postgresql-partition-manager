package postgresql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/qonto/postgresql-partition-manager/internal/infra/retry"
	"github.com/qonto/postgresql-partition-manager/internal/infra/uuid7"
)

// ErrUnsupportedPartitionStrategy represents an error indicating that the partitioning strategy on the table is not supported.
var (
	ErrUnsupportedPartitionStrategy = errors.New("unsupported partitioning strategy")
	ErrPartitionNotFound            = errors.New("partition not found")
)

type PostgreSQL struct {
	ctx    context.Context
	db     PgxIface
	logger slog.Logger
}

type PgxIface interface {
	Close(context.Context) error
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	PgConn() *pgconn.PgConn
}

func New(logger slog.Logger, db PgxIface) *PostgreSQL {
	return &PostgreSQL{
		ctx:    context.TODO(),
		db:     db,
		logger: logger,
	}
}

func (p PostgreSQL) CreatePartition(partitionConfiguration PartitionConfiguration, partition Partition) error {
	p.logger.Debug("Creating partition", "schema", partition.Schema, "table", partition.Name)

	tableExists, err := p.tableExists(partition.ToTable())
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if !tableExists {
		query := fmt.Sprintf("CREATE TABLE %s (LIKE %s)", partition.QualifiedName(), partition.ParentTable)
		p.logger.Debug("Create table", "query", query)

		_, err := p.db.Exec(context.Background(), query)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}

		p.logger.Info("Table created", "schema", partition.Schema, "table", partition.Name)
	} else {
		p.logger.Info("Table already exists, skip", "schema", partition.Schema, "table", partition.Name)
	}

	lowerBoundTime, upperBoundTime, err := parseBounds(partition)
	if err != nil {
		return fmt.Errorf("failed to bounds: %w", err)
	}

	partitionSettings, err := p.GetPartitionSettings(partition.GetParentTable())
	if err != nil {
		return fmt.Errorf("failed to get partition settings: %w", err)
	}

	switch partitionSettings.KeyType {
	case DateColumnType:
		partition.LowerBound = lowerBoundTime.Format("2006-01-02")
		partition.UpperBound = upperBoundTime.Format("2006-01-02")
	case DateTimeColumnType:
		partition.LowerBound = lowerBoundTime.Format("2006-01-02 00:00:00")
		partition.UpperBound = upperBoundTime.Format("2006-01-02 00:00:00")
	case UUIDColumnType:
		partition.LowerBound = uuid7.FromTime(lowerBoundTime)
		partition.UpperBound = uuid7.FromTime(upperBoundTime)
	default:
		return ErrUnsupportedPartitionStrategy
	}

	partitionAttached, err := p.isPartitionIsAttached(partition)
	if err != nil {
		return fmt.Errorf("failed to check partition attachment status: %w", err)
	}

	if partitionAttached {
		p.logger.Info("Table is already attached to the parent table, skip", "schema", partition.Schema, "table", partition.Name)

		return nil
	}

	maxRetries := 3

	err = retry.WithRetry(maxRetries, func(attempt int) error {
		err := p.attachPartition(partition)
		if err != nil {
			p.logger.Warn("fail to attach partition", "error", err, "schema", partition.Schema, "table", partition.Name, "attempt", attempt, "max_retries", maxRetries)
		}

		return err
	})
	if err != nil {
		return fmt.Errorf("failed to attach partition after retries: %w", err)
	}

	p.logger.Info("Partition attached to parent table", "schema", partition.Schema, "table", partition.Name, "parent_table", partition.GetParentTable().Name)

	return nil
}

func (p PostgreSQL) tableExists(table Table) (bool, error) {
	query := `SELECT EXISTS(
		        SELECT c.oid
		        FROM pg_class c
		        JOIN pg_namespace n ON n.oid = c.relnamespace
		        WHERE n.nspname = $1 AND c.relname = $2
			);`

	var exists bool

	err := p.db.QueryRow(context.Background(), query, table.Schema, table.Name).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to get table: %w", err)
	}

	return exists, nil
}

func (p PostgreSQL) GetPartitionSettings(table Table) (PartitionSettings, error) {
	var partkeydef []string

	maxRetries := 3

	err := retry.WithRetry(maxRetries, func(attempt int) error {
		// pg_get_partkeydef() is a system function returning the definition of a partitioning key
		// It return a text string: <partitioningStrategy> (<partitioning key definition>)
		// Example for RANGE (created_at)
		query := fmt.Sprintf(`
		SELECT regexp_match(partkeydef, '^(.*) \((.*)\)$')
		FROM pg_catalog.pg_get_partkeydef('%s'::regclass) as partkeydef
		`, table.QualifiedName())

		err := p.db.QueryRow(p.ctx, query).Scan(&partkeydef)
		if err != nil {
			p.logger.Warn("failed to get partitioning key", "error", err, "schema", table.Schema, "table", table.Name, "attempt", attempt, "max_retries", maxRetries)

			return fmt.Errorf("failed to get partition key: %w", err)
		}

		return nil
	})
	if err != nil {
		return PartitionSettings{}, fmt.Errorf("failed to get partitioning key after retries: %w", err)
	}

	if len(partkeydef) == 0 {
		return PartitionSettings{}, ErrPartitionNotFound
	}

	settings := PartitionSettings{
		Key: partkeydef[1],
	}

	rawPartitionStrategy := partkeydef[0]
	switch rawPartitionStrategy {
	case string(RangePartitionStrategy):
		settings.Strategy = RangePartitionStrategy
	case string(ListPartitionStrategy):
		settings.Strategy = ListPartitionStrategy
	case string(HashPartitionStrategy):
		settings.Strategy = HashPartitionStrategy
	default:
		return settings, fmt.Errorf("%w: %s", ErrUnsupportedPartitionStrategy, rawPartitionStrategy)
	}

	keyColumn := Column{
		Schema: table.Schema,
		Table:  table.Name,
		Name:   settings.Key,
	}

	settings.KeyType, err = p.getColumnDataType(keyColumn)
	if err != nil {
		return settings, fmt.Errorf("failed to get partition key data type: %s: %w", rawPartitionStrategy, err)
	}

	return settings, nil
}

func (p PostgreSQL) isPartitionIsAttached(partition Partition) (bool, error) {
	query := `SELECT EXISTS(
		SELECT 1 FROM pg_inherits WHERE inhrelid = $1::regclass
	)`

	var exists bool

	err := p.db.QueryRow(context.Background(), query, partition.QualifiedName()).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check partition attachment: %w", err)
	}

	return exists, nil
}

func (p PostgreSQL) attachPartition(partition Partition) error {
	query := fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM ('%s') TO ('%s')", partition.GetParentTable().QualifiedName(), partition.QualifiedName(), partition.LowerBound, partition.UpperBound)
	p.logger.Debug("Attach partition", "query", query, "partition", partition.Name, "table", partition.GetParentTable().Name)

	_, err := p.db.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("failed to attach partition: %w", err)
	}

	return nil
}

// Function detachPartitionConcurrently detaches specified partition from the parent table.
// The partition exists as standalone table after detaching.
// More info: https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-DETACH-PARTITION
func (p PostgreSQL) detachPartitionConcurrently(partition Partition) error {
	query := fmt.Sprintf(`ALTER TABLE %s DETACH PARTITION %s CONCURRENTLY;`, partition.GetParentTable().Name, partition.QualifiedName())
	p.logger.Debug("Detach partition", "schema", partition.Schema, "table", partition.Name, "query", query, "parent_table", partition.GetParentTable().Name)

	_, err := p.db.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("failed to detach partition from the parent table: %w", err)
	}

	return nil
}

// Function finalizePartitionDetach finalize a partition detach operation.
// It's required when a partition is in "detach pending" status.
// More info: https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-DETACH-PARTITION
func (p PostgreSQL) finalizePartitionDetach(partition Partition) error {
	query := fmt.Sprintf(`ALTER TABLE %s DETACH PARTITION %s FINALIZE;`, partition.GetParentTable().Name, partition.QualifiedName())
	p.logger.Debug("finialize detach partition", "schema", partition.Schema, "table", partition.Name, "query", query, "parent_table", partition.GetParentTable().Name)

	_, err := p.db.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("failed to finalize partition detach: %w", err)
	}

	return nil
}

func (p PostgreSQL) dropTable(table Table) error {
	query := fmt.Sprintf("DROP TABLE %s", table.QualifiedName())
	p.logger.Debug("Drop table", "schema", table.Schema, "table", table.Name, "query", query)

	_, err := p.db.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	return nil
}

func (p PostgreSQL) ListPartitions(table Table) (partitions []Partition, err error) {
	query := fmt.Sprintf(`
	WITH parts as (
		SELECT
		   relnamespace::regnamespace as schema,
		   c.oid::pg_catalog.regclass AS part_name,
		   regexp_match(pg_get_expr(c.relpartbound, c.oid),
					  'FOR VALUES FROM \(''(.*)''\) TO \(''(.*)''\)') AS bounds
		 FROM
		   pg_catalog.pg_class c JOIN pg_catalog.pg_inherits i ON (c.oid = i.inhrelid)
		 WHERE i.inhparent = '%s'::regclass
		   AND c.relkind='r'
	)
	SELECT
		schema,
		part_name as name,
		'%s' as parentTable,
		bounds[1]::text AS lower_bound,
		bounds[2]::text AS upper_bound
	FROM parts
	ORDER BY part_name;`, table.QualifiedName(), table.Name)

	rows, err := p.db.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("failed to list partitions: %w", err)
	}

	rawPartitions, err := pgx.CollectRows(rows, pgx.RowToStructByName[Partition])
	if err != nil {
		return nil, fmt.Errorf("failed to cast list: %w", err)
	}

	for _, partition := range rawPartitions {
		lowerBound, upperBound, err := parseBounds(partition)
		if err != nil {
			return nil, fmt.Errorf("failed to parse bounds: %w", err)
		}

		partition.LowerBound = lowerBound
		partition.UpperBound = upperBound

		partitions = append(partitions, partition)
	}

	return partitions, nil
}

func (p PostgreSQL) DetachPartition(partition Partition) error {
	p.logger.Debug("Detach partition", "schema", partition.Schema, "table", partition.Name)

	maxRetries := 3

	err := retry.WithRetry(maxRetries, func(attempt int) error {
		err := p.detachPartitionConcurrently(partition)
		if err != nil {
			// detachPartitionConcurrently() could fail if the specified partition is in pending detach status
			// It could occurred when a previous detach partition concurrently operation was canceled or interrupted
			// It prevent any other detach operations on the table
			// More info: https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-DETACH-PARTITION
			// To unblock the situation, we try to finalize the detach operation on Object Not In Prerequisite State error
			if isPostgreSQLErrorCode(err, ObjectNotInPrerequisiteStatePostgreSQLErrorCode) {
				p.logger.Warn("Table is already pending detach in partitioned, retry with finalize", "error", err, "schema", partition.Schema, "table", partition.Name)

				finalizeErr := p.finalizePartitionDetach(partition)
				if finalizeErr == nil {
					err = nil // Returns a success since the partition detach operation has been completed
				}
			} else {
				p.logger.Warn("Fail to detach partition", "error", err, "schema", partition.Schema, "table", partition.Name, "attempt", attempt, "max_retries", maxRetries)
			}
		}

		return err
	})
	if err != nil {
		return fmt.Errorf("failed to detach partition after retries: %w", err)
	}

	return nil
}

func (p PostgreSQL) DeletePartition(partition Partition) error {
	p.logger.Debug("Deleting partition", "schema", partition.Schema, "table", partition.Name)

	maxRetries := 3

	err := retry.WithRetry(maxRetries, func(attempt int) error {
		err := p.dropTable(partition.ToTable())
		if err != nil {
			p.logger.Warn("Fail to drop table", "error", err, "schema", partition.Schema, "table", partition.Name, "attempt", attempt, "max_retries", maxRetries)
		}

		return err
	})
	if err != nil {
		return fmt.Errorf("failed to drop table after retries: %w", err)
	}

	return nil
}
