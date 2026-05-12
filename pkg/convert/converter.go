package convert

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
)

// ConvertDBClient defines the database operations needed by the conversion engine.
type ConvertDBClient interface {
	// Schema introspection
	GetTableColumns(schema, table string) ([]postgresql.ColumnDef, error)
	GetTablePrimaryKey(schema, table string) ([]string, error)
	GetTableIndexes(schema, table string) ([]postgresql.IndexDef, error)
	GetTableForeignKeys(schema, table string) ([]postgresql.ForeignKeyDef, error)
	GetReferencingForeignKeys(schema, table string) ([]postgresql.ForeignKeyDef, error)
	GetTableCheckConstraints(schema, table string) ([]postgresql.CheckConstraintDef, error)
	GetPartitionKeyRange(schema, table, partitionKey string) (min, max time.Time, err error)
	GetTableRowCount(schema, table string) (int64, error)
	IsTableExists(schema, table string) (bool, error)
	HasPrimaryKey(schema, table string) (bool, error)

	// Setup operations
	CreateCDCQueue(schema, table string, pkColumns []string) error
	CreateCDCTriggerFunction(schema, table string, pkColumns []string) error
	InstallCDCTrigger(schema, table string) error
	CreatePartitionedTable(schema, table string, columns []postgresql.ColumnDef, partitionKey string) error
	CreatePartition(schema, parentTable, partitionName, lowerBound, upperBound string) error
	CreateIndex(schema, table string, idx postgresql.IndexDef) error
	CreateForeignKey(schema, table string, fk postgresql.ForeignKeyDef) error
	IsCDCQueueExists(schema, table string) (bool, error)
	IsCDCTriggerExists(schema, table string) (bool, error)

	// Backfill operations
	BackfillBatch(schema, sourceTable, targetTable string, pkColumns []string, afterPK []any, batchSize int) (lastPK []any, rowsCopied int64, err error)

	// Replay operations
	DequeueEvents(schema, table string, batchSize int) ([]postgresql.CDCEvent, error)
	ApplyUpsert(schema, targetTable, sourceTable string, pkColumns []string, pkValues []string) error
	ApplyDelete(schema, targetTable string, pkColumns []string, pkValues []string) error
	GetReplayLag(schema, table string) (int64, error)
	IsCDCQueueEmpty(schema, table string) (bool, error)

	// Cutover operations
	AcquireAdvisoryLock(schema, table string) error
	AcquireExclusiveLock(schema, table string) error
	AcquireShareRowExclusiveLock(schema, table string) error
	DisableTrigger(schema, table, triggerName string) error
	EnableTrigger(schema, table, triggerName string) error
	RenameTable(schema, oldName, newName string) error
	RenameIndex(schema, oldName, newName string) error
	DropTrigger(schema, table, triggerName string) error
	DropTriggerFunction(schema, functionName string) error
	AddForeignKeyNotValid(schema, table string, fk postgresql.ForeignKeyDef) error
	ValidateForeignKey(schema, table, constraintName string) error
	DropForeignKey(schema, table, constraintName string) error
	AnalyzeTable(schema, table string) error

	// Metadata operations
	EnsureMetadataTable() error
	GetMigrationState(schema, table string) (*postgresql.MigrationState, error)
	UpdateMigrationState(schema, table string, state *postgresql.MigrationState) error
	DeleteMigrationState(schema, table string) error

	// Cleanup operations
	DropTable(schema, table string) error
	DropCDCQueue(schema, table string) error

	// Transaction management
	BeginTx(ctx context.Context) (Tx, error)
}

// Tx represents a database transaction for operations that need atomicity.
type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}
