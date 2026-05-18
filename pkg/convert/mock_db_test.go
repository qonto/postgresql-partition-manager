package convert

import (
	"context"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
)

// mockConvertDBClient is a base mock implementation of ConvertDBClient for property tests.
// Embed this in specific test mocks and override only the methods needed.
type mockConvertDBClient struct{}

func (m *mockConvertDBClient) GetTableColumns(schema, table string) ([]postgresql.ColumnDef, error) {
	return nil, nil
}

func (m *mockConvertDBClient) GetTablePrimaryKey(schema, table string) ([]string, error) {
	return nil, nil
}

func (m *mockConvertDBClient) GetTableIndexes(schema, table string) ([]postgresql.IndexDef, error) {
	return nil, nil
}

func (m *mockConvertDBClient) GetTableForeignKeys(schema, table string) ([]postgresql.ForeignKeyDef, error) {
	return nil, nil
}

func (m *mockConvertDBClient) GetReferencingForeignKeys(schema, table string) ([]postgresql.ForeignKeyDef, error) {
	return nil, nil
}

func (m *mockConvertDBClient) GetTableCheckConstraints(schema, table string) ([]postgresql.CheckConstraintDef, error) {
	return nil, nil
}

func (m *mockConvertDBClient) GetPartitionKeyRange(schema, table, partitionKey string) (min, max time.Time, err error) {
	return time.Time{}, time.Time{}, nil
}

func (m *mockConvertDBClient) GetTableRowCount(schema, table string) (int64, error) {
	return 0, nil
}

func (m *mockConvertDBClient) IsTableExists(schema, table string) (bool, error) {
	return false, nil
}

func (m *mockConvertDBClient) HasPrimaryKey(schema, table string) (bool, error) {
	return false, nil
}

func (m *mockConvertDBClient) CreateCDCQueue(schema, table string, pkColumns []string) error {
	return nil
}

func (m *mockConvertDBClient) CreateCDCTriggerFunction(schema, table string, pkColumns []string) error {
	return nil
}

func (m *mockConvertDBClient) InstallCDCTrigger(schema, table string) error {
	return nil
}

func (m *mockConvertDBClient) CreatePartitionedTable(schema, table string, columns []postgresql.ColumnDef, partitionKey string) error {
	return nil
}

func (m *mockConvertDBClient) CreatePartition(schema, parentTable, partitionName, lowerBound, upperBound string) error {
	return nil
}

func (m *mockConvertDBClient) CreateIndex(schema, table string, idx postgresql.IndexDef) error {
	return nil
}

func (m *mockConvertDBClient) CreateForeignKey(schema, table string, fk postgresql.ForeignKeyDef) error {
	return nil
}

func (m *mockConvertDBClient) IsCDCQueueExists(schema, table string) (bool, error) {
	return false, nil
}

func (m *mockConvertDBClient) IsCDCTriggerExists(schema, table string) (bool, error) {
	return false, nil
}

func (m *mockConvertDBClient) BackfillBatch(schema, sourceTable, targetTable string, pkColumns []string, afterPK []any, batchSize int) ([]any, int64, error) {
	return nil, 0, nil
}

func (m *mockConvertDBClient) DequeueEvents(schema, table string, batchSize int) ([]postgresql.CDCEvent, error) {
	return nil, nil
}

func (m *mockConvertDBClient) ApplyUpsert(schema, targetTable, sourceTable string, pkColumns []string, pkValues []string) error {
	return nil
}

func (m *mockConvertDBClient) ApplyDelete(schema, targetTable string, pkColumns []string, pkValues []string) error {
	return nil
}

func (m *mockConvertDBClient) GetReplayLag(schema, table string) (int64, error) {
	return 0, nil
}

func (m *mockConvertDBClient) IsCDCQueueEmpty(schema, table string) (bool, error) {
	return true, nil
}

func (m *mockConvertDBClient) AcquireAdvisoryLock(schema, table string) error {
	return nil
}

func (m *mockConvertDBClient) AcquireExclusiveLock(schema, table string) error {
	return nil
}

func (m *mockConvertDBClient) AcquireShareRowExclusiveLock(schema, table string) error {
	return nil
}

func (m *mockConvertDBClient) DisableTrigger(schema, table, triggerName string) error {
	return nil
}

func (m *mockConvertDBClient) EnableTrigger(schema, table, triggerName string) error {
	return nil
}

func (m *mockConvertDBClient) RenameTable(schema, oldName, newName string) error {
	return nil
}

func (m *mockConvertDBClient) RenameIndex(schema, oldName, newName string) error {
	return nil
}

func (m *mockConvertDBClient) DropTrigger(schema, table, triggerName string) error {
	return nil
}

func (m *mockConvertDBClient) DropTriggerFunction(schema, functionName string) error {
	return nil
}

func (m *mockConvertDBClient) AddForeignKeyNotValid(schema, table string, fk postgresql.ForeignKeyDef) error {
	return nil
}

func (m *mockConvertDBClient) ValidateForeignKey(schema, table, constraintName string) error {
	return nil
}

func (m *mockConvertDBClient) DropForeignKey(schema, table, constraintName string) error {
	return nil
}

func (m *mockConvertDBClient) AnalyzeTable(schema, table string) error {
	return nil
}

func (m *mockConvertDBClient) EnsureMetadataTable() error {
	return nil
}

func (m *mockConvertDBClient) GetMigrationState(schema, table string) (*postgresql.MigrationState, error) {
	return nil, nil
}

func (m *mockConvertDBClient) UpdateMigrationState(schema, table string, state *postgresql.MigrationState) error {
	return nil
}

func (m *mockConvertDBClient) DeleteMigrationState(schema, table string) error {
	return nil
}

func (m *mockConvertDBClient) DropTable(schema, table string) error {
	return nil
}

func (m *mockConvertDBClient) DropCDCQueue(schema, table string) error {
	return nil
}

func (m *mockConvertDBClient) ReassignSequences(schema, oldTable, newTable string) error {
	return nil
}

func (m *mockConvertDBClient) BeginTx(ctx context.Context) (postgresql.Tx, error) {
	return nil, nil
}
