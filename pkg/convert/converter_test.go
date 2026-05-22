package convert

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
)

// converterMockDB is a mock implementation of ConvertDBClient for Converter orchestrator tests.
type converterMockDB struct {
	mockConvertDBClient

	// State management
	migrationState    *postgresql.MigrationState
	migrationStateErr error

	// Track calls for mutation verification
	createCDCQueueCalls           int
	createCDCTriggerFunctionCalls int
	installCDCTriggerCalls        int
	createPartitionedTableCalls   int
	backfillBatchCalls            int
	dropTriggerCalls              int
	dropTriggerFunctionCalls      int
	dropCDCQueueCalls             int
	dropTableCalls                int
	deleteMigrationStateCalls     int
	updateMigrationStateCalls     int

	// Control behavior
	tableExistsMap    map[string]bool
	cdcQueueExistsMap map[string]bool
	cdcTriggerExists  bool
	hasPK             bool
	pkColumns         []string
	columns           []postgresql.ColumnDef
	indexes           []postgresql.IndexDef
	foreignKeys       []postgresql.ForeignKeyDef
	rowCount          int64
}

func (m *converterMockDB) EnsureMetadataTable() error {
	return nil
}

func (m *converterMockDB) GetMigrationState(schema, table string) (*postgresql.MigrationState, error) {
	if m.migrationStateErr != nil {
		return nil, m.migrationStateErr
	}

	return m.migrationState, nil
}

func (m *converterMockDB) UpdateMigrationState(schema, table string, state *postgresql.MigrationState) error {
	m.updateMigrationStateCalls++
	m.migrationState = state

	return nil
}

func (m *converterMockDB) DeleteMigrationState(schema, table string) error {
	m.deleteMigrationStateCalls++

	return nil
}

func (m *converterMockDB) IsTableExists(schema, table string) (bool, error) {
	key := fmt.Sprintf("%s.%s", schema, table)
	if exists, ok := m.tableExistsMap[key]; ok {
		return exists, nil
	}

	return false, nil
}

func (m *converterMockDB) HasPrimaryKey(schema, table string) (bool, error) {
	return m.hasPK, nil
}

func (m *converterMockDB) GetTablePrimaryKey(schema, table string) ([]string, error) {
	return m.pkColumns, nil
}

func (m *converterMockDB) GetTableColumns(schema, table string) ([]postgresql.ColumnDef, error) {
	return m.columns, nil
}

func (m *converterMockDB) GetTableIndexes(schema, table string) ([]postgresql.IndexDef, error) {
	return m.indexes, nil
}

func (m *converterMockDB) GetTableForeignKeys(schema, table string) ([]postgresql.ForeignKeyDef, error) {
	return m.foreignKeys, nil
}

func (m *converterMockDB) GetPartitionKeyRange(schema, table, partitionKey string) (min, max time.Time, err error) {
	return time.Now().AddDate(0, -1, 0), time.Now(), nil
}

func (m *converterMockDB) GetTableRowCount(schema, table string) (int64, error) {
	return m.rowCount, nil
}

func (m *converterMockDB) IsCDCQueueExists(schema, table string) (bool, error) {
	key := fmt.Sprintf("%s.%s", schema, table)
	if exists, ok := m.cdcQueueExistsMap[key]; ok {
		return exists, nil
	}

	return false, nil
}

func (m *converterMockDB) IsCDCTriggerExists(schema, table string) (bool, error) {
	return m.cdcTriggerExists, nil
}

func (m *converterMockDB) CreateCDCQueue(schema, table string, pkColumns []string) error {
	m.createCDCQueueCalls++

	return nil
}

func (m *converterMockDB) CreateCDCTriggerFunction(schema, table string, pkColumns []string) error {
	m.createCDCTriggerFunctionCalls++

	return nil
}

func (m *converterMockDB) InstallCDCTrigger(schema, table string) error {
	m.installCDCTriggerCalls++

	return nil
}

func (m *converterMockDB) CreatePartitionedTable(schema, table string, columns []postgresql.ColumnDef, partitionKey string) error {
	m.createPartitionedTableCalls++

	return nil
}

func (m *converterMockDB) CreatePartition(schema, parentTable, partitionName, lowerBound, upperBound string) error {
	return nil
}

func (m *converterMockDB) CreateIndex(schema, table string, idx postgresql.IndexDef) error {
	return nil
}

func (m *converterMockDB) CreateForeignKey(schema, table string, fk postgresql.ForeignKeyDef) error {
	return nil
}

func (m *converterMockDB) BackfillBatch(schema, sourceTable, targetTable string, pkColumns []string, afterPK []any, batchSize int) ([]any, int64, error) {
	m.backfillBatchCalls++

	return nil, 0, nil
}

func (m *converterMockDB) DropTrigger(schema, table, triggerName string) error {
	m.dropTriggerCalls++

	return nil
}

func (m *converterMockDB) DropTriggerFunction(schema, functionName string) error {
	m.dropTriggerFunctionCalls++

	return nil
}

func (m *converterMockDB) DropCDCQueue(schema, table string) error {
	m.dropCDCQueueCalls++

	return nil
}

func (m *converterMockDB) DropTable(schema, table string) error {
	m.dropTableCalls++

	return nil
}

func (m *converterMockDB) GetReplayLag(schema, table string) (int64, error) {
	return 0, nil
}

func newTestConfig() partition.Configuration {
	return partition.Configuration{
		Schema:           "public",
		Table:            "events",
		PartitionKey:     "created_at",
		Interval:         partition.Daily,
		Retention:        90,
		PreProvisioned:   7,
		CleanupPolicy:    partition.Drop,
		BatchSize:        10000,
		ReplayBatchSize:  1000,
		LockTimeout:      5,
		StatementTimeout: 30,
	}
}

func newTestLogger() slog.Logger {
	return *slog.New(slog.NewTextHandler(os.Stdout, nil))
}

// --- State Transition Validation Tests ---

func TestConverter_Backfill_FromSetup_Succeeds(t *testing.T) {
	db := &converterMockDB{
		migrationState: &postgresql.MigrationState{
			Schema: "public",
			Table:  "events",
			Phase:  string(PhaseSetup),
		},
		hasPK:     true,
		pkColumns: []string{"id"},
	}

	converter := New(newTestLogger(), db, newTestConfig(), false)

	err := converter.Backfill(context.Background())
	if err != nil {
		t.Fatalf("expected Backfill from setup to succeed, got: %v", err)
	}
}

func TestConverter_Replay_FromBackfill_Succeeds(t *testing.T) {
	db := &converterMockDB{
		migrationState: &postgresql.MigrationState{
			Schema: "public",
			Table:  "events",
			Phase:  string(PhaseBackfill),
		},
		hasPK:     true,
		pkColumns: []string{"id"},
	}

	converter := New(newTestLogger(), db, newTestConfig(), false)

	err := converter.Replay(context.Background())
	if err != nil {
		t.Fatalf("expected Replay from backfill to succeed, got: %v", err)
	}
}

func TestConverter_Verify_FromReplay_Succeeds(t *testing.T) {
	db := &converterMockDB{
		migrationState: &postgresql.MigrationState{
			Schema: "public",
			Table:  "events",
			Phase:  string(PhaseReplay),
		},
		rowCount: 100,
	}

	converter := New(newTestLogger(), db, newTestConfig(), false)

	result, err := converter.Verify(context.Background(), VerifyOptions{})
	if err != nil {
		t.Fatalf("expected Verify from replay to succeed, got: %v", err)
	}

	if result == nil {
		t.Fatal("expected non-nil verify result")
	}
}

func TestConverter_Backfill_FromReplay_Fails(t *testing.T) {
	db := &converterMockDB{
		migrationState: &postgresql.MigrationState{
			Schema: "public",
			Table:  "events",
			Phase:  string(PhaseReplay),
		},
	}

	converter := New(newTestLogger(), db, newTestConfig(), false)

	err := converter.Backfill(context.Background())
	if err == nil {
		t.Fatal("expected Backfill from replay to fail (invalid transition)")
	}
}

// --- Dry-Run Mode Tests ---

func TestConverter_DryRun_Setup_NoMutations(t *testing.T) {
	db := &converterMockDB{
		migrationState: nil, // Will be initialized to setup
		hasPK:          true,
		pkColumns:      []string{"id"},
		tableExistsMap: map[string]bool{
			"public.events":             true,
			"public.events_partitioned": false,
		},
	}

	converter := New(newTestLogger(), db, newTestConfig(), true)

	err := converter.Setup(context.Background())
	if err != nil {
		t.Fatalf("unexpected error in dry-run setup: %v", err)
	}

	if db.createCDCQueueCalls != 0 {
		t.Errorf("expected 0 CreateCDCQueue calls in dry-run, got %d", db.createCDCQueueCalls)
	}

	if db.createCDCTriggerFunctionCalls != 0 {
		t.Errorf("expected 0 CreateCDCTriggerFunction calls in dry-run, got %d", db.createCDCTriggerFunctionCalls)
	}

	if db.installCDCTriggerCalls != 0 {
		t.Errorf("expected 0 InstallCDCTrigger calls in dry-run, got %d", db.installCDCTriggerCalls)
	}

	if db.createPartitionedTableCalls != 0 {
		t.Errorf("expected 0 CreatePartitionedTable calls in dry-run, got %d", db.createPartitionedTableCalls)
	}
}

func TestConverter_DryRun_Backfill_NoMutations(t *testing.T) {
	db := &converterMockDB{
		migrationState: &postgresql.MigrationState{
			Schema: "public",
			Table:  "events",
			Phase:  string(PhaseSetup),
		},
		hasPK:     true,
		pkColumns: []string{"id"},
		rowCount:  50000,
	}

	converter := New(newTestLogger(), db, newTestConfig(), true)

	err := converter.Backfill(context.Background())
	if err != nil {
		t.Fatalf("unexpected error in dry-run backfill: %v", err)
	}

	if db.backfillBatchCalls != 0 {
		t.Errorf("expected 0 BackfillBatch calls in dry-run, got %d", db.backfillBatchCalls)
	}
}

func TestConverter_DryRun_Cleanup_NoMutations(t *testing.T) {
	db := &converterMockDB{
		migrationState: &postgresql.MigrationState{
			Schema: "public",
			Table:  "events",
			Phase:  string(PhaseCutover),
		},
		tableExistsMap: map[string]bool{
			"public.events_old": true,
		},
		cdcQueueExistsMap: map[string]bool{
			"public.events": true,
		},
	}

	converter := New(newTestLogger(), db, newTestConfig(), true)

	err := converter.Cleanup(context.Background(), true, false)
	if err != nil {
		t.Fatalf("unexpected error in dry-run cleanup: %v", err)
	}

	if db.dropTriggerCalls != 0 {
		t.Errorf("expected 0 DropTrigger calls in dry-run, got %d", db.dropTriggerCalls)
	}

	if db.dropTriggerFunctionCalls != 0 {
		t.Errorf("expected 0 DropTriggerFunction calls in dry-run, got %d", db.dropTriggerFunctionCalls)
	}

	if db.dropCDCQueueCalls != 0 {
		t.Errorf("expected 0 DropCDCQueue calls in dry-run, got %d", db.dropCDCQueueCalls)
	}

	if db.dropTableCalls != 0 {
		t.Errorf("expected 0 DropTable calls in dry-run, got %d", db.dropTableCalls)
	}

	if db.deleteMigrationStateCalls != 0 {
		t.Errorf("expected 0 DeleteMigrationState calls in dry-run, got %d", db.deleteMigrationStateCalls)
	}
}

// --- Cleanup Ordering and Force Mode Tests ---

func TestConverter_Cleanup_NoConfirmNoForce_ReturnsError(t *testing.T) {
	db := &converterMockDB{
		migrationState: &postgresql.MigrationState{
			Schema: "public",
			Table:  "events",
			Phase:  string(PhaseCutover),
		},
	}

	converter := New(newTestLogger(), db, newTestConfig(), false)

	err := converter.Cleanup(context.Background(), false, false)
	if err == nil {
		t.Fatal("expected error when cleanup called without confirm or force")
	}
}

func TestConverter_Cleanup_ForceMode_BypassesCleanupEnginePhaseCheck(t *testing.T) {
	// Force mode bypasses the CleanupEngine's internal phase validation.
	// The Converter's state machine still requires a valid transition (cutover → cleanup),
	// but force mode ensures the CleanupEngine doesn't reject based on its own phase check.
	db := &converterMockDB{
		migrationState: &postgresql.MigrationState{
			Schema: "public",
			Table:  "events",
			Phase:  string(PhaseCutover), // Valid for state machine transition
		},
		tableExistsMap: map[string]bool{
			"public.events_old": true,
		},
		cdcQueueExistsMap: map[string]bool{
			"public.events": true,
		},
	}

	converter := New(newTestLogger(), db, newTestConfig(), false)

	// Force mode: no --confirm needed
	err := converter.Cleanup(context.Background(), false, true)
	if err != nil {
		t.Fatalf("expected force mode to succeed, got: %v", err)
	}

	// Verify cleanup operations were performed
	if db.dropTriggerCalls == 0 {
		t.Error("expected trigger drop with force mode")
	}

	if db.dropCDCQueueCalls == 0 {
		t.Error("expected CDC queue drop with force mode")
	}

	if db.dropTableCalls == 0 {
		t.Error("expected table drop with force mode")
	}
}

func TestConverter_Cleanup_InvalidStateTransition_ReturnsError(t *testing.T) {
	// The Converter's state machine rejects cleanup from backfill phase
	// regardless of force flag, because the state machine transition is invalid.
	db := &converterMockDB{
		migrationState: &postgresql.MigrationState{
			Schema: "public",
			Table:  "events",
			Phase:  string(PhaseBackfill), // Not allowed to transition to cleanup
		},
	}

	converter := New(newTestLogger(), db, newTestConfig(), false)

	err := converter.Cleanup(context.Background(), false, true)
	if err == nil {
		t.Fatal("expected error for invalid state machine transition (backfill → cleanup)")
	}
}

func TestConverter_Cleanup_ConfirmWithInvalidPhase_ReturnsError(t *testing.T) {
	db := &converterMockDB{
		migrationState: &postgresql.MigrationState{
			Schema: "public",
			Table:  "events",
			Phase:  string(PhaseBackfill), // Not post-cutover
		},
	}

	converter := New(newTestLogger(), db, newTestConfig(), false)

	err := converter.Cleanup(context.Background(), true, false)
	if err == nil {
		t.Fatal("expected error when cleanup called with confirm but invalid phase")
	}
}

// --- Idempotency Tests (Setup when artifacts already exist) ---

func TestConverter_Setup_CDCQueueAlreadyExists_SkipsCreation(t *testing.T) {
	db := &converterMockDB{
		migrationState: nil, // Will be initialized to setup
		hasPK:          true,
		pkColumns:      []string{"id"},
		columns: []postgresql.ColumnDef{
			{Name: "id", DataType: "bigint", IsNullable: false},
			{Name: "created_at", DataType: "timestamptz", IsNullable: false},
		},
		indexes: []postgresql.IndexDef{
			{Name: "events_pkey", Columns: []string{"id"}, IsUnique: true, IsPrimary: true, Method: "btree"},
		},
		tableExistsMap: map[string]bool{
			"public.events":             true,
			"public.events_partitioned": false,
		},
		cdcQueueExistsMap: map[string]bool{
			"public.events": true, // Queue already exists
		},
		cdcTriggerExists: false,
	}

	converter := New(newTestLogger(), db, newTestConfig(), false)

	err := converter.Setup(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// CDC queue creation should be skipped
	if db.createCDCQueueCalls != 0 {
		t.Errorf("expected 0 CreateCDCQueue calls (already exists), got %d", db.createCDCQueueCalls)
	}

	// Trigger should still be created
	if db.createCDCTriggerFunctionCalls != 1 {
		t.Errorf("expected 1 CreateCDCTriggerFunction call, got %d", db.createCDCTriggerFunctionCalls)
	}

	if db.installCDCTriggerCalls != 1 {
		t.Errorf("expected 1 InstallCDCTrigger call, got %d", db.installCDCTriggerCalls)
	}
}

func TestConverter_Setup_CDCTriggerAlreadyExists_SkipsInstallation(t *testing.T) {
	db := &converterMockDB{
		migrationState: nil, // Will be initialized to setup
		hasPK:          true,
		pkColumns:      []string{"id"},
		columns: []postgresql.ColumnDef{
			{Name: "id", DataType: "bigint", IsNullable: false},
			{Name: "created_at", DataType: "timestamptz", IsNullable: false},
		},
		indexes: []postgresql.IndexDef{
			{Name: "events_pkey", Columns: []string{"id"}, IsUnique: true, IsPrimary: true, Method: "btree"},
		},
		tableExistsMap: map[string]bool{
			"public.events":             true,
			"public.events_partitioned": false,
		},
		cdcQueueExistsMap: map[string]bool{
			"public.events": false,
		},
		cdcTriggerExists: true, // Trigger already exists
	}

	converter := New(newTestLogger(), db, newTestConfig(), false)

	err := converter.Setup(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// CDC queue should be created
	if db.createCDCQueueCalls != 1 {
		t.Errorf("expected 1 CreateCDCQueue call, got %d", db.createCDCQueueCalls)
	}

	// Trigger creation should be skipped
	if db.createCDCTriggerFunctionCalls != 0 {
		t.Errorf("expected 0 CreateCDCTriggerFunction calls (already exists), got %d", db.createCDCTriggerFunctionCalls)
	}

	if db.installCDCTriggerCalls != 0 {
		t.Errorf("expected 0 InstallCDCTrigger calls (already exists), got %d", db.installCDCTriggerCalls)
	}
}

func TestConverter_Setup_TargetTableAlreadyExists_SkipsCreation(t *testing.T) {
	db := &converterMockDB{
		migrationState: nil, // Will be initialized to setup
		hasPK:          true,
		pkColumns:      []string{"id"},
		tableExistsMap: map[string]bool{
			"public.events":             true,
			"public.events_partitioned": true, // Target already exists
		},
		cdcQueueExistsMap: map[string]bool{
			"public.events": false,
		},
		cdcTriggerExists: false,
	}

	converter := New(newTestLogger(), db, newTestConfig(), false)

	err := converter.Setup(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Target table creation should be skipped
	if db.createPartitionedTableCalls != 0 {
		t.Errorf("expected 0 CreatePartitionedTable calls (already exists), got %d", db.createPartitionedTableCalls)
	}
}
