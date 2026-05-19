package convert

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
	"pgregory.net/rapid"
)

// Feature: table-partition-conversion, Property 17: Trigger Disabled Before Final Replay
// For any cutover execution, the trigger disable operation precedes the final replay loop.
// Validates: Design Decision 5; Requirements 7.3

// Feature: table-partition-conversion, Property 18: Queue Empty Assertion Before Rename
// For any cutover execution, rename does not proceed unless queue is verified empty.
// Validates: Design Decision 5; Requirements 7.5

// Feature: table-partition-conversion, Property 19: Advisory Lock Acquired Before Table Locks
// For any cutover execution, advisory lock precedes table-level locks.
// Validates: Design Decision 6; Requirements 7.1

// Feature: table-partition-conversion, Property 20: Deterministic Lock Ordering
// For any cutover execution, source table lock is acquired before target/auxiliary.
// Validates: Design Decision 7; Requirements 7.1

// Feature: table-partition-conversion, Property 21: Referencing FKs Dropped Before Cutover
// For any cutover execution, FK drop precedes rename swap.
// Validates: Design Decision 8; Requirements 7.12

// Feature: table-partition-conversion, Property 22: ANALYZE Executed After Cutover
// For any cutover execution, ANALYZE runs after successful commit.
// Validates: Design Decision 9; Requirements 7.9

// Feature: table-partition-conversion, Property 23: Statement Timeout Set During Cutover
// For any cutover execution, statement_timeout is set (via BeginTx) before lock operations.
// Validates: Requirements 7.2

// --- Recording mock for cutover ordering tests ---

// operationRecord represents a single recorded operation during cutover.
type operationRecord struct {
	op   string
	args []string
}

// cutoverOrderingMock records the order of operations called during cutover.
type cutoverOrderingMock struct {
	mockConvertDBClient
	mu         sync.Mutex
	operations []operationRecord
	fks        []postgresql.ForeignKeyDef
	indexes    []postgresql.IndexDef
}

func (m *cutoverOrderingMock) record(op string, args ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.operations = append(m.operations, operationRecord{op: op, args: args})
}

func (m *cutoverOrderingMock) getOps() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ops := make([]string, len(m.operations))
	for i, r := range m.operations {
		ops[i] = r.op
	}
	return ops
}

func (m *cutoverOrderingMock) GetReferencingForeignKeys(schema, table string) ([]postgresql.ForeignKeyDef, error) {
	m.record("GetReferencingForeignKeys", schema, table)
	return m.fks, nil
}

func (m *cutoverOrderingMock) GetMigrationState(schema, table string) (*postgresql.MigrationState, error) {
	m.record("GetMigrationState", schema, table)
	return &postgresql.MigrationState{
		Schema:    schema,
		Table:     table,
		Phase:     "verify",
		UpdatedAt: time.Now(),
	}, nil
}

func (m *cutoverOrderingMock) UpdateMigrationState(schema, table string, state *postgresql.MigrationState) error {
	m.record("UpdateMigrationState", schema, table)
	return nil
}

func (m *cutoverOrderingMock) BeginTx(ctx context.Context) (postgresql.Tx, error) {
	m.record("BeginTx")
	return &mockOrderingTx{mock: m}, nil
}

func (m *cutoverOrderingMock) AcquireAdvisoryLock(schema, table string) error {
	m.record("AcquireAdvisoryLock", schema, table)
	return nil
}

func (m *cutoverOrderingMock) AcquireExclusiveLock(schema, table string) error {
	m.record("AcquireExclusiveLock", schema, table)
	return nil
}

func (m *cutoverOrderingMock) AcquireShareRowExclusiveLock(schema, table string) error {
	m.record("AcquireShareRowExclusiveLock", schema, table)
	return nil
}

func (m *cutoverOrderingMock) DisableTrigger(schema, table, triggerName string) error {
	m.record("DisableTrigger", schema, table, triggerName)
	return nil
}

func (m *cutoverOrderingMock) DequeueEvents(schema, table string, batchSize int) ([]postgresql.CDCEvent, error) {
	m.record("DequeueEvents", schema, table)
	return nil, nil // empty queue - drain completes immediately
}

func (m *cutoverOrderingMock) IsCDCQueueEmpty(schema, table string) (bool, error) {
	m.record("IsCDCQueueEmpty", schema, table)
	return true, nil
}

func (m *cutoverOrderingMock) DropForeignKey(schema, table, constraintName string) error {
	m.record("DropForeignKey", schema, table, constraintName)
	return nil
}

func (m *cutoverOrderingMock) RenameTable(schema, oldName, newName string) error {
	m.record("RenameTable", schema, oldName+">"+newName)
	return nil
}

func (m *cutoverOrderingMock) AddForeignKeyNotValid(schema, table string, fk postgresql.ForeignKeyDef) error {
	m.record("AddForeignKeyNotValid", schema, table, fk.Name)
	return nil
}

func (m *cutoverOrderingMock) AnalyzeTable(schema, table string) error {
	m.record("AnalyzeTable", schema, table)
	return nil
}

func (m *cutoverOrderingMock) GetTableIndexes(schema, table string) ([]postgresql.IndexDef, error) {
	m.record("GetTableIndexes", schema, table)
	return m.indexes, nil
}

func (m *cutoverOrderingMock) RenameIndex(schema, oldName, newName string) error {
	m.record("RenameIndex", schema, oldName+">"+newName)
	return nil
}

func (m *cutoverOrderingMock) ValidateForeignKey(schema, table, constraintName string) error {
	m.record("ValidateForeignKey", schema, table, constraintName)
	return nil
}

func (m *cutoverOrderingMock) ApplyUpsert(schema, targetTable, sourceTable string, pkColumns []string, pkValues []string) error {
	m.record("ApplyUpsert")
	return nil
}

func (m *cutoverOrderingMock) ApplyDelete(schema, targetTable string, pkColumns []string, pkValues []string) error {
	m.record("ApplyDelete")
	return nil
}

// mockOrderingTx is a mock transaction that records Commit/Rollback.
type mockOrderingTx struct {
	mock *cutoverOrderingMock
}

func (t *mockOrderingTx) Commit(ctx context.Context) error {
	t.mock.record("Commit")
	return nil
}

func (t *mockOrderingTx) Rollback(ctx context.Context) error {
	t.mock.record("Rollback")
	return nil
}

func (t *mockOrderingTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (t *mockOrderingTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return nil
}

func (t *mockOrderingTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return nil, nil
}

// --- Helper functions ---

// indexOfOp returns the first index of op in ops, or -1 if not found.
func indexOfOp(ops []string, op string) int {
	for i, o := range ops {
		if o == op {
			return i
		}
	}
	return -1
}

// lastIndexOfOp returns the last index of op in ops, or -1 if not found.
func lastIndexOfOp(ops []string, op string) int {
	for i := len(ops) - 1; i >= 0; i-- {
		if ops[i] == op {
			return i
		}
	}
	return -1
}

// runCutoverWithMock executes the CutoverEngine.Cutover with the given mock and returns the operation log.
func runCutoverWithMock(mock *cutoverOrderingMock, schema, sourceTable, targetTable string, pkColumns []string) ([]string, error) {
	engine := NewCutoverEngine(
		newTestLogger(),
		mock,
		CutoverEngineConfig{
			Schema:      schema,
			SourceTable: sourceTable,
			TargetTable: targetTable,
			PKColumns:   pkColumns,
			BatchSize:   100,
		},
	)

	err := engine.Cutover(context.Background())
	return mock.getOps(), err
}

// --- Property 17: Trigger Disabled Before Final Replay ---

func TestProperty17_TriggerDisabledBeforeFinalReplay(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{1,10}`).Draw(t, "schema")
		sourceTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,12}`).Draw(t, "sourceTable")
		targetTable := sourceTable + "_partitioned"

		numPKCols := rapid.IntRange(1, 3).Draw(t, "numPKCols")
		pkColumns := make([]string, numPKCols)
		for i := range pkColumns {
			pkColumns[i] = rapid.StringMatching(`[a-z][a-z0-9_]{1,8}`).Draw(t, "pkCol")
		}

		numFKs := rapid.IntRange(0, 3).Draw(t, "numFKs")
		fks := make([]postgresql.ForeignKeyDef, numFKs)
		for i := range fks {
			fks[i] = postgresql.ForeignKeyDef{
				Name:              rapid.StringMatching(`fk_[a-z]{2,8}`).Draw(t, "fkName"),
				Columns:           []string{"col1"},
				ReferencedSchema:  schema,
				ReferencedTable:   rapid.StringMatching(`[a-z][a-z0-9_]{2,8}`).Draw(t, "childTable"),
				ReferencedColumns: []string{"id"},
				OnDelete:          "NO ACTION",
				OnUpdate:          "NO ACTION",
			}
		}

		mock := &cutoverOrderingMock{fks: fks}
		ops, err := runCutoverWithMock(mock, schema, sourceTable, targetTable, pkColumns)
		if err != nil {
			t.Fatalf("cutover failed: %v", err)
		}

		disableIdx := indexOfOp(ops, "DisableTrigger")
		dequeueIdx := indexOfOp(ops, "DequeueEvents")

		if disableIdx == -1 {
			t.Fatal("DisableTrigger was not called")
		}
		if dequeueIdx == -1 {
			t.Fatal("DequeueEvents was not called")
		}
		if disableIdx >= dequeueIdx {
			t.Fatalf("Property 17 violated: DisableTrigger (idx=%d) must precede DequeueEvents (idx=%d)", disableIdx, dequeueIdx)
		}
	})
}

func TestProperty17_TriggerDisabledBeforeFinalReplay_WithFKs(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{1,10}`).Draw(t, "schema")
		sourceTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,12}`).Draw(t, "sourceTable")
		targetTable := sourceTable + "_partitioned"
		pkColumns := []string{"id"}

		// Always have at least one FK to exercise the FK path
		numFKs := rapid.IntRange(1, 5).Draw(t, "numFKs")
		fks := make([]postgresql.ForeignKeyDef, numFKs)
		for i := range fks {
			fks[i] = postgresql.ForeignKeyDef{
				Name:              rapid.StringMatching(`fk_[a-z]{2,8}`).Draw(t, "fkName"),
				Columns:           []string{"ref_id"},
				ReferencedSchema:  schema,
				ReferencedTable:   rapid.StringMatching(`child_[a-z]{2,6}`).Draw(t, "childTable"),
				ReferencedColumns: []string{"id"},
				OnDelete:          "CASCADE",
				OnUpdate:          "NO ACTION",
			}
		}

		mock := &cutoverOrderingMock{fks: fks}
		ops, err := runCutoverWithMock(mock, schema, sourceTable, targetTable, pkColumns)
		if err != nil {
			t.Fatalf("cutover failed: %v", err)
		}

		disableIdx := indexOfOp(ops, "DisableTrigger")
		dequeueIdx := indexOfOp(ops, "DequeueEvents")

		if disableIdx >= dequeueIdx {
			t.Fatalf("Property 17 violated: DisableTrigger (idx=%d) must precede DequeueEvents (idx=%d)", disableIdx, dequeueIdx)
		}
	})
}

// --- Property 18: Queue Empty Assertion Before Rename ---

func TestProperty18_QueueEmptyAssertionBeforeRename(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{1,10}`).Draw(t, "schema")
		sourceTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,12}`).Draw(t, "sourceTable")
		targetTable := sourceTable + "_partitioned"

		numPKCols := rapid.IntRange(1, 3).Draw(t, "numPKCols")
		pkColumns := make([]string, numPKCols)
		for i := range pkColumns {
			pkColumns[i] = rapid.StringMatching(`[a-z][a-z0-9_]{1,8}`).Draw(t, "pkCol")
		}

		numFKs := rapid.IntRange(0, 3).Draw(t, "numFKs")
		fks := make([]postgresql.ForeignKeyDef, numFKs)
		for i := range fks {
			fks[i] = postgresql.ForeignKeyDef{
				Name:              rapid.StringMatching(`fk_[a-z]{2,8}`).Draw(t, "fkName"),
				Columns:           []string{"col1"},
				ReferencedSchema:  schema,
				ReferencedTable:   rapid.StringMatching(`[a-z][a-z0-9_]{2,8}`).Draw(t, "childTable"),
				ReferencedColumns: []string{"id"},
				OnDelete:          "NO ACTION",
				OnUpdate:          "NO ACTION",
			}
		}

		mock := &cutoverOrderingMock{fks: fks}
		ops, err := runCutoverWithMock(mock, schema, sourceTable, targetTable, pkColumns)
		if err != nil {
			t.Fatalf("cutover failed: %v", err)
		}

		emptyCheckIdx := indexOfOp(ops, "IsCDCQueueEmpty")
		renameIdx := indexOfOp(ops, "RenameTable")

		if emptyCheckIdx == -1 {
			t.Fatal("IsCDCQueueEmpty was not called")
		}
		if renameIdx == -1 {
			t.Fatal("RenameTable was not called")
		}
		if emptyCheckIdx >= renameIdx {
			t.Fatalf("Property 18 violated: IsCDCQueueEmpty (idx=%d) must precede RenameTable (idx=%d)", emptyCheckIdx, renameIdx)
		}
	})
}

func TestProperty18_QueueNotEmpty_AbortsBeforeRename(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{1,10}`).Draw(t, "schema")
		sourceTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,12}`).Draw(t, "sourceTable")
		targetTable := sourceTable + "_partitioned"
		pkColumns := []string{"id"}

		// Create a mock where queue is NOT empty
		mock := &cutoverOrderingMock{}
		// Override IsCDCQueueEmpty to return false
		notEmptyMock := &cutoverOrderingNotEmptyMock{cutoverOrderingMock: mock}

		engine := NewCutoverEngine(
			newTestLogger(),
			notEmptyMock,
			CutoverEngineConfig{
				Schema:      schema,
				SourceTable: sourceTable,
				TargetTable: targetTable,
				PKColumns:   pkColumns,
				BatchSize:   100,
			},
		)

		err := engine.Cutover(context.Background())

		// Should fail with ErrQueueNotEmpty
		if err == nil {
			t.Fatal("Expected error when queue is not empty, got nil")
		}

		// Verify no RenameTable was called
		ops := notEmptyMock.getOps()
		renameIdx := indexOfOp(ops, "RenameTable")
		if renameIdx != -1 {
			t.Fatalf("Property 18 violated: RenameTable was called despite queue not being empty")
		}
	})
}

// cutoverOrderingNotEmptyMock extends cutoverOrderingMock but returns false for IsCDCQueueEmpty.
type cutoverOrderingNotEmptyMock struct {
	*cutoverOrderingMock
}

func (m *cutoverOrderingNotEmptyMock) IsCDCQueueEmpty(schema, table string) (bool, error) {
	m.cutoverOrderingMock.record("IsCDCQueueEmpty", schema, table)
	return false, nil // queue is NOT empty
}

// --- Property 19: Advisory Lock Acquired Before Table Locks ---

func TestProperty19_AdvisoryLockBeforeTableLocks(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{1,10}`).Draw(t, "schema")
		sourceTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,12}`).Draw(t, "sourceTable")
		targetTable := sourceTable + "_partitioned"

		numPKCols := rapid.IntRange(1, 3).Draw(t, "numPKCols")
		pkColumns := make([]string, numPKCols)
		for i := range pkColumns {
			pkColumns[i] = rapid.StringMatching(`[a-z][a-z0-9_]{1,8}`).Draw(t, "pkCol")
		}

		numFKs := rapid.IntRange(0, 3).Draw(t, "numFKs")
		fks := make([]postgresql.ForeignKeyDef, numFKs)
		for i := range fks {
			fks[i] = postgresql.ForeignKeyDef{
				Name:              rapid.StringMatching(`fk_[a-z]{2,8}`).Draw(t, "fkName"),
				Columns:           []string{"col1"},
				ReferencedSchema:  schema,
				ReferencedTable:   rapid.StringMatching(`[a-z][a-z0-9_]{2,8}`).Draw(t, "childTable"),
				ReferencedColumns: []string{"id"},
				OnDelete:          "NO ACTION",
				OnUpdate:          "NO ACTION",
			}
		}

		mock := &cutoverOrderingMock{fks: fks}
		ops, err := runCutoverWithMock(mock, schema, sourceTable, targetTable, pkColumns)
		if err != nil {
			t.Fatalf("cutover failed: %v", err)
		}

		advisoryIdx := indexOfOp(ops, "AcquireAdvisoryLock")
		exclusiveIdx := indexOfOp(ops, "AcquireExclusiveLock")
		shareRowIdx := indexOfOp(ops, "AcquireShareRowExclusiveLock")

		if advisoryIdx == -1 {
			t.Fatal("AcquireAdvisoryLock was not called")
		}
		if exclusiveIdx == -1 {
			t.Fatal("AcquireExclusiveLock was not called")
		}
		if shareRowIdx == -1 {
			t.Fatal("AcquireShareRowExclusiveLock was not called")
		}
		if advisoryIdx >= exclusiveIdx {
			t.Fatalf("Property 19 violated: AcquireAdvisoryLock (idx=%d) must precede AcquireExclusiveLock (idx=%d)", advisoryIdx, exclusiveIdx)
		}
		if advisoryIdx >= shareRowIdx {
			t.Fatalf("Property 19 violated: AcquireAdvisoryLock (idx=%d) must precede AcquireShareRowExclusiveLock (idx=%d)", advisoryIdx, shareRowIdx)
		}
	})
}

// --- Property 20: Deterministic Lock Ordering ---

func TestProperty20_DeterministicLockOrdering_SourceBeforeTarget(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{1,10}`).Draw(t, "schema")
		sourceTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,12}`).Draw(t, "sourceTable")
		targetTable := sourceTable + "_partitioned"

		numPKCols := rapid.IntRange(1, 3).Draw(t, "numPKCols")
		pkColumns := make([]string, numPKCols)
		for i := range pkColumns {
			pkColumns[i] = rapid.StringMatching(`[a-z][a-z0-9_]{1,8}`).Draw(t, "pkCol")
		}

		numFKs := rapid.IntRange(0, 4).Draw(t, "numFKs")
		fks := make([]postgresql.ForeignKeyDef, numFKs)
		for i := range fks {
			fks[i] = postgresql.ForeignKeyDef{
				Name:              rapid.StringMatching(`fk_[a-z]{2,8}`).Draw(t, "fkName"),
				Columns:           []string{"col1"},
				ReferencedSchema:  schema,
				ReferencedTable:   rapid.StringMatching(`[a-z][a-z0-9_]{2,8}`).Draw(t, "childTable"),
				ReferencedColumns: []string{"id"},
				OnDelete:          "NO ACTION",
				OnUpdate:          "NO ACTION",
			}
		}

		mock := &cutoverOrderingMock{fks: fks}
		ops, err := runCutoverWithMock(mock, schema, sourceTable, targetTable, pkColumns)
		if err != nil {
			t.Fatalf("cutover failed: %v", err)
		}

		exclusiveIdx := indexOfOp(ops, "AcquireExclusiveLock")
		shareRowIdx := indexOfOp(ops, "AcquireShareRowExclusiveLock")

		if exclusiveIdx == -1 {
			t.Fatal("AcquireExclusiveLock was not called")
		}
		if shareRowIdx == -1 {
			t.Fatal("AcquireShareRowExclusiveLock was not called")
		}
		if exclusiveIdx >= shareRowIdx {
			t.Fatalf("Property 20 violated: AcquireExclusiveLock on source (idx=%d) must precede AcquireShareRowExclusiveLock on target (idx=%d)", exclusiveIdx, shareRowIdx)
		}
	})
}

// --- Property 21: Referencing FKs Dropped Before Cutover ---

func TestProperty21_FKsDroppedBeforeRenameSwap(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{1,10}`).Draw(t, "schema")
		sourceTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,12}`).Draw(t, "sourceTable")
		targetTable := sourceTable + "_partitioned"
		pkColumns := []string{"id"}

		// Always have at least one FK to test the ordering
		numFKs := rapid.IntRange(1, 5).Draw(t, "numFKs")
		fks := make([]postgresql.ForeignKeyDef, numFKs)
		for i := range fks {
			fks[i] = postgresql.ForeignKeyDef{
				Name:              rapid.StringMatching(`fk_[a-z]{2,8}`).Draw(t, "fkName"),
				Columns:           []string{"ref_id"},
				ReferencedSchema:  schema,
				ReferencedTable:   rapid.StringMatching(`child_[a-z]{2,6}`).Draw(t, "childTable"),
				ReferencedColumns: []string{"id"},
				OnDelete:          "CASCADE",
				OnUpdate:          "NO ACTION",
			}
		}

		mock := &cutoverOrderingMock{fks: fks}
		ops, err := runCutoverWithMock(mock, schema, sourceTable, targetTable, pkColumns)
		if err != nil {
			t.Fatalf("cutover failed: %v", err)
		}

		// Find the last DropForeignKey and the first RenameTable
		lastDropFKIdx := lastIndexOfOp(ops, "DropForeignKey")
		firstRenameIdx := indexOfOp(ops, "RenameTable")

		if lastDropFKIdx == -1 {
			t.Fatal("DropForeignKey was not called despite having referencing FKs")
		}
		if firstRenameIdx == -1 {
			t.Fatal("RenameTable was not called")
		}
		if lastDropFKIdx >= firstRenameIdx {
			t.Fatalf("Property 21 violated: last DropForeignKey (idx=%d) must precede first RenameTable (idx=%d)", lastDropFKIdx, firstRenameIdx)
		}
	})
}

func TestProperty21_NoFKs_RenameStillProceeds(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{1,10}`).Draw(t, "schema")
		sourceTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,12}`).Draw(t, "sourceTable")
		targetTable := sourceTable + "_partitioned"
		pkColumns := []string{"id"}

		// No FKs
		mock := &cutoverOrderingMock{fks: nil}
		ops, err := runCutoverWithMock(mock, schema, sourceTable, targetTable, pkColumns)
		if err != nil {
			t.Fatalf("cutover failed: %v", err)
		}

		// No DropForeignKey should be called
		dropFKIdx := indexOfOp(ops, "DropForeignKey")
		if dropFKIdx != -1 {
			t.Fatal("DropForeignKey was called despite having no referencing FKs")
		}

		// RenameTable should still be called
		renameIdx := indexOfOp(ops, "RenameTable")
		if renameIdx == -1 {
			t.Fatal("RenameTable was not called")
		}
	})
}

// --- Property 22: ANALYZE Executed After Cutover ---

func TestProperty22_AnalyzeAfterCommit(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{1,10}`).Draw(t, "schema")
		sourceTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,12}`).Draw(t, "sourceTable")
		targetTable := sourceTable + "_partitioned"

		numPKCols := rapid.IntRange(1, 3).Draw(t, "numPKCols")
		pkColumns := make([]string, numPKCols)
		for i := range pkColumns {
			pkColumns[i] = rapid.StringMatching(`[a-z][a-z0-9_]{1,8}`).Draw(t, "pkCol")
		}

		numFKs := rapid.IntRange(0, 3).Draw(t, "numFKs")
		fks := make([]postgresql.ForeignKeyDef, numFKs)
		for i := range fks {
			fks[i] = postgresql.ForeignKeyDef{
				Name:              rapid.StringMatching(`fk_[a-z]{2,8}`).Draw(t, "fkName"),
				Columns:           []string{"col1"},
				ReferencedSchema:  schema,
				ReferencedTable:   rapid.StringMatching(`[a-z][a-z0-9_]{2,8}`).Draw(t, "childTable"),
				ReferencedColumns: []string{"id"},
				OnDelete:          "NO ACTION",
				OnUpdate:          "NO ACTION",
			}
		}

		mock := &cutoverOrderingMock{fks: fks}
		ops, err := runCutoverWithMock(mock, schema, sourceTable, targetTable, pkColumns)
		if err != nil {
			t.Fatalf("cutover failed: %v", err)
		}

		commitIdx := indexOfOp(ops, "Commit")
		analyzeIdx := indexOfOp(ops, "AnalyzeTable")

		if commitIdx == -1 {
			t.Fatal("Commit was not called")
		}
		if analyzeIdx == -1 {
			t.Fatal("AnalyzeTable was not called")
		}
		if analyzeIdx <= commitIdx {
			t.Fatalf("Property 22 violated: AnalyzeTable (idx=%d) must come after Commit (idx=%d)", analyzeIdx, commitIdx)
		}
	})
}

func TestProperty22_AnalyzeAfterCommit_WithIndexes(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{1,10}`).Draw(t, "schema")
		sourceTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,12}`).Draw(t, "sourceTable")
		targetTable := sourceTable + "_partitioned"
		pkColumns := []string{"id"}

		// Generate some indexes for post-cutover rename
		numIndexes := rapid.IntRange(1, 4).Draw(t, "numIndexes")
		indexes := make([]postgresql.IndexDef, numIndexes)
		for i := range indexes {
			suffix := rapid.StringMatching(`_[a-z]{2,6}_idx`).Draw(t, "suffix")
			indexes[i] = postgresql.IndexDef{
				Name:    targetTable + suffix,
				Columns: []string{"col1"},
				Method:  "btree",
			}
		}

		mock := &cutoverOrderingMock{fks: nil, indexes: indexes}
		ops, err := runCutoverWithMock(mock, schema, sourceTable, targetTable, pkColumns)
		if err != nil {
			t.Fatalf("cutover failed: %v", err)
		}

		commitIdx := indexOfOp(ops, "Commit")
		analyzeIdx := indexOfOp(ops, "AnalyzeTable")

		if commitIdx == -1 {
			t.Fatal("Commit was not called")
		}
		if analyzeIdx == -1 {
			t.Fatal("AnalyzeTable was not called")
		}
		if analyzeIdx <= commitIdx {
			t.Fatalf("Property 22 violated: AnalyzeTable (idx=%d) must come after Commit (idx=%d)", analyzeIdx, commitIdx)
		}
	})
}

// --- Property 23: Statement Timeout Set During Cutover ---

func TestProperty23_BeginTxBeforeLockOperations(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{1,10}`).Draw(t, "schema")
		sourceTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,12}`).Draw(t, "sourceTable")
		targetTable := sourceTable + "_partitioned"

		numPKCols := rapid.IntRange(1, 3).Draw(t, "numPKCols")
		pkColumns := make([]string, numPKCols)
		for i := range pkColumns {
			pkColumns[i] = rapid.StringMatching(`[a-z][a-z0-9_]{1,8}`).Draw(t, "pkCol")
		}

		numFKs := rapid.IntRange(0, 3).Draw(t, "numFKs")
		fks := make([]postgresql.ForeignKeyDef, numFKs)
		for i := range fks {
			fks[i] = postgresql.ForeignKeyDef{
				Name:              rapid.StringMatching(`fk_[a-z]{2,8}`).Draw(t, "fkName"),
				Columns:           []string{"col1"},
				ReferencedSchema:  schema,
				ReferencedTable:   rapid.StringMatching(`[a-z][a-z0-9_]{2,8}`).Draw(t, "childTable"),
				ReferencedColumns: []string{"id"},
				OnDelete:          "NO ACTION",
				OnUpdate:          "NO ACTION",
			}
		}

		mock := &cutoverOrderingMock{fks: fks}
		ops, err := runCutoverWithMock(mock, schema, sourceTable, targetTable, pkColumns)
		if err != nil {
			t.Fatalf("cutover failed: %v", err)
		}

		beginTxIdx := indexOfOp(ops, "BeginTx")
		advisoryIdx := indexOfOp(ops, "AcquireAdvisoryLock")
		exclusiveIdx := indexOfOp(ops, "AcquireExclusiveLock")
		shareRowIdx := indexOfOp(ops, "AcquireShareRowExclusiveLock")

		if beginTxIdx == -1 {
			t.Fatal("BeginTx was not called")
		}
		if advisoryIdx == -1 {
			t.Fatal("AcquireAdvisoryLock was not called")
		}
		if exclusiveIdx == -1 {
			t.Fatal("AcquireExclusiveLock was not called")
		}
		if shareRowIdx == -1 {
			t.Fatal("AcquireShareRowExclusiveLock was not called")
		}

		// BeginTx (which sets lock_timeout and statement_timeout) must precede all lock operations
		if beginTxIdx >= advisoryIdx {
			t.Fatalf("Property 23 violated: BeginTx (idx=%d) must precede AcquireAdvisoryLock (idx=%d)", beginTxIdx, advisoryIdx)
		}
		if beginTxIdx >= exclusiveIdx {
			t.Fatalf("Property 23 violated: BeginTx (idx=%d) must precede AcquireExclusiveLock (idx=%d)", beginTxIdx, exclusiveIdx)
		}
		if beginTxIdx >= shareRowIdx {
			t.Fatalf("Property 23 violated: BeginTx (idx=%d) must precede AcquireShareRowExclusiveLock (idx=%d)", beginTxIdx, shareRowIdx)
		}
	})
}

func TestProperty23_BeginTxBeforeAllMutations(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{1,10}`).Draw(t, "schema")
		sourceTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,12}`).Draw(t, "sourceTable")
		targetTable := sourceTable + "_partitioned"
		pkColumns := []string{"id"}

		mock := &cutoverOrderingMock{fks: nil}
		ops, err := runCutoverWithMock(mock, schema, sourceTable, targetTable, pkColumns)
		if err != nil {
			t.Fatalf("cutover failed: %v", err)
		}

		beginTxIdx := indexOfOp(ops, "BeginTx")
		disableIdx := indexOfOp(ops, "DisableTrigger")
		renameIdx := indexOfOp(ops, "RenameTable")

		if beginTxIdx == -1 {
			t.Fatal("BeginTx was not called")
		}

		// BeginTx must precede DisableTrigger and RenameTable
		if disableIdx != -1 && beginTxIdx >= disableIdx {
			t.Fatalf("Property 23 violated: BeginTx (idx=%d) must precede DisableTrigger (idx=%d)", beginTxIdx, disableIdx)
		}
		if renameIdx != -1 && beginTxIdx >= renameIdx {
			t.Fatalf("Property 23 violated: BeginTx (idx=%d) must precede RenameTable (idx=%d)", beginTxIdx, renameIdx)
		}
	})
}
