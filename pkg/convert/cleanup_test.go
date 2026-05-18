package convert

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
)

// cleanupMockDB is a mock implementation of ConvertDBClient for cleanup tests.
type cleanupMockDB struct {
	mockConvertDBClient

	// Track which operations were called
	dropTriggerCalls         []dropTriggerCall
	dropTriggerFunctionCalls []string
	dropCDCQueueCalls        []string
	dropTableCalls           []dropTableCall
	deleteMigrationCalls     []deleteMigrationCall

	// Control behavior
	tableExistsMap     map[string]bool
	cdcQueueExistsMap  map[string]bool
	migrationState     *postgresql.MigrationState
	migrationStateErr  error
	dropTriggerErr     error
	dropTriggerFuncErr error
	dropCDCQueueErr    error
	dropTableErr       error
	deleteStateErr     error
}

type dropTriggerCall struct {
	schema  string
	table   string
	trigger string
}

type dropTableCall struct {
	schema string
	table  string
}

type deleteMigrationCall struct {
	schema string
	table  string
}

func (m *cleanupMockDB) IsTableExists(schema, table string) (bool, error) {
	key := fmt.Sprintf("%s.%s", schema, table)
	if exists, ok := m.tableExistsMap[key]; ok {
		return exists, nil
	}

	return false, nil
}

func (m *cleanupMockDB) IsCDCQueueExists(schema, table string) (bool, error) {
	key := fmt.Sprintf("%s.%s", schema, table)
	if exists, ok := m.cdcQueueExistsMap[key]; ok {
		return exists, nil
	}

	return false, nil
}

func (m *cleanupMockDB) GetMigrationState(schema, table string) (*postgresql.MigrationState, error) {
	if m.migrationStateErr != nil {
		return nil, m.migrationStateErr
	}

	return m.migrationState, nil
}

func (m *cleanupMockDB) DropTrigger(schema, table, triggerName string) error {
	m.dropTriggerCalls = append(m.dropTriggerCalls, dropTriggerCall{schema, table, triggerName})

	return m.dropTriggerErr
}

func (m *cleanupMockDB) DropTriggerFunction(schema, functionName string) error {
	m.dropTriggerFunctionCalls = append(m.dropTriggerFunctionCalls, functionName)

	return m.dropTriggerFuncErr
}

func (m *cleanupMockDB) DropCDCQueue(schema, table string) error {
	m.dropCDCQueueCalls = append(m.dropCDCQueueCalls, fmt.Sprintf("%s.%s", schema, table))

	return m.dropCDCQueueErr
}

func (m *cleanupMockDB) DropTable(schema, table string) error {
	m.dropTableCalls = append(m.dropTableCalls, dropTableCall{schema, table})

	return m.dropTableErr
}

func (m *cleanupMockDB) DeleteMigrationState(schema, table string) error {
	m.deleteMigrationCalls = append(m.deleteMigrationCalls, deleteMigrationCall{schema, table})

	return m.deleteStateErr
}

func newTestCleanupEngine(db *cleanupMockDB) *CleanupEngine {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	return NewCleanupEngine(*logger, db, CleanupEngineConfig{
		Schema:      "public",
		SourceTable: "events",
	})
}

func TestCleanup_NoConfirmNoForce_ReturnsError(t *testing.T) {
	db := &cleanupMockDB{}
	engine := newTestCleanupEngine(db)

	err := engine.Cleanup(context.Background(), false, false)

	if !errors.Is(err, ErrConfirmRequired) {
		t.Errorf("expected ErrConfirmRequired, got: %v", err)
	}

	// Verify no destructive operations were performed
	if len(db.dropTriggerCalls) > 0 {
		t.Error("expected no trigger drops without confirm/force")
	}

	if len(db.dropCDCQueueCalls) > 0 {
		t.Error("expected no CDC queue drops without confirm/force")
	}

	if len(db.dropTableCalls) > 0 {
		t.Error("expected no table drops without confirm/force")
	}

	if len(db.deleteMigrationCalls) > 0 {
		t.Error("expected no metadata deletions without confirm/force")
	}
}

func TestCleanup_ConfirmWithInvalidPhase_ReturnsError(t *testing.T) {
	db := &cleanupMockDB{
		migrationState: &postgresql.MigrationState{
			Schema: "public",
			Table:  "events",
			Phase:  string(PhaseBackfill),
		},
	}
	engine := newTestCleanupEngine(db)

	err := engine.Cleanup(context.Background(), true, false)

	if err == nil {
		t.Fatal("expected error for invalid phase")
	}

	if !errors.Is(err, ErrCleanupPhaseInvalid) {
		t.Errorf("expected ErrCleanupPhaseInvalid, got: %v", err)
	}
}

func TestCleanup_ConfirmWithCutoverPhase_Succeeds(t *testing.T) {
	db := &cleanupMockDB{
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
	engine := newTestCleanupEngine(db)

	err := engine.Cleanup(context.Background(), true, false)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify all cleanup operations were performed in order
	if len(db.dropTriggerCalls) != 1 {
		t.Errorf("expected 1 trigger drop, got %d", len(db.dropTriggerCalls))
	} else {
		if db.dropTriggerCalls[0].table != "events_old" {
			t.Errorf("expected trigger drop on events_old, got %s", db.dropTriggerCalls[0].table)
		}

		if db.dropTriggerCalls[0].trigger != "ppm_cdc_events" {
			t.Errorf("expected trigger name ppm_cdc_events, got %s", db.dropTriggerCalls[0].trigger)
		}
	}

	if len(db.dropTriggerFunctionCalls) != 1 {
		t.Errorf("expected 1 trigger function drop, got %d", len(db.dropTriggerFunctionCalls))
	} else if db.dropTriggerFunctionCalls[0] != "ppm_cdc_trigger_events" {
		t.Errorf("expected function name ppm_cdc_trigger_events, got %s", db.dropTriggerFunctionCalls[0])
	}

	if len(db.dropCDCQueueCalls) != 1 {
		t.Errorf("expected 1 CDC queue drop, got %d", len(db.dropCDCQueueCalls))
	}

	if len(db.dropTableCalls) != 1 {
		t.Errorf("expected 1 table drop, got %d", len(db.dropTableCalls))
	} else if db.dropTableCalls[0].table != "events_old" {
		t.Errorf("expected table drop on events_old, got %s", db.dropTableCalls[0].table)
	}

	if len(db.deleteMigrationCalls) != 1 {
		t.Errorf("expected 1 metadata deletion, got %d", len(db.deleteMigrationCalls))
	}
}

func TestCleanup_ForceMode_BypassesPhaseValidation(t *testing.T) {
	db := &cleanupMockDB{
		migrationState: &postgresql.MigrationState{
			Schema: "public",
			Table:  "events",
			Phase:  string(PhaseBackfill), // Not in post-cutover phase
		},
		tableExistsMap: map[string]bool{
			"public.events_old": true,
		},
		cdcQueueExistsMap: map[string]bool{
			"public.events": true,
		},
	}
	engine := newTestCleanupEngine(db)

	err := engine.Cleanup(context.Background(), false, true)

	if err != nil {
		t.Fatalf("unexpected error with force mode: %v", err)
	}

	// Verify all cleanup operations were performed
	if len(db.dropTriggerCalls) != 1 {
		t.Errorf("expected 1 trigger drop with force, got %d", len(db.dropTriggerCalls))
	}

	if len(db.dropCDCQueueCalls) != 1 {
		t.Errorf("expected 1 CDC queue drop with force, got %d", len(db.dropCDCQueueCalls))
	}

	if len(db.dropTableCalls) != 1 {
		t.Errorf("expected 1 table drop with force, got %d", len(db.dropTableCalls))
	}

	if len(db.deleteMigrationCalls) != 1 {
		t.Errorf("expected 1 metadata deletion with force, got %d", len(db.deleteMigrationCalls))
	}
}

func TestCleanup_SkipsAlreadyRemovedArtifacts(t *testing.T) {
	db := &cleanupMockDB{
		migrationState: &postgresql.MigrationState{
			Schema: "public",
			Table:  "events",
			Phase:  string(PhaseCutover),
		},
		tableExistsMap: map[string]bool{
			"public.events_old": false, // Already removed
		},
		cdcQueueExistsMap: map[string]bool{
			"public.events": false, // Already removed
		},
	}
	engine := newTestCleanupEngine(db)

	err := engine.Cleanup(context.Background(), true, false)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Trigger drop should be skipped because source_old doesn't exist
	if len(db.dropTriggerCalls) != 0 {
		t.Errorf("expected 0 trigger drops (table doesn't exist), got %d", len(db.dropTriggerCalls))
	}

	// CDC queue drop should be skipped
	if len(db.dropCDCQueueCalls) != 0 {
		t.Errorf("expected 0 CDC queue drops (doesn't exist), got %d", len(db.dropCDCQueueCalls))
	}

	// Table drop should be skipped
	if len(db.dropTableCalls) != 0 {
		t.Errorf("expected 0 table drops (doesn't exist), got %d", len(db.dropTableCalls))
	}

	// Metadata deletion should still happen
	if len(db.deleteMigrationCalls) != 1 {
		t.Errorf("expected 1 metadata deletion, got %d", len(db.deleteMigrationCalls))
	}
}

func TestCleanup_NoMigrationState_SkipsMetadataDeletion(t *testing.T) {
	db := &cleanupMockDB{
		migrationState: nil, // No state exists
		tableExistsMap: map[string]bool{
			"public.events_old": false,
		},
		cdcQueueExistsMap: map[string]bool{
			"public.events": false,
		},
	}
	engine := newTestCleanupEngine(db)

	// With force mode since there's no state to validate phase
	err := engine.Cleanup(context.Background(), false, true)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Metadata deletion should be skipped since no state exists
	if len(db.deleteMigrationCalls) != 0 {
		t.Errorf("expected 0 metadata deletions (no state), got %d", len(db.deleteMigrationCalls))
	}
}

func TestCleanup_DropCDCQueueError_ReturnsError(t *testing.T) {
	db := &cleanupMockDB{
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
		dropCDCQueueErr: errors.New("permission denied"),
	}
	engine := newTestCleanupEngine(db)

	err := engine.Cleanup(context.Background(), true, false)

	if err == nil {
		t.Fatal("expected error when CDC queue drop fails")
	}

	if !errors.Is(err, db.dropCDCQueueErr) {
		t.Errorf("expected permission denied error, got: %v", err)
	}
}

func TestCleanup_DropTableError_ReturnsError(t *testing.T) {
	db := &cleanupMockDB{
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
		dropTableErr: errors.New("table in use"),
	}
	engine := newTestCleanupEngine(db)

	err := engine.Cleanup(context.Background(), true, false)

	if err == nil {
		t.Fatal("expected error when table drop fails")
	}

	if !errors.Is(err, db.dropTableErr) {
		t.Errorf("expected 'table in use' error, got: %v", err)
	}
}

func TestCleanup_ConfirmWithNoState_ProceedsGracefully(t *testing.T) {
	db := &cleanupMockDB{
		migrationState: nil, // No state - validateCleanupPhase allows this
		tableExistsMap: map[string]bool{
			"public.events_old": true,
		},
		cdcQueueExistsMap: map[string]bool{
			"public.events": true,
		},
	}
	engine := newTestCleanupEngine(db)

	err := engine.Cleanup(context.Background(), true, false)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// All operations should proceed
	if len(db.dropTriggerCalls) != 1 {
		t.Errorf("expected 1 trigger drop, got %d", len(db.dropTriggerCalls))
	}

	if len(db.dropCDCQueueCalls) != 1 {
		t.Errorf("expected 1 CDC queue drop, got %d", len(db.dropCDCQueueCalls))
	}

	if len(db.dropTableCalls) != 1 {
		t.Errorf("expected 1 table drop, got %d", len(db.dropTableCalls))
	}
}

func TestCleanup_ForceWithConfirm_Succeeds(t *testing.T) {
	// When both flags are provided, force takes precedence (no phase validation)
	db := &cleanupMockDB{
		migrationState: &postgresql.MigrationState{
			Schema: "public",
			Table:  "events",
			Phase:  string(PhaseReplay), // Not post-cutover
		},
		tableExistsMap: map[string]bool{
			"public.events_old": true,
		},
		cdcQueueExistsMap: map[string]bool{
			"public.events": true,
		},
	}
	engine := newTestCleanupEngine(db)

	// Both confirm and force provided - force bypasses phase check
	err := engine.Cleanup(context.Background(), true, true)

	if err != nil {
		t.Fatalf("unexpected error with both confirm and force: %v", err)
	}
}

func TestCleanup_TriggerDropError_ContinuesGracefully(t *testing.T) {
	// Trigger drop errors are logged as warnings but don't stop cleanup
	db := &cleanupMockDB{
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
		dropTriggerErr: errors.New("trigger does not exist"),
	}
	engine := newTestCleanupEngine(db)

	err := engine.Cleanup(context.Background(), true, false)

	if err != nil {
		t.Fatalf("unexpected error (trigger drop errors should be non-fatal): %v", err)
	}

	// Other operations should still proceed
	if len(db.dropCDCQueueCalls) != 1 {
		t.Errorf("expected 1 CDC queue drop, got %d", len(db.dropCDCQueueCalls))
	}

	if len(db.dropTableCalls) != 1 {
		t.Errorf("expected 1 table drop, got %d", len(db.dropTableCalls))
	}
}
