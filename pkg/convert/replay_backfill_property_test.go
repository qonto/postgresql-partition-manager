package convert

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
	"pgregory.net/rapid"
)

// Feature: table-partition-conversion, Property 8: Replay Event Dispatch
// For any CDC event, if the operation is INSERT or UPDATE the replay engine SHALL execute
// an upsert on the target table, and if the operation is DELETE the replay engine SHALL
// execute a delete by primary key on the target table.
// Validates: Requirements 5.2, 5.3

// Feature: table-partition-conversion, Property 13: Backfill Resumability
// For any sequence of successful backfill batches where each batch records its last processed
// primary key, re-executing backfill SHALL resume processing from the row immediately following
// the last recorded primary key value, never re-processing already-completed rows.
// Validates: Requirements 4.4

// --- Mock DB Client for Property Tests ---

// replayTrackingMock tracks which operations are called during replay dispatch.
type replayTrackingMock struct {
	mockConvertDBClient
	upsertCalls []upsertCall
	deleteCalls []deleteCall
}

type upsertCall struct {
	schema      string
	targetTable string
	sourceTable string
	pkColumns   []string
	pkValues    []string
}

type deleteCall struct {
	schema      string
	targetTable string
	pkColumns   []string
	pkValues    []string
}

func (m *replayTrackingMock) ApplyUpsert(schema, targetTable, sourceTable string, pkColumns []string, pkValues []string) error {
	m.upsertCalls = append(m.upsertCalls, upsertCall{
		schema:      schema,
		targetTable: targetTable,
		sourceTable: sourceTable,
		pkColumns:   pkColumns,
		pkValues:    pkValues,
	})

	return nil
}

func (m *replayTrackingMock) ApplyDelete(schema, targetTable string, pkColumns []string, pkValues []string) error {
	m.deleteCalls = append(m.deleteCalls, deleteCall{
		schema:      schema,
		targetTable: targetTable,
		pkColumns:   pkColumns,
		pkValues:    pkValues,
	})

	return nil
}

// backfillResumeMock tracks BackfillBatch calls and records the afterPK parameter
// to verify resumability behavior.
type backfillResumeMock struct {
	mockConvertDBClient
	migrationState   *postgresql.MigrationState
	batchCallAfterPK [][]any // Records the afterPK argument for each BackfillBatch call
	batchResults     []backfillBatchResult
	batchCallIdx     int
}

type backfillBatchResult struct {
	lastPK     []any
	rowsCopied int64
}

func (m *backfillResumeMock) GetMigrationState(schema, table string) (*postgresql.MigrationState, error) {
	return m.migrationState, nil
}

func (m *backfillResumeMock) UpdateMigrationState(schema, table string, state *postgresql.MigrationState) error {
	m.migrationState = state

	return nil
}

func (m *backfillResumeMock) GetTableRowCount(schema, table string) (int64, error) {
	return 1000, nil // Arbitrary non-zero count
}

func (m *backfillResumeMock) BackfillBatch(schema, sourceTable, targetTable string, pkColumns []string, afterPK []any, batchSize int) ([]any, int64, error) {
	m.batchCallAfterPK = append(m.batchCallAfterPK, afterPK)

	if m.batchCallIdx >= len(m.batchResults) {
		// No more batches — signal completion
		return nil, 0, nil
	}

	result := m.batchResults[m.batchCallIdx]
	m.batchCallIdx++

	return result.lastPK, result.rowsCopied, nil
}

// --- Property 8 Tests ---

func TestProperty8_ReplayEventDispatch_InsertTriggersUpsert(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate arbitrary CDC event with INSERT operation
		seqID := rapid.Int64Range(1, 1000000).Draw(t, "seqID")
		numPKValues := rapid.IntRange(1, 5).Draw(t, "numPKValues")
		pkValues := make([]string, numPKValues)

		for i := range pkValues {
			pkValues[i] = rapid.StringMatching(`[a-zA-Z0-9]{1,20}`).Draw(t, fmt.Sprintf("pkValue_%d", i))
		}

		event := postgresql.CDCEvent{
			SeqID:     seqID,
			Operation: "INSERT",
			PKValues:  pkValues,
			CreatedAt: time.Now(),
		}

		mock := &replayTrackingMock{}
		pkColumns := make([]string, numPKValues)

		for i := range pkColumns {
			pkColumns[i] = fmt.Sprintf("col_%d", i)
		}

		engine := NewReplayEngine(*slog.Default(), mock, ReplayEngineConfig{
			Schema:      "public",
			SourceTable: "source",
			TargetTable: "target",
			PKColumns:   pkColumns,
			BatchSize:   1000,
		})

		err := engine.dispatchEvent(event)
		if err != nil {
			t.Fatalf("dispatchEvent returned error for INSERT event: %v", err)
		}

		// INSERT must trigger exactly one upsert call
		if len(mock.upsertCalls) != 1 {
			t.Fatalf("expected exactly 1 upsert call for INSERT, got %d", len(mock.upsertCalls))
		}

		// No delete calls
		if len(mock.deleteCalls) != 0 {
			t.Fatalf("expected 0 delete calls for INSERT, got %d", len(mock.deleteCalls))
		}

		// Verify PK values passed correctly
		call := mock.upsertCalls[0]
		if len(call.pkValues) != len(pkValues) {
			t.Fatalf("expected %d PK values in upsert call, got %d", len(pkValues), len(call.pkValues))
		}

		for i, v := range call.pkValues {
			if v != pkValues[i] {
				t.Fatalf("upsert pkValues[%d] = %q, expected %q", i, v, pkValues[i])
			}
		}
	})
}

func TestProperty8_ReplayEventDispatch_UpdateTriggersUpsert(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate arbitrary CDC event with UPDATE operation
		seqID := rapid.Int64Range(1, 1000000).Draw(t, "seqID")
		numPKValues := rapid.IntRange(1, 5).Draw(t, "numPKValues")
		pkValues := make([]string, numPKValues)

		for i := range pkValues {
			pkValues[i] = rapid.StringMatching(`[a-zA-Z0-9]{1,20}`).Draw(t, fmt.Sprintf("pkValue_%d", i))
		}

		event := postgresql.CDCEvent{
			SeqID:     seqID,
			Operation: "UPDATE",
			PKValues:  pkValues,
			CreatedAt: time.Now(),
		}

		mock := &replayTrackingMock{}
		pkColumns := make([]string, numPKValues)

		for i := range pkColumns {
			pkColumns[i] = fmt.Sprintf("col_%d", i)
		}

		engine := NewReplayEngine(*slog.Default(), mock, ReplayEngineConfig{
			Schema:      "public",
			SourceTable: "source",
			TargetTable: "target",
			PKColumns:   pkColumns,
			BatchSize:   1000,
		})

		err := engine.dispatchEvent(event)
		if err != nil {
			t.Fatalf("dispatchEvent returned error for UPDATE event: %v", err)
		}

		// UPDATE must trigger exactly one upsert call
		if len(mock.upsertCalls) != 1 {
			t.Fatalf("expected exactly 1 upsert call for UPDATE, got %d", len(mock.upsertCalls))
		}

		// No delete calls
		if len(mock.deleteCalls) != 0 {
			t.Fatalf("expected 0 delete calls for UPDATE, got %d", len(mock.deleteCalls))
		}

		// Verify PK values passed correctly
		call := mock.upsertCalls[0]
		if len(call.pkValues) != len(pkValues) {
			t.Fatalf("expected %d PK values in upsert call, got %d", len(pkValues), len(call.pkValues))
		}

		for i, v := range call.pkValues {
			if v != pkValues[i] {
				t.Fatalf("upsert pkValues[%d] = %q, expected %q", i, v, pkValues[i])
			}
		}
	})
}

func TestProperty8_ReplayEventDispatch_DeleteTriggersDelete(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate arbitrary CDC event with DELETE operation
		seqID := rapid.Int64Range(1, 1000000).Draw(t, "seqID")
		numPKValues := rapid.IntRange(1, 5).Draw(t, "numPKValues")
		pkValues := make([]string, numPKValues)

		for i := range pkValues {
			pkValues[i] = rapid.StringMatching(`[a-zA-Z0-9]{1,20}`).Draw(t, fmt.Sprintf("pkValue_%d", i))
		}

		event := postgresql.CDCEvent{
			SeqID:     seqID,
			Operation: "DELETE",
			PKValues:  pkValues,
			CreatedAt: time.Now(),
		}

		mock := &replayTrackingMock{}
		pkColumns := make([]string, numPKValues)

		for i := range pkColumns {
			pkColumns[i] = fmt.Sprintf("col_%d", i)
		}

		engine := NewReplayEngine(*slog.Default(), mock, ReplayEngineConfig{
			Schema:      "public",
			SourceTable: "source",
			TargetTable: "target",
			PKColumns:   pkColumns,
			BatchSize:   1000,
		})

		err := engine.dispatchEvent(event)
		if err != nil {
			t.Fatalf("dispatchEvent returned error for DELETE event: %v", err)
		}

		// DELETE must trigger exactly one delete call
		if len(mock.deleteCalls) != 1 {
			t.Fatalf("expected exactly 1 delete call for DELETE, got %d", len(mock.deleteCalls))
		}

		// No upsert calls
		if len(mock.upsertCalls) != 0 {
			t.Fatalf("expected 0 upsert calls for DELETE, got %d", len(mock.upsertCalls))
		}

		// Verify PK values passed correctly
		call := mock.deleteCalls[0]
		if len(call.pkValues) != len(pkValues) {
			t.Fatalf("expected %d PK values in delete call, got %d", len(pkValues), len(call.pkValues))
		}

		for i, v := range call.pkValues {
			if v != pkValues[i] {
				t.Fatalf("delete pkValues[%d] = %q, expected %q", i, v, pkValues[i])
			}
		}
	})
}

func TestProperty8_ReplayEventDispatch_OperationDeterminesAction(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate arbitrary CDC event with random valid operation
		operations := []string{"INSERT", "UPDATE", "DELETE"}
		opIdx := rapid.IntRange(0, len(operations)-1).Draw(t, "operationIdx")
		operation := operations[opIdx]

		seqID := rapid.Int64Range(1, 1000000).Draw(t, "seqID")
		numPKValues := rapid.IntRange(1, 5).Draw(t, "numPKValues")
		pkValues := make([]string, numPKValues)

		for i := range pkValues {
			pkValues[i] = rapid.StringMatching(`[a-zA-Z0-9]{1,20}`).Draw(t, fmt.Sprintf("pkValue_%d", i))
		}

		event := postgresql.CDCEvent{
			SeqID:     seqID,
			Operation: operation,
			PKValues:  pkValues,
			CreatedAt: time.Now(),
		}

		mock := &replayTrackingMock{}
		pkColumns := make([]string, numPKValues)

		for i := range pkColumns {
			pkColumns[i] = fmt.Sprintf("col_%d", i)
		}

		engine := NewReplayEngine(*slog.Default(), mock, ReplayEngineConfig{
			Schema:      "public",
			SourceTable: "source",
			TargetTable: "target",
			PKColumns:   pkColumns,
			BatchSize:   1000,
		})

		err := engine.dispatchEvent(event)
		if err != nil {
			t.Fatalf("dispatchEvent returned error for %s event: %v", operation, err)
		}

		switch operation {
		case "INSERT", "UPDATE":
			if len(mock.upsertCalls) != 1 {
				t.Fatalf("expected 1 upsert call for %s, got %d", operation, len(mock.upsertCalls))
			}

			if len(mock.deleteCalls) != 0 {
				t.Fatalf("expected 0 delete calls for %s, got %d", operation, len(mock.deleteCalls))
			}
		case "DELETE":
			if len(mock.deleteCalls) != 1 {
				t.Fatalf("expected 1 delete call for DELETE, got %d", len(mock.deleteCalls))
			}

			if len(mock.upsertCalls) != 0 {
				t.Fatalf("expected 0 upsert calls for DELETE, got %d", len(mock.upsertCalls))
			}
		}
	})
}

// --- Property 13 Tests ---

func TestProperty13_BackfillResumability_ResumesFromLastRecordedPK(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a sequence of batch results simulating successful batches
		numBatches := rapid.IntRange(1, 10).Draw(t, "numBatches")
		batchResults := make([]backfillBatchResult, numBatches)

		// Generate increasing PK values for each batch
		for i := range batchResults {
			pkValue := fmt.Sprintf("%d", (i+1)*100)
			batchResults[i] = backfillBatchResult{
				lastPK:     []any{pkValue},
				rowsCopied: int64(rapid.IntRange(1, 100).Draw(t, fmt.Sprintf("rowsCopied_%d", i))),
			}
		}

		// First run: start from nil (no previous state)
		mock1 := &backfillResumeMock{
			migrationState: &postgresql.MigrationState{
				Schema:         "public",
				Table:          "events",
				Phase:          "backfill",
				LastBackfillPK: nil, // No previous progress
			},
			batchResults: batchResults,
		}

		engine1 := NewBackfillEngine(*slog.Default(), mock1, BackfillEngineConfig{
			Schema:      "public",
			SourceTable: "events",
			TargetTable: "events_partitioned",
			PKColumns:   []string{"id"},
			BatchSize:   100,
		})

		err := engine1.Run(context.Background())
		if err != nil {
			t.Fatalf("first backfill run failed: %v", err)
		}

		// Verify first batch was called with nil afterPK (start from beginning)
		if len(mock1.batchCallAfterPK) == 0 {
			t.Fatal("expected at least one batch call in first run")
		}

		if mock1.batchCallAfterPK[0] != nil {
			t.Fatalf("first run should start with nil afterPK, got %v", mock1.batchCallAfterPK[0])
		}

		// Get the last PK that was recorded
		lastRecordedPK := mock1.migrationState.LastBackfillPK
		if len(lastRecordedPK) == 0 {
			t.Fatal("expected migration state to have recorded last PK after first run")
		}

		// Second run: should resume from the last recorded PK
		// Simulate one more batch available after the resume point
		resumeBatchResults := []backfillBatchResult{
			{
				lastPK:     []any{fmt.Sprintf("%d", (numBatches+1)*100)},
				rowsCopied: 50,
			},
		}

		mock2 := &backfillResumeMock{
			migrationState: &postgresql.MigrationState{
				Schema:         "public",
				Table:          "events",
				Phase:          "backfill",
				LastBackfillPK: lastRecordedPK, // Resume from last recorded PK
			},
			batchResults: resumeBatchResults,
		}

		engine2 := NewBackfillEngine(*slog.Default(), mock2, BackfillEngineConfig{
			Schema:      "public",
			SourceTable: "events",
			TargetTable: "events_partitioned",
			PKColumns:   []string{"id"},
			BatchSize:   100,
		})

		err = engine2.Run(context.Background())
		if err != nil {
			t.Fatalf("second backfill run (resume) failed: %v", err)
		}

		// Verify the second run started from the last recorded PK (not nil)
		if len(mock2.batchCallAfterPK) == 0 {
			t.Fatal("expected at least one batch call in second run")
		}

		firstCallAfterPK := mock2.batchCallAfterPK[0]
		if firstCallAfterPK == nil {
			t.Fatal("resumed backfill should NOT start with nil afterPK")
		}

		// The afterPK should match the last recorded PK from the first run
		if len(firstCallAfterPK) != len(lastRecordedPK) {
			t.Fatalf("resumed afterPK length %d != last recorded PK length %d",
				len(firstCallAfterPK), len(lastRecordedPK))
		}

		for i, v := range firstCallAfterPK {
			vStr := fmt.Sprintf("%v", v)
			if vStr != lastRecordedPK[i] {
				t.Fatalf("resumed afterPK[%d] = %q, expected %q (last recorded PK)",
					i, vStr, lastRecordedPK[i])
			}
		}
	})
}

func TestProperty13_BackfillResumability_NeverReprocessesCompletedRows(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a last recorded PK (simulating a previous successful run)
		numPKCols := rapid.IntRange(1, 3).Draw(t, "numPKCols")
		lastRecordedPK := make([]string, numPKCols)

		for i := range lastRecordedPK {
			lastRecordedPK[i] = fmt.Sprintf("%d", rapid.IntRange(1, 10000).Draw(t, fmt.Sprintf("lastPK_%d", i)))
		}

		// Simulate a single batch available after the resume point
		nextPK := make([]any, numPKCols)
		for i := range nextPK {
			nextPK[i] = fmt.Sprintf("%d", rapid.IntRange(10001, 20000).Draw(t, fmt.Sprintf("nextPK_%d", i)))
		}

		batchResults := []backfillBatchResult{
			{
				lastPK:     nextPK,
				rowsCopied: int64(rapid.IntRange(1, 100).Draw(t, "rowsCopied")),
			},
		}

		mock := &backfillResumeMock{
			migrationState: &postgresql.MigrationState{
				Schema:         "public",
				Table:          "events",
				Phase:          "backfill",
				LastBackfillPK: lastRecordedPK,
			},
			batchResults: batchResults,
		}

		pkColumns := make([]string, numPKCols)
		for i := range pkColumns {
			pkColumns[i] = fmt.Sprintf("pk_col_%d", i)
		}

		engine := NewBackfillEngine(*slog.Default(), mock, BackfillEngineConfig{
			Schema:      "public",
			SourceTable: "events",
			TargetTable: "events_partitioned",
			PKColumns:   pkColumns,
			BatchSize:   100,
		})

		err := engine.Run(context.Background())
		if err != nil {
			t.Fatalf("backfill resume run failed: %v", err)
		}

		// Verify the first BackfillBatch call used the last recorded PK as afterPK
		if len(mock.batchCallAfterPK) == 0 {
			t.Fatal("expected at least one batch call")
		}

		firstCallAfterPK := mock.batchCallAfterPK[0]
		if firstCallAfterPK == nil {
			t.Fatalf("expected non-nil afterPK when resuming from %v", lastRecordedPK)
		}

		// The afterPK must equal the last recorded PK — this ensures we start
		// from the NEXT row (the DB query uses WHERE pk > afterPK)
		if len(firstCallAfterPK) != len(lastRecordedPK) {
			t.Fatalf("afterPK length %d != lastRecordedPK length %d",
				len(firstCallAfterPK), len(lastRecordedPK))
		}

		for i, v := range firstCallAfterPK {
			vStr := fmt.Sprintf("%v", v)
			if vStr != lastRecordedPK[i] {
				t.Fatalf("afterPK[%d] = %q, expected %q — would re-process completed rows",
					i, vStr, lastRecordedPK[i])
			}
		}
	})
}

func TestProperty13_BackfillResumability_EachBatchRecordsProgress(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a sequence of batches
		numBatches := rapid.IntRange(1, 5).Draw(t, "numBatches")
		batchResults := make([]backfillBatchResult, numBatches)

		expectedLastPKs := make([]string, numBatches)

		for i := range batchResults {
			pkValue := fmt.Sprintf("%d", (i+1)*100)
			expectedLastPKs[i] = pkValue
			batchResults[i] = backfillBatchResult{
				lastPK:     []any{pkValue},
				rowsCopied: int64(rapid.IntRange(1, 100).Draw(t, fmt.Sprintf("rowsCopied_%d", i))),
			}
		}

		mock := &backfillResumeMock{
			migrationState: &postgresql.MigrationState{
				Schema:         "public",
				Table:          "events",
				Phase:          "backfill",
				LastBackfillPK: nil,
			},
			batchResults: batchResults,
		}

		engine := NewBackfillEngine(*slog.Default(), mock, BackfillEngineConfig{
			Schema:      "public",
			SourceTable: "events",
			TargetTable: "events_partitioned",
			PKColumns:   []string{"id"},
			BatchSize:   100,
		})

		err := engine.Run(context.Background())
		if err != nil {
			t.Fatalf("backfill run failed: %v", err)
		}

		// After all batches complete, the migration state must record the last PK
		// from the final batch
		if mock.migrationState == nil {
			t.Fatal("expected migration state to be non-nil after run")
		}

		finalLastPK := mock.migrationState.LastBackfillPK
		if len(finalLastPK) == 0 {
			t.Fatal("expected migration state to have recorded last PK")
		}

		// The final recorded PK should be the last batch's PK
		expectedFinalPK := expectedLastPKs[numBatches-1]
		if finalLastPK[0] != expectedFinalPK {
			t.Fatalf("final recorded PK = %q, expected %q (last batch PK)",
				finalLastPK[0], expectedFinalPK)
		}
	})
}
