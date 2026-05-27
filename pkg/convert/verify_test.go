package convert

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
)

// verifyMock is a mock implementation of ConvertDBClient for verify tests.
type verifyMock struct {
	mockConvertDBClient
	sourceRowCount int64
	targetRowCount int64
	replayLag      int64
	sourceCountErr error
	targetCountErr error
	replayLagErr   error
}

func (m *verifyMock) GetTableRowCount(schema, table string) (int64, error) {
	if table == "events" {
		return m.sourceRowCount, m.sourceCountErr
	}

	return m.targetRowCount, m.targetCountErr
}

func (m *verifyMock) GetReplayLag(schema, table string) (int64, error) {
	return m.replayLag, m.replayLagErr
}

func TestVerify_ReadyForCutover(t *testing.T) {
	mock := &verifyMock{
		sourceRowCount: 1000,
		targetRowCount: 1000,
		replayLag:      0,
	}

	engine := NewVerifyEngine(
		*slog.New(slog.NewTextHandler(os.Stdout, nil)),
		mock,
		VerifyEngineConfig{
			Schema:      "public",
			SourceTable: "events",
			TargetTable: "events_partitioned",
		},
	)

	result, err := engine.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.ReadyForCutover {
		t.Error("expected ReadyForCutover to be true")
	}

	if result.SourceRowCount != 1000 {
		t.Errorf("expected SourceRowCount=1000, got %d", result.SourceRowCount)
	}

	if result.TargetRowCount != 1000 {
		t.Errorf("expected TargetRowCount=1000, got %d", result.TargetRowCount)
	}

	if result.RowDifference != 0 {
		t.Errorf("expected RowDifference=0, got %d", result.RowDifference)
	}

	if result.ReplayLag != 0 {
		t.Errorf("expected ReplayLag=0, got %d", result.ReplayLag)
	}
}

func TestVerify_ReadyWithReplayLag(t *testing.T) {
	mock := &verifyMock{
		sourceRowCount: 1000,
		targetRowCount: 1000,
		replayLag:      42,
	}

	engine := NewVerifyEngine(
		*slog.New(slog.NewTextHandler(os.Stdout, nil)),
		mock,
		VerifyEngineConfig{
			Schema:      "public",
			SourceTable: "events",
			TargetTable: "events_partitioned",
		},
	)

	result, err := engine.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.ReadyForCutover {
		t.Error("expected ReadyForCutover to be true (replay lag will be applied during cutover)")
	}

	if result.ReplayLag != 42 {
		t.Errorf("expected ReplayLag=42, got %d", result.ReplayLag)
	}

	if result.RowDifference != 0 {
		t.Errorf("expected RowDifference=0, got %d", result.RowDifference)
	}
}

func TestVerify_ReadyWithRowDifference(t *testing.T) {
	mock := &verifyMock{
		sourceRowCount: 1000,
		targetRowCount: 950,
		replayLag:      0,
	}

	engine := NewVerifyEngine(
		*slog.New(slog.NewTextHandler(os.Stdout, nil)),
		mock,
		VerifyEngineConfig{
			Schema:      "public",
			SourceTable: "events",
			TargetTable: "events_partitioned",
		},
	)

	result, err := engine.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.ReadyForCutover {
		t.Error("expected ReadyForCutover to be true (pending changes will be replayed during cutover)")
	}

	if result.RowDifference != 50 {
		t.Errorf("expected RowDifference=50, got %d", result.RowDifference)
	}
}

func TestVerify_ReadyWithTargetHasMoreRows(t *testing.T) {
	mock := &verifyMock{
		sourceRowCount: 900,
		targetRowCount: 1000,
		replayLag:      0,
	}

	engine := NewVerifyEngine(
		*slog.New(slog.NewTextHandler(os.Stdout, nil)),
		mock,
		VerifyEngineConfig{
			Schema:      "public",
			SourceTable: "events",
			TargetTable: "events_partitioned",
		},
	)

	result, err := engine.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.ReadyForCutover {
		t.Error("expected ReadyForCutover to be true")
	}

	// Absolute difference
	if result.RowDifference != 100 {
		t.Errorf("expected RowDifference=100 (absolute), got %d", result.RowDifference)
	}
}

func TestVerify_ReadyWithBothNonZero(t *testing.T) {
	mock := &verifyMock{
		sourceRowCount: 1000,
		targetRowCount: 980,
		replayLag:      5,
	}

	engine := NewVerifyEngine(
		*slog.New(slog.NewTextHandler(os.Stdout, nil)),
		mock,
		VerifyEngineConfig{
			Schema:      "public",
			SourceTable: "events",
			TargetTable: "events_partitioned",
		},
	)

	result, err := engine.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.ReadyForCutover {
		t.Error("expected ReadyForCutover to be true (pending changes will be replayed during cutover)")
	}

	if result.RowDifference != 20 {
		t.Errorf("expected RowDifference=20, got %d", result.RowDifference)
	}

	if result.ReplayLag != 5 {
		t.Errorf("expected ReplayLag=5, got %d", result.ReplayLag)
	}
}

func TestVerify_ErrorGettingSourceRowCount(t *testing.T) {
	mock := &verifyMock{
		sourceCountErr: errors.New("connection refused"),
	}

	engine := NewVerifyEngine(
		*slog.New(slog.NewTextHandler(os.Stdout, nil)),
		mock,
		VerifyEngineConfig{
			Schema:      "public",
			SourceTable: "events",
			TargetTable: "events_partitioned",
		},
	)

	_, err := engine.Verify(context.Background())
	if err == nil {
		t.Fatal("expected error when source row count fails")
	}

	if !errors.Is(err, mock.sourceCountErr) {
		t.Errorf("expected wrapped source count error, got: %v", err)
	}
}

func TestVerify_ErrorGettingTargetRowCount(t *testing.T) {
	mock := &verifyMock{
		sourceRowCount: 1000,
		targetCountErr: errors.New("table not found"),
	}

	engine := NewVerifyEngine(
		*slog.New(slog.NewTextHandler(os.Stdout, nil)),
		mock,
		VerifyEngineConfig{
			Schema:      "public",
			SourceTable: "events",
			TargetTable: "events_partitioned",
		},
	)

	_, err := engine.Verify(context.Background())
	if err == nil {
		t.Fatal("expected error when target row count fails")
	}

	if !errors.Is(err, mock.targetCountErr) {
		t.Errorf("expected wrapped target count error, got: %v", err)
	}
}

func TestVerify_ErrorGettingReplayLag(t *testing.T) {
	mock := &verifyMock{
		sourceRowCount: 1000,
		targetRowCount: 1000,
		replayLagErr:   errors.New("queue table missing"),
	}

	engine := NewVerifyEngine(
		*slog.New(slog.NewTextHandler(os.Stdout, nil)),
		mock,
		VerifyEngineConfig{
			Schema:      "public",
			SourceTable: "events",
			TargetTable: "events_partitioned",
		},
	)

	_, err := engine.Verify(context.Background())
	if err == nil {
		t.Fatal("expected error when replay lag fails")
	}

	if !errors.Is(err, mock.replayLagErr) {
		t.Errorf("expected wrapped replay lag error, got: %v", err)
	}
}

func TestVerify_ZeroRows_Ready(t *testing.T) {
	mock := &verifyMock{
		sourceRowCount: 0,
		targetRowCount: 0,
		replayLag:      0,
	}

	engine := NewVerifyEngine(
		*slog.New(slog.NewTextHandler(os.Stdout, nil)),
		mock,
		VerifyEngineConfig{
			Schema:      "public",
			SourceTable: "events",
			TargetTable: "events_partitioned",
		},
	)

	result, err := engine.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.ReadyForCutover {
		t.Error("expected ReadyForCutover to be true when both tables have zero rows and no lag")
	}
}
