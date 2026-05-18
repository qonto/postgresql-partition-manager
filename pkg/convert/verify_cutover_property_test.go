package convert

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
	"pgregory.net/rapid"
)

// Feature: table-partition-conversion, Property 9: Verify Readiness Determination
// readyForCutover is true if and only if replayLag == 0 AND rowCountDifference == 0.
// Validates: Requirements 6.3, 6.4

// Feature: table-partition-conversion, Property 10: Index Rename Round-Trip
// Renaming an index from target prefix to source prefix (post-cutover), then from source
// prefix back to target prefix (post-rollback), produces the original index name.
// Validates: Requirements 7.5, 8.5

// --- Mock DB Client for Property 9 ---

// verifyPropertyMock is a mock that returns configurable row counts and replay lag.
type verifyPropertyMock struct {
	mockConvertDBClient
	sourceRowCount int64
	targetRowCount int64
	replayLag      int64
	sourceTable    string
}

func (m *verifyPropertyMock) GetTableRowCount(schema, table string) (int64, error) {
	if table == m.sourceTable {
		return m.sourceRowCount, nil
	}

	return m.targetRowCount, nil
}

func (m *verifyPropertyMock) GetReplayLag(schema, table string) (int64, error) {
	return m.replayLag, nil
}

// --- Property 9 Tests ---

func TestProperty9_VerifyReadiness_TrueIffLagZeroAndDiffZero(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate arbitrary non-negative row counts and replay lag
		sourceRowCount := rapid.Int64Range(0, 1000000).Draw(t, "sourceRowCount")
		targetRowCount := rapid.Int64Range(0, 1000000).Draw(t, "targetRowCount")
		replayLag := rapid.Int64Range(0, 100000).Draw(t, "replayLag")

		mock := &verifyPropertyMock{
			sourceRowCount: sourceRowCount,
			targetRowCount: targetRowCount,
			replayLag:      replayLag,
			sourceTable:    "events",
		}

		engine := NewVerifyEngine(
			*slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
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

		// Compute expected readiness
		rowDifference := sourceRowCount - targetRowCount
		if rowDifference < 0 {
			rowDifference = -rowDifference
		}

		expectedReady := replayLag == 0 && rowDifference == 0

		if result.ReadyForCutover != expectedReady {
			t.Fatalf("ReadyForCutover=%v, expected=%v (replayLag=%d, rowDifference=%d, sourceRows=%d, targetRows=%d)",
				result.ReadyForCutover, expectedReady, replayLag, rowDifference, sourceRowCount, targetRowCount)
		}
	})
}

func TestProperty9_VerifyReadiness_TrueOnlyWhenBothConditionsMet(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate cases where at least one condition is NOT met
		sourceRowCount := rapid.Int64Range(0, 1000000).Draw(t, "sourceRowCount")
		// Ensure row difference is non-zero by making target different from source
		offset := rapid.Int64Range(1, 1000).Draw(t, "offset")
		targetRowCount := sourceRowCount + offset
		replayLag := rapid.Int64Range(0, 100000).Draw(t, "replayLag")

		mock := &verifyPropertyMock{
			sourceRowCount: sourceRowCount,
			targetRowCount: targetRowCount,
			replayLag:      replayLag,
			sourceTable:    "events",
		}

		engine := NewVerifyEngine(
			*slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
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

		// Row difference is non-zero (offset >= 1), so should never be ready
		if result.ReadyForCutover {
			t.Fatalf("ReadyForCutover should be false when row difference is non-zero (source=%d, target=%d, lag=%d)",
				sourceRowCount, targetRowCount, replayLag)
		}
	})
}

func TestProperty9_VerifyReadiness_FalseWhenOnlyLagIsNonZero(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Equal row counts but non-zero replay lag
		rowCount := rapid.Int64Range(0, 1000000).Draw(t, "rowCount")
		replayLag := rapid.Int64Range(1, 100000).Draw(t, "replayLag")

		mock := &verifyPropertyMock{
			sourceRowCount: rowCount,
			targetRowCount: rowCount,
			replayLag:      replayLag,
			sourceTable:    "events",
		}

		engine := NewVerifyEngine(
			*slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
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

		if result.ReadyForCutover {
			t.Fatalf("ReadyForCutover should be false when replayLag=%d > 0 (rows match at %d)",
				replayLag, rowCount)
		}
	})
}

func TestProperty9_VerifyReadiness_TrueWhenBothZero(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Equal row counts and zero replay lag — should always be ready
		rowCount := rapid.Int64Range(0, 1000000).Draw(t, "rowCount")

		mock := &verifyPropertyMock{
			sourceRowCount: rowCount,
			targetRowCount: rowCount,
			replayLag:      0,
			sourceTable:    "events",
		}

		engine := NewVerifyEngine(
			*slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
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
			t.Fatalf("ReadyForCutover should be true when replayLag=0 and rows match (rowCount=%d)",
				rowCount)
		}
	})
}

// --- Property 10 Tests ---

// indexRenamePostCutover simulates the post-cutover index rename logic from CutoverEngine:
// replace target table prefix with source table prefix (first occurrence only).
func indexRenamePostCutover(indexName, targetTable, sourceTable string) string {
	return strings.Replace(indexName, targetTable, sourceTable, 1)
}

// indexRenamePostRollback simulates the post-rollback index rename logic from CutoverEngine:
// replace source table prefix with target table prefix (first occurrence only).
func indexRenamePostRollback(indexName, sourceTable, targetTable string) string {
	return strings.Replace(indexName, sourceTable, targetTable, 1)
}

func TestProperty10_IndexRenameRoundTrip_ProducesOriginalName(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a target table name (valid PostgreSQL identifier)
		targetTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,15}`).Draw(t, "targetTable")

		// Generate a source table name that is different from target
		sourceTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,15}`).Filter(func(s string) bool {
			return s != targetTable && !strings.HasPrefix(s, targetTable) && !strings.HasPrefix(targetTable, s)
		}).Draw(t, "sourceTable")

		// Generate an index suffix
		indexSuffix := rapid.StringMatching(`_[a-z][a-z0-9_]{0,10}_idx`).Draw(t, "indexSuffix")

		// Original index name has target table as prefix (as created during setup)
		originalIndexName := targetTable + indexSuffix

		// Step 1: Post-cutover rename (target prefix → source prefix)
		renamedAfterCutover := indexRenamePostCutover(originalIndexName, targetTable, sourceTable)

		// Verify the rename actually changed the prefix
		if !strings.HasPrefix(renamedAfterCutover, sourceTable) {
			t.Fatalf("after cutover rename, index should have source prefix: got %q (original=%q, target=%q, source=%q)",
				renamedAfterCutover, originalIndexName, targetTable, sourceTable)
		}

		// Step 2: Post-rollback rename (source prefix → target prefix)
		renamedAfterRollback := indexRenamePostRollback(renamedAfterCutover, sourceTable, targetTable)

		// The round-trip should produce the original name
		if renamedAfterRollback != originalIndexName {
			t.Fatalf("round-trip rename failed: original=%q, afterCutover=%q, afterRollback=%q (target=%q, source=%q)",
				originalIndexName, renamedAfterCutover, renamedAfterRollback, targetTable, sourceTable)
		}
	})
}

func TestProperty10_IndexRenameRoundTrip_MultipleIndexes(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate table names
		targetTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,15}`).Draw(t, "targetTable")
		sourceTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,15}`).Filter(func(s string) bool {
			return s != targetTable && !strings.HasPrefix(s, targetTable) && !strings.HasPrefix(targetTable, s)
		}).Draw(t, "sourceTable")

		// Generate multiple index suffixes
		numIndexes := rapid.IntRange(1, 5).Draw(t, "numIndexes")
		suffixes := make([]string, numIndexes)

		for i := 0; i < numIndexes; i++ {
			suffixes[i] = rapid.StringMatching(`_[a-z][a-z0-9_]{0,8}_idx`).Draw(t, "suffix")
		}

		// For each index, verify the round-trip property
		for _, suffix := range suffixes {
			originalName := targetTable + suffix

			// Post-cutover: target → source
			afterCutover := indexRenamePostCutover(originalName, targetTable, sourceTable)

			// Post-rollback: source → target
			afterRollback := indexRenamePostRollback(afterCutover, sourceTable, targetTable)

			if afterRollback != originalName {
				t.Fatalf("round-trip failed for index %q: afterCutover=%q, afterRollback=%q",
					originalName, afterCutover, afterRollback)
			}
		}
	})
}

func TestProperty10_IndexRenameRoundTrip_PreservesNonPrefixedIndexes(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate table names
		targetTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,15}`).Draw(t, "targetTable")
		sourceTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,15}`).Filter(func(s string) bool {
			return s != targetTable && !strings.HasPrefix(s, targetTable) && !strings.HasPrefix(targetTable, s)
		}).Draw(t, "sourceTable")

		// Generate an index name that does NOT start with the target table prefix
		// This simulates indexes that should not be renamed
		indexName := rapid.StringMatching(`[a-z][a-z0-9_]{5,20}`).Filter(func(s string) bool {
			return !strings.HasPrefix(s, targetTable) && !strings.HasPrefix(s, sourceTable)
		}).Draw(t, "indexName")

		// Post-cutover rename should not change the name (no prefix match)
		afterCutover := indexRenamePostCutover(indexName, targetTable, sourceTable)

		// In the actual CutoverEngine, non-prefixed indexes are skipped via HasPrefix check.
		// But the rename function itself (strings.Replace) won't change it if prefix doesn't match.
		// The actual engine checks HasPrefix before calling RenameIndex.
		// Here we verify the strings.Replace behavior: if target is not a prefix, name is unchanged.
		if !strings.Contains(indexName, targetTable) {
			if afterCutover != indexName {
				t.Fatalf("non-prefixed index should remain unchanged: original=%q, after=%q",
					indexName, afterCutover)
			}
		}
	})
}

// --- Mock for Property 10 integration with CutoverEngine ---

// indexRenameMock tracks index rename operations to verify the CutoverEngine's
// post-cutover rename logic.
type indexRenameMock struct {
	mockConvertDBClient
	indexes      []postgresql.IndexDef
	renamedPairs []indexRenamePair
}

type indexRenamePair struct {
	oldName string
	newName string
}

func (m *indexRenameMock) GetTableIndexes(schema, table string) ([]postgresql.IndexDef, error) {
	return m.indexes, nil
}

func (m *indexRenameMock) RenameIndex(schema, oldName, newName string) error {
	m.renamedPairs = append(m.renamedPairs, indexRenamePair{oldName: oldName, newName: newName})

	return nil
}

func TestProperty10_IndexRenameRoundTrip_ViaEngine(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate table names
		targetTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,10}`).Draw(t, "targetTable")
		sourceTable := rapid.StringMatching(`[a-z][a-z0-9_]{2,10}`).Filter(func(s string) bool {
			return s != targetTable && !strings.HasPrefix(s, targetTable) && !strings.HasPrefix(targetTable, s)
		}).Draw(t, "sourceTable")

		// Generate indexes with target table prefix
		numIndexes := rapid.IntRange(1, 4).Draw(t, "numIndexes")
		indexes := make([]postgresql.IndexDef, numIndexes)

		for i := 0; i < numIndexes; i++ {
			suffix := rapid.StringMatching(`_[a-z][a-z0-9_]{0,6}_idx`).Draw(t, "suffix")
			indexes[i] = postgresql.IndexDef{
				Name:    targetTable + suffix,
				Columns: []string{"col1"},
				Method:  "btree",
			}
		}

		// Simulate post-cutover rename
		cutoverMock := &indexRenameMock{indexes: indexes}
		cutoverEngine := NewCutoverEngine(
			*slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
			cutoverMock,
			CutoverEngineConfig{
				Schema:      "public",
				SourceTable: sourceTable,
				TargetTable: targetTable,
				PKColumns:   []string{"id"},
				BatchSize:   100,
			},
		)

		err := cutoverEngine.renameIndexesPostCutover(context.Background())
		if err != nil {
			t.Fatalf("unexpected error in renameIndexesPostCutover: %v", err)
		}

		// Verify all indexes were renamed with source prefix
		if len(cutoverMock.renamedPairs) != numIndexes {
			t.Fatalf("expected %d renames, got %d", numIndexes, len(cutoverMock.renamedPairs))
		}

		// Now simulate post-rollback: the renamed indexes (with source prefix) should be
		// renamed back to target prefix
		renamedIndexes := make([]postgresql.IndexDef, len(cutoverMock.renamedPairs))
		for i, pair := range cutoverMock.renamedPairs {
			renamedIndexes[i] = postgresql.IndexDef{
				Name:    pair.newName,
				Columns: []string{"col1"},
				Method:  "btree",
			}
		}

		rollbackMock := &indexRenameMock{indexes: renamedIndexes}
		rollbackEngine := NewCutoverEngine(
			*slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
			rollbackMock,
			CutoverEngineConfig{
				Schema:      "public",
				SourceTable: sourceTable,
				TargetTable: targetTable,
				PKColumns:   []string{"id"},
				BatchSize:   100,
			},
		)

		err = rollbackEngine.renameIndexesPostRollback(context.Background())
		if err != nil {
			t.Fatalf("unexpected error in renameIndexesPostRollback: %v", err)
		}

		// Verify round-trip: rollback renames should produce original index names
		if len(rollbackMock.renamedPairs) != numIndexes {
			t.Fatalf("expected %d rollback renames, got %d", numIndexes, len(rollbackMock.renamedPairs))
		}

		for i, pair := range rollbackMock.renamedPairs {
			originalName := indexes[i].Name
			if pair.newName != originalName {
				t.Fatalf("round-trip failed for index %d: original=%q, afterCutover=%q, afterRollback=%q",
					i, originalName, cutoverMock.renamedPairs[i].newName, pair.newName)
			}
		}
	})
}
