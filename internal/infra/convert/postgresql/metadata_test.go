//nolint:wsl_v5
package postgresql_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v3"
	convertpg "github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
	"github.com/qonto/postgresql-partition-manager/internal/infra/logger"
	"github.com/stretchr/testify/assert"
)

var ErrPostgreSQLConnectionFailure = &pgconn.PgError{
	Code: "08006",
}

func setupConvertMock(t *testing.T, queryMatcher pgxmock.QueryMatcher) (pgxmock.PgxConnIface, *convertpg.Client) {
	t.Helper()

	mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(queryMatcher))
	if err != nil {
		t.Fatalf("ERROR: Fail to initialize PostgreSQL mock: %s", err)
	}

	defer mock.Close(context.TODO()) //nolint:errcheck

	log, err := logger.New(false, "text")
	if err != nil {
		t.Fatalf("ERROR: Fail to initialize logger: %s", err)
	}

	client := convertpg.New(*log, mock)

	return mock, client
}

func TestEnsureMetadataTable(t *testing.T) {
	mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

	t.Run("creates metadata table successfully", func(t *testing.T) {
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS ppm_migration_metadata").
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		err := client.EnsureMetadataTable()
		assert.NoError(t, err)
	})

	t.Run("idempotent - succeeds when called multiple times", func(t *testing.T) {
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS ppm_migration_metadata").
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		err := client.EnsureMetadataTable()
		assert.NoError(t, err)

		mock.ExpectExec("CREATE TABLE IF NOT EXISTS ppm_migration_metadata").
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		err = client.EnsureMetadataTable()
		assert.NoError(t, err)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS ppm_migration_metadata").
			WillReturnError(ErrPostgreSQLConnectionFailure)

		err := client.EnsureMetadataTable()
		assert.Error(t, err)
	})
}

func TestGetMigrationState(t *testing.T) {
	schema := "public"
	table := "events"

	mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

	t.Run("returns nil when no state exists", func(t *testing.T) {
		mock.ExpectQuery("SELECT").
			WithArgs(schema, table).
			WillReturnError(pgx.ErrNoRows)

		state, err := client.GetMigrationState(schema, table)
		assert.NoError(t, err)
		assert.Nil(t, state)
	})

	t.Run("returns state with correct fields", func(t *testing.T) {
		now := time.Now().Truncate(time.Microsecond)
		droppedFKsJSON := []byte(`[{"Name":"fk_test","Columns":["col1"],"ReferencedSchema":"public","ReferencedTable":"ref_table","ReferencedColumns":["id"],"OnDelete":"CASCADE","OnUpdate":"NO ACTION"}]`)

		rows := mock.NewRows([]string{
			"schema_name", "table_name", "phase", "last_backfill_pk",
			"last_replay_seq", "dropped_fks", "phase_started_at", "updated_at",
		}).AddRow(
			schema, table, "backfill", []string{"100", "abc"},
			int64(42), droppedFKsJSON, now, now,
		)

		mock.ExpectQuery("SELECT").
			WithArgs(schema, table).
			WillReturnRows(rows)

		state, err := client.GetMigrationState(schema, table)
		assert.NoError(t, err)
		assert.NotNil(t, state)
		assert.Equal(t, schema, state.Schema)
		assert.Equal(t, table, state.Table)
		assert.Equal(t, "backfill", state.Phase)
		assert.Equal(t, []string{"100", "abc"}, state.LastBackfillPK)
		assert.Equal(t, int64(42), state.LastReplaySeq)
		assert.Len(t, state.DroppedForeignKeys, 1)
		assert.Equal(t, "fk_test", state.DroppedForeignKeys[0].Name)
		assert.Equal(t, []string{"col1"}, state.DroppedForeignKeys[0].Columns)
		assert.Equal(t, "CASCADE", state.DroppedForeignKeys[0].OnDelete)
	})

	t.Run("returns state with empty dropped FKs", func(t *testing.T) {
		now := time.Now().Truncate(time.Microsecond)
		droppedFKsJSON := []byte(`[]`)

		rows := mock.NewRows([]string{
			"schema_name", "table_name", "phase", "last_backfill_pk",
			"last_replay_seq", "dropped_fks", "phase_started_at", "updated_at",
		}).AddRow(
			schema, table, "setup", nil,
			int64(0), droppedFKsJSON, now, now,
		)

		mock.ExpectQuery("SELECT").
			WithArgs(schema, table).
			WillReturnRows(rows)

		state, err := client.GetMigrationState(schema, table)
		assert.NoError(t, err)
		assert.NotNil(t, state)
		assert.Equal(t, "setup", state.Phase)
		assert.Empty(t, state.DroppedForeignKeys)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock.ExpectQuery("SELECT").
			WithArgs(schema, table).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		_, err := client.GetMigrationState(schema, table)
		assert.Error(t, err)
	})
}

func TestUpdateMigrationState(t *testing.T) {
	schema := "public"
	table := "events"

	t.Run("inserts new state successfully", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		state := &convertpg.MigrationState{
			Schema:         schema,
			Table:          table,
			Phase:          "setup",
			LastBackfillPK: nil,
			LastReplaySeq:  0,
		}

		mock.ExpectExec("INSERT INTO ppm_migration_metadata").
			WithArgs(schema, table, "setup", pgxmock.AnyArg(), int64(0), pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		err := client.UpdateMigrationState(schema, table, state)
		assert.NoError(t, err)
	})

	t.Run("updates existing state with backfill progress", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		now := time.Now()
		state := &convertpg.MigrationState{
			Schema:         schema,
			Table:          table,
			Phase:          "backfill",
			LastBackfillPK: []string{"500"},
			LastReplaySeq:  0,
			PhaseStartedAt: now,
		}

		mock.ExpectExec("INSERT INTO ppm_migration_metadata").
			WithArgs(schema, table, "backfill", pgxmock.AnyArg(), int64(0), pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		err := client.UpdateMigrationState(schema, table, state)
		assert.NoError(t, err)
	})

	t.Run("updates state with dropped foreign keys", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		now := time.Now()
		state := &convertpg.MigrationState{
			Schema:         schema,
			Table:          table,
			Phase:          "cutover",
			LastBackfillPK: []string{"1000"},
			LastReplaySeq:  99,
			PhaseStartedAt: now,
			DroppedForeignKeys: []convertpg.ForeignKeyDef{
				{
					Name:              "fk_child_parent",
					Columns:           []string{"parent_id"},
					ReferencedSchema:  "public",
					ReferencedTable:   "parents",
					ReferencedColumns: []string{"id"},
					OnDelete:          "CASCADE",
					OnUpdate:          "NO ACTION",
				},
			},
		}

		mock.ExpectExec("INSERT INTO ppm_migration_metadata").
			WithArgs(schema, table, "cutover", pgxmock.AnyArg(), int64(99), pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		err := client.UpdateMigrationState(schema, table, state)
		assert.NoError(t, err)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		state := &convertpg.MigrationState{
			Phase: "setup",
		}

		mock.ExpectExec("INSERT INTO ppm_migration_metadata").
			WithArgs(schema, table, "setup", pgxmock.AnyArg(), int64(0), pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		err := client.UpdateMigrationState(schema, table, state)
		assert.Error(t, err)
	})
}

func TestDeleteMigrationState(t *testing.T) {
	schema := "public"
	table := "events"

	mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

	t.Run("deletes state successfully", func(t *testing.T) {
		mock.ExpectExec("DELETE FROM ppm_migration_metadata").
			WithArgs(schema, table).
			WillReturnResult(pgxmock.NewResult("DELETE", 1))

		err := client.DeleteMigrationState(schema, table)
		assert.NoError(t, err)
	})

	t.Run("succeeds even when no state exists", func(t *testing.T) {
		mock.ExpectExec("DELETE FROM ppm_migration_metadata").
			WithArgs(schema, table).
			WillReturnResult(pgxmock.NewResult("DELETE", 0))

		err := client.DeleteMigrationState(schema, table)
		assert.NoError(t, err)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock.ExpectExec("DELETE FROM ppm_migration_metadata").
			WithArgs(schema, table).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		err := client.DeleteMigrationState(schema, table)
		assert.Error(t, err)
	})
}
