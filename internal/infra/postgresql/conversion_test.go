//nolint:wsl_v5
package postgresql_test

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
)

func TestIsConversionInProgress(t *testing.T) {
	schema := testSchema
	table := testTable

	mock, p := setupMock(t, pgxmock.QueryMatcherRegexp)
	query := `SELECT phase FROM ppm_migration_metadata`

	t.Run("returns false when no row exists", func(t *testing.T) {
		mock.ExpectQuery(query).
			WithArgs(schema, table).
			WillReturnError(pgx.ErrNoRows)

		inProgress, err := p.IsConversionInProgress(schema, table)
		assert.NoError(t, err)
		assert.False(t, inProgress)
	})

	t.Run("returns false for terminal phase cutover_complete", func(t *testing.T) {
		rows := mock.NewRows([]string{"phase"}).AddRow("cutover_complete")
		mock.ExpectQuery(query).
			WithArgs(schema, table).
			WillReturnRows(rows)

		inProgress, err := p.IsConversionInProgress(schema, table)
		assert.NoError(t, err)
		assert.False(t, inProgress)
	})

	t.Run("returns false for terminal phase rollback_complete", func(t *testing.T) {
		rows := mock.NewRows([]string{"phase"}).AddRow("rollback_complete")
		mock.ExpectQuery(query).
			WithArgs(schema, table).
			WillReturnRows(rows)

		inProgress, err := p.IsConversionInProgress(schema, table)
		assert.NoError(t, err)
		assert.False(t, inProgress)
	})

	t.Run("returns true for active phase setup", func(t *testing.T) {
		rows := mock.NewRows([]string{"phase"}).AddRow("setup")
		mock.ExpectQuery(query).
			WithArgs(schema, table).
			WillReturnRows(rows)

		inProgress, err := p.IsConversionInProgress(schema, table)
		assert.NoError(t, err)
		assert.True(t, inProgress)
	})

	t.Run("returns true for active phase backfill", func(t *testing.T) {
		rows := mock.NewRows([]string{"phase"}).AddRow("backfill")
		mock.ExpectQuery(query).
			WithArgs(schema, table).
			WillReturnRows(rows)

		inProgress, err := p.IsConversionInProgress(schema, table)
		assert.NoError(t, err)
		assert.True(t, inProgress)
	})

	t.Run("returns true for active phase replay", func(t *testing.T) {
		rows := mock.NewRows([]string{"phase"}).AddRow("replay")
		mock.ExpectQuery(query).
			WithArgs(schema, table).
			WillReturnRows(rows)

		inProgress, err := p.IsConversionInProgress(schema, table)
		assert.NoError(t, err)
		assert.True(t, inProgress)
	})

	t.Run("returns false when metadata table does not exist", func(t *testing.T) {
		mock.ExpectQuery(query).
			WithArgs(schema, table).
			WillReturnError(&pgconn.PgError{Code: "42P01"})

		inProgress, err := p.IsConversionInProgress(schema, table)
		assert.NoError(t, err)
		assert.False(t, inProgress)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock.ExpectQuery(query).
			WithArgs(schema, table).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		inProgress, err := p.IsConversionInProgress(schema, table)
		assert.Error(t, err)
		assert.False(t, inProgress)
	})
}
