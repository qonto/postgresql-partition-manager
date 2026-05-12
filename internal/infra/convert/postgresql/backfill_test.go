//nolint:wsl_v5
package postgresql_test

import (
	"testing"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
)

func TestBackfillBatch(t *testing.T) {
	schema := "public"
	sourceTable := "events"
	targetTable := "events_new"

	t.Run("copies first batch with single PK (no afterPK)", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		// Expect the INSERT...SELECT via CTE
		mock.ExpectExec(`WITH batch_rows AS \(SELECT \* FROM "public"\."events" ORDER BY "id" LIMIT \$1\) INSERT INTO "public"\."events_new" SELECT \* FROM batch_rows ON CONFLICT DO NOTHING`).
			WithArgs(100).
			WillReturnResult(pgxmock.NewResult("INSERT", 100))

		// Expect the query for last PK in batch
		rows := mock.NewRows([]string{"id"}).AddRow(int64(100))
		mock.ExpectQuery(`SELECT "id" FROM \(SELECT "id" FROM "public"\."events" ORDER BY "id" LIMIT \$1\) AS batch ORDER BY "id" DESC LIMIT 1`).
			WithArgs(100).
			WillReturnRows(rows)

		lastPK, rowsCopied, err := client.BackfillBatch(schema, sourceTable, targetTable, []string{"id"}, nil, 100)
		assert.NoError(t, err)
		assert.Equal(t, int64(100), rowsCopied)
		assert.Equal(t, []any{int64(100)}, lastPK)
	})

	t.Run("copies subsequent batch with single PK (with afterPK)", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		// Expect the INSERT...SELECT with WHERE clause
		mock.ExpectExec(`WITH batch_rows AS \(SELECT \* FROM "public"\."events" WHERE \("id"\) > \(\$1\) ORDER BY "id" LIMIT \$2\) INSERT INTO "public"\."events_new" SELECT \* FROM batch_rows ON CONFLICT DO NOTHING`).
			WithArgs(int64(100), 50).
			WillReturnResult(pgxmock.NewResult("INSERT", 50))

		// Expect the query for last PK in batch
		rows := mock.NewRows([]string{"id"}).AddRow(int64(150))
		mock.ExpectQuery(`SELECT "id" FROM \(SELECT "id" FROM "public"\."events" WHERE \("id"\) > \(\$1\) ORDER BY "id" LIMIT \$2\) AS batch ORDER BY "id" DESC LIMIT 1`).
			WithArgs(int64(100), 50).
			WillReturnRows(rows)

		lastPK, rowsCopied, err := client.BackfillBatch(schema, sourceTable, targetTable, []string{"id"}, []any{int64(100)}, 50)
		assert.NoError(t, err)
		assert.Equal(t, int64(50), rowsCopied)
		assert.Equal(t, []any{int64(150)}, lastPK)
	})

	t.Run("copies batch with composite PK", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		// Expect the INSERT...SELECT with composite WHERE clause
		mock.ExpectExec(`WITH batch_rows AS \(SELECT \* FROM "public"\."events" WHERE \("tenant_id", "event_id"\) > \(\$1, \$2\) ORDER BY "tenant_id", "event_id" LIMIT \$3\) INSERT INTO "public"\."events_new" SELECT \* FROM batch_rows ON CONFLICT DO NOTHING`).
			WithArgs("tenant_a", int64(500), 1000).
			WillReturnResult(pgxmock.NewResult("INSERT", 1000))

		// Expect the query for last PK in batch
		rows := mock.NewRows([]string{"tenant_id", "event_id"}).AddRow("tenant_a", int64(1500))
		mock.ExpectQuery(`SELECT "tenant_id", "event_id" FROM \(SELECT "tenant_id", "event_id" FROM "public"\."events" WHERE \("tenant_id", "event_id"\) > \(\$1, \$2\) ORDER BY "tenant_id", "event_id" LIMIT \$3\) AS batch ORDER BY "tenant_id", "event_id" DESC LIMIT 1`).
			WithArgs("tenant_a", int64(500), 1000).
			WillReturnRows(rows)

		lastPK, rowsCopied, err := client.BackfillBatch(schema, sourceTable, targetTable, []string{"tenant_id", "event_id"}, []any{"tenant_a", int64(500)}, 1000)
		assert.NoError(t, err)
		assert.Equal(t, int64(1000), rowsCopied)
		assert.Equal(t, []any{"tenant_a", int64(1500)}, lastPK)
	})

	t.Run("returns nil lastPK when no rows in batch", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		// Expect the INSERT...SELECT (0 rows affected)
		mock.ExpectExec(`WITH batch_rows AS \(SELECT \* FROM "public"\."events" WHERE \("id"\) > \(\$1\) ORDER BY "id" LIMIT \$2\) INSERT INTO "public"\."events_new" SELECT \* FROM batch_rows ON CONFLICT DO NOTHING`).
			WithArgs(int64(9999), 100).
			WillReturnResult(pgxmock.NewResult("INSERT", 0))

		// Expect the query for last PK — returns empty result
		rows := mock.NewRows([]string{"id"})
		mock.ExpectQuery(`SELECT "id" FROM \(SELECT "id" FROM "public"\."events" WHERE \("id"\) > \(\$1\) ORDER BY "id" LIMIT \$2\) AS batch ORDER BY "id" DESC LIMIT 1`).
			WithArgs(int64(9999), 100).
			WillReturnRows(rows)

		lastPK, rowsCopied, err := client.BackfillBatch(schema, sourceTable, targetTable, []string{"id"}, []any{int64(9999)}, 100)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), rowsCopied)
		assert.Nil(t, lastPK)
	})

	t.Run("handles conflicts with ON CONFLICT DO NOTHING", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		// Some rows conflict, so rowsCopied < batchSize
		mock.ExpectExec(`WITH batch_rows AS \(SELECT \* FROM "public"\."events" ORDER BY "id" LIMIT \$1\) INSERT INTO "public"\."events_new" SELECT \* FROM batch_rows ON CONFLICT DO NOTHING`).
			WithArgs(100).
			WillReturnResult(pgxmock.NewResult("INSERT", 85))

		// Last PK still reflects the full batch range
		rows := mock.NewRows([]string{"id"}).AddRow(int64(100))
		mock.ExpectQuery(`SELECT "id" FROM \(SELECT "id" FROM "public"\."events" ORDER BY "id" LIMIT \$1\) AS batch ORDER BY "id" DESC LIMIT 1`).
			WithArgs(100).
			WillReturnRows(rows)

		lastPK, rowsCopied, err := client.BackfillBatch(schema, sourceTable, targetTable, []string{"id"}, nil, 100)
		assert.NoError(t, err)
		assert.Equal(t, int64(85), rowsCopied)
		assert.Equal(t, []any{int64(100)}, lastPK)
	})

	t.Run("returns error when INSERT fails", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`WITH batch_rows AS`).
			WithArgs(100).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		_, _, err := client.BackfillBatch(schema, sourceTable, targetTable, []string{"id"}, nil, 100)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to execute backfill batch")
	})

	t.Run("returns error when last PK query fails", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`WITH batch_rows AS`).
			WithArgs(int64(50), 100).
			WillReturnResult(pgxmock.NewResult("INSERT", 100))

		mock.ExpectQuery(`SELECT "id" FROM`).
			WithArgs(int64(50), 100).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		_, rowsCopied, err := client.BackfillBatch(schema, sourceTable, targetTable, []string{"id"}, []any{int64(50)}, 100)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get last PK in batch")
		assert.Equal(t, int64(100), rowsCopied)
	})

	t.Run("handles schema with special characters", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`WITH batch_rows AS \(SELECT \* FROM "my-schema"\."orders" ORDER BY "order_id" LIMIT \$1\) INSERT INTO "my-schema"\."orders_new" SELECT \* FROM batch_rows ON CONFLICT DO NOTHING`).
			WithArgs(500).
			WillReturnResult(pgxmock.NewResult("INSERT", 500))

		rows := mock.NewRows([]string{"order_id"}).AddRow(int64(500))
		mock.ExpectQuery(`SELECT "order_id" FROM \(SELECT "order_id" FROM "my-schema"\."orders" ORDER BY "order_id" LIMIT \$1\) AS batch ORDER BY "order_id" DESC LIMIT 1`).
			WithArgs(500).
			WillReturnRows(rows)

		lastPK, rowsCopied, err := client.BackfillBatch("my-schema", "orders", "orders_new", []string{"order_id"}, nil, 500)
		assert.NoError(t, err)
		assert.Equal(t, int64(500), rowsCopied)
		assert.Equal(t, []any{int64(500)}, lastPK)
	})
}
