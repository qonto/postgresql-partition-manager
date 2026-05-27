//nolint:wsl_v5
package postgresql_test

import (
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
)

func TestDequeueEvents(t *testing.T) {
	schema := "public"
	table := "events"

	t.Run("dequeues batch of events with single PK", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		now := time.Now()
		rows := mock.NewRows([]string{"seq_id", "operation", "pk_values", "created_at"}).
			AddRow(int64(1), "INSERT", []string{"100"}, now).
			AddRow(int64(2), "UPDATE", []string{"101"}, now).
			AddRow(int64(3), "DELETE", []string{"102"}, now)

		mock.ExpectQuery(`DELETE FROM "public"\."events_cdc_queue".*WHERE seq_id IN.*SELECT seq_id FROM "public"\."events_cdc_queue".*ORDER BY seq_id.*LIMIT \$1.*FOR UPDATE SKIP LOCKED.*RETURNING seq_id, operation, pk_values, created_at`).
			WithArgs(100).
			WillReturnRows(rows)

		events, err := client.DequeueEvents(schema, table, 100)
		assert.NoError(t, err)
		assert.Len(t, events, 3)
		assert.Equal(t, int64(1), events[0].SeqID)
		assert.Equal(t, "INSERT", events[0].Operation)
		assert.Equal(t, []string{"100"}, events[0].PKValues)
		assert.Equal(t, int64(2), events[1].SeqID)
		assert.Equal(t, "UPDATE", events[1].Operation)
		assert.Equal(t, int64(3), events[2].SeqID)
		assert.Equal(t, "DELETE", events[2].Operation)
		assert.Equal(t, []string{"102"}, events[2].PKValues)
	})

	t.Run("dequeues events with composite PK values", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		now := time.Now()
		rows := mock.NewRows([]string{"seq_id", "operation", "pk_values", "created_at"}).
			AddRow(int64(10), "INSERT", []string{"tenant_a", "500"}, now).
			AddRow(int64(11), "UPDATE", []string{"tenant_b", "600"}, now)

		mock.ExpectQuery(`DELETE FROM "public"\."events_cdc_queue".*LIMIT \$1.*RETURNING`).
			WithArgs(50).
			WillReturnRows(rows)

		events, err := client.DequeueEvents(schema, table, 50)
		assert.NoError(t, err)
		assert.Len(t, events, 2)
		assert.Equal(t, []string{"tenant_a", "500"}, events[0].PKValues)
		assert.Equal(t, []string{"tenant_b", "600"}, events[1].PKValues)
	})

	t.Run("returns empty slice when no events in queue", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		rows := mock.NewRows([]string{"seq_id", "operation", "pk_values", "created_at"})

		mock.ExpectQuery(`DELETE FROM "public"\."events_cdc_queue".*LIMIT \$1.*RETURNING`).
			WithArgs(1000).
			WillReturnRows(rows)

		events, err := client.DequeueEvents(schema, table, 1000)
		assert.NoError(t, err)
		assert.Empty(t, events)
	})

	t.Run("respects batch size limit", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		now := time.Now()
		rows := mock.NewRows([]string{"seq_id", "operation", "pk_values", "created_at"}).
			AddRow(int64(1), "INSERT", []string{"1"}, now)

		// Batch size of 1 should only return 1 event
		mock.ExpectQuery(`DELETE FROM "public"\."events_cdc_queue".*LIMIT \$1.*RETURNING`).
			WithArgs(1).
			WillReturnRows(rows)

		events, err := client.DequeueEvents(schema, table, 1)
		assert.NoError(t, err)
		assert.Len(t, events, 1)
	})

	t.Run("returns error on query failure", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectQuery(`DELETE FROM "public"\."events_cdc_queue"`).
			WithArgs(100).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		events, err := client.DequeueEvents(schema, table, 100)
		assert.Error(t, err)
		assert.Nil(t, events)
		assert.Contains(t, err.Error(), "failed to dequeue events")
	})

	t.Run("handles schema with special characters", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		now := time.Now()
		rows := mock.NewRows([]string{"seq_id", "operation", "pk_values", "created_at"}).
			AddRow(int64(1), "INSERT", []string{"42"}, now)

		mock.ExpectQuery(`DELETE FROM "my-schema"\."orders_cdc_queue".*LIMIT \$1.*RETURNING`).
			WithArgs(10).
			WillReturnRows(rows)

		events, err := client.DequeueEvents("my-schema", "orders", 10)
		assert.NoError(t, err)
		assert.Len(t, events, 1)
	})
}

func TestApplyUpsert(t *testing.T) {
	schema := "public"
	targetTable := "events_new"
	sourceTable := "events"

	t.Run("applies upsert with single PK column", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		// First, the method queries column names from information_schema
		colRows := mock.NewRows([]string{"column_name"}).
			AddRow("id").
			AddRow("name").
			AddRow("created_at")
		mock.ExpectQuery(`SELECT column_name.*FROM information_schema.columns.*WHERE table_schema = \$1 AND table_name = \$2.*ORDER BY ordinal_position`).
			WithArgs(schema, sourceTable).
			WillReturnRows(colRows)

		// Then it queries the target table's PK columns
		pkRows := mock.NewRows([]string{"attname"}).
			AddRow("id").
			AddRow("created_at")
		mock.ExpectQuery(`SELECT a.attname.*FROM pg_index.*WHERE i.indisprimary`).
			WithArgs(schema, targetTable).
			WillReturnRows(pkRows)

		// Finally, it executes the upsert query
		mock.ExpectExec(`INSERT INTO "public"\."events_new" \("id", "name", "created_at"\).*SELECT "id", "name", "created_at" FROM "public"\."events".*WHERE \("id"\) = \(\$1\).*ON CONFLICT \("id", "created_at"\) DO UPDATE SET.*"name" = EXCLUDED\."name"`).
			WithArgs("42").
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		err := client.ApplyUpsert(schema, targetTable, sourceTable, []string{"id"}, []string{"42"})
		assert.NoError(t, err)
	})

	t.Run("applies upsert with composite PK columns", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		// Column names query
		colRows := mock.NewRows([]string{"column_name"}).
			AddRow("tenant_id").
			AddRow("event_id").
			AddRow("data").
			AddRow("created_at")
		mock.ExpectQuery(`SELECT column_name.*FROM information_schema.columns`).
			WithArgs(schema, sourceTable).
			WillReturnRows(colRows)

		// Target PK columns query (includes partition key)
		pkRows := mock.NewRows([]string{"attname"}).
			AddRow("tenant_id").
			AddRow("event_id").
			AddRow("created_at")
		mock.ExpectQuery(`SELECT a.attname.*FROM pg_index.*WHERE i.indisprimary`).
			WithArgs(schema, targetTable).
			WillReturnRows(pkRows)

		// Upsert execution with composite WHERE clause
		mock.ExpectExec(`INSERT INTO "public"\."events_new".*SELECT.*FROM "public"\."events".*WHERE \("tenant_id", "event_id"\) = \(\$1, \$2\).*ON CONFLICT \("tenant_id", "event_id", "created_at"\) DO UPDATE SET.*"data" = EXCLUDED\."data"`).
			WithArgs("tenant_a", "500").
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		err := client.ApplyUpsert(schema, targetTable, sourceTable, []string{"tenant_id", "event_id"}, []string{"tenant_a", "500"})
		assert.NoError(t, err)
	})

	t.Run("uses DO NOTHING when all columns are PK", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		// All columns are part of the target PK
		colRows := mock.NewRows([]string{"column_name"}).
			AddRow("id").
			AddRow("created_at")
		mock.ExpectQuery(`SELECT column_name.*FROM information_schema.columns`).
			WithArgs(schema, sourceTable).
			WillReturnRows(colRows)

		pkRows := mock.NewRows([]string{"attname"}).
			AddRow("id").
			AddRow("created_at")
		mock.ExpectQuery(`SELECT a.attname.*FROM pg_index.*WHERE i.indisprimary`).
			WithArgs(schema, targetTable).
			WillReturnRows(pkRows)

		// When all columns are PK, it should use DO NOTHING
		mock.ExpectExec(`INSERT INTO "public"\."events_new".*ON CONFLICT \("id", "created_at"\) DO NOTHING`).
			WithArgs("42").
			WillReturnResult(pgxmock.NewResult("INSERT", 0))

		err := client.ApplyUpsert(schema, targetTable, sourceTable, []string{"id"}, []string{"42"})
		assert.NoError(t, err)
	})

	t.Run("returns error when column query fails", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectQuery(`SELECT column_name.*FROM information_schema.columns`).
			WithArgs(schema, sourceTable).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		err := client.ApplyUpsert(schema, targetTable, sourceTable, []string{"id"}, []string{"42"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get columns for upsert")
	})

	t.Run("returns error when upsert execution fails", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		colRows := mock.NewRows([]string{"column_name"}).
			AddRow("id").
			AddRow("name")
		mock.ExpectQuery(`SELECT column_name.*FROM information_schema.columns`).
			WithArgs(schema, sourceTable).
			WillReturnRows(colRows)

		pkRows := mock.NewRows([]string{"attname"}).
			AddRow("id")
		mock.ExpectQuery(`SELECT a.attname.*FROM pg_index.*WHERE i.indisprimary`).
			WithArgs(schema, targetTable).
			WillReturnRows(pkRows)

		mock.ExpectExec(`INSERT INTO "public"\."events_new"`).
			WithArgs("42").
			WillReturnError(ErrPostgreSQLConnectionFailure)

		err := client.ApplyUpsert(schema, targetTable, sourceTable, []string{"id"}, []string{"42"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to apply upsert")
	})

	t.Run("falls back to source PK when target PK query returns empty", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		colRows := mock.NewRows([]string{"column_name"}).
			AddRow("id").
			AddRow("name").
			AddRow("created_at")
		mock.ExpectQuery(`SELECT column_name.*FROM information_schema.columns`).
			WithArgs(schema, sourceTable).
			WillReturnRows(colRows)

		// Empty PK result — fallback to source PK columns
		pkRows := mock.NewRows([]string{"attname"})
		mock.ExpectQuery(`SELECT a.attname.*FROM pg_index.*WHERE i.indisprimary`).
			WithArgs(schema, targetTable).
			WillReturnRows(pkRows)

		// Should use source PK ("id") for ON CONFLICT
		mock.ExpectExec(`INSERT INTO "public"\."events_new".*ON CONFLICT \("id"\) DO UPDATE SET.*"name" = EXCLUDED\."name".*"created_at" = EXCLUDED\."created_at"`).
			WithArgs("42").
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		err := client.ApplyUpsert(schema, targetTable, sourceTable, []string{"id"}, []string{"42"})
		assert.NoError(t, err)
	})

	t.Run("handles schema with special characters", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		colRows := mock.NewRows([]string{"column_name"}).
			AddRow("order_id").
			AddRow("amount")
		mock.ExpectQuery(`SELECT column_name.*FROM information_schema.columns`).
			WithArgs("my-schema", "orders").
			WillReturnRows(colRows)

		pkRows := mock.NewRows([]string{"attname"}).
			AddRow("order_id")
		mock.ExpectQuery(`SELECT a.attname.*FROM pg_index.*WHERE i.indisprimary`).
			WithArgs("my-schema", "orders_new").
			WillReturnRows(pkRows)

		mock.ExpectExec(`INSERT INTO "my-schema"\."orders_new".*SELECT.*FROM "my-schema"\."orders".*ON CONFLICT \("order_id"\) DO UPDATE SET`).
			WithArgs("99").
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		err := client.ApplyUpsert("my-schema", "orders_new", "orders", []string{"order_id"}, []string{"99"})
		assert.NoError(t, err)
	})
}

func TestApplyDelete(t *testing.T) {
	schema := "public"
	targetTable := "events_new"

	t.Run("deletes row with single PK", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`DELETE FROM "public"\."events_new" WHERE \("id"\) = \(\$1\)`).
			WithArgs("42").
			WillReturnResult(pgxmock.NewResult("DELETE", 1))

		err := client.ApplyDelete(schema, targetTable, []string{"id"}, []string{"42"})
		assert.NoError(t, err)
	})

	t.Run("deletes row with composite PK", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`DELETE FROM "public"\."events_new" WHERE \("tenant_id", "event_id"\) = \(\$1, \$2\)`).
			WithArgs("tenant_a", "500").
			WillReturnResult(pgxmock.NewResult("DELETE", 1))

		err := client.ApplyDelete(schema, targetTable, []string{"tenant_id", "event_id"}, []string{"tenant_a", "500"})
		assert.NoError(t, err)
	})

	t.Run("succeeds even when row does not exist (0 rows affected)", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`DELETE FROM "public"\."events_new" WHERE \("id"\) = \(\$1\)`).
			WithArgs("999").
			WillReturnResult(pgxmock.NewResult("DELETE", 0))

		err := client.ApplyDelete(schema, targetTable, []string{"id"}, []string{"999"})
		assert.NoError(t, err)
	})

	t.Run("returns error on query failure", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`DELETE FROM "public"\."events_new"`).
			WithArgs("42").
			WillReturnError(ErrPostgreSQLConnectionFailure)

		err := client.ApplyDelete(schema, targetTable, []string{"id"}, []string{"42"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to apply delete")
	})

	t.Run("handles schema with special characters", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`DELETE FROM "my-schema"\."orders_new" WHERE \("order_id"\) = \(\$1\)`).
			WithArgs("123").
			WillReturnResult(pgxmock.NewResult("DELETE", 1))

		err := client.ApplyDelete("my-schema", "orders_new", []string{"order_id"}, []string{"123"})
		assert.NoError(t, err)
	})
}

func TestGetReplayLag(t *testing.T) {
	schema := "public"
	table := "events"

	t.Run("returns count of pending events", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		rows := mock.NewRows([]string{"count"}).AddRow(int64(42))
		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM "public"\."events_cdc_queue"`).
			WillReturnRows(rows)

		lag, err := client.GetReplayLag(schema, table)
		assert.NoError(t, err)
		assert.Equal(t, int64(42), lag)
	})

	t.Run("returns zero when queue is empty", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		rows := mock.NewRows([]string{"count"}).AddRow(int64(0))
		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM "public"\."events_cdc_queue"`).
			WillReturnRows(rows)

		lag, err := client.GetReplayLag(schema, table)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), lag)
	})

	t.Run("returns error on query failure", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM "public"\."events_cdc_queue"`).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		_, err := client.GetReplayLag(schema, table)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get replay lag")
	})
}

func TestIsCDCQueueEmpty(t *testing.T) {
	schema := "public"
	table := "events"

	t.Run("returns true when queue is empty", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		rows := mock.NewRows([]string{"not_exists"}).AddRow(true)
		mock.ExpectQuery(`SELECT NOT EXISTS\(SELECT 1 FROM "public"\."events_cdc_queue"\)`).
			WillReturnRows(rows)

		empty, err := client.IsCDCQueueEmpty(schema, table)
		assert.NoError(t, err)
		assert.True(t, empty)
	})

	t.Run("returns false when queue has events", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		rows := mock.NewRows([]string{"not_exists"}).AddRow(false)
		mock.ExpectQuery(`SELECT NOT EXISTS\(SELECT 1 FROM "public"\."events_cdc_queue"\)`).
			WillReturnRows(rows)

		empty, err := client.IsCDCQueueEmpty(schema, table)
		assert.NoError(t, err)
		assert.False(t, empty)
	})

	t.Run("returns error on query failure", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectQuery(`SELECT NOT EXISTS`).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		_, err := client.IsCDCQueueEmpty(schema, table)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to check if CDC queue is empty")
	})
}
