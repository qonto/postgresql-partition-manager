//nolint:wsl_v5
package postgresql_test

import (
	"testing"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
)

func TestCreateCDCQueue(t *testing.T) {
	schema := "public"
	table := "events"

	t.Run("creates CDC queue table and index successfully", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE TABLE "public"\."events_cdc_queue"`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		mock.ExpectExec(`CREATE INDEX "public"\."idx_events_cdc_queue_seq" ON "public"\."events_cdc_queue"`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		err := client.CreateCDCQueue(schema, table, []string{"id"})
		assert.NoError(t, err)
	})

	t.Run("returns error when table creation fails", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE TABLE "public"\."events_cdc_queue"`).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		err := client.CreateCDCQueue(schema, table, []string{"id"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create CDC queue table")
	})

	t.Run("returns error when index creation fails", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE TABLE "public"\."events_cdc_queue"`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		mock.ExpectExec(`CREATE INDEX "public"\."idx_events_cdc_queue_seq"`).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		err := client.CreateCDCQueue(schema, table, []string{"id"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create CDC queue index")
	})

	t.Run("handles schema with special characters", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE TABLE "my-schema"\."orders_cdc_queue"`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		mock.ExpectExec(`CREATE INDEX "my-schema"\."idx_orders_cdc_queue_seq" ON "my-schema"\."orders_cdc_queue"`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		err := client.CreateCDCQueue("my-schema", "orders", []string{"order_id"})
		assert.NoError(t, err)
	})
}

func TestCreateCDCTriggerFunction(t *testing.T) {
	schema := "public"
	table := "events"

	t.Run("creates trigger function with single PK column", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE OR REPLACE FUNCTION "public"\."ppm_cdc_trigger_events"\(\)`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		err := client.CreateCDCTriggerFunction(schema, table, []string{"id"})
		assert.NoError(t, err)
	})

	t.Run("creates trigger function with composite PK columns", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE OR REPLACE FUNCTION "public"\."ppm_cdc_trigger_events"\(\)`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		err := client.CreateCDCTriggerFunction(schema, table, []string{"tenant_id", "event_id"})
		assert.NoError(t, err)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE OR REPLACE FUNCTION "public"\."ppm_cdc_trigger_events"\(\)`).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		err := client.CreateCDCTriggerFunction(schema, table, []string{"id"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create CDC trigger function")
	})
}

func TestInstallCDCTrigger(t *testing.T) {
	schema := "public"
	table := "events"

	t.Run("installs trigger successfully", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE TRIGGER "ppm_cdc_events".*AFTER INSERT OR UPDATE OR DELETE ON "public"\."events".*FOR EACH ROW EXECUTE FUNCTION "public"\."ppm_cdc_trigger_events"\(\)`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		err := client.InstallCDCTrigger(schema, table)
		assert.NoError(t, err)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE TRIGGER "ppm_cdc_events"`).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		err := client.InstallCDCTrigger(schema, table)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to install CDC trigger")
	})
}

func TestIsCDCQueueExists(t *testing.T) {
	schema := "public"
	table := "events"

	t.Run("returns true when queue exists", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		rows := mock.NewRows([]string{"exists"}).AddRow(true)
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs(schema, "events_cdc_queue").
			WillReturnRows(rows)

		exists, err := client.IsCDCQueueExists(schema, table)
		assert.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("returns false when queue does not exist", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		rows := mock.NewRows([]string{"exists"}).AddRow(false)
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs(schema, "events_cdc_queue").
			WillReturnRows(rows)

		exists, err := client.IsCDCQueueExists(schema, table)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectQuery("SELECT EXISTS").
			WithArgs(schema, "events_cdc_queue").
			WillReturnError(ErrPostgreSQLConnectionFailure)

		_, err := client.IsCDCQueueExists(schema, table)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to check if CDC queue exists")
	})
}

func TestIsCDCTriggerExists(t *testing.T) {
	schema := "public"
	table := "events"

	t.Run("returns true when trigger exists", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		rows := mock.NewRows([]string{"exists"}).AddRow(true)
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs(schema, table, "ppm_cdc_events").
			WillReturnRows(rows)

		exists, err := client.IsCDCTriggerExists(schema, table)
		assert.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("returns false when trigger does not exist", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		rows := mock.NewRows([]string{"exists"}).AddRow(false)
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs(schema, table, "ppm_cdc_events").
			WillReturnRows(rows)

		exists, err := client.IsCDCTriggerExists(schema, table)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectQuery("SELECT EXISTS").
			WithArgs(schema, table, "ppm_cdc_events").
			WillReturnError(ErrPostgreSQLConnectionFailure)

		_, err := client.IsCDCTriggerExists(schema, table)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to check if CDC trigger exists")
	})
}
