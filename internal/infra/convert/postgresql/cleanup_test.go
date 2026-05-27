//nolint:wsl_v5
package postgresql_test

import (
	"testing"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
)

func TestDropTable(t *testing.T) {
	schema := "public"
	table := "events_old"

	t.Run("drops table successfully", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`DROP TABLE IF EXISTS "public"\."events_old"`).
			WillReturnResult(pgxmock.NewResult("DROP", 0))

		err := client.DropTable(schema, table)
		assert.NoError(t, err)
	})

	t.Run("returns error when drop fails", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`DROP TABLE IF EXISTS "public"\."events_old"`).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		err := client.DropTable(schema, table)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to drop table")
	})

	t.Run("works with custom schema", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`DROP TABLE IF EXISTS "my_schema"\."orders_old"`).
			WillReturnResult(pgxmock.NewResult("DROP", 0))

		err := client.DropTable("my_schema", "orders_old")
		assert.NoError(t, err)
	})
}

func TestDropCDCQueue(t *testing.T) {
	schema := "public"
	table := "events"

	t.Run("drops CDC queue table successfully", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`DROP TABLE IF EXISTS "public"\."events_cdc_queue"`).
			WillReturnResult(pgxmock.NewResult("DROP", 0))

		err := client.DropCDCQueue(schema, table)
		assert.NoError(t, err)
	})

	t.Run("returns error when drop fails", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`DROP TABLE IF EXISTS "public"\."events_cdc_queue"`).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		err := client.DropCDCQueue(schema, table)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to drop CDC queue")
	})

	t.Run("works with custom schema", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`DROP TABLE IF EXISTS "my_schema"\."orders_cdc_queue"`).
			WillReturnResult(pgxmock.NewResult("DROP", 0))

		err := client.DropCDCQueue("my_schema", "orders")
		assert.NoError(t, err)
	})
}
