//nolint:wsl_v5
package postgresql_test

import (
	"testing"

	"github.com/pashagolub/pgxmock/v3"
	infra "github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"github.com/stretchr/testify/assert"
)

func TestCreateCDCQueue(t *testing.T) {
	schema := "public"
	table := "events"

	t.Run("creates CDC queue table and index successfully", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE TABLE "public"\."events_cdc_queue"`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		mock.ExpectExec(`CREATE INDEX "idx_events_cdc_queue_seq" ON "public"\."events_cdc_queue"`).
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

		mock.ExpectExec(`CREATE INDEX "idx_events_cdc_queue_seq"`).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		err := client.CreateCDCQueue(schema, table, []string{"id"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create CDC queue index")
	})

	t.Run("handles schema with special characters", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE TABLE "my-schema"\."orders_cdc_queue"`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		mock.ExpectExec(`CREATE INDEX "idx_orders_cdc_queue_seq" ON "my-schema"\."orders_cdc_queue"`).
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

func TestCreatePartitionedTable(t *testing.T) {
	schema := "public"
	table := "events_new"

	t.Run("creates partitioned table with basic columns", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE TABLE "public"\."events_new".*PARTITION BY RANGE \("created_at"\)`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		columns := []infra.ColumnDef{
			{Name: "id", DataType: "bigint", IsNullable: false},
			{Name: "name", DataType: "text", IsNullable: true},
			{Name: "created_at", DataType: "timestamptz", IsNullable: false},
		}

		err := client.CreatePartitionedTable(schema, table, columns, "created_at")
		assert.NoError(t, err)
	})

	t.Run("creates partitioned table with default values", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE TABLE "public"\."events_new".*PARTITION BY RANGE`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		defaultVal := "now()"
		columns := []infra.ColumnDef{
			{Name: "id", DataType: "bigint", IsNullable: false},
			{Name: "created_at", DataType: "timestamptz", IsNullable: false, DefaultValue: &defaultVal},
		}

		err := client.CreatePartitionedTable(schema, table, columns, "created_at")
		assert.NoError(t, err)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE TABLE "public"\."events_new"`).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		columns := []infra.ColumnDef{
			{Name: "id", DataType: "bigint", IsNullable: false},
		}

		err := client.CreatePartitionedTable(schema, table, columns, "created_at")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create partitioned table")
	})

	t.Run("handles schema with special characters", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE TABLE "my-schema"\."orders_new".*PARTITION BY RANGE \("order_date"\)`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		columns := []infra.ColumnDef{
			{Name: "id", DataType: "integer", IsNullable: false},
			{Name: "order_date", DataType: "date", IsNullable: false},
		}

		err := client.CreatePartitionedTable("my-schema", "orders_new", columns, "order_date")
		assert.NoError(t, err)
	})
}

func TestCreatePartition(t *testing.T) {
	schema := "public"
	parentTable := "events_new"

	t.Run("creates partition successfully", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE TABLE "public"\."events_new_20240101" PARTITION OF "public"\."events_new" FOR VALUES FROM \('2024-01-01'\) TO \('2024-02-01'\)`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		err := client.CreatePartition(schema, parentTable, "events_new_20240101", "2024-01-01", "2024-02-01")
		assert.NoError(t, err)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE TABLE "public"\."events_new_20240101" PARTITION OF`).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		err := client.CreatePartition(schema, parentTable, "events_new_20240101", "2024-01-01", "2024-02-01")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create partition")
	})

	t.Run("handles different date ranges", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE TABLE "public"\."events_new_20231215" PARTITION OF "public"\."events_new" FOR VALUES FROM \('2023-12-15'\) TO \('2023-12-22'\)`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		err := client.CreatePartition(schema, parentTable, "events_new_20231215", "2023-12-15", "2023-12-22")
		assert.NoError(t, err)
	})
}

func TestCreateIndex(t *testing.T) {
	schema := "public"
	table := "events_new"

	t.Run("creates primary key constraint", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`ALTER TABLE "public"\."events_new" ADD CONSTRAINT "events_new_pkey" PRIMARY KEY \("id", "created_at"\)`).
			WillReturnResult(pgxmock.NewResult("ALTER", 0))

		idx := infra.IndexDef{
			Name:      "events_new_pkey",
			Columns:   []string{"id", "created_at"},
			IsPrimary: true,
			IsUnique:  true,
			Method:    "btree",
		}

		err := client.CreateIndex(schema, table, idx)
		assert.NoError(t, err)
	})

	t.Run("creates regular btree index", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE INDEX "idx_events_name" ON "public"\."events_new" \("name"\)`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		idx := infra.IndexDef{
			Name:    "idx_events_name",
			Columns: []string{"name"},
			Method:  "btree",
		}

		err := client.CreateIndex(schema, table, idx)
		assert.NoError(t, err)
	})

	t.Run("creates unique index", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE UNIQUE INDEX "idx_events_email" ON "public"\."events_new" \("email"\)`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		idx := infra.IndexDef{
			Name:     "idx_events_email",
			Columns:  []string{"email"},
			IsUnique: true,
			Method:   "btree",
		}

		err := client.CreateIndex(schema, table, idx)
		assert.NoError(t, err)
	})

	t.Run("creates index with non-btree method", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE INDEX "idx_events_data" ON "public"\."events_new" USING gin \("data"\)`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		idx := infra.IndexDef{
			Name:    "idx_events_data",
			Columns: []string{"data"},
			Method:  "gin",
		}

		err := client.CreateIndex(schema, table, idx)
		assert.NoError(t, err)
	})

	t.Run("creates partial index with predicate", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		predicate := "status = 'active'"
		mock.ExpectExec(`CREATE INDEX "idx_events_active" ON "public"\."events_new" \("status"\) WHERE status = 'active'`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		idx := infra.IndexDef{
			Name:      "idx_events_active",
			Columns:   []string{"status"},
			Method:    "btree",
			Predicate: &predicate,
		}

		err := client.CreateIndex(schema, table, idx)
		assert.NoError(t, err)
	})

	t.Run("creates expression index", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		expression := "lower(email)"
		mock.ExpectExec(`CREATE INDEX "idx_events_lower_email" ON "public"\."events_new" \(lower\(email\)\)`).
			WillReturnResult(pgxmock.NewResult("CREATE", 0))

		idx := infra.IndexDef{
			Name:       "idx_events_lower_email",
			Columns:    []string{"email"},
			Method:     "btree",
			Expression: &expression,
		}

		err := client.CreateIndex(schema, table, idx)
		assert.NoError(t, err)
	})

	t.Run("returns error on connection failure for primary key", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`ALTER TABLE "public"\."events_new" ADD CONSTRAINT`).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		idx := infra.IndexDef{
			Name:      "events_new_pkey",
			Columns:   []string{"id"},
			IsPrimary: true,
			Method:    "btree",
		}

		err := client.CreateIndex(schema, table, idx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create primary key")
	})

	t.Run("returns error on connection failure for regular index", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`CREATE INDEX "idx_events_name"`).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		idx := infra.IndexDef{
			Name:    "idx_events_name",
			Columns: []string{"name"},
			Method:  "btree",
		}

		err := client.CreateIndex(schema, table, idx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create index")
	})
}

func TestCreateForeignKey(t *testing.T) {
	schema := "public"
	table := "events_new"

	t.Run("creates foreign key with default actions", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`ALTER TABLE "public"\."events_new" ADD CONSTRAINT "fk_events_user" FOREIGN KEY \("user_id"\) REFERENCES "public"\."users" \("id"\)`).
			WillReturnResult(pgxmock.NewResult("ALTER", 0))

		fk := infra.ForeignKeyDef{
			Name:              "fk_events_user",
			Columns:           []string{"user_id"},
			ReferencedSchema:  "public",
			ReferencedTable:   "users",
			ReferencedColumns: []string{"id"},
			OnDelete:          "NO ACTION",
			OnUpdate:          "NO ACTION",
		}

		err := client.CreateForeignKey(schema, table, fk)
		assert.NoError(t, err)
	})

	t.Run("creates foreign key with CASCADE on delete", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`ALTER TABLE "public"\."events_new" ADD CONSTRAINT "fk_events_user" FOREIGN KEY \("user_id"\) REFERENCES "public"\."users" \("id"\) ON DELETE CASCADE`).
			WillReturnResult(pgxmock.NewResult("ALTER", 0))

		fk := infra.ForeignKeyDef{
			Name:              "fk_events_user",
			Columns:           []string{"user_id"},
			ReferencedSchema:  "public",
			ReferencedTable:   "users",
			ReferencedColumns: []string{"id"},
			OnDelete:          "CASCADE",
			OnUpdate:          "NO ACTION",
		}

		err := client.CreateForeignKey(schema, table, fk)
		assert.NoError(t, err)
	})

	t.Run("creates foreign key with SET NULL on update", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`ALTER TABLE "public"\."events_new" ADD CONSTRAINT "fk_events_category" FOREIGN KEY \("category_id"\) REFERENCES "public"\."categories" \("id"\) ON UPDATE SET NULL`).
			WillReturnResult(pgxmock.NewResult("ALTER", 0))

		fk := infra.ForeignKeyDef{
			Name:              "fk_events_category",
			Columns:           []string{"category_id"},
			ReferencedSchema:  "public",
			ReferencedTable:   "categories",
			ReferencedColumns: []string{"id"},
			OnDelete:          "NO ACTION",
			OnUpdate:          "SET NULL",
		}

		err := client.CreateForeignKey(schema, table, fk)
		assert.NoError(t, err)
	})

	t.Run("creates composite foreign key", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`ALTER TABLE "public"\."events_new" ADD CONSTRAINT "fk_events_composite" FOREIGN KEY \("tenant_id", "org_id"\) REFERENCES "public"\."organizations" \("tenant_id", "id"\)`).
			WillReturnResult(pgxmock.NewResult("ALTER", 0))

		fk := infra.ForeignKeyDef{
			Name:              "fk_events_composite",
			Columns:           []string{"tenant_id", "org_id"},
			ReferencedSchema:  "public",
			ReferencedTable:   "organizations",
			ReferencedColumns: []string{"tenant_id", "id"},
			OnDelete:          "NO ACTION",
			OnUpdate:          "NO ACTION",
		}

		err := client.CreateForeignKey(schema, table, fk)
		assert.NoError(t, err)
	})

	t.Run("creates foreign key referencing different schema", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`ALTER TABLE "public"\."events_new" ADD CONSTRAINT "fk_events_ref" FOREIGN KEY \("ref_id"\) REFERENCES "other_schema"\."references" \("id"\)`).
			WillReturnResult(pgxmock.NewResult("ALTER", 0))

		fk := infra.ForeignKeyDef{
			Name:              "fk_events_ref",
			Columns:           []string{"ref_id"},
			ReferencedSchema:  "other_schema",
			ReferencedTable:   "references",
			ReferencedColumns: []string{"id"},
			OnDelete:          "NO ACTION",
			OnUpdate:          "NO ACTION",
		}

		err := client.CreateForeignKey(schema, table, fk)
		assert.NoError(t, err)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock, client := setupConvertMock(t, pgxmock.QueryMatcherRegexp)

		mock.ExpectExec(`ALTER TABLE "public"\."events_new" ADD CONSTRAINT`).
			WillReturnError(ErrPostgreSQLConnectionFailure)

		fk := infra.ForeignKeyDef{
			Name:              "fk_events_user",
			Columns:           []string{"user_id"},
			ReferencedSchema:  "public",
			ReferencedTable:   "users",
			ReferencedColumns: []string{"id"},
			OnDelete:          "NO ACTION",
			OnUpdate:          "NO ACTION",
		}

		err := client.CreateForeignKey(schema, table, fk)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create foreign key")
	})
}
