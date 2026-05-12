//nolint:wsl_v5
package postgresql_test

import (
	"testing"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"github.com/stretchr/testify/assert"
)

func TestGetTableColumns(t *testing.T) {
	schema := testSchema
	table := testTable

	mock, p := setupMock(t, pgxmock.QueryMatcherRegexp)
	query := `SELECT`

	t.Run("returns columns with correct definitions", func(t *testing.T) {
		defaultVal := "now()"
		rows := mock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default", "is_generated"}).
			AddRow("id", "bigint", false, nil, false).
			AddRow("name", "text", true, nil, false).
			AddRow("created_at", "timestamp with time zone", false, &defaultVal, false).
			AddRow("computed", "integer", false, nil, true)

		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnRows(rows)
		columns, err := p.GetTableColumns(schema, table)

		assert.NoError(t, err)
		assert.Len(t, columns, 4)

		assert.Equal(t, "id", columns[0].Name)
		assert.Equal(t, "bigint", columns[0].DataType)
		assert.False(t, columns[0].IsNullable)
		assert.Nil(t, columns[0].DefaultValue)
		assert.False(t, columns[0].IsGenerated)

		assert.Equal(t, "name", columns[1].Name)
		assert.Equal(t, "text", columns[1].DataType)
		assert.True(t, columns[1].IsNullable)
		assert.Nil(t, columns[1].DefaultValue)

		assert.Equal(t, "created_at", columns[2].Name)
		assert.Equal(t, "timestamp with time zone", columns[2].DataType)
		assert.False(t, columns[2].IsNullable)
		assert.NotNil(t, columns[2].DefaultValue)
		assert.Equal(t, "now()", *columns[2].DefaultValue)

		assert.Equal(t, "computed", columns[3].Name)
		assert.True(t, columns[3].IsGenerated)
	})

	t.Run("returns empty slice for table with no columns", func(t *testing.T) {
		rows := mock.NewRows([]string{"column_name", "data_type", "is_nullable", "column_default", "is_generated"})
		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnRows(rows)
		columns, err := p.GetTableColumns(schema, table)

		assert.NoError(t, err)
		assert.Empty(t, columns)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnError(ErrPostgreSQLConnectionFailure)
		_, err := p.GetTableColumns(schema, table)
		assert.Error(t, err)
	})
}

func TestGetTableIndexes(t *testing.T) {
	schema := testSchema
	table := testTable

	mock, p := setupMock(t, pgxmock.QueryMatcherRegexp)
	query := `SELECT`

	t.Run("returns indexes with correct definitions", func(t *testing.T) {
		predicate := "active = true"
		rows := mock.NewRows([]string{"index_name", "is_unique", "is_primary", "method", "predicate", "expression", "columns"}).
			AddRow("my_table_pkey", true, true, "btree", nil, nil, []string{"id"}).
			AddRow("idx_my_table_name", false, false, "btree", nil, nil, []string{"name"}).
			AddRow("idx_my_table_active", false, false, "btree", &predicate, nil, []string{"status"})

		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnRows(rows)
		indexes, err := p.GetTableIndexes(schema, table)

		assert.NoError(t, err)
		assert.Len(t, indexes, 3)

		// Primary key index
		assert.Equal(t, "my_table_pkey", indexes[0].Name)
		assert.True(t, indexes[0].IsUnique)
		assert.True(t, indexes[0].IsPrimary)
		assert.Equal(t, "btree", indexes[0].Method)
		assert.Nil(t, indexes[0].Predicate)
		assert.Nil(t, indexes[0].Expression)
		assert.Equal(t, []string{"id"}, indexes[0].Columns)

		// Regular index
		assert.Equal(t, "idx_my_table_name", indexes[1].Name)
		assert.False(t, indexes[1].IsUnique)
		assert.False(t, indexes[1].IsPrimary)
		assert.Equal(t, "btree", indexes[1].Method)
		assert.Equal(t, []string{"name"}, indexes[1].Columns)

		// Partial index
		assert.Equal(t, "idx_my_table_active", indexes[2].Name)
		assert.NotNil(t, indexes[2].Predicate)
		assert.Equal(t, "active = true", *indexes[2].Predicate)
	})

	t.Run("returns empty slice for table with no indexes", func(t *testing.T) {
		rows := mock.NewRows([]string{"index_name", "is_unique", "is_primary", "method", "predicate", "expression", "columns"})
		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnRows(rows)
		indexes, err := p.GetTableIndexes(schema, table)

		assert.NoError(t, err)
		assert.Empty(t, indexes)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnError(ErrPostgreSQLConnectionFailure)
		_, err := p.GetTableIndexes(schema, table)
		assert.Error(t, err)
	})
}

func TestGetTableForeignKeys(t *testing.T) {
	schema := testSchema
	table := testTable

	mock, p := setupMock(t, pgxmock.QueryMatcherRegexp)
	query := `SELECT`

	t.Run("returns foreign keys with correct definitions", func(t *testing.T) {
		rows := mock.NewRows([]string{"name", "columns", "referenced_schema", "referenced_table", "referenced_columns", "on_delete", "on_update"}).
			AddRow("fk_orders_customer", []string{"customer_id"}, "public", "customers", []string{"id"}, "CASCADE", "NO ACTION").
			AddRow("fk_orders_product", []string{"product_id", "variant_id"}, "inventory", "products", []string{"id", "variant_id"}, "RESTRICT", "CASCADE")

		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnRows(rows)
		fks, err := p.GetTableForeignKeys(schema, table)

		assert.NoError(t, err)
		assert.Len(t, fks, 2)

		// Single-column FK with CASCADE delete
		assert.Equal(t, "fk_orders_customer", fks[0].Name)
		assert.Equal(t, []string{"customer_id"}, fks[0].Columns)
		assert.Equal(t, "public", fks[0].ReferencedSchema)
		assert.Equal(t, "customers", fks[0].ReferencedTable)
		assert.Equal(t, []string{"id"}, fks[0].ReferencedColumns)
		assert.Equal(t, "CASCADE", fks[0].OnDelete)
		assert.Equal(t, "NO ACTION", fks[0].OnUpdate)

		// Composite FK with RESTRICT delete and CASCADE update
		assert.Equal(t, "fk_orders_product", fks[1].Name)
		assert.Equal(t, []string{"product_id", "variant_id"}, fks[1].Columns)
		assert.Equal(t, "inventory", fks[1].ReferencedSchema)
		assert.Equal(t, "products", fks[1].ReferencedTable)
		assert.Equal(t, []string{"id", "variant_id"}, fks[1].ReferencedColumns)
		assert.Equal(t, "RESTRICT", fks[1].OnDelete)
		assert.Equal(t, "CASCADE", fks[1].OnUpdate)
	})

	t.Run("returns empty slice for table with no foreign keys", func(t *testing.T) {
		rows := mock.NewRows([]string{"name", "columns", "referenced_schema", "referenced_table", "referenced_columns", "on_delete", "on_update"})
		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnRows(rows)
		fks, err := p.GetTableForeignKeys(schema, table)

		assert.NoError(t, err)
		assert.Empty(t, fks)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnError(ErrPostgreSQLConnectionFailure)
		_, err := p.GetTableForeignKeys(schema, table)
		assert.Error(t, err)
	})
}

func TestGetReferencingForeignKeys(t *testing.T) {
	schema := testSchema
	table := testTable

	mock, p := setupMock(t, pgxmock.QueryMatcherRegexp)
	query := `SELECT`

	t.Run("returns referencing foreign keys", func(t *testing.T) {
		rows := mock.NewRows([]string{"name", "columns", "source_schema", "source_table", "referenced_columns", "on_delete", "on_update"}).
			AddRow("fk_line_items_order", []string{"order_id"}, "public", "line_items", []string{"id"}, "CASCADE", "NO ACTION")

		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnRows(rows)
		fks, err := p.GetReferencingForeignKeys(schema, table)

		assert.NoError(t, err)
		assert.Len(t, fks, 1)
		assert.Equal(t, "fk_line_items_order", fks[0].Name)
		assert.Equal(t, []string{"order_id"}, fks[0].Columns)
		assert.Equal(t, "public", fks[0].ReferencedSchema)
		assert.Equal(t, "line_items", fks[0].ReferencedTable)
		assert.Equal(t, []string{"id"}, fks[0].ReferencedColumns)
		assert.Equal(t, "CASCADE", fks[0].OnDelete)
		assert.Equal(t, "NO ACTION", fks[0].OnUpdate)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnError(ErrPostgreSQLConnectionFailure)
		_, err := p.GetReferencingForeignKeys(schema, table)
		assert.Error(t, err)
	})
}

func TestGetTableCheckConstraints(t *testing.T) {
	schema := testSchema
	table := testTable

	mock, p := setupMock(t, pgxmock.QueryMatcherRegexp)
	query := `SELECT`

	t.Run("returns check constraints", func(t *testing.T) {
		rows := mock.NewRows([]string{"name", "expression"}).
			AddRow("chk_positive_amount", "CHECK ((amount > 0))").
			AddRow("chk_status_valid", "CHECK ((status = ANY (ARRAY['active'::text, 'inactive'::text])))")

		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnRows(rows)
		constraints, err := p.GetTableCheckConstraints(schema, table)

		assert.NoError(t, err)
		assert.Len(t, constraints, 2)
		assert.Equal(t, "chk_positive_amount", constraints[0].Name)
		assert.Equal(t, "CHECK ((amount > 0))", constraints[0].Expression)
		assert.Equal(t, "chk_status_valid", constraints[1].Name)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnError(ErrPostgreSQLConnectionFailure)
		_, err := p.GetTableCheckConstraints(schema, table)
		assert.Error(t, err)
	})
}

func TestGetTablePrimaryKey(t *testing.T) {
	schema := testSchema
	table := testTable

	mock, p := setupMock(t, pgxmock.QueryMatcherRegexp)
	query := `SELECT`

	t.Run("returns single-column primary key", func(t *testing.T) {
		rows := mock.NewRows([]string{"attname"}).AddRow("id")
		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnRows(rows)
		pk, err := p.GetTablePrimaryKey(schema, table)

		assert.NoError(t, err)
		assert.Equal(t, []string{"id"}, pk)
	})

	t.Run("returns composite primary key", func(t *testing.T) {
		rows := mock.NewRows([]string{"attname"}).AddRow("tenant_id").AddRow("id")
		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnRows(rows)
		pk, err := p.GetTablePrimaryKey(schema, table)

		assert.NoError(t, err)
		assert.Equal(t, []string{"tenant_id", "id"}, pk)
	})

	t.Run("returns empty slice for table without primary key", func(t *testing.T) {
		rows := mock.NewRows([]string{"attname"})
		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnRows(rows)
		pk, err := p.GetTablePrimaryKey(schema, table)

		assert.NoError(t, err)
		assert.Empty(t, pk)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnError(ErrPostgreSQLConnectionFailure)
		_, err := p.GetTablePrimaryKey(schema, table)
		assert.Error(t, err)
	})
}

func TestHasPrimaryKey(t *testing.T) {
	schema := testSchema
	table := testTable

	mock, p := setupMock(t, pgxmock.QueryMatcherRegexp)
	query := `SELECT EXISTS`

	t.Run("returns true when table has primary key", func(t *testing.T) {
		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnRows(mock.NewRows([]string{"exists"}).AddRow(true))
		hasPK, err := p.HasPrimaryKey(schema, table)

		assert.NoError(t, err)
		assert.True(t, hasPK)
	})

	t.Run("returns false when table has no primary key", func(t *testing.T) {
		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnRows(mock.NewRows([]string{"exists"}).AddRow(false))
		hasPK, err := p.HasPrimaryKey(schema, table)

		assert.NoError(t, err)
		assert.False(t, hasPK)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnError(ErrPostgreSQLConnectionFailure)
		_, err := p.HasPrimaryKey(schema, table)
		assert.Error(t, err)
	})
}

func TestGetTableRowCount(t *testing.T) {
	schema := testSchema
	table := testTable

	mock, p := setupMock(t, pgxmock.QueryMatcherRegexp)
	query := `SELECT COALESCE`

	t.Run("returns row count", func(t *testing.T) {
		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnRows(mock.NewRows([]string{"count"}).AddRow(int64(42000)))
		count, err := p.GetTableRowCount(schema, table)

		assert.NoError(t, err)
		assert.Equal(t, int64(42000), count)
	})

	t.Run("returns error on connection failure", func(t *testing.T) {
		mock.ExpectQuery(query).WithArgs(schema, table).WillReturnError(ErrPostgreSQLConnectionFailure)
		_, err := p.GetTableRowCount(schema, table)
		assert.Error(t, err)
	})
}

// Verify that the type aliases in the convert package correctly reference the parent types.
func TestColumnDefStructFields(t *testing.T) {
	defaultVal := "nextval('seq')"
	col := postgresql.ColumnDef{
		Name:         "id",
		DataType:     "bigint",
		IsNullable:   false,
		DefaultValue: &defaultVal,
		IsGenerated:  false,
	}

	assert.Equal(t, "id", col.Name)
	assert.Equal(t, "bigint", col.DataType)
	assert.False(t, col.IsNullable)
	assert.Equal(t, "nextval('seq')", *col.DefaultValue)
	assert.False(t, col.IsGenerated)
}

func TestIndexDefStructFields(t *testing.T) {
	predicate := "deleted_at IS NULL"
	idx := postgresql.IndexDef{
		Name:      "idx_active_users",
		Columns:   []string{"email", "tenant_id"},
		IsUnique:  true,
		IsPrimary: false,
		Predicate: &predicate,
		Method:    "btree",
	}

	assert.Equal(t, "idx_active_users", idx.Name)
	assert.Equal(t, []string{"email", "tenant_id"}, idx.Columns)
	assert.True(t, idx.IsUnique)
	assert.False(t, idx.IsPrimary)
	assert.Equal(t, "deleted_at IS NULL", *idx.Predicate)
	assert.Equal(t, "btree", idx.Method)
}

func TestForeignKeyDefStructFields(t *testing.T) {
	fk := postgresql.ForeignKeyDef{
		Name:              "fk_orders_customer",
		Columns:           []string{"customer_id"},
		ReferencedSchema:  "public",
		ReferencedTable:   "customers",
		ReferencedColumns: []string{"id"},
		OnDelete:          "CASCADE",
		OnUpdate:          "NO ACTION",
	}

	assert.Equal(t, "fk_orders_customer", fk.Name)
	assert.Equal(t, []string{"customer_id"}, fk.Columns)
	assert.Equal(t, "public", fk.ReferencedSchema)
	assert.Equal(t, "customers", fk.ReferencedTable)
	assert.Equal(t, []string{"id"}, fk.ReferencedColumns)
	assert.Equal(t, "CASCADE", fk.OnDelete)
	assert.Equal(t, "NO ACTION", fk.OnUpdate)
}
