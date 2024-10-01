//nolint:golint,wsl,goconst
package postgresql_test

import (
	"fmt"
	"testing"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"github.com/stretchr/testify/assert"
)

func generateTable(t *testing.T) (schema, table, fullQualifiedTable, parent string) {
	t.Helper()

	schema = "public"
	table = "my_table"
	fullQualifiedTable = fmt.Sprintf("%s.%s", schema, table)
	parent = "my_parent_table"

	return
}

func TestIsPartitionAttached(t *testing.T) {
	schema, table, fullQualifiedTable, _ := generateTable(t)

	mock, p := setupMock(t, pgxmock.QueryMatcherRegexp)
	query := "SELECT EXISTS"

	mock.ExpectQuery(query).WithArgs(fullQualifiedTable).WillReturnRows(mock.NewRows([]string{"EXISTS"}).AddRow(true))
	exists, err := p.IsPartitionAttached(schema, table)
	assert.Nil(t, err, "IsPartitionAttached should succeed")
	assert.True(t, exists, "Table should be attached")

	mock.ExpectQuery(query).WithArgs(fullQualifiedTable).WillReturnRows(mock.NewRows([]string{"EXISTS"}).AddRow(false))
	exists, err = p.IsPartitionAttached(schema, table)
	assert.Nil(t, err, "IsPartitionAttached should succeed")
	assert.False(t, exists, "Table should not be attached")

	mock.ExpectQuery(query).WithArgs(fullQualifiedTable).WillReturnError(ErrPostgreSQLConnectionFailure)
	_, err = p.IsPartitionAttached(schema, table)
	assert.Error(t, err, "IsPartitionAttached should fail")
}

func TestAttachPartition(t *testing.T) {
	schema, table, _, parent := generateTable(t)
	lowerBound := "2024-01-30"
	upperBound := "2024-01-31"

	mock, p := setupMock(t, pgxmock.QueryMatcherEqual)
	query := fmt.Sprintf(`ALTER TABLE %s.%s ATTACH PARTITION %s.%s FOR VALUES FROM ('%s') TO ('%s')`, schema, parent, schema, table, lowerBound, upperBound)

	mock.ExpectExec(query).WillReturnResult(pgxmock.NewResult("ALTER", 1))
	err := p.AttachPartition(schema, table, parent, lowerBound, upperBound)
	assert.Nil(t, err, "AttachPartition should succeed")

	mock.ExpectExec(query).WillReturnError(ErrPostgreSQLConnectionFailure)
	err = p.AttachPartition(schema, table, parent, lowerBound, upperBound)
	assert.Error(t, err, "AttachPartition should fail")
}

func TestDetachPartitionConcurrently(t *testing.T) {
	schema, table, _, parent := generateTable(t)

	mock, p := setupMock(t, pgxmock.QueryMatcherEqual)
	query := fmt.Sprintf(`ALTER TABLE %s.%s DETACH PARTITION %s.%s CONCURRENTLY`, schema, parent, schema, table)

	mock.ExpectExec(query).WillReturnResult(pgxmock.NewResult("ALTER", 1))
	err := p.DetachPartitionConcurrently(schema, table, parent)
	assert.Nil(t, err, "AttachPartition should succeed")

	mock.ExpectExec(query).WillReturnError(ErrPostgreSQLConnectionFailure)
	err = p.DetachPartitionConcurrently(schema, table, parent)
	assert.Error(t, err, "AttachPartition should fail")
}

func TestFinalizePartitionDetach(t *testing.T) {
	schema, table, _, parent := generateTable(t)

	mock, p := setupMock(t, pgxmock.QueryMatcherEqual)

	query := fmt.Sprintf(`ALTER TABLE %s.%s DETACH PARTITION %s.%s FINALIZE`, schema, parent, schema, table)

	mock.ExpectExec(query).WillReturnResult(pgxmock.NewResult("ALTER", 1))
	err := p.FinalizePartitionDetach(schema, table, parent)
	assert.Nil(t, err, "AttachPartition should succeed")

	mock.ExpectExec(query).WillReturnError(ErrPostgreSQLConnectionFailure)
	err = p.FinalizePartitionDetach(schema, table, parent)
	assert.Error(t, err, "AttachPartition should fail")
}

func TestGetPartitionSettings(t *testing.T) {
	schema, table, _, _ := generateTable(t)
	expectedStrategy := "RANGE"
	expectedKey := "created_at"

	mock, p := setupMock(t, pgxmock.QueryMatcherRegexp)

	query := `SELECT regexp_match`

	mock.ExpectQuery(query).WillReturnRows(mock.NewRows([]string{"partkeydef"}).AddRow([]string{expectedStrategy, expectedKey}))
	strategy, key, err := p.GetPartitionSettings(schema, table)
	assert.Nil(t, err, "GetPartitionSettings should succeed")
	assert.Equal(t, strategy, expectedStrategy, "Strategy should match")
	assert.Equal(t, key, expectedKey, "Key should match")

	mock.ExpectQuery(query).WillReturnRows(mock.NewRows([]string{"partkeydef"}).AddRow([]string{}))
	_, _, err = p.GetPartitionSettings(schema, table)
	assert.Error(t, err, "GetPartitionSettings should fail")
	assert.ErrorIs(t, err, postgresql.ErrTableIsNotPartitioned)

	mock.ExpectQuery(query).WillReturnError(ErrPostgreSQLConnectionFailure)
	_, _, err = p.GetPartitionSettings(schema, table)
	assert.Error(t, err, "GetPartitionSettings should fail")
}

func TestListPartitions(t *testing.T) {
	schema, table, parent, _ := generateTable(t)

	mock, p := setupMock(t, pgxmock.QueryMatcherRegexp)
	query := `WITH parts as`

	expectedPartitions := []postgresql.PartitionResult{
		{
			Schema:      schema,
			ParentTable: parent,
			Name:        fmt.Sprintf("%s_%s", parent, "2024_01_29"),
			LowerBound:  "2024-01-29",
			UpperBound:  "2024-01-30",
		},
		{
			Schema:      table,
			ParentTable: parent,
			Name:        fmt.Sprintf("%s_%s", parent, "2024_01_30"),
			LowerBound:  "2024-01-30",
			UpperBound:  "2024-01-31",
		},
	}

	rows := mock.NewRows([]string{"schema", "name", "parentTable", "lowerBound", "upperBound"})
	for _, p := range expectedPartitions {
		rows.AddRow(p.Schema, p.Name, p.ParentTable, p.LowerBound, p.UpperBound)
	}
	mock.ExpectQuery(query).WillReturnRows(rows)
	result, err := p.ListPartitions(schema, table)
	assert.Nil(t, err, "ListPartitions should succeed")
	assert.Equal(t, result, expectedPartitions, "Partitions should be match")

	rows = mock.NewRows([]string{"invalidColumn"}).AddRow("invalidColumn")
	mock.ExpectQuery(query).WillReturnRows(rows)
	_, err = p.ListPartitions(schema, table)
	assert.Error(t, err, "ListPartitions should fail")

	mock.ExpectQuery(query).WillReturnError(ErrPostgreSQLConnectionFailure)
	_, err = p.ListPartitions(schema, table)
	assert.Error(t, err, "ListPartitions should fail")
}
