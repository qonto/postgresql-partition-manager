//nolint:golint,wsl
package postgresql_test

import (
	"fmt"
	"testing"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
)

func TestCreateTableLikeTable(t *testing.T) {
	schema := "public"
	table := "my_table"
	parentTable := "parent_table"

	query := fmt.Sprintf(`CREATE TABLE %s.%s (LIKE %s.%s INCLUDING ALL)`, schema, table, schema, parentTable)

	mock, p := setupMock(t, pgxmock.QueryMatcherEqual)

	mock.ExpectExec(query).WillReturnResult(pgxmock.NewResult("CREATE", 1))
	err := p.CreateTableLikeTable(schema, table, parentTable)
	assert.Nil(t, err, "DropTable should succeed")

	mock.ExpectExec(query).WillReturnError(ErrPostgreSQLConnectionFailure)
	err = p.CreateTableLikeTable(schema, table, parentTable)
	assert.Error(t, err, "DropTable should fail")
}

func TestDropTable(t *testing.T) {
	schema := "public"
	table := "my_table"
	fullQualifiedName := fmt.Sprintf("%s.%s", schema, table)
	query := fmt.Sprintf(`DROP TABLE %s`, fullQualifiedName)

	mock, p := setupMock(t, pgxmock.QueryMatcherEqual)

	mock.ExpectExec(query).WillReturnResult(pgxmock.NewResult("DROP", 1))
	err := p.DropTable(schema, table)
	assert.Nil(t, err, "DropTable should succeed")

	mock.ExpectExec(query).WillReturnError(ErrPostgreSQLConnectionFailure)
	err = p.DropTable(schema, table)
	assert.Error(t, err, "DropTable should fail")
}

func TestIsTableExists(t *testing.T) {
	schema := "public"
	table := "my_table"

	query := "SELECT EXISTS"

	mock, p := setupMock(t, pgxmock.QueryMatcherRegexp)

	mock.ExpectQuery(query).WithArgs(schema, table).WillReturnRows(mock.NewRows([]string{"EXISTS"}).AddRow(true))
	exists, err := p.IsTableExists(schema, table)
	assert.Nil(t, err, "IsTableExists should succeed")
	assert.True(t, exists, "Table should exists")

	mock.ExpectQuery(query).WithArgs(schema, table).WillReturnRows(mock.NewRows([]string{"EXISTS"}).AddRow(false))
	exists, err = p.IsTableExists(schema, table)
	assert.Nil(t, err, "IsTableExists should succeed")
	assert.False(t, exists, "Table should not exists")

	mock.ExpectQuery(query).WillReturnError(ErrPostgreSQLConnectionFailure)
	_, err = p.IsTableExists(schema, table)
	assert.Error(t, err, "IsTableExists should fail")
}
