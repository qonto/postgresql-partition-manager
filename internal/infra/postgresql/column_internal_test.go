package postgresql

import (
	"fmt"
	"testing"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/qonto/postgresql-partition-manager/internal/infra/logger"
	"github.com/stretchr/testify/assert"
)

func TestGetColumn(t *testing.T) {
	column := Column{
		Schema: "public",
		Table:  "my_table",
		Name:   "my_column",
	}

	const query = `SELECT data_type as columnType FROM information_schema.columns WHERE table_schema = \$1 AND table_name = \$2 AND column_name = \$3`

	mock, err := pgxmock.NewConn()
	if err != nil {
		fmt.Println("ERROR: Fail to initialize PostgreSQL mock: %w", err)
		panic(err)
	}

	logger, err := logger.New(false, "text")
	if err != nil {
		fmt.Println("ERROR: Fail to initialize logger: %w", err)
		panic(err)
	}

	p := New(*logger, mock)

	testCases := []struct {
		name             string
		postgreSQLcolumn string
		dataType         ColumnType
	}{
		{
			"Date",
			"date",
			DateColumnType,
		},
		{
			"Date time",
			"timestamp without time zone",
			DateTimeColumnType,
		},
		{
			"UUID",
			"uuid",
			UUIDColumnType,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock.ExpectQuery(query).WithArgs(column.Schema, column.Table, column.Name).WillReturnRows(mock.NewRows([]string{"columnType"}).AddRow(tc.postgreSQLcolumn))
			dataType, err := p.getColumnDataType(column)

			assert.Nil(t, err, "getColumnDataType should succeed")
			assert.Equal(t, dataType, tc.dataType, "Column type should match")
		})
	}
}
