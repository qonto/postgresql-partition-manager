package postgresql

import (
	"testing"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/qonto/postgresql-partition-manager/internal/infra/logger"
	"github.com/stretchr/testify/assert"
)

func TestGetColumn(t *testing.T) {
	schema := "public"
	table := "my_table"
	column := "my_column"

	const query = `SELECT data_type as columnType FROM information_schema.columns WHERE table_schema = \$1 AND table_name = \$2 AND column_name = \$3`

	mock, err := pgxmock.NewConn()
	if err != nil {
		t.Fatalf("ERROR: Fail to initialize PostgreSQL mock: %s", err)
	}

	logger, err := logger.New(false, "text")
	if err != nil {
		t.Fatalf("ERROR: Fail to initialize logger: %s", err)
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
			Date,
		},
		{
			"Date time",
			"timestamp",
			DateTime,
		},
		{
			"Date time without time zone",
			"timestamp without time zone",
			DateTime,
		},
		{
			"Date time with time zone",
			"timestamp with time zone",
			DateTimeWithTZ,
		},
		{
			"UUID",
			"uuid",
			UUID,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock.ExpectQuery(query).WithArgs(schema, table, column).WillReturnRows(mock.NewRows([]string{"columnType"}).AddRow(tc.postgreSQLcolumn))
			dataType, err := p.GetColumnDataType(schema, table, column)

			assert.Nil(t, err, "getColumnDataType should succeed")
			assert.Equal(t, dataType, tc.dataType, "Column type should match")
		})
	}
}
