package postgresql

import (
	"fmt"
	"testing"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/qonto/postgresql-partition-manager/internal/infra/logger"
	"github.com/stretchr/testify/assert"
)

func getMock(t *testing.T) (pgxmock.PgxConnIface, *PostgreSQL) {
	t.Helper()

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

	return mock, p
}

func TestTableExists(t *testing.T) {
	mock, p := getMock(t)

	existingTable := Table{Schema: "public", Name: "my_table"}
	existingTables := []Table{existingTable}

	query := `SELECT EXISTS\( SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = \$1 AND c.relname = \$2 \)`
	for _, table := range existingTables {
		mock.ExpectQuery(query).WithArgs(table.Schema, table.Name).WillReturnRows(mock.NewRows([]string{"exists"}).AddRow(true))
	}

	tableWithSameNameInDifferentSchema := Table{Schema: "another_schema", Name: "my_table"}
	tableWithSameSchemaButDifferentName := Table{Schema: "public", Name: "another_table"}
	missingTables := []Table{tableWithSameNameInDifferentSchema, tableWithSameSchemaButDifferentName}

	for _, table := range missingTables {
		mock.ExpectQuery(query).WithArgs(table.Schema, table.Name).WillReturnRows(mock.NewRows([]string{"exists"}).AddRow(false))
	}

	testCases := []struct {
		name   string
		table  Table
		exists bool
	}{
		{
			"Existing table",
			existingTable,
			true,
		},
		{
			"Table with same name in different schema",
			tableWithSameNameInDifferentSchema,
			false,
		},
		{
			"Table with same schema, but different name",
			tableWithSameSchemaButDifferentName,
			false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			exists, err := p.tableExists(tc.table)

			assert.Nil(t, err, "tableExists should succeed")
			assert.Equal(t, exists, tc.exists, "Exists should match")
		})
	}
}

func TestIsPartitionIsAttached(t *testing.T) {
	mock, p := getMock(t)

	attachedPartition := Partition{ParentTable: "partioned_table", Schema: "public", Name: "partioned_table_2024"}
	unattachedPartition := Partition{ParentTable: "partioned_table", Schema: "public", Name: "partioned_table_1999"}

	query := `SELECT EXISTS\( SELECT 1 FROM pg_inherits WHERE inhrelid = \$1::regclass \)`
	mock.ExpectQuery(query).WithArgs(attachedPartition.QualifiedName()).WillReturnRows(mock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(query).WithArgs(unattachedPartition.QualifiedName()).WillReturnRows(mock.NewRows([]string{"exists"}).AddRow(false))

	testCases := []struct {
		name      string
		partition Partition
		exists    bool
	}{
		{
			"Attached partition",
			attachedPartition,
			true,
		},
		{
			"Unattached partition",
			unattachedPartition,
			false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			exists, err := p.isPartitionIsAttached(tc.partition)

			assert.Nil(t, err, "isPartitionIsAttached should succeed")
			assert.Equal(t, exists, tc.exists, "Exists should match")
		})
	}
}

func TestDropTable(t *testing.T) {
	mock, p := getMock(t)

	table := Table{Schema: "public", Name: "my_table"}

	query := fmt.Sprintf("DROP TABLE %s", table.QualifiedName())
	mock.ExpectExec(query).WillReturnResult(pgxmock.NewResult("DROP", 1))

	err := p.dropTable(table)
	assert.Nil(t, err, "dropTable should succeed")
}

func TestGetPartitionSettings(t *testing.T) {
	mock, p := getMock(t)

	queryPartitionSettings := `SELECT regexp_match\(partkeydef` // Partial query
	queryColumn := `SELECT data_type as columnType FROM information_schema.columns WHERE table_schema = \$1 AND table_name = \$2 AND column_name = \$3`

	testCases := []struct {
		name             string
		column           Column
		expectedSettings PartitionSettings
	}{
		{
			"Date partition",
			Column{
				Schema:   "public",
				Table:    "my_table",
				Name:     "created_at",
				DataType: DateColumnType,
			},
			PartitionSettings{
				Strategy: RangePartitionStrategy,
				Key:      "created_at",
				KeyType:  DateColumnType,
			},
		},
		{
			"Datetime partition",
			Column{
				Schema:   "public",
				Table:    "my_table",
				Name:     "hour",
				DataType: DateTimeColumnType,
			},
			PartitionSettings{
				Strategy: RangePartitionStrategy,
				Key:      "hour",
				KeyType:  DateTimeColumnType,
			},
		},
		{
			"UUID partition",
			Column{
				Schema:   "public",
				Table:    "my_table",
				Name:     "id",
				DataType: UUIDColumnType,
			},
			PartitionSettings{
				Strategy: RangePartitionStrategy,
				Key:      "id",
				KeyType:  UUIDColumnType,
			},
		},
		{
			"List partition",
			Column{
				Schema:   "public",
				Table:    "my_table",
				Name:     "created_at",
				DataType: UUIDColumnType,
			},
			PartitionSettings{
				Strategy: ListPartitionStrategy,
				Key:      "created_at",
				KeyType:  UUIDColumnType,
			},
		},
		{
			"Hash partition",
			Column{
				Schema:   "public",
				Table:    "my_table",
				Name:     "created_at",
				DataType: UUIDColumnType,
			},
			PartitionSettings{
				Strategy: HashPartitionStrategy,
				Key:      "created_at",
				KeyType:  UUIDColumnType,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock.ExpectQuery(queryPartitionSettings).WillReturnRows(mock.NewRows([]string{"partkeydef"}).AddRow([]string{string(tc.expectedSettings.Strategy), tc.column.Name}))
			mock.ExpectQuery(queryColumn).WithArgs(tc.column.Schema, tc.column.Table, tc.column.Name).WillReturnRows(mock.NewRows([]string{"columnType"}).AddRow(string(tc.column.DataType)))

			table := Table{Schema: tc.column.Schema, Name: tc.column.Table}
			settings, err := p.GetPartitionSettings(table)

			assert.Nil(t, err, "GetPartitionSettings should succeed")
			assert.Equal(t, settings, tc.expectedSettings, "partition settings should succeed")
		})
	}
}

func TestGetPartitionSettingsErrors(t *testing.T) {
	mock, p := getMock(t)

	queryPartitionSettings := `SELECT regexp_match\(partkeydef` // Partial query
	queryColumn := `SELECT data_type as columnType FROM information_schema.columns WHERE table_schema = \$1 AND table_name = \$2 AND column_name = \$3`

	testCases := []struct {
		name             string
		column           Column
		expectedSettings PartitionSettings
	}{
		{
			"Missing partition",
			Column{
				Schema:   "public",
				Table:    "my_table",
				Name:     "created_at",
				DataType: DateColumnType,
			},
			PartitionSettings{
				Strategy: RangePartitionStrategy,
				Key:      "created_at",
				KeyType:  DateColumnType,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			maxRetries := 3
			for attempt := 1; attempt <= maxRetries; attempt++ {
				mock.ExpectQuery(queryPartitionSettings).WillReturnRows(mock.NewRows([]string{"x"}))
				mock.ExpectQuery(queryPartitionSettings).WillReturnRows(mock.NewRows([]string{"x"}))
				mock.ExpectQuery(queryPartitionSettings).WillReturnRows(mock.NewRows([]string{"x"}))
			}
			mock.ExpectQuery(queryColumn).WithArgs(tc.column.Schema, tc.column.Table, tc.column.Name).WillReturnRows(mock.NewRows([]string{"x"}))

			table := Table{Schema: tc.column.Schema, Name: tc.column.Table}
			_, err := p.GetPartitionSettings(table)

			assert.Error(t, err, ErrPartitionNotFound)
		})
	}
}
