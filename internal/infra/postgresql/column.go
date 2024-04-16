package postgresql

import (
	"errors"
	"fmt"
	"strings"
)

const (
	DateColumnType     ColumnType = "date"
	DateTimeColumnType ColumnType = "timestamp"
	UUIDColumnType     ColumnType = "uuid"
)

// ErrUnsupportedPartitionKeyType represents an error indicating that the column type for partitioning is not supported.
var ErrUnsupportedPartitionKeyType = errors.New("unsupported partition key type")

type ColumnType string

type Column struct {
	Schema   string
	Table    string
	Name     string
	DataType ColumnType
}

func (c Column) String() string {
	return strings.Join([]string{c.Schema, c.Table, c.Name}, ".")
}

// Return the PostgreSQL data type of the specified column
func (p PostgreSQL) getColumnDataType(column Column) (ColumnType, error) {
	var columnType string

	query := `SELECT
		data_type as columnType
	FROM information_schema.columns
	WHERE
		table_schema = $1
		AND table_name = $2
		AND column_name = $3`

	err := p.db.QueryRow(p.ctx, query, column.Schema, column.Table, column.Name).Scan(&columnType)
	if err != nil {
		return "", fmt.Errorf("failed to get %s column type: %w", column, err)
	}

	switch columnType {
	case "date":
		return DateColumnType, nil
	case "timestamp":
		return DateTimeColumnType, nil
	case "timestamp without time zone":
		return DateTimeColumnType, nil
	case "uuid":
		return UUIDColumnType, nil
	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedPartitionKeyType, columnType)
	}
}
