package postgresql

import (
	"errors"
	"fmt"
)

// ErrUnsupportedPartitionKeyType represents an error indicating that the column type for partitioning is not supported.
var ErrUnsupportedPartitionKeyType = errors.New("unsupported partition key column type")

type ColumnType string

const (
	DateColumnType     ColumnType = "date"
	DateTimeColumnType ColumnType = "timestamp"
	UUIDColumnType     ColumnType = "uuid"
)

func (p Postgres) GetColumnDataType(schema, table, column string) (ColumnType, error) {
	var columnType string

	query := `SELECT
		data_type as columnType
	FROM information_schema.columns
	WHERE
		table_schema = $1
		AND table_name = $2
		AND column_name = $3`

	err := p.conn.QueryRow(p.ctx, query, schema, table, column).Scan(&columnType)
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
