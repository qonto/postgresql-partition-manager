// Package postgresql provides methods to interact with PostgreSQL internal resources (tables, columns, ...)
package postgresql

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
)

type ConnectionSettings struct {
	URL              string
	StatementTimeout int
	LockTimeout      int
}

func GetDatabaseConnection(c ConnectionSettings) (*pgx.Conn, error) {
	config, err := pgx.ParseConfig(c.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database connection: %w", err)
	}

	config.RuntimeParams["statement_timeout"] = strconv.Itoa(c.StatementTimeout)
	config.RuntimeParams["lock_timeout"] = strconv.Itoa(c.LockTimeout)

	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to the database: %w", err)
	}

	return conn, nil
}
