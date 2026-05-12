package postgresql

import (
	"context"
	"log/slog"

	infra "github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
)

// Client provides database operations for the table partition conversion feature.
// It reuses the existing postgresql package's PgxIface for database connectivity.
type Client struct {
	ctx    context.Context
	conn   infra.PgxIface
	logger slog.Logger
}

// New creates a new Client with the given logger and connection.
func New(logger slog.Logger, conn infra.PgxIface) *Client {
	return &Client{
		ctx:    context.TODO(),
		conn:   conn,
		logger: logger,
	}
}
