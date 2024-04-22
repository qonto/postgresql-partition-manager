// Package postgresql provides methods to interact with PostgreSQL server
package postgresql

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type PgxIface interface {
	Close(context.Context) error
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	PgConn() *pgconn.PgConn
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Postgres struct {
	ctx    context.Context
	conn   PgxIface
	logger slog.Logger
}

func New(logger slog.Logger, conn PgxIface) *Postgres {
	return &Postgres{
		ctx:    context.TODO(),
		conn:   conn,
		logger: logger,
	}
}
