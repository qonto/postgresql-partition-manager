package postgresql

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	infra "github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
)

const (
	// defaultLockTimeout is the default lock_timeout in seconds for cutover transactions.
	defaultLockTimeout = 5
	// defaultStatementTimeout is the default statement_timeout in seconds for cutover transactions.
	defaultStatementTimeout = 30
)

// Tx represents a database transaction for operations that need atomicity.
// This interface is defined here (infrastructure layer) to avoid import cycles
// with the core layer (pkg/convert) which references it.
type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Client provides database operations for the table partition conversion feature.
// It reuses the existing postgresql package's PgxIface for database connectivity.
type Client struct {
	ctx              context.Context
	conn             infra.PgxIface
	logger           slog.Logger
	lockTimeout      int // seconds, used by BeginTx for SET LOCAL lock_timeout
	statementTimeout int // seconds, used by BeginTx for SET LOCAL statement_timeout
}

// New creates a new Client with the given logger and connection.
func New(logger slog.Logger, conn infra.PgxIface) *Client {
	return &Client{
		ctx:              context.TODO(),
		conn:             conn,
		logger:           logger,
		lockTimeout:      defaultLockTimeout,
		statementTimeout: defaultStatementTimeout,
	}
}

// NewWithTimeouts creates a new Client with custom lock and statement timeouts.
// These timeouts are used by BeginTx when setting SET LOCAL lock_timeout and statement_timeout.
func NewWithTimeouts(logger slog.Logger, conn infra.PgxIface, lockTimeout, statementTimeout int) *Client {
	if lockTimeout <= 0 {
		lockTimeout = defaultLockTimeout
	}

	if statementTimeout <= 0 {
		statementTimeout = defaultStatementTimeout
	}

	return &Client{
		ctx:              context.TODO(),
		conn:             conn,
		logger:           logger,
		lockTimeout:      lockTimeout,
		statementTimeout: statementTimeout,
	}
}
