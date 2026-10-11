package hook

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Compile-time checks.
var (
	_ Runner         = (*PostgreSQLRunner)(nil)
	_ RenderedConfig = (*PostgreSQLConfig)(nil)
)

// LogAttrs implements RenderedConfig, returning the resolved SQL query for structured logging.
func (c *PostgreSQLConfig) LogAttrs() []any {
	return []any{"sql_query", c.SQLQuery}
}

// validatePostgreSQLConfig checks that a postgresql hook's raw config has the required fields.
func validatePostgreSQLConfig(config map[string]interface{}) error {
	if config == nil {
		return ErrPostgreSQLConfigRequired
	}

	if _, ok := config["sql_query"]; !ok {
		return ErrPostgreSQLQueryRequired
	}

	return nil
}

// resolvePostgreSQLConfig renders template variables in postgresql hook configuration fields.
func resolvePostgreSQLConfig(config map[string]interface{}, partition PartitionContext) (RenderedConfig, error) {
	pgCfg := &PostgreSQLConfig{}

	if query, ok := config["sql_query"]; ok {
		rendered, err := Render(fmt.Sprintf("%v", query), partition)
		if err != nil {
			return nil, fmt.Errorf("rendering sql_query: %w", err)
		}

		pgCfg.SQLQuery = rendered
	}

	return pgCfg, nil
}

// PgxConn defines the minimal interface needed for executing SQL statements.
type PgxConn interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Close(ctx context.Context) error
}

// ConnectorFunc is a function that creates a new database connection.
// It takes a context and a connection URL, and returns a PgxConn or an error.
type ConnectorFunc func(ctx context.Context, connectionURL string) (PgxConn, error)

// DefaultConnector creates a real pgx connection.
func DefaultConnector(ctx context.Context, connectionURL string) (PgxConn, error) {
	return pgx.Connect(ctx, connectionURL)
}

// PostgreSQLRunner executes SQL statements against a PostgreSQL database.
type PostgreSQLRunner struct {
	logger    slog.Logger
	connector ConnectorFunc
}

// NewPostgreSQLRunner creates a new PostgreSQLRunner with the given logger.
// It uses the default pgx connector for database connections.
func NewPostgreSQLRunner(logger slog.Logger) *PostgreSQLRunner {
	return &PostgreSQLRunner{
		logger:    logger,
		connector: DefaultConnector,
	}
}

// NewPostgreSQLRunnerWithConnector creates a new PostgreSQLRunner with a custom connector.
// This is primarily used for testing to inject mock connections.
func NewPostgreSQLRunnerWithConnector(logger slog.Logger, connector ConnectorFunc) *PostgreSQLRunner {
	return &PostgreSQLRunner{
		logger:    logger,
		connector: connector,
	}
}

// Run executes the SQL query defined in the resolved hook.
// It opens a separate database connection (not reusing the main PPM connection),
// executes the query using the provided context for timeout support,
// and closes the connection after execution.
func (r *PostgreSQLRunner) Run(ctx context.Context, hook *ResolvedHook) error {
	pgCfg, ok := hook.Config.(*PostgreSQLConfig)
	if !ok {
		return fmt.Errorf("postgresql configuration is nil for hook %q", hook.Name)
	}

	r.logger.Debug("Executing postgresql hook",
		"hook", hook.Name,
		"sql_query", pgCfg.SQLQuery,
	)

	conn, err := r.connector(ctx, hook.ConnectionURL)
	if err != nil {
		return fmt.Errorf("postgresql hook %q failed to connect: %w", hook.Name, err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, pgCfg.SQLQuery)
	if err != nil {
		return fmt.Errorf("postgresql hook %q failed to execute query: %w", hook.Name, err)
	}

	return nil
}
