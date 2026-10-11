package hook

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockPgxConn implements PgxConn for testing.
type mockPgxConn struct {
	execFunc  func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	closeFunc func(ctx context.Context) error
	closed    bool
}

func (m *mockPgxConn) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if m.execFunc != nil {
		return m.execFunc(ctx, sql, arguments...)
	}

	return pgconn.NewCommandTag(""), nil
}

func (m *mockPgxConn) Close(ctx context.Context) error {
	m.closed = true

	if m.closeFunc != nil {
		return m.closeFunc(ctx)
	}

	return nil
}

func TestPostgreSQLRunner_Run(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	tests := []struct {
		name        string
		hook        *ResolvedHook
		connector   ConnectorFunc
		expectError bool
		errContains string
	}{
		{
			name: "successful SQL execution",
			hook: &ResolvedHook{
				Name:          "test-success",
				Type:          PostgreSQLType,
				ConnectionURL: "postgresql://user:pass@localhost:5432/testdb",
				Config: &PostgreSQLConfig{
					SQLQuery: "VACUUM ANALYZE public.events",
				},
			},
			connector: func(ctx context.Context, connectionURL string) (PgxConn, error) {
				return &mockPgxConn{
					execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
						return pgconn.NewCommandTag("VACUUM"), nil
					},
				}, nil
			},
			expectError: false,
		},
		{
			name: "failing SQL execution",
			hook: &ResolvedHook{
				Name:          "test-sql-failure",
				Type:          PostgreSQLType,
				ConnectionURL: "postgresql://user:pass@localhost:5432/testdb",
				Config: &PostgreSQLConfig{
					SQLQuery: "INVALID SQL STATEMENT",
				},
			},
			connector: func(ctx context.Context, connectionURL string) (PgxConn, error) {
				return &mockPgxConn{
					execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
						return pgconn.NewCommandTag(""), errors.New("syntax error at or near \"INVALID\"")
					},
				}, nil
			},
			expectError: true,
			errContains: "failed to execute query",
		},
		{
			name: "nil postgresql config",
			hook: &ResolvedHook{
				Name:          "test-nil-config",
				Type:          PostgreSQLType,
				ConnectionURL: "postgresql://user:pass@localhost:5432/testdb",
				Config:        nil,
			},
			connector: func(ctx context.Context, connectionURL string) (PgxConn, error) {
				return &mockPgxConn{}, nil
			},
			expectError: true,
			errContains: "postgresql configuration is nil",
		},
		{
			name: "connection failure",
			hook: &ResolvedHook{
				Name:          "test-conn-failure",
				Type:          PostgreSQLType,
				ConnectionURL: "postgresql://user:pass@unreachable:5432/testdb",
				Config: &PostgreSQLConfig{
					SQLQuery: "SELECT 1",
				},
			},
			connector: func(ctx context.Context, connectionURL string) (PgxConn, error) {
				return nil, errors.New("connection refused")
			},
			expectError: true,
			errContains: "failed to connect",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			runner := NewPostgreSQLRunnerWithConnector(*logger, tc.connector)
			err := runner.Run(context.Background(), tc.hook)

			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPostgreSQLRunner_SeparateConnection(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	expectedURL := "postgresql://hookuser:hookpass@hookhost:5433/hookdb"
	var receivedURL string

	mock := &mockPgxConn{
		execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("SELECT 1"), nil
		},
	}

	connector := func(ctx context.Context, connectionURL string) (PgxConn, error) {
		receivedURL = connectionURL
		return mock, nil
	}

	runner := NewPostgreSQLRunnerWithConnector(*logger, connector)

	hook := &ResolvedHook{
		Name:          "test-separate-connection",
		Type:          PostgreSQLType,
		ConnectionURL: expectedURL,
		Config: &PostgreSQLConfig{
			SQLQuery: "SELECT 1",
		},
	}

	err := runner.Run(context.Background(), hook)
	require.NoError(t, err)

	// Verify the connector was called with the hook's ConnectionURL
	assert.Equal(t, expectedURL, receivedURL)

	// Verify the connection was closed after execution
	assert.True(t, mock.closed, "connection should be closed after execution")
}

func TestPostgreSQLRunner_ConnectionClosedOnFailure(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	mock := &mockPgxConn{
		execFunc: func(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag(""), errors.New("query failed")
		},
	}

	connector := func(ctx context.Context, connectionURL string) (PgxConn, error) {
		return mock, nil
	}

	runner := NewPostgreSQLRunnerWithConnector(*logger, connector)

	hook := &ResolvedHook{
		Name:          "test-close-on-failure",
		Type:          PostgreSQLType,
		ConnectionURL: "postgresql://user:pass@localhost:5432/testdb",
		Config: &PostgreSQLConfig{
			SQLQuery: "INVALID",
		},
	}

	err := runner.Run(context.Background(), hook)
	require.Error(t, err)

	// Verify the connection was still closed even on query failure
	assert.True(t, mock.closed, "connection should be closed even when query fails")
}
