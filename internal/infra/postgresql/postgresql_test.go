package postgresql_test

import (
	"context"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/pashagolub/pgxmock/v3"
	"github.com/qonto/postgresql-partition-manager/internal/infra/logger"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
)

var ErrPostgreSQLConnectionFailure = &pgconn.PgError{
	Code: "08006",
}

func setupMock(t *testing.T, queryMatcher pgxmock.QueryMatcher) (pgxmock.PgxConnIface, *postgresql.Postgres) {
	t.Helper()

	mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(queryMatcher))
	if err != nil {
		t.Fatalf("ERROR: Fail to initialize PostgreSQL mock: %s", err)
	}

	defer mock.Close(context.TODO()) //nolint:golint,errcheck

	logger, err := logger.New(false, "text")
	if err != nil {
		t.Fatalf("ERROR: Fail to initialize logger: %s", err)
	}

	client := postgresql.New(*logger, mock)

	return mock, client
}
