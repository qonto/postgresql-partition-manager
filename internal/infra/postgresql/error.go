package postgresql

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	ObjectNotInPrerequisiteStatePostgreSQLErrorCode = "55000"
)

func isPostgreSQLErrorCode(err error, errorCode string) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr) && pgErr.Code == errorCode
}
