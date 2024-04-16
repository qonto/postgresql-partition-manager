package postgresql

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"gotest.tools/assert"
)

var ErrGeneric = errors.New("a generic error")

func TestPostgreSQLError(t *testing.T) {
	testCases := []struct {
		name     string
		error    error
		code     string
		expected bool
	}{
		{
			name:     "ObjectNotInPrerequisiteState",
			error:    &pgconn.PgError{Code: "55000"},
			code:     "55000",
			expected: true,
		},
		{
			name:     "Non match error code",
			error:    &pgconn.PgError{Code: "42"},
			code:     "55000",
			expected: false,
		},
		{
			name:     "Generic error",
			error:    ErrGeneric,
			code:     "",
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, isPostgreSQLErrorCode(tc.error, tc.code), tc.expected, "Error code should match")
		})
	}
}
