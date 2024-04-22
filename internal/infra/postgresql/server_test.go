//nolint:golint,wsl
package postgresql_test

import (
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
)

func TestGetServerTime(t *testing.T) {
	mock, p := setupMock(t, pgxmock.QueryMatcherEqual)
	query := `SELECT NOW() AT TIME ZONE 'UTC' as serverTime`

	currrentTime := time.Now()

	mock.ExpectQuery(query).WillReturnRows(mock.NewRows([]string{"serverTime"}).AddRow(currrentTime))
	serverTime, err := p.GetServerTime()
	assert.Nil(t, err, "GetServerTime should succeed")
	assert.Equal(t, serverTime, currrentTime, "Time should match")

	mock.ExpectQuery(query).WillReturnError(ErrPostgreSQLConnectionFailure)
	_, err = p.GetServerTime()
	assert.Error(t, err, "GetServerTime should fail")
}

func TestGetEngineVersion(t *testing.T) {
	mock, p := setupMock(t, pgxmock.QueryMatcherEqual)
	query := `SHOW server_version`

	testCases := []struct {
		output  string
		version int64
	}{
		{
			"16.2 (Debian 16.2-1.pgdg120+2)",
			16,
		},
		{
			"14.1",
			14,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.output, func(t *testing.T) {
			mock.ExpectQuery(query).WillReturnRows(mock.NewRows([]string{"server_version"}).AddRow(tc.output))

			version, err := p.GetEngineVersion()
			assert.Nil(t, err, "GetEngineVersion should succeed")
			assert.Equal(t, version, tc.version, "Version mismatch")
		})
	}

	mock.ExpectQuery(query).WillReturnError(ErrPostgreSQLConnectionFailure)
	_, err := p.GetEngineVersion()
	assert.Error(t, err, "GetEngineVersion should fail")

	mock.ExpectQuery(query).WillReturnRows(mock.NewRows([]string{"server_version"}).AddRow("unexpected version format"))
	_, err = p.GetEngineVersion()
	assert.Error(t, err, "GetEngineVersion should fail")
}
