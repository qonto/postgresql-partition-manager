package ppm_test

import (
	"context"
	"testing"
	"time"

	"github.com/qonto/postgresql-partition-manager/pkg/ppm"
	"github.com/stretchr/testify/assert"
)

func TestServerRequirements(t *testing.T) {
	shouldSucceed := true
	shouldFail := false

	testCases := []struct {
		name          string
		serverVersion int64
		serverTime    time.Time
		expected      bool
	}{
		{
			"Synchronized PostgreSQL 14",
			14,
			time.Now(),
			shouldSucceed,
		},
		{
			"Unsupported PostgreSQL 13",
			13,
			time.Now(),
			shouldFail,
		},
		{
			"PostgreSQL 14 server in the future",
			14,
			time.Now().Add(time.Second * 30),
			shouldFail,
		},
		{
			"PostgreSQL 14 server in the past",
			14,
			time.Now().Add(time.Second * -30),
			shouldFail,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset mock on every test case
			logger, postgreSQLMock := setupMocks(t)
			checker := ppm.New(context.TODO(), *logger, postgreSQLMock, nil)

			postgreSQLMock.On("GetEngineVersion").Return(tc.serverVersion, nil).Once()
			postgreSQLMock.On("GetServerTime").Return(tc.serverTime, nil).Once()

			err := checker.CheckServerRequirements()

			if tc.expected == shouldSucceed {
				assert.Nil(t, err, "ServerRequirement should match")
			} else {
				assert.NotNil(t, err, "error checking time drift")
			}
		})
	}
}
