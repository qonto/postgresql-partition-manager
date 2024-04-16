package postgresql_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v3"
	"github.com/qonto/postgresql-partition-manager/internal/infra/logger"
	"github.com/qonto/postgresql-partition-manager/internal/infra/postgresql"
	"github.com/stretchr/testify/assert"
)

func TestGetServerTime(t *testing.T) {
	mock, err := pgxmock.NewConn()
	if err != nil {
		fmt.Println("ERROR: Fail to initialize PostgreSQL mock: %w", err)
		panic(err)
	}

	currrentTime := time.Now()
	query := `SELECT NOW\(\) AT TIME ZONE \'UTC\' as serverTime`
	mock.ExpectQuery(query).WillReturnRows(mock.NewRows([]string{"serverTime"}).AddRow(currrentTime))

	logger, err := logger.New(false, "text")
	if err != nil {
		fmt.Println("ERROR: Fail to initialize logger: %w", err)
		panic(err)
	}

	p := postgresql.New(*logger, mock)
	serverTime, err := p.GetServerTime()

	assert.Nil(t, err, "GetServerTime should succeed")
	assert.Equal(t, serverTime, currrentTime, "Time should match")
}
