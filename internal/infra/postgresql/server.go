package postgresql

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

var ErrUnkownServerVersion = errors.New("could not find server version")

func (p *PostgreSQL) GetVersion() (int64, error) {
	serverVersionStr := p.db.PgConn().ParameterStatus("server_version")
	serverVersionStr = regexp.MustCompile(`^[0-9]+`).FindString(serverVersionStr)

	if serverVersionStr == "" {
		return 0, ErrUnkownServerVersion
	}

	serverVersion, err := strconv.ParseInt(serverVersionStr, 10, 64)
	if err != nil {
		return 0, ErrUnkownServerVersion
	}

	return serverVersion, nil
}

func (p *PostgreSQL) GetServerTime() (time.Time, error) {
	var serverTime time.Time

	err := p.db.QueryRow(context.Background(), "SELECT NOW() AT TIME ZONE 'UTC' as serverTime").Scan(&serverTime)
	if err != nil {
		return time.Time{}, fmt.Errorf("could not get server time: %w", err)
	}

	return serverTime, nil
}
