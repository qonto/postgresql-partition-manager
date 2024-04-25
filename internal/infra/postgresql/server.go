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

func (p *Postgres) GetEngineVersion() (int64, error) {
	var serverVersionRaw string

	err := p.conn.QueryRow(context.Background(), "SHOW server_version").Scan(&serverVersionRaw)
	if err != nil {
		return 0, fmt.Errorf("could not get server version: %w", err)
	}

	serverVersionStr := regexp.MustCompile(`^[0-9]+`).FindString(serverVersionRaw)
	if serverVersionStr == "" {
		return 0, ErrUnkownServerVersion
	}

	serverVersion, err := strconv.ParseInt(serverVersionStr, 10, 64)
	if err != nil {
		return 0, ErrUnkownServerVersion
	}

	return serverVersion, nil
}

func (p *Postgres) GetServerTime() (serverTime time.Time, err error) {
	err = p.conn.QueryRow(context.Background(), "SELECT NOW() AT TIME ZONE 'UTC' as serverTime").Scan(&serverTime)
	if err != nil {
		return time.Time{}, fmt.Errorf("could not get server time: %w", err)
	}

	return serverTime, nil
}
