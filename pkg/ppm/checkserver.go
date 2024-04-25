package ppm

import (
	"errors"
	"fmt"
	"time"
)

const (
	postgreSQLMinimalVersion = 14               // Minimal supported PostgreSQL version
	timeDriftTolerance       = 10 * time.Second // Maximum time drift between client and server
)

var (
	ErrUnsupportedServer = errors.New("unsupported PostgreSQL version")
	ErrTimeDrift         = errors.New("client and server time drift")
)

func (p *PPM) CheckServerRequirements() error {
	if err := p.requirePostgreSQLSupportedVersion(); err != nil {
		return fmt.Errorf("error checking PostgreSQL version: %w", err)
	}

	if err := p.requireNoTimeDrift(); err != nil {
		return fmt.Errorf("error checking time drift: %w", err)
	}

	return nil
}

func (p *PPM) requirePostgreSQLSupportedVersion() error {
	version, err := p.db.GetEngineVersion()
	if err != nil {
		return fmt.Errorf("failed to fetch PostgreSQL version: %w", err)
	}

	if version < postgreSQLMinimalVersion {
		p.logger.Error("Unsupported PostgreSQL version", "current_version", version, "minimal_version", postgreSQLMinimalVersion)

		return ErrUnsupportedServer
	}

	return nil
}

func (p *PPM) requireNoTimeDrift() error {
	isSync, err := p.clientAndServerAreTimeSynchronized(timeDriftTolerance)
	if err != nil {
		p.logger.Error("Error checking time synchronization", "error", err)

		return fmt.Errorf("error checking time synchronization: %w", err)
	}

	if !isSync {
		p.logger.Error("Client and server times are not synchronized within the tolerance.", "time_tolerance", timeDriftTolerance)

		return ErrTimeDrift
	}

	return nil
}

func (p *PPM) clientAndServerAreTimeSynchronized(tolerance time.Duration) (bool, error) {
	serverTime, err := p.db.GetServerTime()
	if err != nil {
		return false, fmt.Errorf("failed to get server time: %w", err)
	}

	clientTime := time.Now().UTC()
	diff := clientTime.Sub(serverTime)

	// Check if the absolute difference is within the tolerance
	if diff < 0 {
		diff = -diff
	}

	isSync := diff <= tolerance

	return isSync, nil
}
