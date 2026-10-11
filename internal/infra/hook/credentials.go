package hook

import (
	"errors"
	"fmt"
	"net/url"
)

const defaultPostgreSQLPort = "5432"

// ErrInvalidConnectionURL is returned when the connection URL cannot be parsed.
var ErrInvalidConnectionURL = errors.New("invalid PostgreSQL connection URL")

// ExtractCredentials parses a PostgreSQL connection URL and returns a map of
// environment variables (PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD).
// It handles standard PostgreSQL connection URL format:
// postgresql://user:password@host:port/dbname
func ExtractCredentials(connectionURL string) (map[string]string, error) {
	parsed, err := url.Parse(connectionURL)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidConnectionURL, err)
	}

	if parsed.Scheme != "postgresql" && parsed.Scheme != "postgres" {
		return nil, fmt.Errorf("%w: unsupported scheme %q, expected \"postgresql\" or \"postgres\"", ErrInvalidConnectionURL, parsed.Scheme)
	}

	host := parsed.Hostname()
	port := parsed.Port()

	if port == "" {
		port = defaultPostgreSQLPort
	}

	// Database name is the path without the leading slash
	database := ""
	if len(parsed.Path) > 1 {
		database = parsed.Path[1:]
	}

	user := ""
	password := ""

	if parsed.User != nil {
		user = parsed.User.Username()
		password, _ = parsed.User.Password()
	}

	credentials := map[string]string{
		"PGHOST":     host,
		"PGPORT":     port,
		"PGDATABASE": database,
		"PGUSER":     user,
		"PGPASSWORD": password,
	}

	return credentials, nil
}
