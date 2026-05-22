package postgresql

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// IsConversionInProgress checks if a table has an active conversion in progress
// by querying the ppm_migration_metadata table.
func (p *Postgres) IsConversionInProgress(schema, table string) (bool, error) {
	query := `
		SELECT phase FROM ppm_migration_metadata
		WHERE schema_name = $1 AND table_name = $2`

	var phase string
	err := p.conn.QueryRow(p.ctx, query, schema, table).Scan(&phase)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}

		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			return false, nil
		}

		return false, fmt.Errorf("failed to check conversion status for %s.%s: %w", schema, table, err)
	}

	// Terminal phases mean conversion is complete
	if phase == "cutover_complete" || phase == "rollback_complete" {
		return false, nil
	}

	return true, nil
}
