package postgresql

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const createMetadataTableSQL = `
CREATE TABLE IF NOT EXISTS ppm_migration_metadata (
    schema_name         TEXT NOT NULL,
    table_name          TEXT NOT NULL,
    phase               TEXT NOT NULL CHECK (phase IN ('setup', 'backfill', 'replay', 'verify', 'cutover', 'cleanup', 'rollback_complete')),
    last_backfill_pk    TEXT[],
    last_replay_seq     BIGINT DEFAULT 0,
    dropped_fks         JSONB DEFAULT '[]'::JSONB,
    phase_started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_name, table_name)
)`

// EnsureMetadataTable creates the ppm_migration_metadata table if it does not already exist.
func (c *Client) EnsureMetadataTable() error {
	c.logger.Debug("Ensuring migration metadata table exists")

	_, err := c.conn.Exec(c.ctx, createMetadataTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create migration metadata table: %w", err)
	}

	return nil
}

// GetMigrationState retrieves the migration state for the given schema and table.
// Returns nil if no state record exists.
func (c *Client) GetMigrationState(schema, table string) (*MigrationState, error) {
	query := `
		SELECT
			schema_name,
			table_name,
			phase,
			last_backfill_pk,
			last_replay_seq,
			dropped_fks,
			phase_started_at,
			updated_at
		FROM ppm_migration_metadata
		WHERE schema_name = $1 AND table_name = $2`

	var state MigrationState
	var droppedFKsJSON []byte
	var lastBackfillPK []string

	err := c.conn.QueryRow(c.ctx, query, schema, table).Scan(
		&state.Schema,
		&state.Table,
		&state.Phase,
		&lastBackfillPK,
		&state.LastReplaySeq,
		&droppedFKsJSON,
		&state.PhaseStartedAt,
		&state.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get migration state: %w", err)
	}

	state.LastBackfillPK = lastBackfillPK

	if len(droppedFKsJSON) > 0 {
		if err := json.Unmarshal(droppedFKsJSON, &state.DroppedForeignKeys); err != nil {
			return nil, fmt.Errorf("failed to unmarshal dropped_fks: %w", err)
		}
	}

	return &state, nil
}

// UpdateMigrationState inserts or updates the migration state for the given schema and table.
// It uses an UPSERT (INSERT ... ON CONFLICT DO UPDATE) to atomically create or update the record.
func (c *Client) UpdateMigrationState(schema, table string, state *MigrationState) error {
	droppedFKsJSON, err := json.Marshal(state.DroppedForeignKeys)
	if err != nil {
		return fmt.Errorf("failed to marshal dropped_fks: %w", err)
	}

	// Use now() for updated_at to ensure consistency with the database clock.
	// If PhaseStartedAt is zero, use now() as well (initial insert).
	var phaseStartedAt time.Time
	if state.PhaseStartedAt.IsZero() {
		phaseStartedAt = time.Now()
	} else {
		phaseStartedAt = state.PhaseStartedAt
	}

	query := `
		INSERT INTO ppm_migration_metadata (
			schema_name, table_name, phase, last_backfill_pk, last_replay_seq,
			dropped_fks, phase_started_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, now())
		ON CONFLICT (schema_name, table_name) DO UPDATE SET
			phase = EXCLUDED.phase,
			last_backfill_pk = EXCLUDED.last_backfill_pk,
			last_replay_seq = EXCLUDED.last_replay_seq,
			dropped_fks = EXCLUDED.dropped_fks,
			phase_started_at = EXCLUDED.phase_started_at,
			updated_at = now()`

	_, err = c.conn.Exec(c.ctx, query,
		schema,
		table,
		state.Phase,
		state.LastBackfillPK,
		state.LastReplaySeq,
		droppedFKsJSON,
		phaseStartedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to update migration state: %w", err)
	}

	return nil
}

// DeleteMigrationState removes the migration state record for the given schema and table.
func (c *Client) DeleteMigrationState(schema, table string) error {
	query := `DELETE FROM ppm_migration_metadata WHERE schema_name = $1 AND table_name = $2`

	_, err := c.conn.Exec(c.ctx, query, schema, table)
	if err != nil {
		return fmt.Errorf("failed to delete migration state: %w", err)
	}

	return nil
}
