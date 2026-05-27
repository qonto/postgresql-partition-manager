//go:build integration

//nolint:wsl_v5
package postgresql_test

import (
	"context"
	"testing"

	convertpg "github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
	"github.com/qonto/postgresql-partition-manager/internal/infra/logger"
	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/pkg/convert"
	"github.com/stretchr/testify/require"
)

// queryIdentityGeneration queries the identity_generation value for a column.
// Returns empty string if the column has no identity attribute (NULL in information_schema).
func queryIdentityGeneration(t *testing.T, db *testDB, schema, table, column string) string {
	t.Helper()

	ctx := context.Background()

	var identityGen *string
	err := db.conn.QueryRow(ctx, `
		SELECT identity_generation
		FROM information_schema.columns
		WHERE table_schema = $1
			AND table_name = $2
			AND column_name = $3`, schema, table, column).Scan(&identityGen)
	require.NoError(t, err, "failed to query identity_generation for %s.%s.%s", schema, table, column)

	if identityGen == nil {
		return ""
	}

	return *identityGen
}

// TestProperty2_PreservationNonIdentityColumnsUnchanged tests that for all columns
// with no identity attribute (SERIAL, plain integer with sequence default), the cutover
// function produces the same identity_generation value (empty) as before cutover.
//
// **Validates: Requirements 3.1, 3.2**
//
// Property 2: Preservation - Non-Identity Columns Unchanged After Cutover
func TestProperty2_PreservationNonIdentityColumnsUnchanged(t *testing.T) {
	tests := []struct {
		name      string
		tableDDL  string
		insertDDL string
	}{
		{
			name: "BIGSERIAL",
			tableDDL: `CREATE TABLE public.events (
				id BIGSERIAL PRIMARY KEY,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				name TEXT NOT NULL,
				value INTEGER NOT NULL DEFAULT 0
			)`,
			insertDDL: `INSERT INTO public.events (created_at, name, value)
				SELECT
					'2024-01-01'::timestamptz + (i * interval '1 hour'),
					'event_' || i,
					i
				FROM generate_series(1, 20) AS i`,
		},
		{
			name: "PlainIntWithSequence",
			tableDDL: `CREATE SEQUENCE public.events_id_seq;
			CREATE TABLE public.events (
				id BIGINT NOT NULL DEFAULT nextval('public.events_id_seq') PRIMARY KEY,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				name TEXT NOT NULL,
				value INTEGER NOT NULL DEFAULT 0
			);
			ALTER SEQUENCE public.events_id_seq OWNED BY public.events.id`,
			insertDDL: `INSERT INTO public.events (created_at, name, value)
				SELECT
					'2024-01-01'::timestamptz + (i * interval '1 hour'),
					'event_' || i,
					i
				FROM generate_series(1, 20) AS i`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, cleanup := setupPostgresContainer(t)
			defer cleanup()

			ctx := context.Background()

			_, err := db.conn.Exec(ctx, tc.tableDDL)
			require.NoError(t, err, "failed to create source table")

			_, err = db.conn.Exec(ctx, tc.insertDDL)
			require.NoError(t, err, "failed to insert test data")

			// Query identity_generation BEFORE cutover — should be empty for non-identity columns
			identityGenBefore := queryIdentityGeneration(t, db, testSchema, testTable, "id")
			require.Equal(t, "", identityGenBefore, "pre-cutover identity_generation should be empty")

			// Create converter and run full lifecycle through cutover
			log, err := logger.New(false, "text")
			require.NoError(t, err)

			client := convertpg.NewWithTimeouts(*log, db.conn, 5, 30)

			partConfig := partition.Configuration{
				Schema:         testSchema,
				Table:          testTable,
				PartitionKey:   "created_at",
				Interval:       partition.Daily,
				Retention:      30,
				PreProvisioned: 7,
				CleanupPolicy:  partition.Drop,
				Convert: &partition.ConvertSettings{
					BackfillBatchSize: 100,
					ReplayBatchSize:   50,
					LockTimeout:       5,
					StatementTimeout:  30,
				},
			}

			converter := convert.New(*log, client, partConfig, false)

			err = converter.Setup(ctx)
			require.NoError(t, err, "setup should succeed")

			err = converter.Backfill(ctx)
			require.NoError(t, err, "backfill should succeed")

			err = converter.Replay(ctx)
			require.NoError(t, err, "replay should succeed")

			_, err = converter.Verify(ctx, convert.VerifyOptions{})
			require.NoError(t, err, "verify should succeed")

			err = converter.Cutover(ctx)
			require.NoError(t, err, "cutover should succeed")

			// Verify the table is now partitioned
			relkind := getTableRelkind(t, db.conn, testSchema, testTable)
			require.Equal(t, "p", relkind, "table should be partitioned after cutover")

			// CRITICAL PROPERTY ASSERTION:
			// Query identity_generation AFTER cutover — it should remain empty
			identityGenAfter := queryIdentityGeneration(t, db, testSchema, testTable, "id")
			require.Equal(t, identityGenBefore, identityGenAfter,
				"non-identity columns should be unchanged by cutover")
		})
	}
}

// TestProperty2_PreservationByDefaultIdentityObservation tests that BY DEFAULT identity
// columns retain their identity_generation = 'BY DEFAULT' after cutover.
//
// **Validates: Requirements 3.1, 3.2**
//
// Property 2: Preservation - BY DEFAULT Identity Columns Unchanged After Cutover
func TestProperty2_PreservationByDefaultIdentityObservation(t *testing.T) {
	tests := []struct {
		name     string
		tableDDL string
	}{
		{
			name: "SimplePK",
			tableDDL: `CREATE TABLE public.events (
				id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				name TEXT NOT NULL,
				value INTEGER NOT NULL DEFAULT 0
			)`,
		},
		{
			name: "CompositePK",
			tableDDL: `CREATE TABLE public.events (
				id BIGINT GENERATED BY DEFAULT AS IDENTITY,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				name TEXT NOT NULL,
				value INTEGER NOT NULL DEFAULT 0,
				PRIMARY KEY (id, created_at)
			)`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, cleanup := setupPostgresContainer(t)
			defer cleanup()

			ctx := context.Background()

			_, err := db.conn.Exec(ctx, tc.tableDDL)
			require.NoError(t, err, "failed to create source table")

			// Insert test data (BY DEFAULT allows direct inserts without OVERRIDING SYSTEM VALUE)
			_, err = db.conn.Exec(ctx, `
				INSERT INTO public.events (created_at, name, value)
				SELECT
					'2024-01-01'::timestamptz + (i * interval '1 hour'),
					'event_' || i,
					i
				FROM generate_series(1, 20) AS i`)
			require.NoError(t, err, "failed to insert test data")

			// Query identity_generation BEFORE cutover
			identityGenBefore := queryIdentityGeneration(t, db, testSchema, testTable, "id")
			require.Equal(t, "BY DEFAULT", identityGenBefore,
				"pre-cutover identity_generation should be 'BY DEFAULT'")

			// Create converter and run full lifecycle through cutover
			log, err := logger.New(false, "text")
			require.NoError(t, err)

			client := convertpg.NewWithTimeouts(*log, db.conn, 5, 30)

			partConfig := partition.Configuration{
				Schema:         testSchema,
				Table:          testTable,
				PartitionKey:   "created_at",
				Interval:       partition.Daily,
				Retention:      30,
				PreProvisioned: 7,
				CleanupPolicy:  partition.Drop,
				Convert: &partition.ConvertSettings{
					BackfillBatchSize: 100,
					ReplayBatchSize:   50,
					LockTimeout:       5,
					StatementTimeout:  30,
				},
			}

			converter := convert.New(*log, client, partConfig, false)

			err = converter.Setup(ctx)
			require.NoError(t, err, "setup should succeed")

			err = converter.Backfill(ctx)
			require.NoError(t, err, "backfill should succeed")

			err = converter.Replay(ctx)
			require.NoError(t, err, "replay should succeed")

			_, err = converter.Verify(ctx, convert.VerifyOptions{})
			require.NoError(t, err, "verify should succeed")

			err = converter.Cutover(ctx)
			require.NoError(t, err, "cutover should succeed")

			// Verify the table is now partitioned
			relkind := getTableRelkind(t, db.conn, testSchema, testTable)
			require.Equal(t, "p", relkind, "table should be partitioned after cutover")

			// CRITICAL PROPERTY ASSERTION:
			// Query identity_generation AFTER cutover — should remain 'BY DEFAULT'
			identityGenAfter := queryIdentityGeneration(t, db, testSchema, testTable, "id")
			require.Equal(t, identityGenBefore, identityGenAfter,
				"BY DEFAULT identity columns should be preserved by cutover")
		})
	}
}
