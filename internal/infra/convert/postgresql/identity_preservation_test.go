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
	"pgregory.net/rapid"
)

// nonIdentityStrategy represents column strategies that have NO identity attribute.
// These are the strategies where identity_generation is empty/NULL.
type nonIdentityStrategy int

const (
	strategySerial   nonIdentityStrategy = iota // BIGSERIAL (no identity attribute, uses sequence default)
	strategyPlainInt                            // Plain BIGINT with explicit sequence default
)

func (s nonIdentityStrategy) String() string {
	switch s {
	case strategySerial:
		return "SERIAL"
	case strategyPlainInt:
		return "PLAIN_INT"
	default:
		return "UNKNOWN"
	}
}

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
//
// This test runs on UNFIXED code and is EXPECTED TO PASS because columns that never
// had an identity attribute remain without one after cutover. The bug only affects
// columns that have an identity attribute (both ALWAYS and BY DEFAULT lose their
// identity attribute during cutover, but non-identity columns are unaffected).
func TestProperty2_PreservationNonIdentityColumnsUnchanged(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Generate a random non-identity strategy (SERIAL or plain integer)
		strategyIdx := rapid.IntRange(0, 1).Draw(rt, "strategy")
		strategy := nonIdentityStrategy(strategyIdx)

		t.Logf("Testing preservation with strategy: %s", strategy)

		// Setup a fresh PostgreSQL container for each property check
		db, cleanup := setupPostgresContainer(t)
		defer cleanup()

		ctx := context.Background()

		// Create the source table based on strategy
		var tableDDL, insertDDL string

		switch strategy {
		case strategySerial:
			tableDDL = `CREATE TABLE public.events (
				id BIGSERIAL PRIMARY KEY,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				name TEXT NOT NULL,
				value INTEGER NOT NULL DEFAULT 0
			)`
			insertDDL = `INSERT INTO public.events (created_at, name, value)
				SELECT
					'2024-01-01'::timestamptz + (i * interval '1 hour'),
					'event_' || i,
					i
				FROM generate_series(1, 50) AS i`
		case strategyPlainInt:
			tableDDL = `CREATE SEQUENCE public.events_id_seq;
			CREATE TABLE public.events (
				id BIGINT NOT NULL DEFAULT nextval('public.events_id_seq') PRIMARY KEY,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				name TEXT NOT NULL,
				value INTEGER NOT NULL DEFAULT 0
			);
			ALTER SEQUENCE public.events_id_seq OWNED BY public.events.id`
			insertDDL = `INSERT INTO public.events (created_at, name, value)
				SELECT
					'2024-01-01'::timestamptz + (i * interval '1 hour'),
					'event_' || i,
					i
				FROM generate_series(1, 50) AS i`
		}

		_, err := db.conn.Exec(ctx, tableDDL)
		require.NoError(t, err, "failed to create source table with strategy %s", strategy)

		_, err = db.conn.Exec(ctx, insertDDL)
		require.NoError(t, err, "failed to insert test data")

		// Query identity_generation BEFORE cutover — should be empty for non-identity columns
		identityGenBefore := queryIdentityGeneration(t, db, testSchema, testTable, "id")
		require.Equal(t, "", identityGenBefore,
			"pre-cutover identity_generation should be empty for strategy %s", strategy)

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

		// Phase 1: Setup
		err = converter.Setup(ctx)
		require.NoError(t, err, "setup should succeed for strategy %s", strategy)

		// Phase 2: Backfill
		err = converter.Backfill(ctx)
		require.NoError(t, err, "backfill should succeed for strategy %s", strategy)

		// Phase 3: Replay
		err = converter.Replay(ctx)
		require.NoError(t, err, "replay should succeed for strategy %s", strategy)

		// Phase 4: Verify
		_, err = converter.Verify(ctx, convert.VerifyOptions{})
		require.NoError(t, err, "verify should succeed for strategy %s", strategy)

		// Phase 5: Cutover
		err = converter.Cutover(ctx)
		require.NoError(t, err, "cutover should succeed for strategy %s", strategy)

		// Verify the table is now partitioned
		relkind := getTableRelkind(t, db.conn, testSchema, testTable)
		require.Equal(t, "p", relkind, "table should be partitioned after cutover")

		// CRITICAL PROPERTY ASSERTION:
		// Query identity_generation AFTER cutover — it should remain empty
		identityGenAfter := queryIdentityGeneration(t, db, testSchema, testTable, "id")

		// Property: for all columns where identity_generation is empty (non-identity columns),
		// the cutover function produces the same identity_generation value (empty) as before cutover
		if identityGenBefore != identityGenAfter {
			rt.Fatalf("Preservation property violated for strategy %s: "+
				"identity_generation was %q before cutover but %q after cutover. "+
				"Non-identity columns should be unchanged by cutover.",
				strategy, identityGenBefore, identityGenAfter)
		}
	})
}

// TestProperty2_PreservationByDefaultIdentityObservation documents the observed behavior
// of BY DEFAULT identity columns on UNFIXED code. This test observes that the unfixed code
// also loses the identity attribute for BY DEFAULT columns (identity_generation becomes empty).
//
// **Validates: Requirements 3.1, 3.2**
//
// Property 2: Preservation - BY DEFAULT Identity Columns
//
// OBSERVATION: On unfixed code, BY DEFAULT identity columns ALSO lose their identity
// attribute after cutover (identity_generation becomes empty/NULL). This means the fix
// must also restore BY DEFAULT identity columns, not just ALWAYS columns.
//
// This test encodes the DESIRED behavior (BY DEFAULT should be preserved) and is
// expected to FAIL on unfixed code — similar to the bug condition test for ALWAYS.
// After the fix is implemented, this test should PASS.
//
// NOTE: The task expected this to pass on unfixed code, but observation shows that
// the CreatePartitionedTable function does not preserve ANY identity attributes —
// it only copies the nextval() default expression. The fix must handle both ALWAYS
// and BY DEFAULT identity columns.
func TestProperty2_PreservationByDefaultIdentityObservation(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Generate whether to use a simple PK or composite PK
		useCompositePK := rapid.Bool().Draw(rt, "useCompositePK")

		t.Logf("Testing BY DEFAULT identity preservation (composite PK: %v)", useCompositePK)

		db, cleanup := setupPostgresContainer(t)
		defer cleanup()

		ctx := context.Background()

		// Create a table with GENERATED BY DEFAULT AS IDENTITY
		var tableDDL string
		if useCompositePK {
			tableDDL = `CREATE TABLE public.events (
				id BIGINT GENERATED BY DEFAULT AS IDENTITY,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				name TEXT NOT NULL,
				value INTEGER NOT NULL DEFAULT 0,
				PRIMARY KEY (id, created_at)
			)`
		} else {
			tableDDL = `CREATE TABLE public.events (
				id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
				name TEXT NOT NULL,
				value INTEGER NOT NULL DEFAULT 0
			)`
		}

		_, err := db.conn.Exec(ctx, tableDDL)
		require.NoError(t, err, "failed to create source table")

		// Insert test data (BY DEFAULT allows direct inserts without OVERRIDING SYSTEM VALUE)
		_, err = db.conn.Exec(ctx, `
			INSERT INTO public.events (created_at, name, value)
			SELECT
				'2024-01-01'::timestamptz + (i * interval '1 hour'),
				'event_' || i,
				i
			FROM generate_series(1, 50) AS i`)
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

		// Run full lifecycle
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

		// Property: for BY DEFAULT identity columns, the cutover function should
		// preserve the identity_generation value as 'BY DEFAULT'
		if identityGenBefore != identityGenAfter {
			rt.Fatalf("Preservation property violated for BY DEFAULT identity column: "+
				"identity_generation was %q before cutover but %q after cutover. "+
				"BY DEFAULT identity columns should be preserved by cutover.",
				identityGenBefore, identityGenAfter)
		}
	})
}
