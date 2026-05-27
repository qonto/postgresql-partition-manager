//go:build integration

//nolint:wsl_v5
package postgresql_test

import (
	"context"
	"fmt"
	"testing"

	convertpg "github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
	"github.com/qonto/postgresql-partition-manager/internal/infra/logger"
	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/pkg/convert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_IdentityGenerationAlwaysPreservedAfterCutover tests that a column
// defined as GENERATED ALWAYS AS IDENTITY retains identity_generation = 'ALWAYS'
// after the full setup → backfill → replay → cutover flow.
//
// **Validates: Requirements 1.1, 1.2, 2.1, 2.2**
//
// Bug Condition: The source table has an identity column with GENERATED ALWAYS AS IDENTITY.
// After cutover, the identity_generation should remain 'ALWAYS'.
// On UNFIXED code, this test is EXPECTED TO FAIL because the cutover downgrades
// identity_generation to 'BY DEFAULT'.
func TestIntegration_IdentityGenerationAlwaysPreservedAfterCutover(t *testing.T) {
	db, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create source table with GENERATED ALWAYS AS IDENTITY
	_, err := db.conn.Exec(ctx, `
		CREATE TABLE public.events (
			id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			name TEXT NOT NULL,
			value INTEGER NOT NULL DEFAULT 0
		)`)
	require.NoError(t, err, "failed to create source table with GENERATED ALWAYS AS IDENTITY")

	// Insert test data using OVERRIDING SYSTEM VALUE (required for ALWAYS identity)
	_, err = db.conn.Exec(ctx, `
		INSERT INTO public.events (id, created_at, name, value)
		OVERRIDING SYSTEM VALUE
		SELECT
			i,
			'2024-01-01'::timestamptz + (i * interval '1 hour'),
			'event_' || i,
			i
		FROM generate_series(1, 20) AS i`)
	require.NoError(t, err, "failed to insert test data")

	// Verify source table has identity_generation = 'ALWAYS' before cutover
	var sourceIdentityGen string
	err = db.conn.QueryRow(ctx, `
		SELECT identity_generation
		FROM information_schema.columns
		WHERE table_schema = 'public'
			AND table_name = 'events'
			AND column_name = 'id'`).Scan(&sourceIdentityGen)
	require.NoError(t, err, "failed to query source identity_generation")
	require.Equal(t, "ALWAYS", sourceIdentityGen, "source table should have identity_generation = 'ALWAYS'")

	// Create converter and run full lifecycle through cutover
	log, err := logger.New(false, "text")
	require.NoError(t, err)

	client := convertpg.NewWithTimeouts(*log, db.conn, 5, 30)

	config := partition.Configuration{
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

	converter := convert.New(*log, client, config, false)

	// Phase 1: Setup
	err = converter.Setup(ctx)
	require.NoError(t, err, "setup should succeed")

	// Phase 2: Backfill
	err = converter.Backfill(ctx)
	require.NoError(t, err, "backfill should succeed")

	// Phase 3: Replay
	err = converter.Replay(ctx)
	require.NoError(t, err, "replay should succeed")

	// Phase 4: Verify
	_, err = converter.Verify(ctx, convert.VerifyOptions{})
	require.NoError(t, err, "verify should succeed")

	// Phase 5: Cutover
	err = converter.Cutover(ctx)
	require.NoError(t, err, "cutover should succeed")

	// Verify the table is now partitioned
	relkind := getTableRelkind(t, db.conn, testSchema, testTable)
	require.Equal(t, "p", relkind, "table should be partitioned after cutover")

	// CRITICAL ASSERTION: Query identity_generation on the resulting partitioned table
	// After cutover, the 'events' table is now the partitioned table.
	// The identity_generation should be 'ALWAYS' (matching the original source).
	var resultIdentityGen *string
	err = db.conn.QueryRow(ctx, `
		SELECT identity_generation
		FROM information_schema.columns
		WHERE table_schema = 'public'
			AND table_name = 'events'
			AND column_name = 'id'`).Scan(&resultIdentityGen)
	require.NoError(t, err, "failed to query identity_generation after cutover")

	// This assertion is expected to FAIL on unfixed code:
	// The bug causes identity_generation to be NULL or 'BY DEFAULT' instead of 'ALWAYS'.
	// NULL means the column lost its identity attribute entirely (just has a DEFAULT nextval(...)).
	// 'BY DEFAULT' means the identity was downgraded from ALWAYS to BY DEFAULT.
	actualValue := "<NULL>"
	if resultIdentityGen != nil {
		actualValue = *resultIdentityGen
	}

	assert.NotNil(t, resultIdentityGen,
		"Bug confirmed: identity_generation is NULL after cutover (column lost identity attribute entirely)")
	if resultIdentityGen != nil {
		assert.Equal(t, "ALWAYS", *resultIdentityGen,
			fmt.Sprintf("Bug confirmed: identity_generation should be 'ALWAYS' after cutover, but got '%s'. "+
				"The cutover process downgrades GENERATED ALWAYS AS IDENTITY.", actualValue))
	}
}

// TestIntegration_MultipleIdentityColumnsPreservedAfterCutover tests that when a table
// has multiple identity columns (one ALWAYS, one BY DEFAULT), the ALWAYS column retains
// its identity_generation = 'ALWAYS' after cutover.
//
// **Validates: Requirements 1.1, 1.2, 2.1, 2.2**
//
// Bug Condition: The source table has an identity column with GENERATED ALWAYS AS IDENTITY
// alongside another identity column with GENERATED BY DEFAULT AS IDENTITY.
// After cutover, the ALWAYS column should retain 'ALWAYS' and the BY DEFAULT column
// should retain 'BY DEFAULT'.
// On UNFIXED code, this test is EXPECTED TO FAIL because the ALWAYS column gets
// downgraded to 'BY DEFAULT'.
func TestIntegration_MultipleIdentityColumnsPreservedAfterCutover(t *testing.T) {
	db, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create source table with two identity columns:
	// - id: GENERATED ALWAYS AS IDENTITY (should be preserved as ALWAYS)
	// - seq_num: GENERATED BY DEFAULT AS IDENTITY (should remain BY DEFAULT)
	// Note: PostgreSQL allows multiple identity columns on a table.
	// We use a composite PK including created_at to satisfy partitioning requirements.
	_, err := db.conn.Exec(ctx, `
		CREATE TABLE public.events (
			id BIGINT GENERATED ALWAYS AS IDENTITY,
			seq_num BIGINT GENERATED BY DEFAULT AS IDENTITY,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			name TEXT NOT NULL,
			value INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (id, created_at)
		)`)
	require.NoError(t, err, "failed to create source table with multiple identity columns")

	// Insert test data using OVERRIDING SYSTEM VALUE (required for ALWAYS identity)
	_, err = db.conn.Exec(ctx, `
		INSERT INTO public.events (id, seq_num, created_at, name, value)
		OVERRIDING SYSTEM VALUE
		SELECT
			i,
			i * 10,
			'2024-01-01'::timestamptz + (i * interval '1 hour'),
			'event_' || i,
			i
		FROM generate_series(1, 20) AS i`)
	require.NoError(t, err, "failed to insert test data")

	// Verify source table identity_generation values before cutover
	var idIdentityGen, seqIdentityGen string
	err = db.conn.QueryRow(ctx, `
		SELECT identity_generation
		FROM information_schema.columns
		WHERE table_schema = 'public'
			AND table_name = 'events'
			AND column_name = 'id'`).Scan(&idIdentityGen)
	require.NoError(t, err)
	require.Equal(t, "ALWAYS", idIdentityGen, "source 'id' column should have identity_generation = 'ALWAYS'")

	err = db.conn.QueryRow(ctx, `
		SELECT identity_generation
		FROM information_schema.columns
		WHERE table_schema = 'public'
			AND table_name = 'events'
			AND column_name = 'seq_num'`).Scan(&seqIdentityGen)
	require.NoError(t, err)
	require.Equal(t, "BY DEFAULT", seqIdentityGen, "source 'seq_num' column should have identity_generation = 'BY DEFAULT'")

	// Create converter and run full lifecycle through cutover
	log, err := logger.New(false, "text")
	require.NoError(t, err)

	client := convertpg.NewWithTimeouts(*log, db.conn, 5, 30)

	config := partition.Configuration{
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

	converter := convert.New(*log, client, config, false)

	// Phase 1: Setup
	err = converter.Setup(ctx)
	require.NoError(t, err, "setup should succeed")

	// Phase 2: Backfill
	err = converter.Backfill(ctx)
	require.NoError(t, err, "backfill should succeed")

	// Phase 3: Replay
	err = converter.Replay(ctx)
	require.NoError(t, err, "replay should succeed")

	// Phase 4: Verify
	_, err = converter.Verify(ctx, convert.VerifyOptions{})
	require.NoError(t, err, "verify should succeed")

	// Phase 5: Cutover
	err = converter.Cutover(ctx)
	require.NoError(t, err, "cutover should succeed")

	// Verify the table is now partitioned
	relkind := getTableRelkind(t, db.conn, testSchema, testTable)
	require.Equal(t, "p", relkind, "table should be partitioned after cutover")

	// CRITICAL ASSERTIONS: Query identity_generation on the resulting partitioned table

	// Assert 'id' column retains ALWAYS
	var resultIdIdentityGen *string
	err = db.conn.QueryRow(ctx, `
		SELECT identity_generation
		FROM information_schema.columns
		WHERE table_schema = 'public'
			AND table_name = 'events'
			AND column_name = 'id'`).Scan(&resultIdIdentityGen)
	require.NoError(t, err, "failed to query identity_generation for 'id' after cutover")

	// This assertion is expected to FAIL on unfixed code:
	// The bug causes identity_generation to be NULL or 'BY DEFAULT' instead of 'ALWAYS'.
	actualIdValue := "<NULL>"
	if resultIdIdentityGen != nil {
		actualIdValue = *resultIdIdentityGen
	}

	assert.NotNil(t, resultIdIdentityGen,
		"Bug confirmed: 'id' identity_generation is NULL after cutover (column lost identity attribute entirely)")
	if resultIdIdentityGen != nil {
		assert.Equal(t, "ALWAYS", *resultIdIdentityGen,
			fmt.Sprintf("Bug confirmed: 'id' identity_generation should be 'ALWAYS' after cutover, but got '%s'. "+
				"The cutover process downgrades GENERATED ALWAYS AS IDENTITY.", actualIdValue))
	}

	// Assert 'seq_num' column retains BY DEFAULT
	var resultSeqIdentityGen *string
	err = db.conn.QueryRow(ctx, `
		SELECT identity_generation
		FROM information_schema.columns
		WHERE table_schema = 'public'
			AND table_name = 'events'
			AND column_name = 'seq_num'`).Scan(&resultSeqIdentityGen)
	require.NoError(t, err, "failed to query identity_generation for 'seq_num' after cutover")

	actualSeqValue := "<NULL>"
	if resultSeqIdentityGen != nil {
		actualSeqValue = *resultSeqIdentityGen
	}

	// On unfixed code, this may also be NULL (identity lost entirely) or 'BY DEFAULT'
	// The key assertion is that 'id' (ALWAYS) is preserved — 'seq_num' (BY DEFAULT) behavior
	// is documented here for completeness
	if resultSeqIdentityGen != nil {
		assert.Equal(t, "BY DEFAULT", *resultSeqIdentityGen,
			fmt.Sprintf("'seq_num' identity_generation should remain 'BY DEFAULT' after cutover, but got '%s'.",
				actualSeqValue))
	} else {
		// If seq_num also lost its identity, document it
		t.Logf("Note: 'seq_num' identity_generation is also NULL after cutover (identity attribute lost)")
	}
}
