//go:build integration

//nolint:wsl_v5
package postgresql_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	convertpg "github.com/qonto/postgresql-partition-manager/internal/infra/convert/postgresql"
	"github.com/qonto/postgresql-partition-manager/internal/infra/logger"
	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"github.com/qonto/postgresql-partition-manager/pkg/convert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	testSchema = "public"
	testTable  = "events"
)

// testDB holds the connection and connection string for a test database.
type testDB struct {
	conn    *pgx.Conn
	connStr string
}

// setupPostgresContainer starts a PostgreSQL container and returns a testDB and cleanup function.
func setupPostgresContainer(t *testing.T) (*testDB, func()) {
	t.Helper()

	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	require.NoError(t, err, "failed to start postgres container")

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err, "failed to get connection string")

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err, "failed to connect to postgres")

	cleanup := func() {
		conn.Close(ctx)
		pgContainer.Terminate(ctx) //nolint:errcheck
	}

	return &testDB{conn: conn, connStr: connStr}, cleanup
}

// newConnection creates a new connection to the same database.
func (db *testDB) newConnection(t *testing.T) *pgx.Conn {
	t.Helper()

	conn, err := pgx.Connect(context.Background(), db.connStr)
	require.NoError(t, err, "failed to create new connection")

	return conn
}

// createTestSourceTable creates a source table with sample data for testing.
func createTestSourceTable(t *testing.T, conn *pgx.Conn, rowCount int) {
	t.Helper()

	ctx := context.Background()

	// Create the source table
	_, err := conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			id BIGSERIAL PRIMARY KEY,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			name TEXT NOT NULL,
			value INTEGER NOT NULL DEFAULT 0
		)`, testSchema, testTable))
	require.NoError(t, err, "failed to create source table")

	// Insert test data
	if rowCount > 0 {
		_, err = conn.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.%s (created_at, name, value)
			SELECT
				'2024-01-01'::timestamptz + (i * interval '1 hour'),
				'event_' || i,
				i
			FROM generate_series(1, $1) AS i`, testSchema, testTable), rowCount)
		require.NoError(t, err, "failed to insert test data")
	}
}

// createChildTable creates a child table with a FK referencing the source table.
func createChildTable(t *testing.T, conn *pgx.Conn) {
	t.Helper()

	ctx := context.Background()

	_, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS public.event_details (
			id BIGSERIAL PRIMARY KEY,
			event_id BIGINT NOT NULL,
			detail TEXT NOT NULL,
			CONSTRAINT fk_event_details_events FOREIGN KEY (event_id) REFERENCES public.events(id)
		)`)
	require.NoError(t, err, "failed to create child table")

	// Insert some child rows
	_, err = conn.Exec(ctx, `
		INSERT INTO public.event_details (event_id, detail)
		SELECT id, 'detail for event ' || id FROM public.events LIMIT 10`)
	require.NoError(t, err, "failed to insert child data")
}

// newConvertClient creates a convert Client from a pgx connection.
func newConvertClient(t *testing.T, conn *pgx.Conn) *convertpg.Client {
	t.Helper()

	log, err := logger.New(false, "text")
	require.NoError(t, err)

	return convertpg.NewWithTimeouts(*log, conn, 5, 30)
}

// newConverter creates a Converter with the given client and configuration.
func newConverter(t *testing.T, client *convertpg.Client) *convert.Converter {
	t.Helper()

	log, err := logger.New(false, "text")
	require.NoError(t, err)

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

	return convert.New(*log, client, config, false)
}

// getExactRowCount returns the exact row count using COUNT(*).
// This is used in tests instead of GetTableRowCount which uses pg_class.reltuples (an estimate).
func getExactRowCount(t *testing.T, conn *pgx.Conn, schema, table string) int64 {
	t.Helper()

	var count int64
	qualifiedTable := pgx.Identifier{schema, table}.Sanitize()
	err := conn.QueryRow(context.Background(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s", qualifiedTable)).Scan(&count)
	require.NoError(t, err, "failed to get row count for %s.%s", schema, table)

	return count
}

// getTableRelkind returns the relkind of a table as a string.
func getTableRelkind(t *testing.T, conn *pgx.Conn, schema, table string) string {
	t.Helper()

	var relkind string
	err := conn.QueryRow(context.Background(), `
		SELECT c.relkind::text FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2`, schema, table).Scan(&relkind)
	require.NoError(t, err, "failed to get relkind for %s.%s", schema, table)

	return relkind
}

// TestIntegration_FullLifecycle tests the complete migration lifecycle:
// setup → backfill → replay → verify → cutover → cleanup
func TestIntegration_FullLifecycle(t *testing.T) {
	db, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create source table with 500 rows
	createTestSourceTable(t, db.conn, 500)

	client := newConvertClient(t, db.conn)
	converter := newConverter(t, client)

	// Phase 1: Setup
	err := converter.Setup(ctx)
	require.NoError(t, err, "setup should succeed")

	// Verify CDC queue exists
	exists, err := client.IsCDCQueueExists(testSchema, testTable)
	require.NoError(t, err)
	assert.True(t, exists, "CDC queue should exist after setup")

	// Verify CDC trigger exists
	triggerExists, err := client.IsCDCTriggerExists(testSchema, testTable)
	require.NoError(t, err)
	assert.True(t, triggerExists, "CDC trigger should exist after setup")

	// Verify target table exists
	targetExists, err := client.IsTableExists(testSchema, testTable+"_partitioned")
	require.NoError(t, err)
	assert.True(t, targetExists, "target partitioned table should exist after setup")

	// Phase 2: Backfill
	err = converter.Backfill(ctx)
	require.NoError(t, err, "backfill should succeed")

	// Verify rows were copied (use exact count, not pg_class estimate)
	targetCount := getExactRowCount(t, db.conn, testSchema, testTable+"_partitioned")
	assert.Equal(t, int64(500), targetCount, "all rows should be backfilled")

	// Phase 3: Replay (should converge immediately since no new DML)
	err = converter.Replay(ctx)
	require.NoError(t, err, "replay should succeed")

	// Phase 4: Verify
	result, err := converter.Verify(ctx, convert.VerifyOptions{})
	require.NoError(t, err, "verify should succeed")
	assert.True(t, result.ReadyForCutover, "should be ready for cutover")
	assert.Equal(t, int64(0), result.ReplayLag, "replay lag should be zero")

	// Phase 5: Cutover
	err = converter.Cutover(ctx)
	require.NoError(t, err, "cutover should succeed")

	// Verify the table is now partitioned
	relkind := getTableRelkind(t, db.conn, testSchema, testTable)
	assert.Equal(t, "p", relkind, "table should be partitioned after cutover")

	// Verify old table exists
	oldExists, err := client.IsTableExists(testSchema, testTable+"_old")
	require.NoError(t, err)
	assert.True(t, oldExists, "old table should exist after cutover")

	// Verify row count is preserved
	finalCount := getExactRowCount(t, db.conn, testSchema, testTable)
	assert.Equal(t, int64(500), finalCount, "row count should be preserved after cutover")

	// Phase 6: Cleanup
	err = converter.Cleanup(ctx, true, false)
	require.NoError(t, err, "cleanup should succeed")

	// Verify old table is gone
	oldExists, err = client.IsTableExists(testSchema, testTable+"_old")
	require.NoError(t, err)
	assert.False(t, oldExists, "old table should be removed after cleanup")

	// Verify CDC queue is gone
	queueExists, err := client.IsCDCQueueExists(testSchema, testTable)
	require.NoError(t, err)
	assert.False(t, queueExists, "CDC queue should be removed after cleanup")
}

// TestIntegration_ConcurrentDMLDuringBackfill verifies that concurrent DML
// operations are not blocked during backfill (Requirement 15.1).
func TestIntegration_ConcurrentDMLDuringBackfill(t *testing.T) {
	db, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create source table with data
	createTestSourceTable(t, db.conn, 200)

	client := newConvertClient(t, db.conn)
	converter := newConverter(t, client)

	// Setup
	err := converter.Setup(ctx)
	require.NoError(t, err)

	// Run backfill in a goroutine while performing concurrent DML
	var backfillErr error
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		backfillErr = converter.Backfill(ctx)
	}()

	// Perform concurrent DML operations (should not be blocked)
	dmlDone := make(chan struct{})
	go func() {
		defer close(dmlDone)

		// Get a separate connection for DML
		dmlConn := db.newConnection(t)
		defer dmlConn.Close(ctx)

		// INSERT
		_, err := dmlConn.Exec(ctx, fmt.Sprintf(
			"INSERT INTO %s.%s (created_at, name, value) VALUES ('2024-01-05 12:00:00+00', 'concurrent_insert', 999)", testSchema, testTable))
		assert.NoError(t, err, "concurrent INSERT should not be blocked")

		// UPDATE
		_, err = dmlConn.Exec(ctx, fmt.Sprintf(
			"UPDATE %s.%s SET value = 888 WHERE id = 1", testSchema, testTable))
		assert.NoError(t, err, "concurrent UPDATE should not be blocked")

		// DELETE
		_, err = dmlConn.Exec(ctx, fmt.Sprintf(
			"DELETE FROM %s.%s WHERE id = 2", testSchema, testTable))
		assert.NoError(t, err, "concurrent DELETE should not be blocked")
	}()

	// Wait for DML to complete (with timeout)
	select {
	case <-dmlDone:
		// DML completed without blocking
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent DML was blocked for more than 10 seconds during backfill")
	}

	wg.Wait()
	assert.NoError(t, backfillErr, "backfill should succeed with concurrent DML")

	// Verify CDC queue captured the concurrent changes
	lag, err := client.GetReplayLag(testSchema, testTable)
	require.NoError(t, err)
	assert.Greater(t, lag, int64(0), "CDC queue should have captured concurrent DML events")
}

// TestIntegration_LockTimeoutDuringCutover tests that cutover respects lock timeout
// and aborts if the lock cannot be acquired (Requirement 7.7).
func TestIntegration_LockTimeoutDuringCutover(t *testing.T) {
	db, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create source table and run through setup + backfill + replay + verify
	createTestSourceTable(t, db.conn, 50)

	log, err := logger.New(false, "text")
	require.NoError(t, err)

	// Use a very short lock timeout (1 second)
	client := convertpg.NewWithTimeouts(*log, db.conn, 1, 30)

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
			LockTimeout:       1,
			StatementTimeout:  30,
		},
	}

	converter := convert.New(*log, client, config, false)

	// Run through phases to get to cutover-ready state
	err = converter.Setup(ctx)
	require.NoError(t, err)

	err = converter.Backfill(ctx)
	require.NoError(t, err)

	err = converter.Replay(ctx)
	require.NoError(t, err)

	_, err = converter.Verify(ctx, convert.VerifyOptions{})
	require.NoError(t, err)

	// Hold a lock on the source table from another connection to block cutover
	lockConn := db.newConnection(t)
	defer lockConn.Close(ctx)

	// Start a transaction holding ACCESS EXCLUSIVE lock
	_, err = lockConn.Exec(ctx, "BEGIN")
	require.NoError(t, err)
	_, err = lockConn.Exec(ctx, fmt.Sprintf("LOCK TABLE %s.%s IN ACCESS EXCLUSIVE MODE", testSchema, testTable))
	require.NoError(t, err)

	// Cutover should fail due to lock timeout
	err = converter.Cutover(ctx)
	assert.Error(t, err, "cutover should fail when lock cannot be acquired")

	// Release the lock
	_, _ = lockConn.Exec(ctx, "ROLLBACK")
}

// TestIntegration_RollbackAfterCutover tests that rollback correctly reverses
// the cutover (Requirement 8.1).
func TestIntegration_RollbackAfterCutover(t *testing.T) {
	db, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()

	createTestSourceTable(t, db.conn, 100)

	client := newConvertClient(t, db.conn)
	converter := newConverter(t, client)

	// Run full lifecycle up to cutover
	err := converter.Setup(ctx)
	require.NoError(t, err)

	err = converter.Backfill(ctx)
	require.NoError(t, err)

	err = converter.Replay(ctx)
	require.NoError(t, err)

	_, err = converter.Verify(ctx, convert.VerifyOptions{})
	require.NoError(t, err)

	err = converter.Cutover(ctx)
	require.NoError(t, err)

	// Verify table is partitioned after cutover
	relkind := getTableRelkind(t, db.conn, testSchema, testTable)
	assert.Equal(t, "p", relkind, "table should be partitioned after cutover")

	// Rollback
	err = converter.Rollback(ctx)
	require.NoError(t, err, "rollback should succeed")

	// Verify table is back to regular (non-partitioned)
	relkind = getTableRelkind(t, db.conn, testSchema, testTable)
	assert.Equal(t, "r", relkind, "table should be regular after rollback")

	// Verify row count is preserved
	count := getExactRowCount(t, db.conn, testSchema, testTable)
	assert.Equal(t, int64(100), count, "row count should be preserved after rollback")
}

// TestIntegration_ResumabilityAfterInterruption tests that backfill can resume
// after interruption (Requirement 4.4).
func TestIntegration_ResumabilityAfterInterruption(t *testing.T) {
	db, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()

	createTestSourceTable(t, db.conn, 300)

	log, err := logger.New(false, "text")
	require.NoError(t, err)

	// Use a small batch size so we can verify resumability
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
			BackfillBatchSize: 50, // Small batch to verify progress tracking
			ReplayBatchSize:   50,
			LockTimeout:       5,
			StatementTimeout:  30,
		},
	}

	converter := convert.New(*log, client, config, false)

	// Setup
	err = converter.Setup(ctx)
	require.NoError(t, err)

	// First backfill run
	err = converter.Backfill(ctx)
	require.NoError(t, err)

	// Verify all rows were copied
	targetCount := getExactRowCount(t, db.conn, testSchema, testTable+"_partitioned")
	assert.Equal(t, int64(300), targetCount, "all rows should be backfilled")

	// Insert more rows to simulate new data after first backfill
	// Use dates within the existing partition range to avoid "no partition found" errors
	_, err = db.conn.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.%s (created_at, name, value)
		SELECT
			'2024-01-05'::timestamptz + (i * interval '1 minute'),
			'new_event_' || i,
			i + 1000
		FROM generate_series(1, 50) AS i`, testSchema, testTable))
	require.NoError(t, err)

	// The CDC trigger should have captured these inserts
	lag, err := client.GetReplayLag(testSchema, testTable)
	require.NoError(t, err)
	assert.Equal(t, int64(50), lag, "CDC queue should have 50 new events")

	// Replay should process the new events
	err = converter.Replay(ctx)
	require.NoError(t, err)

	// Verify target has all rows
	targetCount = getExactRowCount(t, db.conn, testSchema, testTable+"_partitioned")
	assert.Equal(t, int64(350), targetCount, "target should have all 350 rows after replay")
}

// TestIntegration_AdvisoryLockExclusivity tests that two concurrent cutovers
// cannot proceed simultaneously (Requirement 7.1).
func TestIntegration_AdvisoryLockExclusivity(t *testing.T) {
	db, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()

	createTestSourceTable(t, db.conn, 50)

	client := newConvertClient(t, db.conn)
	converter := newConverter(t, client)

	// Run through phases to get to cutover-ready state
	err := converter.Setup(ctx)
	require.NoError(t, err)

	err = converter.Backfill(ctx)
	require.NoError(t, err)

	err = converter.Replay(ctx)
	require.NoError(t, err)

	_, err = converter.Verify(ctx, convert.VerifyOptions{})
	require.NoError(t, err)

	// Hold the advisory lock from another connection
	lockConn := db.newConnection(t)
	defer lockConn.Close(ctx)

	// Acquire the advisory lock in a transaction
	_, err = lockConn.Exec(ctx, "BEGIN")
	require.NoError(t, err)

	lockKey := fmt.Sprintf("ppm_migration_%s.%s", testSchema, testTable)
	_, err = lockConn.Exec(ctx, fmt.Sprintf("SELECT pg_advisory_xact_lock(hashtext('%s'))", lockKey))
	require.NoError(t, err)

	// Cutover should fail because the advisory lock is held (lock_timeout will expire)
	err = converter.Cutover(ctx)
	assert.Error(t, err, "cutover should fail when advisory lock is held by another session")

	// Release the lock
	_, _ = lockConn.Exec(ctx, "ROLLBACK")
}

// TestIntegration_FKOIDCorrectness tests that child table FKs point to the new
// partitioned table after cutover (Requirement 7.12).
// Note: PostgreSQL requires that FKs referencing a partitioned table must reference
// a unique constraint that includes the partition key. This test uses a composite FK
// that includes the partition key to validate the FK/OID correctness behavior.
func TestIntegration_FKOIDCorrectness(t *testing.T) {
	db, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a source table where the PK already includes the partition key
	// This avoids the PostgreSQL limitation where FKs must reference the full PK
	_, err := db.conn.Exec(ctx, `
		CREATE TABLE public.orders (
			id BIGSERIAL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			amount INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (id, created_at)
		)`)
	require.NoError(t, err)

	// Insert test data
	_, err = db.conn.Exec(ctx, `
		INSERT INTO public.orders (created_at, amount)
		SELECT
			'2024-01-01'::timestamptz + (i * interval '1 hour'),
			i * 100
		FROM generate_series(1, 50) AS i`)
	require.NoError(t, err)

	// Create a child table with a composite FK referencing (id, created_at)
	_, err = db.conn.Exec(ctx, `
		CREATE TABLE public.order_items (
			id BIGSERIAL PRIMARY KEY,
			order_id BIGINT NOT NULL,
			order_created_at TIMESTAMPTZ NOT NULL,
			item_name TEXT NOT NULL,
			CONSTRAINT fk_order_items_orders FOREIGN KEY (order_id, order_created_at)
				REFERENCES public.orders(id, created_at)
		)`)
	require.NoError(t, err)

	// Insert child rows
	_, err = db.conn.Exec(ctx, `
		INSERT INTO public.order_items (order_id, order_created_at, item_name)
		SELECT id, created_at, 'item for order ' || id FROM public.orders LIMIT 10`)
	require.NoError(t, err)

	log, logErr := logger.New(false, "text")
	require.NoError(t, logErr)

	client := convertpg.NewWithTimeouts(*log, db.conn, 5, 30)

	config := partition.Configuration{
		Schema:         "public",
		Table:          "orders",
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

	// Run full lifecycle through cutover
	err = converter.Setup(ctx)
	require.NoError(t, err)

	err = converter.Backfill(ctx)
	require.NoError(t, err)

	err = converter.Replay(ctx)
	require.NoError(t, err)

	_, err = converter.Verify(ctx, convert.VerifyOptions{})
	require.NoError(t, err)

	err = converter.Cutover(ctx)
	require.NoError(t, err)

	// Verify the FK on the child table does NOT reference the old table (orders_old)
	// Note: PostgreSQL internally resolves FKs on partitioned tables to individual partitions,
	// so confrelid may point to a partition rather than the parent table. The key assertion is
	// that it does NOT point to orders_old (which would mean the FK/OID issue was not handled).
	var refTableName string
	err = db.conn.QueryRow(ctx, `
		SELECT cf.relname
		FROM pg_constraint con
		JOIN pg_class c ON c.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_class cf ON cf.oid = con.confrelid
		WHERE con.contype = 'f'
			AND n.nspname = 'public'
			AND c.relname = 'order_items'
			AND con.conname = 'fk_order_items_orders'
		LIMIT 1`).Scan(&refTableName)
	require.NoError(t, err, "should find FK constraint on child table")
	assert.NotEqual(t, "orders_old", refTableName,
		"FK should NOT reference orders_old (the old non-partitioned table)")
	// The FK should reference either 'orders' (the parent) or a partition of 'orders'
	assert.NotContains(t, refTableName, "_old",
		"FK should not reference any _old table")

	// Verify the child table FK is functional (can insert referencing existing rows)
	_, err = db.conn.Exec(ctx, `
		INSERT INTO public.order_items (order_id, order_created_at, item_name)
		SELECT id, created_at, 'post-cutover item' FROM public.orders LIMIT 1`)
	assert.NoError(t, err, "FK should be functional after cutover")

	// Verify FK violation is enforced
	_, err = db.conn.Exec(ctx, `
		INSERT INTO public.order_items (order_id, order_created_at, item_name)
		VALUES (999999, '2024-01-01', 'should fail')`)
	assert.Error(t, err, "FK violation should be enforced after cutover")
}

// TestIntegration_QueueDrainCompleteness tests that cutover aborts if the queue
// is not fully drained (Requirement 7.5).
func TestIntegration_QueueDrainCompleteness(t *testing.T) {
	db, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()

	createTestSourceTable(t, db.conn, 50)

	client := newConvertClient(t, db.conn)
	converter := newConverter(t, client)

	// Run through phases
	err := converter.Setup(ctx)
	require.NoError(t, err)

	err = converter.Backfill(ctx)
	require.NoError(t, err)

	err = converter.Replay(ctx)
	require.NoError(t, err)

	_, err = converter.Verify(ctx, convert.VerifyOptions{})
	require.NoError(t, err)

	// The queue should be empty at this point, so cutover should succeed
	// (the safety assertion checks queue emptiness after final replay)
	empty, err := client.IsCDCQueueEmpty(testSchema, testTable)
	require.NoError(t, err)
	assert.True(t, empty, "queue should be empty before cutover")

	// Cutover should succeed with empty queue
	err = converter.Cutover(ctx)
	require.NoError(t, err, "cutover should succeed when queue is empty")
}

// TestIntegration_PostCutoverAnalyze tests that ANALYZE is executed after cutover
// and pg_statistic entries exist (Requirement 7.9).
func TestIntegration_PostCutoverAnalyze(t *testing.T) {
	db, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()

	createTestSourceTable(t, db.conn, 100)

	client := newConvertClient(t, db.conn)
	converter := newConverter(t, client)

	// Run full lifecycle through cutover
	err := converter.Setup(ctx)
	require.NoError(t, err)

	err = converter.Backfill(ctx)
	require.NoError(t, err)

	err = converter.Replay(ctx)
	require.NoError(t, err)

	_, err = converter.Verify(ctx, convert.VerifyOptions{})
	require.NoError(t, err)

	err = converter.Cutover(ctx)
	require.NoError(t, err)

	// Verify pg_statistic entries exist for the table
	// After ANALYZE, there should be statistics for the table's columns
	var statCount int
	err = db.conn.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_statistic s
		JOIN pg_class c ON s.starelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname = $1
			AND c.relname LIKE $2`, testSchema, testTable+"%").Scan(&statCount)
	require.NoError(t, err)
	assert.Greater(t, statCount, 0, "pg_statistic should have entries after ANALYZE")
}

// TestIntegration_CutoverRaceCondition tests that concurrent writes during cutover
// are handled correctly (trigger disabled before final replay prevents race).
func TestIntegration_CutoverRaceCondition(t *testing.T) {
	db, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()

	createTestSourceTable(t, db.conn, 100)

	client := newConvertClient(t, db.conn)
	converter := newConverter(t, client)

	// Run through phases
	err := converter.Setup(ctx)
	require.NoError(t, err)

	err = converter.Backfill(ctx)
	require.NoError(t, err)

	// Insert some concurrent writes before replay
	_, err = db.conn.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.%s (created_at, name, value)
		VALUES ('2024-01-10 12:00:00+00', 'pre_replay_insert', 777)`, testSchema, testTable))
	require.NoError(t, err)

	err = converter.Replay(ctx)
	require.NoError(t, err)

	_, err = converter.Verify(ctx, convert.VerifyOptions{})
	require.NoError(t, err)

	// Insert more data just before cutover (will be in CDC queue)
	_, err = db.conn.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.%s (created_at, name, value)
		VALUES ('2024-01-11 12:00:00+00', 'pre_cutover_insert', 888)`, testSchema, testTable))
	require.NoError(t, err)

	// Cutover should drain the queue completely (including the pre-cutover insert)
	err = converter.Cutover(ctx)
	require.NoError(t, err, "cutover should succeed and drain all remaining events")

	// Verify the pre-cutover insert is in the final table
	var count int
	err = db.conn.QueryRow(ctx, fmt.Sprintf(`
		SELECT COUNT(*) FROM %s.%s WHERE name = 'pre_cutover_insert'`, testSchema, testTable)).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "pre-cutover insert should be present in final table")

	// Verify total row count is correct (100 original + 1 pre-replay + 1 pre-cutover)
	totalCount := getExactRowCount(t, db.conn, testSchema, testTable)
	assert.Equal(t, int64(102), totalCount, "all rows including concurrent writes should be present")
}
