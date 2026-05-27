# Convert Command — Online Table Partitioning

The `convert` command performs an **online, CDC-based migration** of a non-partitioned PostgreSQL table to a range-partitioned table. The source table remains fully readable and writable throughout the entire process — there is no extended downtime.

The approach is conceptually similar to tools like `pg_repack` or `pgroll`, but specialized for partition conversion: it creates a new partitioned table, backfills historical data, captures ongoing changes via a trigger-based CDC queue, and performs an atomic cutover swap.

## Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                         Source Table (events)                            │
│                                                                         │
│  ┌──────────────┐     ┌──────────────────────┐                         │
│  │ CDC Trigger  │────▶│  events_cdc_queue     │                         │
│  │ (AFTER ROW)  │     │  (seq_id, op, pk)    │                         │
│  └──────────────┘     └──────────────────────┘                         │
└─────────────────────────────────────────────────────────────────────────┘
         │                         │
         │ Backfill (PK order)     │ Replay (dequeue + apply)
         ▼                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   Target Table (events_partitioned)                      │
│                                                                         │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐          │
│  │ 2024_01    │ │ 2024_02    │ │ 2024_03    │ │ ...        │          │
│  └────────────┘ └────────────┘ └────────────┘ └────────────┘          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Migration Phases

The conversion is split into discrete, resumable phases executed as separate CLI commands. Each phase validates the current migration state before proceeding, ensuring safe re-execution and preventing out-of-order operations.

### Phase Lifecycle (State Machine)

```text
init ──▶ setup ──▶ backfill ──▶ replay ──▶ verify ──▶ cutover ──▶ cleanup
                       │            │          │          │
                       └──(retry)───┘          │          └──▶ rollback ──▶ setup
                                               │
                                               └──(back to replay if not ready)
```

Valid transitions:

| Current Phase | Allowed Next Phases         |
|---------------|-----------------------------|
| setup         | backfill                    |
| backfill      | replay, backfill (re-run), cutover_complete |
| replay        | cutover_complete, replay (re-run) |
| verify        | cutover_complete, replay, backfill |
| cutover_complete | cleanup, rollback        |
| rollback_complete | setup (start over)      |

## Sub-Commands

### 0. `convert init`

Initializes the migration metadata table. This is a one-time prerequisite before any conversion can begin.

**What it does:**

1. Creates the `ppm_migration_metadata` table if it does not already exist
2. The table tracks migration state for all tables being converted (schema, table, phase, timestamps, checkpoints, dropped FK definitions)

**Flags:**

| Flag | Description |
|------|-------------|
| `--dry-run` | Preview the SQL that would be executed without making changes |

**Idempotency:** Uses `CREATE TABLE IF NOT EXISTS` — safe to run multiple times.

**When to run:** Once per database, before the first `convert setup`. Subsequent `setup` commands also call `EnsureMetadataTable` internally, but running `init` explicitly is recommended for visibility.

---

### 0b. `convert status [table-name]`

Displays the current migration state for a table or lists all tracked migrations.

**What it does:**

1. Without arguments + `--all`: lists all migrations in the metadata table (schema, table, phase, timestamps)
2. With a table name: shows detailed status for that specific migration (phase, timestamps, last replay seq, last backfill PK, dropped FK definitions)

**Flags:**

| Flag | Description |
|------|-------------|
| `--all`, `-a` | List all tracked migrations instead of a single table |

**Output example (single table):**

```text
Schema:           public
Table:            events
Phase:            replay
Phase started at: 2024-03-15 14:30:22
Updated at:       2024-03-15 14:35:10
Last replay seq:  45230
Last backfill PK: [1234567]
```

**Output example (all migrations):**

```text
SCHEMA  TABLE          PHASE             STARTED AT           UPDATED AT
------  -----          -----             ----------           ----------
public  events         replay            2024-03-15 14:30:22  2024-03-15 14:35:10
public  audit_logs     cutover_complete  2024-03-14 09:00:00  2024-03-14 09:01:12
```

---

### 0c. `convert drop-metadata`

Removes the `ppm_migration_metadata` table entirely. This is a destructive operation intended for cleanup after all conversions are complete.

**Flags:**

| Flag | Description |
|------|-------------|
| `--confirm` | Required to actually perform the drop. Without it, the command refuses to proceed |
| `--force` | Bypass the safety check that prevents dropping when active migrations exist |

**Safety checks:**

- Refuses to drop if any migration rows exist in the table (active migrations), unless `--force` is used
- Requires explicit `--confirm` flag to prevent accidental execution

**When to use:** After all table conversions are complete and `convert cleanup` has been run for every table. This removes the metadata infrastructure itself.

---

### 1. `convert setup <table-name>`

Prepares the migration infrastructure. This is the entry point for each table conversion.

**What it does:**

1. **Ensures metadata table exists** (calls `EnsureMetadataTable` internally)

2. **Validates prerequisites**
   - Source table must exist
   - Source table must have a primary key (required for CDC tracking)

3. **Creates the CDC queue table** (`<schema>.<table>_cdc_queue`)
   - Columns: `seq_id` (BIGSERIAL), `operation` (TEXT), `pk_values` (TEXT[]), `created_at` (TIMESTAMPTZ)
   - Stores every INSERT, UPDATE, DELETE that occurs on the source table after this point

4. **Creates and installs the CDC trigger**
   - A PL/pgSQL function `ppm_cdc_trigger_<table>()` that captures the operation type and primary key values
   - An `AFTER INSERT OR UPDATE OR DELETE` row-level trigger `ppm_cdc_<table>` on the source table
   - From this moment, all DML on the source table is captured

5. **Creates the target partitioned table** (`<schema>.<table>_partitioned`)
   - Same column definitions as the source
   - `PARTITION BY RANGE (<partition_key>)`
   - Creates partitions covering the existing data range (min/max of the partition key) plus pre-provisioned future partitions

6. **Replicates indexes**
   - Primary key is recreated with the partition key appended (PostgreSQL requirement for partitioned tables)
   - Unique indexes also get the partition key appended
   - Non-unique indexes are replicated as-is with adjusted naming

7. **Replicates foreign keys** from the source table onto the target

8. **Initializes migration metadata** in `ppm_migration_metadata` table

**Idempotency:** Each artifact is checked before creation. Re-running setup after a partial failure is safe.

**PostgreSQL objects created:**

| Object | Name |
|--------|------|
| Table | `<schema>.<table>_cdc_queue` |
| Function | `<schema>.ppm_cdc_trigger_<table>()` |
| Trigger | `ppm_cdc_<table>` ON `<schema>.<table>` |
| Table | `<schema>.<table>_partitioned` |
| Partitions | `<schema>.<table>_partitioned_<suffix>` |
| Metadata row | `ppm_migration_metadata` |

---

### 2. `convert backfill <table-name>`

Copies all existing rows from the source table to the target partitioned table.

**What it does:**

1. Reads the primary key columns of the source table
2. Resumes from the last checkpoint if a previous run was interrupted (reads `last_backfill_pk` from metadata)
3. Copies rows in batches ordered by primary key:
   ```sql
   INSERT INTO <target>
   SELECT * FROM <source>
   WHERE (pk_columns) > (last_pk_values)
   ORDER BY pk_columns
   LIMIT <batch_size>
   ON CONFLICT DO NOTHING
   ```
4. After each batch, persists the last processed PK to metadata (checkpoint)
5. Logs progress every N batches (rows processed, percentage, ETA)
6. Handles deadlocks with automatic retry (up to 3 attempts, 1s delay)

**Key properties:**

- **Resumable**: If interrupted, re-running picks up from the last checkpoint
- **Non-blocking**: Uses regular INSERT with ON CONFLICT DO NOTHING — no locks on the source table
- **Idempotent**: ON CONFLICT DO NOTHING ensures duplicate rows are harmless
- **Batch size**: Configurable via `backfillBatchSize` (default: 10,000 rows)

**Impact on the source table:** None. The backfill only reads from the source.

---

### 3. `convert replay <table-name>`

Applies captured CDC events from the queue to the target table, bringing it closer to convergence with the source.

**What it does:**

1. Dequeues events from `<table>_cdc_queue` in batches (atomic DELETE ... RETURNING)
2. For each event, dispatches by operation type:
   - **INSERT / UPDATE**: Fetches the current row from the source table and upserts into the target
     ```sql
     INSERT INTO <target> SELECT * FROM <source> WHERE pk = values
     ON CONFLICT (pk) DO UPDATE SET ...
     ```
   - **DELETE**: Deletes the matching row from the target
     ```sql
     DELETE FROM <target> WHERE pk = values
     ```
3. If a source row no longer exists for an INSERT/UPDATE event, the event is skipped (logged as warning)
4. Exits when the queue is empty and replay lag is zero (convergence)
5. Persists `last_replay_seq` to metadata after each batch

**Key properties:**

- **Convergence-based**: Runs until the queue is fully drained, then exits
- **Source-of-truth lookup**: For INSERT/UPDATE, always reads the current state from the source (not the event payload). This handles the case where multiple events for the same row are in the queue — only the final state matters
- **Deadlock retry**: Automatic retry on PostgreSQL deadlock (error 40P01)
- **Batch size**: Configurable via `replayBatchSize` (default: 1,000 events)

**When to run:** After backfill completes. Can be run multiple times — each run drains whatever has accumulated since the last run.

---

### 4. `convert verify <table-name>`

Checks whether the target table has converged with the source and is ready for cutover.

**What it does:**

1. Optionally runs `ANALYZE` on both tables (with `--with-analyze`) for accurate row counts
2. Compares row counts between source and target tables
3. Checks the replay lag (number of unprocessed events in the CDC queue)
4. Reports readiness

**Flags:**

| Flag | Description |
|------|-------------|
| `--dry-run` | Preview what would be done without making changes |
| `--with-analyze` | Run ANALYZE on source and target tables before verification for accurate row counts |

**Important:** Without `--with-analyze`, row counts are **estimated** (based on `pg_class.reltuples`). These values may be inaccurate after bulk operations. Use `--with-analyze` for precise counts when needed.

**Verify is informational only** — it does NOT modify the migration state. It can be called at any time regardless of the current phase.

**Output:**

```text
Verification Results:
  Source row count:  1,234,567
  Target row count:  1,234,567
  Row difference:    0
  CDC queue size:    0
  Ready for cutover: true
```

When row counts are estimated:

```text
  ⚠️  WARNING: Row counts are ESTIMATED (based on pg_class.reltuples).
     These values may be inaccurate, especially after bulk operations.
     Use --with-analyze for accurate counts (runs ANALYZE on both tables).
```

---

### 5. `convert cutover <table-name>`

The critical phase: atomically swaps the source and target tables. This is the only step that briefly blocks writes.

**Sequence within a single transaction:**

```text
BEGIN (lock_timeout=<configured>, statement_timeout=<configured>)
│
├─ 1. Record referencing FK definitions in metadata (for rollback)
│
├─ 2. Acquire advisory lock (prevents concurrent cutover attempts)
│     SELECT pg_advisory_xact_lock(hashtext('ppm_migration_<schema>.<table>'))
│
├─ 3. Acquire ACCESS EXCLUSIVE lock on child tables (FK referencing tables)
│     (prevents deadlocks — see "Lock Ordering" below)
│
├─ 4. Acquire ACCESS EXCLUSIVE lock on source table
│     (blocks all reads and writes on the source)
│
├─ 5. Acquire SHARE ROW EXCLUSIVE lock on target table
│     (prevents concurrent DDL on the target)
│
├─ 6. Disable CDC trigger
│     ALTER TABLE <source> DISABLE TRIGGER ppm_cdc_<table>
│
├─ 7. Final replay drain (process any remaining events in the queue)
│     Loop: dequeue + apply until queue is empty
│
├─ 8. Assert CDC queue is empty
│     (abort transaction if not — safety check)
│
├─ 9. Drop referencing foreign keys from child tables
│     (FKs pointing TO the source table from other tables)
│
├─ 10. Rename swap
│      ALTER TABLE <source> RENAME TO <table>_old
│      ALTER TABLE <table>_partitioned RENAME TO <table>
│
├─ 11. Recreate foreign keys with NOT VALID
│      (pointing to the new partitioned table — see "FK Recreation" below)
│
└─ COMMIT
```

**Post-cutover operations (separate transactions, non-blocking):**

1. `ANALYZE <table>` — update planner statistics for the new partitioned table
2. Rename indexes — replace `_partitioned` prefix with original table name (also renames old table indexes to avoid conflicts)
3. Restore identity generation — for columns that had `GENERATED ALWAYS AS IDENTITY` or `GENERATED BY DEFAULT AS IDENTITY` on the source table, the original identity strategy is restored on the partitioned table (see "Identity Generation Preservation" below)
4. `ALTER TABLE ... VALIDATE CONSTRAINT` — validate the NOT VALID foreign keys (full table scan, but does not block writes)

**Lock duration:** The exclusive lock is held only for the duration of the final drain + rename. With a well-converged migration (replay lag near zero), this is typically **sub-second**.

**Safety mechanisms:**

- `lock_timeout`: If the lock cannot be acquired within the configured timeout, the operation fails cleanly (no partial state)
- Queue empty assertion: If events arrive between trigger disable and the assertion (should not happen with exclusive lock), the transaction is aborted
- FK definitions are recorded in metadata before dropping, enabling rollback

**Exit codes:**

- `0`: Cutover successful
- `7`: Lock timeout (could not acquire lock in time — retry later)
- `8`: Queue not empty assertion failed
- `11`: Other cutover failure

---

### Lock Ordering Strategy (Deadlock Prevention)

During cutover, locks are acquired in a **deterministic order** to prevent deadlocks:

1. **Child tables first** (tables with FKs referencing the source) — ACCESS EXCLUSIVE
2. **Source table** — ACCESS EXCLUSIVE
3. **Target table** — SHARE ROW EXCLUSIVE

**Why child tables must be locked first:**

Without this ordering, a classic deadlock scenario can occur:

```text
Transaction A (cutover):
  1. Acquires ACCESS EXCLUSIVE on source table (events)
  2. Tries to lock child table (event_comments) → BLOCKED

Transaction B (application INSERT):
  1. Holds RowExclusive on child table (event_comments) — from INSERT
  2. Tries to check FK on source table (events) → BLOCKED by Transaction A

Result: DEADLOCK
```

By locking child tables **before** the source table, the cutover transaction ensures no concurrent transaction can hold a conflicting lock on a child table while waiting for the source.

**Lock types explained:**

| Lock | Target | Purpose |
|------|--------|---------|
| ACCESS EXCLUSIVE | Child tables (FK referencing) | Prevent concurrent DML that would check FK against source |
| ACCESS EXCLUSIVE | Source table | Block all reads/writes during rename swap |
| SHARE ROW EXCLUSIVE | Target table | Prevent concurrent DDL while allowing reads |
| Advisory lock | `hashtext('ppm_migration_<schema>.<table>')` | Prevent concurrent cutover/rollback attempts |

---

### Foreign Key Handling During Cutover

#### Why FKs Must Be Dropped

When other tables have foreign keys **referencing** the source table (incoming FKs), those constraints must be dropped before the rename swap. PostgreSQL foreign keys reference a specific table OID — renaming the table does not update FK references automatically. Without dropping them:

- The FK would still point to the old table (now `<table>_old`)
- INSERT/UPDATE on child tables would validate against the wrong table
- The constraint would become logically broken

#### FK Drop and Recreation Sequence

1. **Before the transaction**: FK definitions are recorded in `ppm_migration_metadata` (for rollback)
2. **Inside the transaction** (after locking child tables): FKs are dropped
3. **Inside the transaction** (after rename): FKs are recreated with `NOT VALID`
4. **After commit**: `VALIDATE CONSTRAINT` runs a full scan (non-blocking for writes)

#### NOT VALID Semantics

Creating a FK with `NOT VALID` is **instant** — it does not scan the table. It only enforces the constraint for new writes. The subsequent `VALIDATE CONSTRAINT` performs the full scan to verify existing rows, but:

- It only acquires a `SHARE UPDATE EXCLUSIVE` lock on the child table (allows concurrent DML)
- It acquires a `ROW SHARE` lock on the referenced table (allows concurrent DML)
- If validation fails, the constraint remains `NOT VALID` but is not dropped

#### FK Recreation Limitations on Partitioned Tables

PostgreSQL requires that any unique constraint on a partitioned table includes **all partition key columns**. As a consequence, FKs referencing a partitioned table can only target column sets that match an existing unique constraint on the parent table.

Today, PPM automatically recreates a single FK shape after cutover:

- **Supported (recreated automatically):** the FK references exactly the partition key column. The partitioned table always exposes a unique constraint matching it (the partition key is required to be part of the PK / any unique index).
- **Not yet supported (skipped with a warning):** all other shapes — for example, FKs that reference the original PK columns when the partitioned PK had to be widened, FKs referencing a non-partition-key column, or composite FKs.

Skipped FKs remain recorded in `ppm_migration_metadata` (visible via `convert status`). See the [Foreign Key Handling](#foreign-key-handling) section for the manual migration strategy.

Support for additional FK shapes will land in a future release.

---

### Identity Generation Preservation

#### Background

During the backfill and replay phases, the target partitioned table uses `GENERATED BY DEFAULT AS IDENTITY` semantics (or a plain `nextval()` default) for identity columns. This is intentional — it allows PPM to inject data with explicit values via `INSERT ... ON CONFLICT DO NOTHING`.

However, if the source table defined a column as `GENERATED ALWAYS AS IDENTITY`, the partitioned table must be restored to that strategy after the cutover swap. Without restoration, the column would silently accept direct inserts without requiring `OVERRIDING SYSTEM VALUE`, changing the application's data integrity guarantees.

#### How It Works

1. **Before the rename swap** (inside the cutover transaction): PPM queries `information_schema.columns.identity_generation` on the source table to capture which columns have `ALWAYS` or `BY DEFAULT` identity strategies.
2. **After commit** (in post-cutover): For each captured identity column, PPM executes:
   ```sql
   ALTER TABLE <schema>.<table> ALTER COLUMN <col> DROP IDENTITY IF EXISTS;
   ALTER TABLE <schema>.<table> ALTER COLUMN <col> ADD GENERATED {ALWAYS|BY DEFAULT} AS IDENTITY;
   ```

#### Behavior

| Source column definition | After cutover |
|--------------------------|---------------|
| `GENERATED ALWAYS AS IDENTITY` | Restored to `GENERATED ALWAYS AS IDENTITY` |
| `GENERATED BY DEFAULT AS IDENTITY` | Restored to `GENERATED BY DEFAULT AS IDENTITY` |
| `SERIAL` / `BIGSERIAL` | Unchanged (no identity attribute) |
| Plain integer with sequence default | Unchanged (no identity attribute) |

#### Error Handling

Identity restoration runs **after** the atomic rename swap has committed. If restoration fails for a column (e.g., due to a transient error), PPM logs a warning but does **not** fail the cutover. The table is already live under its new name — the identity strategy can be manually corrected if needed:

```sql
ALTER TABLE <schema>.<table> ALTER COLUMN <col> DROP IDENTITY IF EXISTS;
ALTER TABLE <schema>.<table> ALTER COLUMN <col> ADD GENERATED ALWAYS AS IDENTITY;
```

---

### 6. `convert rollback <table-name>`

Reverses a completed cutover, restoring the original non-partitioned table.

**Prerequisites:** The `<table>_old` table must still exist (i.e., cleanup has not been run).

**Sequence:**

```text
BEGIN (lock_timeout=<configured>, statement_timeout=<configured>)
│
├─ 1. Verify <table>_old exists
│
├─ 2. Drop post-cutover foreign keys (pointing to partitioned table)
│
├─ 3. Acquire advisory lock
│
├─ 4. Acquire ACCESS EXCLUSIVE on current table (partitioned)
│
├─ 5. Acquire SHARE ROW EXCLUSIVE on <table>_old
│
├─ 6. Reverse rename
│     ALTER TABLE <table> RENAME TO <table>_partitioned
│     ALTER TABLE <table>_old RENAME TO <table>
│
└─ COMMIT
```

**Post-rollback operations:**

1. Restore original foreign keys from metadata (recreated with `NOT VALID`)
2. Re-enable CDC trigger on the restored source table
3. Rename indexes back to original names
4. `ANALYZE` the restored table

**After rollback:** The migration state returns to a point where you can start over from `setup`.

**Important:** Rollback does NOT replay changes that occurred on the partitioned table after cutover. Any writes that happened between cutover and rollback are lost on the original table. Use rollback only if you detect issues immediately after cutover.

---

### 7. `convert cleanup <table-name>`

Removes all migration artifacts after confirming the cutover is successful.

**Flags:**

| Flag | Description |
|------|-------------|
| `--confirm` | Required to actually perform cleanup. Without it, only lists what would be removed |
| `--force` | Bypass phase validation (use if metadata is in an inconsistent state) |

**What it removes (in order):**

1. CDC trigger `ppm_cdc_<table>` from `<table>_old`
2. CDC trigger function `ppm_cdc_trigger_<table>()`
3. CDC queue table `<table>_cdc_queue`
4. Old source table `<table>_old` (after reassigning sequence ownership)
5. Migration metadata entry

**Sequence reassignment:** Before dropping `<table>_old`, any sequences owned by it (e.g., from BIGSERIAL columns) are reassigned to the new partitioned table. Without this, dropping the old table would cascade-drop the sequence, breaking the new table's DEFAULT values.

**Idempotency:** Each artifact is checked before removal. Missing artifacts are skipped gracefully.

---

## Typical Workflow

```bash
# 0. Initialize metadata table (once per database)
postgresql-partition-manager convert init

# 1. Setup: create CDC infrastructure and target table
postgresql-partition-manager convert setup events

# 1b. Check status at any time
postgresql-partition-manager convert status events

# 2. Backfill: copy historical data (can take hours for large tables)
postgresql-partition-manager convert backfill events

# 3. Replay: catch up with changes that occurred during backfill
postgresql-partition-manager convert replay events

# 4. Verify: confirm convergence
postgresql-partition-manager convert verify events --with-analyze

# If not ready, run replay again:
# postgresql-partition-manager convert replay events
# postgresql-partition-manager convert verify events --with-analyze

# 5. Cutover: atomic swap (brief lock)
postgresql-partition-manager convert cutover events

# 6. Monitor application behavior...

# 7. Cleanup: remove old table and migration artifacts
postgresql-partition-manager convert cleanup events --confirm

# 8. (Optional) Remove metadata table when all conversions are done
postgresql-partition-manager convert drop-metadata --confirm
```

## Configuration

### Full Configuration Reference

```yaml
# Enable debug mode (verbose logging)
debug: false

# Log format: "text" (human-readable) or "json" (structured)
log-format: text

# PostgreSQL connection URL
connection-url: postgres://user:password@host:5432/mydb

# Global lock timeout in milliseconds (prevents infinite waits on long-running transactions)
lock-timeout: 300

# Global statement timeout in milliseconds
statement-timeout: 3000

partitions:
  events:                    # Key used as argument to convert commands
    schema: public
    table: events
    partitionKey: created_at
    interval: monthly
    retention: 12
    preProvisioned: 3
    cleanupPolicy: drop
    convert:                 # Optional, conversion-specific tuning
      backfillBatchSize: 10000   # Rows per backfill batch (default: 10000)
      replayBatchSize: 1000      # Events per replay batch (default: 1000)
      lockTimeout: 5             # Seconds, for cutover/rollback lock acquisition (default: 5)
      statementTimeout: 30       # Seconds, for cutover/rollback statements (default: 30)
```

### Minimal Configuration (All Defaults)

The `convert` sub-key is optional. When omitted (or left empty), all convert parameters use their default values:

```yaml
log-format: text
connection-url: postgres://postgres:hackme@localhost/partitions
lock-timeout: 100
statement-timeout: 3000

partitions:
  my-events:
    schema: public
    table: events
    partitionKey: created_at
    interval: daily
    retention: 3
    preProvisioned: 3
    cleanupPolicy: drop
    convert:
    # Empty convert key — defaults will be applied:
    # backfillBatchSize=10000, replayBatchSize=1000, lockTimeout=5, statementTimeout=30
```

This is equivalent to explicitly specifying all default values under `convert`. The partition will be fully managed by both `run` and `convert` commands without any additional configuration.

### Global Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `debug` | Enable verbose debug logging | `false` |
| `log-format` | Log output format: `text` or `json` | `json` |
| `connection-url` | PostgreSQL connection URL | (required) |
| `lock-timeout` | Global lock timeout in milliseconds | (required) |
| `statement-timeout` | Global statement timeout in milliseconds | (required) |

### Partition Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `schema` | PostgreSQL schema | (required) |
| `table` | Source table name | (required) |
| `partitionKey` | Column for range partitioning | (required) |
| `interval` | Partition interval: `daily`, `weekly`, `monthly`, `quarterly`, `yearly` | (required) |
| `retention` | Number of partitions to retain | (required) |
| `preProvisioned` | Number of future partitions to create | (required) |
| `cleanupPolicy` | `drop` or `detach` | (required) |

### Convert Parameters

| Parameter | Description | Default | Constraints |
|-----------|-------------|---------|-------------|
| `backfillBatchSize` | Rows per backfill batch | 10000 | 1–1,000,000 |
| `replayBatchSize` | Events per replay batch | 1000 | 1–1,000,000 |
| `lockTimeout` | Lock timeout in seconds for cutover/rollback | 5 | 1–60 |
| `statementTimeout` | Statement timeout in seconds for cutover/rollback | 30 | 5–120 |

### Interaction with `run` Commands

When a table has an active conversion in progress (metadata exists with a non-terminal phase), the `run check`, `run provisioning`, and `run cleanup` commands automatically skip that table. Once the conversion is complete and `convert cleanup` removes the metadata, the `run` commands resume managing the table normally. No manual configuration change is needed.

## Dry-Run Mode

All sub-commands support `--dry-run` which outputs the SQL statements and operations that would be performed without making any changes:

```bash
postgresql-partition-manager convert cutover events --dry-run
```

## How the CDC Mechanism Works

### Trigger-Based Change Capture

When setup installs the trigger, every row-level DML on the source table inserts a record into the CDC queue:

```sql
-- Simplified trigger logic
INSERT INTO <schema>.<table>_cdc_queue (operation, pk_values)
VALUES (TG_OP, ARRAY[OLD.pk1::text, OLD.pk2::text, ...]);
```

- For INSERT/UPDATE: captures the NEW row's primary key
- For DELETE: captures the OLD row's primary key

### Why Source Lookup Instead of Event Payload?

The replay engine does NOT store the full row in the CDC event. Instead, it stores only the primary key and operation type. During replay, for INSERT/UPDATE events, it fetches the **current** row from the source table.

This design choice:

- **Reduces CDC queue size** (only PK values stored, not full rows)
- **Handles event coalescing naturally** — if a row is updated 100 times during backfill, only the final state matters
- **Simplifies conflict resolution** — the source table is always the source of truth

The tradeoff is that if a row is deleted after an INSERT/UPDATE event was queued, the replay skips it (logged as warning). This is correct behavior: the DELETE event will also be in the queue and will be processed.

## Partition Key in Primary Key

PostgreSQL requires that the partition key be part of any unique index (including the primary key) on a partitioned table. PPM automatically appends the partition key to:

- The primary key constraint
- Any unique indexes

This means the target table's primary key will be `(original_pk_columns, partition_key)`. Applications using the original PK columns for lookups will still work — the additional column in the PK does not affect queries that filter on the original columns.

## Foreign Key Handling

### Outgoing FKs (source table references other tables)

These are replicated directly onto the target partitioned table during setup.

### Incoming FKs (other tables reference the source table)

During cutover:

1. FK definitions are recorded in migration metadata (for rollback)
2. Child tables are locked with ACCESS EXCLUSIVE (deadlock prevention)
3. FKs are dropped within the cutover transaction
4. After the rename swap, FKs are recreated with `NOT VALID` (instant, no table scan)
5. Post-cutover, `VALIDATE CONSTRAINT` runs a full scan to verify integrity (non-blocking for writes)

### FK Recreation Limitations on Partitioned Tables

PostgreSQL requires that any unique constraint on a partitioned table includes **all partition key columns**. When PPM converts a table, the primary key is widened from `(original_pk)` to `(original_pk, partition_key)` whenever the partition key was not already part of the PK.

As a result, incoming FKs that reference columns no longer covered by a unique constraint on the partitioned table **cannot be automatically recreated** by PPM today.

**Currently supported FK shape:**

- The FK references exactly the partition key column. The partitioned table always exposes a unique constraint matching the partition key, so the FK is recreated with `NOT VALID` and validated post-cutover.

**Not yet supported (skipped with a warning):**

- FKs that referenced the original PK columns when the PK had to be widened
- FKs referencing a non-partition-key column without a replicated unique constraint
- Composite FKs that don't match an existing unique constraint on the parent

**PPM behavior:** When a FK cannot be recreated, PPM logs a warning and skips it. The dropped FK definitions remain in `ppm_migration_metadata` (visible via `convert status`) for rollback or manual recreation. Support for additional FK shapes will come later.

#### Example: Why FKs Break

Consider a table `events` with PK `(id)` and a child table `event_details` with `FOREIGN KEY (id) REFERENCES events(id)`:

```sql
-- Before conversion
CREATE TABLE events (
    id UUID NOT NULL,
    category TEXT,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE event_details (
    id UUID NOT NULL,
    message TEXT,
    FOREIGN KEY (id) REFERENCES events(id)
);
```

If you convert `events` with `partitionKey: created_at`, the partitioned PK becomes `(id, created_at)`. The FK `event_details(id) → events(id)` cannot be recreated because there is no unique constraint on just `(id)` in the partitioned table.

If instead the partition key matches the FK column (e.g., `partitionKey: id` for a UUIDv7 column), the FK is recreated automatically.

#### Manual Migration Strategy for Broken FKs

To restore referential integrity after conversion, the child table must be adapted to reference the full composite key:

```sql
-- 1. Add the partition key column to the child table
ALTER TABLE event_details ADD COLUMN event_created_at timestamptz;

-- 2. Update application code to populate the new column on writes

-- 3. Backfill existing rows
UPDATE event_details cd
SET event_created_at = e.created_at
FROM events e
WHERE cd.id = e.id;

-- 4. Make it NOT NULL
ALTER TABLE event_details ALTER COLUMN event_created_at SET NOT NULL;

-- 5. Create the new FK referencing the composite key
ALTER TABLE event_details
ADD CONSTRAINT fk_event_details_event
FOREIGN KEY (id, event_created_at) REFERENCES events(id, created_at) NOT VALID;

-- 6. Validate (non-blocking for writes)
ALTER TABLE event_details VALIDATE CONSTRAINT fk_event_details_event;
```

This migration must be performed **after** the cutover and is the responsibility of the application team. PPM handles the automated part (drop + attempted recreation) but cannot restructure child tables.

## SQL Locks Reference

### Cutover Locks

| Step | Lock Type | Target | Blocks |
|------|-----------|--------|--------|
| 1 | Advisory (transaction-scoped) | `hashtext('ppm_migration_<schema>.<table>')` | Other cutover/rollback attempts |
| 2 | ACCESS EXCLUSIVE | Child tables (FK referencing source) | All operations on child tables |
| 3 | ACCESS EXCLUSIVE | Source table | All reads and writes |
| 4 | SHARE ROW EXCLUSIVE | Target partitioned table | DDL only (reads/writes allowed) |

### Rollback Locks

| Step | Lock Type | Target | Blocks |
|------|-----------|--------|--------|
| 1 | Advisory (transaction-scoped) | `hashtext('ppm_migration_<schema>.<table>')` | Other cutover/rollback attempts |
| 2 | ACCESS EXCLUSIVE | Current table (partitioned, now named as source) | All operations |
| 3 | SHARE ROW EXCLUSIVE | `<table>_old` | DDL only |

### Lock Timeout Behavior

Both cutover and rollback set `lock_timeout` at the transaction level. If any lock cannot be acquired within the configured timeout:

- The transaction is aborted cleanly
- No partial state is left behind
- The command exits with code `7`
- The operation can be safely retried later (e.g., during a low-traffic window)

## Error Handling and Resilience

| Scenario | Behavior |
|----------|----------|
| Deadlock during backfill/replay | Automatic retry (3 attempts, 1s delay) |
| Lock timeout during cutover | Clean failure, no partial state, retry later |
| Interrupted backfill | Resume from last checkpoint on next run |
| Interrupted replay | Re-run drains remaining events |
| Source row deleted before replay | Event skipped with warning log |
| Cutover fails mid-transaction | Transaction rolls back, no rename occurs |
| FK cannot be recreated (does not reference exactly the partition key) | Warning logged, FK skipped |

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Invalid configuration |
| 2 | Internal error |
| 3 | Database connection error |
| 4 | Source table not found |
| 5 | Source table has no primary key |
| 6 | Setup failed |
| 7 | Lock timeout (backfill) or lock timeout (cutover/rollback) |
| 8 | Assertion failed (queue not empty during cutover) |
| 9 | Rollback not applicable (no _old table) |
| 10 | Verify: not ready for cutover |
| 11 | Cutover failed |
| 12 | Rollback failed |
| 13 | Cleanup failed |


## Sequence Diagrams

### Happy Path

```text
User            PPM                    PostgreSQL
 │               │                         │
 │──init────────▶│                         │
 │               │──CREATE metadata table─▶│
 │◀──────────────│                         │
 │               │                         │
 │──setup───────▶│                         │
 │               │──CREATE cdc_queue──────▶│
 │               │──CREATE trigger────────▶│
 │               │──CREATE partitioned────▶│
 │               │──CREATE partitions─────▶│
 │               │──CREATE indexes────────▶│
 │◀──────────────│                         │
 │               │                         │
 │──backfill────▶│                         │
 │               │──INSERT batch 1────────▶│
 │               │──INSERT batch 2────────▶│
 │               │──INSERT batch N────────▶│
 │◀──────────────│                         │
 │               │                         │
 │──replay──────▶│                         │
 │               │──DEQUEUE + APPLY───────▶│
 │               │──(loop until empty)────▶│
 │◀──────────────│                         │
 │               │                         │
 │──verify──────▶│                         │
 │               │──COUNT source──────────▶│
 │               │──COUNT target──────────▶│
 │               │──CHECK lag─────────────▶│
 │◀──ready=true──│                         │
 │               │                         │
 │──cutover─────▶│                         │
 │               │──Record FK defs────────▶│
 │               │──BEGIN────────────────▶ │
 │               │──ADVISORY LOCK────────▶ │
 │               │──LOCK child tables────▶ │  ◀── child writes blocked
 │               │──LOCK source (EXCL)───▶ │  ◀── source writes blocked
 │               │──LOCK target (SRE)────▶ │
 │               │──DISABLE trigger──────▶ │
 │               │──DRAIN queue──────────▶ │
 │               │──ASSERT empty─────────▶ │
 │               │──DROP child FKs───────▶ │
 │               │──RENAME swap──────────▶ │
 │               │──RECREATE FKs─────────▶ │
 │               │──COMMIT───────────────▶ │  ◀── writes resume
 │               │──ANALYZE──────────────▶ │
 │               │──RENAME indexes───────▶ │
 │               │──VALIDATE FKs─────────▶ │
 │◀──────────────│                         │
 │               │                         │
 │──cleanup─────▶│                         │
 │               │──DROP trigger──────────▶│
 │               │──DROP queue────────────▶│
 │               │──DROP _old─────────────▶│
 │◀──────────────│                         │
```

### Cutover with Lock Timeout (Retry Pattern)

```text
User            PPM                    PostgreSQL
 │               │                         │
 │──cutover─────▶│                         │
 │               │──BEGIN────────────────▶ │
 │               │──LOCK child table─────▶ │
 │               │◀──lock_timeout error────│
 │               │──ROLLBACK─────────────▶ │
 │◀──exit 7──────│                         │
 │               │                         │
 │  (wait, retry later)                    │
 │               │                         │
 │──cutover─────▶│                         │
 │               │──BEGIN────────────────▶ │
 │               │──LOCK child table─────▶ │  ◀── acquired
 │               │──LOCK source──────────▶ │  ◀── acquired
 │               │──... (proceed)          │
```

### Rollback After Cutover

```text
User            PPM                    PostgreSQL
 │               │                         │
 │──rollback────▶│                         │
 │               │──CHECK _old exists─────▶│
 │               │──DROP post-cutover FKs─▶│
 │               │──BEGIN────────────────▶ │
 │               │──ADVISORY LOCK────────▶ │
 │               │──LOCK current (EXCL)──▶ │
 │               │──LOCK _old (SRE)──────▶ │
 │               │──RENAME current → _part▶│
 │               │──RENAME _old → original▶│
 │               │──COMMIT───────────────▶ │
 │               │──RESTORE FKs──────────▶ │
 │               │──ENABLE trigger────────▶│
 │               │──RENAME indexes───────▶ │
 │               │──ANALYZE──────────────▶ │
 │◀──────────────│                         │
```

## Complete Sub-Command Reference

| Command | Arguments | Description |
|---------|-----------|-------------|
| `convert init` | — | Create the migration metadata table |
| `convert status` | `[table-name]` | Show migration status (use `--all` for all) |
| `convert setup` | `<table-name>` | Create CDC queue, trigger, and target table |
| `convert backfill` | `<table-name>` | Copy historical data to target |
| `convert replay` | `<table-name>` | Apply CDC events to target |
| `convert verify` | `<table-name>` | Check convergence readiness |
| `convert cutover` | `<table-name>` | Atomic table swap |
| `convert rollback` | `<table-name>` | Reverse a cutover |
| `convert cleanup` | `<table-name>` | Remove migration artifacts |
| `convert drop-metadata` | — | Drop the metadata table entirely |
