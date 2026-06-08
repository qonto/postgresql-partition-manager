# PostgreSQL Hook

The `postgresql` hook type executes SQL statements against the database during the partition cleanup lifecycle. Use it for maintenance operations like `VACUUM`, `ANALYZE`, statistics collection, or custom cleanup queries.

## Configuration

```yaml
hooks:
  after-detach:
    - name: "vacuum-partition"
      type: postgresql
      timeout: 5m
      config:
        sql_query: "VACUUM ANALYZE {{.Schema}}.{{.ParentTable}}"
```

### Config Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sql_query` | string | Yes | SQL statement to execute (supports template variables) |

## Execution Behavior

- The SQL statement is executed in a **separate connection** from the main PPM operations
- The hook uses the same connection parameters (host, port, database, credentials) as PPM
- The query is executed with the configured timeout via context cancellation
- If the SQL statement returns an error, the hook is treated as failed
- Hooks execute **outside of any active transaction** — no locks are held during execution

!!! warning
    Only single SQL statements are supported per hook entry. For multiple statements, define multiple hook entries in sequence.

## Template Variables

The `sql_query` field supports [template variables](index.md#template-variables). Variables are resolved before execution.

```yaml
config:
  sql_query: "VACUUM ANALYZE {{.Schema}}.{{.ParentTable}}"
```

All standard template variables are available: `Schema`, `Table`, `ParentTable`, `PartitionName`, `LowerBound`, `UpperBound`, `DatabaseName`, `Hostname`, `Retention`, `Interval`.

## Examples

### ANALYZE parent table after drop

```yaml
hooks:
  after-drop:
    - name: "analyze-parent"
      type: postgresql
      timeout: 10m
      on_failure: continue
      config:
        sql_query: "ANALYZE {{.Schema}}.{{.ParentTable}}"
```

### Record partition statistics before drop

```yaml
hooks:
  before-drop:
    - name: "record-stats"
      type: postgresql
      timeout: 30s
      on_failure: continue
      config:
        sql_query: >-
          INSERT INTO partition_stats (partition_name, row_count, dropped_at)
          SELECT '{{.Schema}}.{{.Table}}', count(*), now()
          FROM {{.Schema}}.{{.Table}}
```

### Notify via pg_notify

```yaml
hooks:
  after-detach:
    - name: "notify-detach"
      type: postgresql
      timeout: 10s
      on_failure: continue
      config:
        sql_query: "SELECT pg_notify('partition_events', json_build_object('event', 'detached', 'partition', '{{.Schema}}.{{.Table}}', 'parent', '{{.ParentTable}}')::text)"
```

### Update a tracking table

```yaml
hooks:
  after-drop:
    - name: "track-dropped"
      type: postgresql
      timeout: 15s
      on_failure: continue
      config:
        sql_query: >-
          INSERT INTO partition_lifecycle (schema_name, table_name, parent_table, event, occurred_at)
          VALUES ('{{.Schema}}', '{{.Table}}', '{{.ParentTable}}', 'dropped', now())
```

### Combine multiple PostgreSQL hooks

```yaml
hooks:
  after-detach:
    - name: "vacuum-partition"
      type: postgresql
      timeout: 5m
      config:
        sql_query: "VACUUM ANALYZE {{.Schema}}.{{.Table}}"

    - name: "update-statistics"
      type: postgresql
      timeout: 30s
      on_failure: continue
      config:
        sql_query: "ANALYZE {{.Schema}}.{{.ParentTable}}"
```

Hooks execute sequentially — `vacuum-partition` completes before `update-statistics` begins.
