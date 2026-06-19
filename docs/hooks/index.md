# Hooks

Hooks allow you to execute custom actions at specific points during the partition cleanup lifecycle. Use them to archive data, run maintenance SQL, send notifications, or trigger external workflows before or after partitions are detached or dropped.

## Overview

Hooks are defined in the `hooks` section of your configuration and execute at four lifecycle events:

1. **`before-detach`** — Before a partition is detached from the parent table
2. **`after-detach`** — After a partition has been successfully detached
3. **`before-drop`** — Before a detached partition is dropped (only when `cleanupPolicy: drop`)
4. **`after-drop`** — After a partition has been successfully dropped (only when `cleanupPolicy: drop`)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                            Partition Cleanup Flow                            │
├──────────────┬──────────┬──────────────┬───────────┬────────────┬────────────┤
│ before-detach│  DETACH  │ after-detach │before-drop│   DROP     │ after-drop |
│    hooks     │ partition│    hooks     │   hooks   │ partition  │   hooks    |
└──────────────┴──────────┴──────────────┴───────────┴────────────┴────────────┘  
```

## Hook Types

| Type | Description | Documentation |
|------|-------------|---------------|
| `shell` | Execute system commands, scripts, or external tools | [Shell Hook](shell.md) |
| `postgresql` | Execute SQL statements against the database | [PostgreSQL Hook](postgresql.md) |

## Configuration

### Scope

Hooks can be defined at two levels:

- **Global** — Applied to all partitions
- **Per-partition** — Overrides global hooks for that specific partition

When hooks are defined at both levels, the partition-level hooks completely replace the global hooks for that partition.

### Hook Entry Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | — | Hook identifier for logging |
| `type` | string | Yes | — | Runner type: `shell` or `postgresql` |
| `enabled` | bool | No | `true` | Set to `false` to skip without removing |
| `timeout` | duration | No | `300s` | Maximum execution time (e.g., `30s`, `5m`, `1h`) |
| `on_failure` | string | No | — | Override failure behavior: `abort` or `continue` |
| `retry` | object | No | — | Retry configuration (see below) |
| `config` | object | Yes | — | Type-specific configuration |

### Type-Specific Config

Each hook type has its own `config` section. See the dedicated pages for details:

- [Shell Hook Configuration](shell.md#configuration)
- [PostgreSQL Hook Configuration](postgresql.md#configuration)

## Template Variables

Hook configuration fields support Go template syntax (`{{.VariableName}}`). Available variables:

| Variable | Description | Example |
|----------|-------------|---------|
| `{{.Schema}}` | Partition schema | `public` |
| `{{.Table}}` | Partition table name (child) | `logs_2024_06_25` |
| `{{.ParentTable}}` | Parent table name | `logs` |
| `{{.PartitionName}}` | Partition identifier from config | `application_logs` |
| `{{.LowerBound}}` | Partition lower bound | `2024-06-25` |
| `{{.UpperBound}}` | Partition upper bound | `2024-06-26` |
| `{{.DatabaseName}}` | Database name from connection URL | `production` |
| `{{.Hostname}}` | Database hostname from connection URL | `db.example.com` |
| `{{.Retention}}` | Configured retention value | `30` |
| `{{.Interval}}` | Configured interval | `daily` |

!!! warning
    Referencing an undefined template variable causes a configuration error and aborts the cleanup for the affected partition.

## Retry Configuration

Hooks can be configured to retry on failure with configurable backoff.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `retry.attempts` | int | `0` | Number of retry attempts (0 = no retry) |
| `retry.backoff` | string | `exponential` | Backoff strategy: `fixed` or `exponential` |
| `retry.initial_delay` | duration | `5s` | Initial delay between retries |
| `retry.max_delay` | duration | `60s` | Maximum delay (for exponential backoff) |

**Exponential backoff** doubles the delay after each attempt: `initial_delay × 2^(N-1)`, capped at `max_delay`.

**Fixed backoff** waits `initial_delay` between every attempt.

## Failure Behavior

### Default Behavior

- **Before-hooks** (`before-detach`, `before-drop`): failure cancels the operation for the affected partition
- **After-hooks** (`after-detach`, `after-drop`): failure is logged; the operation is already complete
- **After-detach failure with `cleanupPolicy: drop`**: the drop operation is skipped

In all cases, other partitions continue processing normally.

### Override with `on_failure`

| Value | Effect |
|-------|--------|
| `abort` | Stop the entire cleanup process immediately and exit with non-zero code |
| `continue` | Proceed with the operation even if the before-hook fails |

!!! note
    When any hook fails during a run (after all retries), PPM exits with a non-zero exit code — even if all partition operations succeeded.

### Execution Order

Hooks within a lifecycle event execute sequentially in the order defined. If a hook fails, remaining hooks in the same event are skipped.

## Dry-Run Mode

Use `--dry-run` to preview hook execution without side effects:

```bash
postgresql-partition-manager run cleanup --dry-run
postgresql-partition-manager run all --dry-run
```

In dry-run mode:

- Template variables are resolved and logged
- No hooks are executed (no shell commands, no SQL)
- No partitions are detached or dropped
- Configuration errors (invalid templates) are reported as they would in a normal run

## Credential Propagation

When `propagate-credentials: true` is set inside a shell hook's `config` section, that hook receives PostgreSQL connection details as environment variables:

- `PGHOST`
- `PGPORT`
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`

This allows shell hooks to connect to the same database without duplicating credentials.

!!! note
    `propagate-credentials` is a `shell` hook config option. PostgreSQL hooks use the same connection parameters as PPM automatically.

## Examples

### Archive partition to S3 before drop

```yaml
connection-url: postgres://app:secret@db.example.com:5432/production

partitions:
  application_logs:
    schema: public
    table: logs
    partitionKey: created_at
    interval: daily
    retention: 30
    preProvisioned: 7
    cleanupPolicy: drop
    hooks:
      before-drop:
        - name: "archive-to-s3"
          type: shell
          timeout: 10m
          retry:
            attempts: 3
            backoff: exponential
            initial_delay: 10s
            max_delay: 120s
          config:
            command: "/usr/local/bin/archive-partition"
            args:
              - "--schema"
              - "{{.Schema}}"
              - "--table"
              - "{{.Table}}"
            env:
              S3_BUCKET: "my-archive-bucket"
              S3_PREFIX: "{{.DatabaseName}}/{{.ParentTable}}/{{.Table}}"
            propagate-credentials: true
```

### Run VACUUM ANALYZE after detach

```yaml
hooks:
  after-detach:
    - name: "vacuum-after-detach"
      type: postgresql
      timeout: 5m
      retry:
        attempts: 2
        backoff: fixed
        initial_delay: 5s
      config:
        sql_query: "VACUUM ANALYZE {{.Schema}}.{{.Table}}"
```

### Log partition operations to a file

```yaml
hooks:
  before-detach:
    - name: "log-detach"
      type: shell
      timeout: 10s
      on_failure: continue
      config:
        command: "/bin/sh"
        args: ["-c", "echo '{{.Schema}}.{{.Table}} detaching at $(date)' >> /var/log/ppm-hooks.log"]
```

### Global hooks with per-partition override

```yaml
connection-url: postgres://app:secret@db.example.com:5432/production

# Global hooks applied to all partitions
hooks:
  after-detach:
    - name: "notify-detach"
      type: shell
      timeout: 15s
      on_failure: continue
      config:
        command: "/usr/local/bin/notify"
        args: ["Partition {{.Schema}}.{{.Table}} detached"]

partitions:
  events:
    schema: public
    table: events
    partitionKey: created_at
    interval: daily
    retention: 90
    preProvisioned: 7
    cleanupPolicy: drop
    # This partition uses global hooks (after-detach notification)

  sensitive_data:
    schema: private
    table: audit_logs
    partitionKey: created_at
    interval: monthly
    retention: 12
    preProvisioned: 3
    cleanupPolicy: drop
    # Override global hooks — archive before dropping
    hooks:
      before-drop:
        - name: "archive-audit"
          type: shell
          timeout: 30m
          config:
            command: "/usr/local/bin/archive-audit"
            args: ["--partition", "{{.Schema}}.{{.Table}}"]
```

### Multiple hooks in sequence

```yaml
hooks:
  before-drop:
    - name: "verify-backup"
      type: shell
      timeout: 2m
      config:
        command: "/usr/local/bin/verify-backup"
        args: ["--table", "{{.Schema}}.{{.Table}}"]

    - name: "export-stats"
      type: postgresql
      timeout: 30s
      on_failure: continue
      config:
        sql_query: "INSERT INTO partition_stats (partition_name, row_count, dropped_at) SELECT '{{.Schema}}.{{.Table}}', count(*), now() FROM {{.Schema}}.{{.Table}}"
```

### Abort on critical hook failure

```yaml
hooks:
  before-drop:
    - name: "critical-backup"
      type: shell
      timeout: 15m
      on_failure: abort
      retry:
        attempts: 5
        backoff: exponential
        initial_delay: 10s
        max_delay: 120s
      config:
        command: "/usr/local/bin/backup-partition"
        args: ["{{.Schema}}.{{.Table}}"]
```

If this hook fails after all 5 retries, the entire cleanup process stops immediately.
