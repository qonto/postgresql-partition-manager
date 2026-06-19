# Shell Hook

The `shell` hook type executes system commands during the partition cleanup lifecycle. Use it to run scripts, send notifications, archive data, or invoke external tools.

## Configuration

```yaml
hooks:
  before-detach:
    - name: "my-shell-hook"
      type: shell
      timeout: 30s
      config:
        command: "/usr/local/bin/my-script"
        args: ["--partition", "{{.Schema}}.{{.Table}}"]
        env:
          MY_VAR: "my-value"
```

### Config Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `command` | string | Yes | Executable path or command name |
| `args` | list | No | Command arguments (supports template variables) |
| `env` | map | No | Additional environment variables (supports template variables) |
| `propagate-credentials` | bool | No | Inject `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` env vars (default `false`) |

## Execution Behavior

- The command is executed directly via `exec` (not through a shell interpreter)
- The parent process environment variables are inherited
- Additional `env` vars are merged on top of the inherited environment
- `stdout` and `stderr` are captured and logged at debug level
- A non-zero exit code is treated as a failure

!!! tip
    Since commands are executed directly (not via a shell), you cannot use shell features like pipes (`|`), redirection (`>`), or variable expansion (`$VAR`) in the `command` field. To use shell features, invoke a shell explicitly:

    ```yaml
    config:
      command: "/bin/sh"
      args: ["-c", "echo 'hello' >> /tmp/output.log"]
    ```

## Credential Propagation

When `propagate-credentials: true` is set inside a shell hook's `config` section, the hook receives PostgreSQL connection details as environment variables extracted from the PPM connection URL:

| Variable | Description | Example |
|----------|-------------|---------|
| `PGHOST` | Database hostname | `db.example.com` |
| `PGPORT` | Database port | `5432` |
| `PGDATABASE` | Database name | `production` |
| `PGUSER` | Database username | `app_user` |
| `PGPASSWORD` | Database password | `secret` |

This allows shell hooks to connect to the same database as PPM without duplicating credentials in the configuration.

```yaml
hooks:
  before-drop:
    - name: "dump-partition"
      type: shell
      timeout: 10m
      config:
        command: "pg_dump"
        args: ["-t", "{{.Schema}}.{{.Table}}", "-f", "/backups/{{.Table}}.sql"]
        propagate-credentials: true
```

!!! note
    `propagate-credentials` is a `shell` hook config option. PostgreSQL hooks use the same connection parameters as PPM automatically.

## Template Variables

The `command`, `args`, and `env` fields all support [template variables](index.md#template-variables). Variables are resolved before execution.

```yaml
config:
  command: "/usr/local/bin/archive"
  args:
    - "--schema"
    - "{{.Schema}}"
    - "--table"
    - "{{.Table}}"
    - "--database"
    - "{{.DatabaseName}}"
  env:
    S3_PREFIX: "{{.DatabaseName}}/{{.ParentTable}}/{{.Table}}"
    PARTITION_BOUNDS: "{{.LowerBound}}_{{.UpperBound}}"
```

## Examples

### Send a Slack notification

```yaml
hooks:
  after-drop:
    - name: "notify-slack"
      type: shell
      timeout: 15s
      on_failure: continue
      config:
        command: "/usr/local/bin/slack-notify"
        args:
          - "--channel"
          - "#database-ops"
          - "--message"
          - "Partition {{.Schema}}.{{.Table}} dropped from {{.ParentTable}}"
```

### Archive partition to S3 before drop

```yaml
hooks:
  before-drop:
    - name: "archive-to-s3"
      type: shell
      timeout: 10m
      on_failure: abort
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

### Log operations to a file

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

### Run pg_dump with credential propagation

```yaml
hooks:
  before-drop:
    - name: "backup-partition"
      type: shell
      timeout: 30m
      retry:
        attempts: 2
        backoff: fixed
        initial_delay: 30s
      config:
        command: "pg_dump"
        args:
          - "--format=custom"
          - "--table={{.Schema}}.{{.Table}}"
          - "--file=/backups/{{.ParentTable}}/{{.Table}}.dump"
        propagate-credentials: true
```

### Verify backup exists before drop

```yaml
hooks:
  before-drop:
    - name: "verify-backup"
      type: shell
      timeout: 2m
      config:
        command: "/usr/local/bin/verify-backup"
        args: ["--table", "{{.Schema}}.{{.Table}}", "--bucket", "my-archive-bucket"]
```

If this hook fails, the drop operation is cancelled for the partition.
