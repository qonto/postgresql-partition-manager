# Usage

PPM provides a command-line interface built on Cobra for managing PostgreSQL partitions. This page covers all available commands, their options, and common usage patterns.

## Global Flags

These flags are available on all commands:

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--config` | `-c` | | Config file path (default: `$HOME/postgresql-partition-manager.yaml`) |
| `--debug` | `-d` | `false` | Enable debug mode |
| `--log-format` | `-l` | `json` | Log format (`text` or `json`) |
| `--connection-url` | `-u` | | Database connection string |
| `--lock-timeout` | | `100` | Set lock_timeout in milliseconds |
| `--statement-timeout` | | `3000` | Set statement_timeout in milliseconds |

## Commands

See [CLI reference](cli-reference.md) for the full reference.

## Common Patterns

### Daily Scheduled Execution

Run all partition operations daily with a custom config:

```bash
postgresql-partition-manager run all --config /etc/ppm/postgresql-partition-manager.yaml
```

### Validate Before Deploy

Check configuration validity in CI before deploying a new config:

```bash
postgresql-partition-manager validate --config postgresql-partition-manager.yaml
```

### Debug Mode

Enable verbose logging for troubleshooting:

```bash
postgresql-partition-manager run all --debug --log-format text
```

### Custom Timeouts

Increase timeouts for large databases:

```bash
postgresql-partition-manager run all --lock-timeout 500 --statement-timeout 10000
```

### Override Connection URL

Pass the connection URL via flag or environment variable:

```bash
# Via flag
postgresql-partition-manager run all --connection-url "postgres://user:pass@host/db"

# Via environment variable
export POSTGRESQL_PARTITION_MANAGER_CONNECTION_URL="postgres://user:pass@host/db"
postgresql-partition-manager run all
```

## Work Date Override

By default, provisioning and cleanup evaluate what to do at the current date. For testing purposes, a different date can be set through the environment variable `PPM_WORK_DATE`:

```bash
PPM_WORK_DATE=2024-06-15 postgresql-partition-manager run all
```

The date format is `YYYY-MM-DD`.

## Exit Codes

PPM uses specific exit codes to indicate the type of failure:

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Invalid configuration |
| 2 | Internal error |
| 3 | Database connection error |
| 4 | Partition provisioning failed |
| 5 | Partition check failed |
| 6 | Partition cleanup failed |
| 7 | Invalid work date |

Monitor these exit codes in your alerting system to detect partition issues early.
