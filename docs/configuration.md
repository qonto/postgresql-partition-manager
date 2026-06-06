# Configuration

PPM can be configured through a YAML configuration file and environment variables. This page describes all available configuration options and how they interact.

## Configuration File

By default, PPM looks for a `postgresql-partition-manager.yaml` file in the current directory or your home directory. You can specify a custom path with the `--config` flag.

### Full Configuration Example

```yaml
# Enable debug mode
debug: false

# Log format (text or json)
log-format: json

# PostgreSQL connection URL
connection-url: postgres://username:password@localhost:5432/mydb

# Maximum allowed duration of any wait for a lock (milliseconds)
# Prevents infinite locks when long-running transactions occur
lock-timeout: 300

# Maximum allowed duration of any statement (milliseconds)
statement-timeout: 3000

# Partitions definition
partitions:
  my_logs:
    schema: public
    table: logs
    partitionKey: created_at
    interval: daily
    retention: 30
    preProvisioned: 7
    cleanupPolicy: drop
```

## Global Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `connection-url` | PostgreSQL connection URL | |
| `debug` | Enable debug mode | `false` |
| `log-format` | Log format (`text` or `json`) | `json` |
| `lock-timeout` | Maximum allowed duration of any wait for a lock (ms) | `300` |
| `statement-timeout` | Maximum allowed duration of any statement (ms) | `3000` |
| `partitions` | Map of partition configurations | |

## Partition Parameters

Each entry in the `partitions` map defines a managed partition:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `schema` | PostgreSQL schema containing the table | |
| `table` | Table to be partitioned | |
| `partitionKey` | Column used for partitioning | |
| `interval` | Partitioning interval (`daily`, `weekly`, `monthly`, `quarterly`, or `yearly`) | |
| `preProvisioned` | Number of partitions to create in advance | |
| `retention` | Number of partitions to retain | |
| `cleanupPolicy` | Cleanup behavior: `drop` (detach and drop) or `detach` (detach only) | |

## Environment Variables

All configuration parameters can be overridden using environment variables. The prefix is `POSTGRESQL_PARTITION_MANAGER_` followed by the uppercase parameter name with hyphens replaced by underscores.

| Configuration Key | Environment Variable |
|-------------------|---------------------|
| `debug` | `POSTGRESQL_PARTITION_MANAGER_DEBUG` |
| `log-format` | `POSTGRESQL_PARTITION_MANAGER_LOG_FORMAT` |
| `connection-url` | `POSTGRESQL_PARTITION_MANAGER_CONNECTION_URL` |
| `lock-timeout` | `POSTGRESQL_PARTITION_MANAGER_LOCK_TIMEOUT` |
| `statement-timeout` | `POSTGRESQL_PARTITION_MANAGER_STATEMENT_TIMEOUT` |

## Partition Naming

Partition names are automatically generated based on the interval:

| Interval | Pattern | Example |
|----------|---------|---------|
| daily | `<table>_<YYYY>_<DD>_<MM>` | `logs_2024_06_25` |
| weekly | `<table>_w<ISO week>` | `logs_2024_w26` |
| monthly | `<table>_<YYYY>_<MM>` | `logs_2024_06` |
| quarterly | `<table>_<YYYY>_q<quarter>` | `logs_2024_q1` |
| yearly | `<table>_<YYYY>` | `logs_2024` |

Partition names are not configurable.

## Configuration Precedence

Configuration values are resolved in the following order (highest priority first):

1. CLI flags (`--connection-url`, `--debug`, etc.)
2. Environment variables (`POSTGRESQL_PARTITION_MANAGER_*`)
3. Configuration file (`postgresql-partition-manager.yaml`)
4. Default values

## Supported Column Types

The partition key must be a column of one of the following types:

- `date`
- `timestamp`
- `timestamptz`
- `uuid` (UUIDv7)
