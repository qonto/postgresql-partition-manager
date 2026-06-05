# Examples

This page provides real-world usage examples for common PPM deployment scenarios. Each example includes a complete configuration and the commands to run.

## Daily Log Table Partitioning

Partition a high-volume logs table by day, keeping 30 days of data and pre-provisioning 7 days ahead:

```yaml
connection-url: postgres://app_user:password@db.example.com:5432/production

partitions:
  application_logs:
    schema: public
    table: logs
    partitionKey: created_at
    interval: daily
    retention: 30
    preProvisioned: 7
    cleanupPolicy: drop
```

```bash
postgresql-partition-manager run all
```

## Monthly Events with Detach-Only Cleanup

For audit tables where data must be retained but detached from the active table for performance:

```yaml
connection-url: postgres://audit_user@db.example.com/audit_db

partitions:
  audit_events:
    schema: audit
    table: events
    partitionKey: event_date
    interval: monthly
    retention: 12
    preProvisioned: 3
    cleanupPolicy: detach
```

## UUIDv7-Based Partitioning

Partition a table using UUIDv7 primary keys, which encode a timestamp:

```yaml
connection-url: postgres://app@db.example.com/myapp

partitions:
  orders:
    schema: public
    table: orders
    partitionKey: id
    interval: monthly
    retention: 6
    preProvisioned: 2
    cleanupPolicy: drop
```

## Multiple Tables with Different Intervals

Manage several tables with varying partition strategies in a single configuration:

```yaml
connection-url: postgres://admin@db.example.com/production

partitions:
  high_volume_logs:
    schema: public
    table: request_logs
    partitionKey: created_at
    interval: daily
    retention: 14
    preProvisioned: 7
    cleanupPolicy: drop

  session_data:
    schema: public
    table: sessions
    partitionKey: started_at
    interval: weekly
    retention: 12
    preProvisioned: 4
    cleanupPolicy: drop

  analytics:
    schema: reporting
    table: metrics
    partitionKey: recorded_at
    interval: monthly
    retention: 24
    preProvisioned: 3
    cleanupPolicy: detach
```

```bash
postgresql-partition-manager run all
```

## Docker Deployment

Run PPM as a one-shot container with an external configuration file:

```bash
docker run \
  -v ./postgresql-partition-manager.yaml:/app/postgresql-partition-manager.yaml \
  public.ecr.aws/qonto/postgresql-partition-manager:latest \
  run all
```

## Testing with Work Date Override

Test how PPM would behave on a specific date without waiting:

```bash
PPM_WORK_DATE=2024-12-31 postgresql-partition-manager run all --debug --log-format text
```

## Validate Configuration in CI

Add a validation step to your CI pipeline:

```bash
postgresql-partition-manager validate --config configs/ppm-production.yaml
```
