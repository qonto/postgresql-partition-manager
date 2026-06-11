# Getting Started

This guide walks you through installing and running PPM for the first time. By the end, you will have PPM managing partitions on a PostgreSQL database.

## Prerequisites

- PostgreSQL 14 or higher
- A table using [declarative RANGE partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE)
- A partition key column of type `date`, `timestamp`, `timestamptz`, or `uuid`
- **No** default partition set.

## Quick Start

### 1. Install PPM

Download the latest binary for your platform:

```bash
# Using Go install
go install github.com/qonto/postgresql-partition-manager@latest
```

See [Installation](installation.md) for other methods (Docker, Helm, Debian package).

### 2. Create a Sample Partitioned Table

Create a `logs` table partitioned by `created_at`:

```sql
CREATE TABLE public.logs (
    id BIGSERIAL,
    message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
) PARTITION BY RANGE (created_at);
```

> Default partitions are not supported by PPM, that's why we did not initialize one.

### 3. Create a Configuration File

Create a `postgresql-partition-manager.yaml` file:

```yaml
debug: false
log-format: json
connection-url: postgres://username:password@localhost/mydb

partitions:
  my_logs:
    schema: public
    table: logs
    partitionKey: created_at
    interval: monthly
    retention: 12
    preProvisioned: 3
    cleanupPolicy: drop
```

### 4. Validate Your Configuration

```bash
postgresql-partition-manager validate
```

### 5. Run Partition Management

Execute all operations in once (partition provisioning, partition cleanup (detach or drop), and check):

```bash
postgresql-partition-manager run all
```

### 6. Verify Created Partitions

Check the partitions created by PPM:

```bash
psql -c '\d+ public.logs'
```

Expected output:

```sql
                                          Partitioned table "public.logs"
   Column   |           Type           | Collation | Nullable |             Default              | Storage  | Compression | Stats target | Description
------------+--------------------------+-----------+----------+----------------------------------+----------+-------------+--------------+-------------
 id         | bigint                   |           | not null | nextval('logs_id_seq'::regclass) | plain    |             |              |
 message    | text                     |           |          |                                  | extended |             |              |
 created_at | timestamp with time zone |           | not null | now()                            | plain    |             |              |
Partition key: RANGE (created_at)
Not-null constraints:
    "logs_id_not_null" NOT NULL "id"
    "logs_created_at_not_null" NOT NULL "created_at"
Partitions: logs_2025_06 FOR VALUES FROM ('2025-06-01 00:00:00+00') TO ('2025-07-01 00:00:00+00'),
            logs_2025_07 FOR VALUES FROM ('2025-07-01 00:00:00+00') TO ('2025-08-01 00:00:00+00'),
            logs_2025_08 FOR VALUES FROM ('2025-08-01 00:00:00+00') TO ('2025-09-01 00:00:00+00'),
            [...]
```

## What Happens Next?

PPM will:

1. **Provision** future partitions based on the `preProvisioned` setting
2. **Cleanup** outdated partitions beyond the `retention` period
3. **Check** that existing partitions match the expected configuration

## Recommended Setup

We recommend running `postgresql-partition-manager run all` daily via a CRON job or Kubernetes CronJob, with a minimum of 3 pre-provisioned partitions (7 for daily partitioning). Set up alerts on non-zero exit codes to detect partition issues early.

## Next Steps

- [Installation](installation.md) — All installation methods
- [Configuration](configuration.md) — Full configuration reference
- [Usage](usage.md) — Detailed CLI usage patterns
- [Examples](examples.md) — Real-world usage scenarios
