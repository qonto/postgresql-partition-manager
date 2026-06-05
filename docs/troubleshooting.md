# Troubleshooting

This page covers common issues encountered when using PPM and how to resolve them.

## Common Issues

### Configuration File Not Found

**Symptom:** PPM starts without applying your configuration.

**Solution:** Ensure the configuration file is named `postgresql-partition-manager.yaml` and located in either the current directory or your home directory. Alternatively, specify the path explicitly:

```bash
postgresql-partition-manager run all --config /path/to/postgresql-partition-manager.yaml
```

### Database Connection Refused

**Symptom:** Exit code 3 with a "Could not connect to database" error.

**Solution:** Verify your connection URL is correct and the database is reachable:

```bash
# Test connection with psql
psql "postgres://user:password@host:5432/dbname"
```

Check that:

- The hostname and port are correct
- The database user has appropriate permissions
- Network/firewall rules allow the connection
- SSL requirements are met (add `?sslmode=require` if needed)

### Lock Timeout Exceeded

**Symptom:** Operations fail due to lock timeout.

**Solution:** This means another transaction is holding a lock on the partition table. Increase the lock timeout or investigate the blocking transaction:

```bash
postgresql-partition-manager run all --lock-timeout 1000
```

To identify blocking queries in PostgreSQL:

```sql
SELECT pid, state, query, wait_event_type
FROM pg_stat_activity
WHERE wait_event_type = 'Lock';
```

### Statement Timeout Exceeded

**Symptom:** Operations fail with a statement timeout error.

**Solution:** For large tables with many partitions, the default timeout may be insufficient. Increase it:

```bash
postgresql-partition-manager run all --statement-timeout 10000
```

### Invalid Configuration (Exit Code 1)

**Symptom:** PPM exits immediately with code 1.

**Solution:** Run the validate command to get detailed error information:

```bash
postgresql-partition-manager validate --debug --log-format text
```

Common configuration errors:

- Missing required fields (`schema`, `table`, `partitionKey`, `interval`, `retention`, `preProvisioned`, `cleanupPolicy`)
- Invalid `interval` value (must be `daily`, `weekly`, `monthly`, `quarterly`, or `yearly`)
- Invalid `cleanupPolicy` value (must be `drop` or `detach`)

### Partition Check Failed (Exit Code 5)

**Symptom:** `run check` or `run all` exits with code 5.

**Cause:** Existing partitions don't match the expected configuration. This can happen when:

- Partitions were manually created with different boundaries
- The interval was changed without proper migration
- Gaps exist between partitions

**Solution:** Investigate with debug logging enabled:

```bash
postgresql-partition-manager run check --debug --log-format text
```

Review the output to identify which partitions are misaligned and correct them manually or adjust your configuration.

### Partition Provisioning Failed (Exit Code 4)

**Symptom:** `run provisioning` exits with code 4.

**Possible causes:**

- Insufficient permissions to create partitions
- Conflicting partition ranges already exist
- Table does not exist or is not partitioned

**Solution:** Ensure the database user has `CREATE` permission on the schema, and verify the parent table is set up with declarative RANGE partitioning.

### Invalid Work Date (Exit Code 7)

**Symptom:** Exit code 7 when using `PPM_WORK_DATE`.

**Solution:** Ensure the date format is `YYYY-MM-DD`:

```bash
# Correct
PPM_WORK_DATE=2024-06-15 postgresql-partition-manager run all

# Wrong
PPM_WORK_DATE=15-06-2024 postgresql-partition-manager run all
```

## Debug Mode

Enable debug mode for verbose logging to diagnose issues:

```bash
postgresql-partition-manager run all --debug --log-format text
```

This outputs detailed information about each operation, including SQL queries executed and partition discovery results.

## Getting Help

If you encounter an issue not covered here:

1. Check the [GitHub Issues](https://github.com/qonto/postgresql-partition-manager/issues) for known problems
2. Open a new issue with debug logs and your configuration (redact credentials)
