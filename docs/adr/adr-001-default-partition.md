# Do not support PostgreSQL DEFAULT partitions

## Status

Accepted

## Context

PostgreSQL Partition Manager (PPM) is an opinionated tool for PostgreSQL declarative partition management. Its goal is to keep partitioning simple, predictable, secure, and non-blocking for application teams.

PostgreSQL `DEFAULT` partitions can be useful as a safety net: they catch rows that do not match any existing partition. However, they introduce operational complexity that conflicts with PPM’s design goals.

When a new partition is attached, PostgreSQL must ensure that the `DEFAULT` partition does not contain rows that should belong to the new partition. Without a suitable exclusion `CHECK` constraint, PostgreSQL scans the `DEFAULT` partition while holding an `ACCESS EXCLUSIVE` lock. On large tables, this can block production traffic and make partition provisioning unsafe.

Default partitions also require extra lifecycle management:

* monitoring whether rows entered the default partition;
* deciding whether those rows are valid or invalid;
* creating the missing target partitions;
* moving rows out of the default partition;
* vacuuming/analyzing after movement;
* maintaining exclusion constraints to avoid full scans;
* handling lock retries and failure scenarios.

External operational experience confirms that `DEFAULT` partitions can solve rare migration or sparse-data problems, but only with careful, case-specific engineering. They are not a “boring” default for automated partition maintenance.

## Decision

PPM will not support PostgreSQL `DEFAULT` partitions.

PPM will instead require explicit, contiguous RANGE partitions with no gaps between partitions. Missing partitions must be treated as configuration or provisioning errors, not silently absorbed by a default partition.

## Consequences

### Positive

* Partition provisioning remains predictable and easier to reason about.
* PPM avoids hidden full-table scans on default partitions.
* PPM avoids `ACCESS EXCLUSIVE` locking risks on large default partitions.
* Missing partitions fail fast instead of accumulating hidden data.
* Operational ownership remains clear: teams must pre-provision partitions and monitor PPM failures.
* The tool stays aligned with its opinionated scope: simple RANGE partition management without PostgreSQL extensions.

### Negative

* Inserts outside existing partition bounds fail instead of being captured.
* Operators must configure enough `preProvisioned` partitions.
* Application teams need alerting on PPM failures and partition gaps.
* Rare advanced use cases, such as attaching a large sparse historical table as a default partition, must be handled outside PPM.

## Alternatives considered

### Support default partitions as a safety net

Rejected. This would prevent insert failures, but it creates hidden operational debt. Rows in the default partition must later be inspected, moved, and cleaned up. Attaching future partitions can require scans and locks unless careful exclusion constraints are maintained.

### Support default partitions with monitoring only

Rejected. Monitoring detects the problem but does not solve the harder part: safely moving data out and attaching the correct partitions without blocking production workloads.

### Support default partitions for advanced/manual mode

Rejected for now. The safe use of default partitions depends heavily on table size, workload, constraints, lock strategy, and migration context. Encoding this safely would significantly expand PPM’s scope and weaken its “simple and non-blocking by default” design.

## Guidance

Users who need a default partition should manage it outside PPM with a dedicated, case-specific runbook. That runbook should include:

* monitoring for rows in the default partition;
* exclusion `CHECK` constraints before attaching new partitions;
* bounded lock timeouts;
* retry logic;
* data movement procedures;
* post-move `VACUUM ANALYZE`;
* explicit rollback steps.

PPM should document this limitation clearly and recommend sufficient pre-provisioning plus alerting on non-zero exit codes.

## External Resources

- [A Deep Dive into Table partitioning Part 4: How the default partition saved the day](https://www.adyen.com/knowledge-hub/how-the-default-partition-saved-the-day)
- [Postgres Partitioning with a Default Partition](https://www.crunchydata.com/blog/postgres-partitioning-with-a-default-partition)
