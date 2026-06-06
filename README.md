# PostgreSQL Partition Manager

An opinionated tool to streamline PostgreSQL partition management. Handles provisioning, cleanup, and configuration checks for declarative partitions — no extensions required.

**[Documentation](https://qonto.github.io/postgresql-partition-manager/latest/)** | **[Getting Started](https://qonto.github.io/postgresql-partition-manager/latest/getting-started/)**

## Quick Start

Create `postgresql-partition-manager.yaml` configuration file:

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

Launch PPM:

```bash
postgresql-partition-manager run all
```

## Features

- **Automatic provisioning** — create upcoming partitions ahead of time
- **Cleanup management** — delete or detach outdated partitions with configurable retention
- **Configuration checking** — verify partitions match expected configuration
- **Multiple intervals** — daily, weekly, monthly, quarterly, and yearly partitioning
- **Flexible partition keys** — `date`, `timestamp`, `timestamptz`, and `uuid` (UUIDv7) columns
- **Non-blocking** — safe operations with configurable lock and statement timeouts
- **Multiple deployment options** — Helm chart, Docker image, Debian package, Go install

## Installation

```bash
go install github.com/qonto/postgresql-partition-manager@latest
```

See [Installation](https://qonto.github.io/postgresql-partition-manager/latest/installation/) for other methods (Docker, Helm, Debian package).

## Usage

```bash
postgresql-partition-manager validate                    # Validate configuration file
postgresql-partition-manager run check                   # Check partitions match config
postgresql-partition-manager run provisioning            # Create future partitions
postgresql-partition-manager run cleanup                 # Remove outdated partitions
postgresql-partition-manager run all                     # Run provisioning + cleanup + check
```

See the [documentation](https://qonto.github.io/postgresql-partition-manager/latest/usage/) for the complete reference and configuration options.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

MIT. See [LICENSE](LICENSE).
