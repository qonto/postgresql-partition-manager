# PostgreSQL Partition Manager

**PostgreSQL Partition Manager**, or PPM, is an *opinionated tool* designed to streamline the management of PostgreSQL partitions.

PPM operates on [PostgreSQL's Declarative partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITIONING-DECLARATIVE) and does not require any specific PostgreSQL extensions.

> [!TIP]
> The key objective of PPM is to simplify the use of PostgreSQL partitions for developers. By providing a secure, non-blocking, and intuitive tool for partition management
>
> Developers can manage table schema and indexes using existing tools and ORMs, and leverate on PPM to implement `date` and [`UUIDv7`](https://datatracker.ietf.org/doc/html/draft-ietf-uuidrev-rfc4122bis#name-uuid-version-7) column based partitioning for scaling PostgreSQL systems.

PPM will process all referenced partitions and exit with a non-zero code if it detects an anomaly in at least one partition. This process must be closely monitored to trigger manual intervention, if necessary, to avoid system downtime.

**Features**:

- Creation of upcoming partitions
- Delete (or detach) outdated partitions
- Check partitions configuration

**Opinionated limitations**:

- Support of PostgreSQL 14+
- Only supports [`RANGE` partition strategy](https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITIONING-OVERVIEW-RANGE)
- The partition key must be a column of `date`, `timestamp`, or `uuid` type
- Support `daily`, `weekly`, `monthly`, and `yearly` partitioning
- Dates are implemented through UTC timezone
- Partition names are enforced and not configurable

  | Partition interval | Pattern                           | Example           |
  | ------------------ | --------------------------------- | ----------------- |
  | daily              | `<parent_table>_<YYYY>_<DD>_<MM>` | `logs_2024_06_25` |
  | weekly             | `<parent_table>_w<week number>`   | `logs_2024_w26`   |
  | monthly            | `<parent_table>_<YYYY>_<MM>`      | `logs_2024_06`    |
  | yearly             | `<parent_table>_<YYYY>`           | `logs_2024`       |

## Usage

The primary commands include:

- `validate`: Validates the partition configuration file
- `run check`: Checks if partitions match the expected configuration. This is useful for detecting incorrect partitions
- `run provisioning`: Creates future partitions
- `run cleanup`: Removes (or detach) outdated partitions
- `run all`: Execute provisioning, cleanup, and check commands sequentially

For a complete list of available commands, use `postgresql-partition-manager --help`.

> [!TIP]
> We recommend to execute `postgresql-partition-manager run all` every day (e.g., CRON job) with a minimum of 3 pre provisioned partitions (7 for daily partitioning). And raise alerts on non-zero exit code.

## Configuration

Configuration could be defined in `postgresql-partition-manager.yaml` or environment variables (format `POSTGRESQL_PARTITION_MANAGER_<PARAMETER_NAME>`).

| Parameter         | Description                                          | Default |
| ----------------- | ---------------------------------------------------- | ------- |
| connection-url    | PostgreSQL connection URL                            |         |
| debug             | Enable debug mode                                    | false   |
| log-format        | Log format (text or json)                            | json    |
| lock-timeout      | Maximum allowed duration of any wait for a lock (ms) | 300     |
| statement-timeout | Maximum allowed duration of any statement (ms)       | 3000    |
| partitions        | Map of `<partition>`                                 |         |

Partition object:

| Parameter      | Description                                          | Default |
| -------------- | ---------------------------------------------------- | ------- |
| column         | Column used for partitioning                         |         |
| interval       | Partitioning interval (`daily`, `weekly`, `monthly` or `yearly`) |         |
| preProvisioned | Number of partitions to create in advance            |         |
| retention      | Number of partitions to retain                       |         |
| schema         | PostgreSQL schema                                    |         |
| table          | Table to be partitioned                              |         |
| cleanupPolicy  | `detach` refers to detaching only the partition while `drop` refers to both detaching and dropping it |         |

See [full configuration](configs/postgresql-partition-manager/postgresql-partition-manager.yaml).

## Installation

PPM is available as a Docker image, Debian package, and Binary.

<details>
<summary>Docker image</summary>

1. Generate configuration file in `postgresql-partition-manager.yaml` from the docker image

    ```bash
    docker run public.ecr.aws/qonto/postgresql-partition-manager:latest -- cat postgresql-partition-manager.yaml
    ```

1. Launch PPM with a configuration file

    ```bash
    docker run -v ./postgresql-partition-manager.yaml:/app/postgresql-partition-manager.yaml public.ecr.aws/qonto/postgresql-partition-manager:latest
    ```

</details>

<details>
<summary>Debian/Ubuntu</summary>

1. Download the Debian package

    ```bash
    POSTGRESQL_PARTITION_MANAGER_VERSION=0.1.0 # Replace with latest version

    PACKAGE_NAME=postgresql_partition_manager_${POSTGRESQL_PARTITION_MANAGER_VERSION}_$(uname -m).deb
    wget https://github.com/qonto/postgresql-partition-manager/releases/download/${PROMETHEUS_RDS_EXPORTER_VERSION}/${PACKAGE_NAME}
    ```

1. Install package

    ```bash
    dpkg -i ${PACKAGE_NAME}
    ```

1. Customize configuration

    Copy configuration file template

    ```bash
    cp /usr/share/postgresql-partition-manager/postgresql-partition-manager.yaml.sample postgresql-partition-manager.yaml
    ```

    Edit database connection parameter and partition configuration

    ```bash
    vim postgresql-partition-manager.yaml
    ```

</details>

<details>
<summary>Go</summary>

1. PPM could be installed from Go install

    ```bash
    go install github.com/qonto/postgresql-partition-manager@latest
    ```

</details>

## Local development

1. Install requirements

    - [Golang 1.21](https://go.dev/doc/install)

    Optionals:

    - [Orbstack](https://orbstack.dev/) (recommended) or Docker

1. Setup PostgreSQL

    Via docker containers:

    ```bash
    cd scripts/localdev/
    export POSTGRESQL_VERSION=16 # Optional. Override PostgreSQL version
    docker compose up -d postgres
    ```

    Or manually:

    ```sql
    \i scripts/localdev/configuration/postgresql/seeds/00_database.sql
    \i scripts/localdev/configuration/postgresql/seeds/10_by_date.sql
    \i scripts/localdev/configuration/postgresql/seeds/10_by_timestamp.sql
    \i scripts/localdev/configuration/postgresql/seeds/10_by_uuidv7.sql
    ```

1. Build application from the root directory

    ```bash
    make build
    ```

1. Launch PostgreSQL Partition Manager

    ```bash
    ./postgresql-partition-manager
    ```

1. Optional. Create configuration file

    ```bash
    cat > postgresql-partition-manager.yaml << EOF
    ---
    debug: true

    log-format: text

    connection-url: postgres://postgres:hackme@localhost/partitions

    partitions:
      by_date:
        schema: public
        table: by_date
        partitionKey: created_at
        interval: yearly
        retention: 7
        preProvisioned: 7
        cleanupPolicy: drop
      by_timestamp:
        schema: public
        table: by_timestamp
        partitionKey: created_at
        interval: daily
        retention: 7
        preProvisioned: 7
        cleanupPolicy: drop
      by_uuidv7:
        schema: public
        table: by_uuidv7
        partitionKey: id
        interval: monthly
        retention: 3
        preProvisioned: 1
        cleanupPolicy: drop
    EOF
    ```

    Run provisioning script to perform provisioning, clean up, and check operations

    ```bash
    ./postgresql-partition-manager run all
    ```
