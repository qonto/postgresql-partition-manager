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
- Support `daily`, `weekly`, `monthly`, `quarterly`, and `yearly` partitioning
- Dates are implemented through UTC timezone
- Partition names are enforced and not configurable

  | Partition interval | Pattern                                   | Example           |
  | ------------------ | ----------------------------------------- | ----------------- |
  | daily              | `<parent_table>_<YYYY>_<DD>_<MM>`         | `logs_2024_06_25` |
  | weekly             | `<parent_table>_w<week number>`           | `logs_2024_w26`   |
  | quarterly          | `<parent_table>_<YYYY>_q<quarter number>` | `logs_2024_q1`   |
  | monthly            | `<parent_table>_<YYYY>_<MM>`              | `logs_2024_06`    |
  | yearly             | `<parent_table>_<YYYY>`                   | `logs_2024`       |

## Installation

PPM is available as a Docker image, Debian package, and Binary.

<details>
<summary>Helm</summary>

1. Create a [Kubernetes secret](https://kubernetes.io/docs/concepts/configuration/secret) containing PostgreSQL password

    While database credentials could be passed in `connection-url` configuration parameter or `PGUSER` and `PGPASSWORD` environment variables.

    We recommend storing credentials in Kubernetes secret and referring to the secret via the `cronjob.postgresqlPasswordSecret` Helm chart parameter.

    Example of creating secret using `kubectl`:

    ```bash
    kubectl create secret generic postgresql-credentials --from-literal=password=replace_with_your_postgresql_password
    ```

    We recommend the [Kubernetes Secrets Store CSI driver](https://secrets-store-csi-driver.sigs.k8s.io) for production deployment.

    The `cronjob.postgresqlUserSecret` parameter could be used to pass PostgreSQL user, but don't recommend to store username as a secret because it makes audits more difficult and significantly increases security.

1. Create a configuration file

    Copy the following template:

    ```bash
    cat > values.yaml << EOF
    cronjob:
      postgresqlPasswordSecret:
        ref: postgresql-credentials # Specify the Kubernetes secret name containing the PostgreSQL credentials
        key: password # Specify the key containing the password

    configuration:
      debug: false

      connection-url: postgres://my_username@postgres/my_app # TODO replace with your database connection parameters

      partitions:
        #my_partition:
        #  schema: public
        #  table: logs
        #  partitionKey: created_at
        #  interval: daily
        #  retention: 30
        #  preProvisioned: 7
        #  cleanupPolicy: drop
    EOF
    ```

    Edit partitioning settings in `partitions`:

    ```bash
    vim values.yaml
    ```

1. Deploy the chart

    Set PPM version to deploy and Kubernetes target namespace:

    ```bash
    POSTGRESQL_PARTION_MANAGER=0.1.0 # Replace with latest version
    KUBERNETES_NAMESPACE=default # Replace with your namespace
    HELM_RELEASE_NAME=main # Replace with an helm release
    ```

    Then deploy it:

    ```bash
    helm upgrade \
    ${HELM_RELEASE_NAME} \
    oci://public.ecr.aws/qonto/postgresql-partition-manager-chart \
    --version ${POSTGRESQL_PARTION_MANAGER} \
    --install \
    --namespace ${KUBERNETES_NAMESPACE} \
    --values values.yaml
    ```

1. Trigger job manually and verify application logs

    Set a Kubernetes job name:

    ```bash
    MANUAL_JOB=ppm-manually-triggered
    ```

    Trigger job manually:

    ```bash
    kubectl create job --namespace ${KUBERNETES_NAMESPACE} --from=cronjob/${HELM_RELEASE_NAME}-postgresql-partition-manager-chart ${MANUAL_JOB}
    ```

    Check cronjob execution:

    ```bash
    kubectl describe job --namespace ${KUBERNETES_NAMESPACE} ${MANUAL_JOB}
    ```

    Check application logs

    ```bash
    kubectl logs --namespace ${KUBERNETES_NAMESPACE} --selector=job-name=${MANUAL_JOB}
    ```

    Clean up manual job

    ```bash
    kubectl delete job --namespace ${KUBERNETES_NAMESPACE} ${MANUAL_JOB}
    ```

</details>

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
    wget https://github.com/qonto/postgresql-partition-manager/releases/download/${POSTGRESQL_PARTION_MANAGER}/${PACKAGE_NAME}
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
| interval       | Partitioning interval (`daily`, `weekly`, `monthly`, `quarterly` or `yearly`) |         |
| preProvisioned | Number of partitions to create in advance            |         |
| retention      | Number of partitions to retain                       |         |
| schema         | PostgreSQL schema                                    |         |
| table          | Table to be partitioned                              |         |
| cleanupPolicy  | `detach` refers to detaching only the partition while `drop` refers to both detaching and dropping it |         |

See the [full configuration file](configs/postgresql-partition-manager/postgresql-partition-manager.yaml).

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

See [Contributing](CONTRIBUTING.md)

## License

MIT
