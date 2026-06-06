# Installation

PPM is available as a Go binary, Docker image, Debian package, and Helm chart for Kubernetes deployments. Choose the method that best fits your infrastructure.

## Go Install

Install the latest version directly using Go:

```bash
go install github.com/qonto/postgresql-partition-manager@latest
```

## Docker

### Authenticate to AWS ECR Public

Before pulling the image, authenticate to the [AWS ECR Public registry](https://docs.aws.amazon.com/AmazonECR/latest/public/public-registry-auth.html):

```bash
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
```

### Pull the Image

```bash
docker pull public.ecr.aws/qonto/postgresql-partition-manager:latest
```

### Generate a Configuration File

Create a `postgresql-partition-manager.yaml` file in your working directory. See [Configuration](configuration.md) for the full reference.

### Run with a Configuration File

Mount your configuration file and run PPM:

```bash
docker run -v ./postgresql-partition-manager.yaml:/app/postgresql-partition-manager.yaml \
  public.ecr.aws/qonto/postgresql-partition-manager:latest
```

## Debian/Ubuntu Package

### Download and Install

```bash
POSTGRESQL_PARTITION_MANAGER_VERSION=0.1.0  # Replace with latest version

PACKAGE_NAME=postgresql-partition-manager_${POSTGRESQL_PARTITION_MANAGER_VERSION}_$(uname -m).deb
wget https://github.com/qonto/postgresql-partition-manager/releases/download/${POSTGRESQL_PARTITION_MANAGER_VERSION}/${PACKAGE_NAME}
dpkg -i ${PACKAGE_NAME}
```

### Configure

Copy the sample configuration file and customize it:

```bash
cp /usr/share/postgresql-partition-manager/postgresql-partition-manager.yaml.sample postgresql-partition-manager.yaml
vim postgresql-partition-manager.yaml
```

## Helm Chart (Kubernetes)

### 1. Create a Kubernetes Secret

Store your PostgreSQL password in a Kubernetes secret:

```bash
kubectl create secret generic postgresql-credentials \
  --from-literal=password=replace_with_your_postgresql_password
```

For production, consider using the [Kubernetes Secrets Store CSI driver](https://secrets-store-csi-driver.sigs.k8s.io).

### 2. Create a Values File

Copy and edit the Helm charts values:

```bash
cat > values.yaml << EOF
cronjob:
  postgresqlPasswordSecret:
    ref: postgresql-credentials
    key: password

configuration:
  debug: false
  connection-url: postgres://my_username@postgres/my_app

  partitions:
    my_partition:
      schema: public
      table: logs
      partitionKey: created_at
      interval: daily
      retention: 30
      preProvisioned: 7
      cleanupPolicy: drop
EOF

vim values.yaml
```

PPM configuration is located under `configuration` key. See [Configuration](configuration.md) for the full reference.


### 3. Deploy the Chart

```bash
POSTGRESQL_PARTITION_MANAGER_VERSION=0.1.0  # Replace with latest version
KUBERNETES_NAMESPACE=default # Replace with your namespace
HELM_RELEASE_NAME=main # Replace with an helm release

helm upgrade \
  ${HELM_RELEASE_NAME} \
  oci://public.ecr.aws/qonto/postgresql-partition-manager-chart \
  --version ${POSTGRESQL_PARTITION_MANAGER_VERSION} \
  --install \
  --namespace ${KUBERNETES_NAMESPACE} \
  --values values.yaml
```

### 4. Verify Deployment

Trigger a manual job to verify everything works:

```bash
MANUAL_JOB_NAME=ppm-manually-triggered

kubectl create job \
  --namespace ${KUBERNETES_NAMESPACE} \
  --from=cronjob/${HELM_RELEASE_NAME}-postgresql-partition-manager-chart \
  ${MANUAL_JOB_NAME}

kubectl logs --namespace ${KUBERNETES_NAMESPACE} --selector=job-name=${MANUAL_JOB_NAME}
```

Clean up the manual job:

```bash
kubectl delete job --namespace ${KUBERNETES_NAMESPACE} ${MANUAL_JOB_NAME}
```
