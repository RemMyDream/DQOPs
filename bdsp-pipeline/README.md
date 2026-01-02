# BDSP Pipeline Helm Chart

A Helm chart for deploying the BDSP Data Pipeline on Kubernetes.

## Components

This chart deploys the following components:

- **PostgreSQL**: Relational database for data storage
- **MinIO**: S3-compatible object storage for data lake (bronze/silver/gold layers)
- **Apache Spark**: Distributed computing framework (master + workers)
- **Elasticsearch**: Search and analytics engine

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+
- kubectl configured to access your cluster

## Installation

### Using default values

```bash
helm install bdsp ./bdsp-pipeline
```

### Using custom values file

```bash
# Development
helm install bdsp ./bdsp-pipeline -f values-dev.yaml

# Production
helm install bdsp ./bdsp-pipeline -f values-production.yaml
```

### Override specific values

```bash
helm install bdsp ./bdsp-pipeline \
  --set postgres.persistence.size=20Gi \
  --set spark.worker.replicas=3
```

### Using the deployment script

```bash
# Install/upgrade
./helm-deploy.sh deploy

# With custom values
./helm-deploy.sh deploy -f bdsp-pipeline/values-production.yaml

# Dry run
./helm-deploy.sh dry-run

# Check status
./helm-deploy.sh status

# Uninstall
./helm-deploy.sh uninstall
```

## Configuration

The following table lists the configurable parameters and their default values.

### Global Settings

| Parameter | Description | Default |
|-----------|-------------|---------|
| `namespace` | Kubernetes namespace | `dqops` |
| `global.storageClass` | Storage class for PVCs | `""` (default) |

### PostgreSQL

| Parameter | Description | Default |
|-----------|-------------|---------|
| `postgres.enabled` | Enable PostgreSQL | `true` |
| `postgres.image.repository` | Image repository | `postgres` |
| `postgres.image.tag` | Image tag | `15` |
| `postgres.service.port` | Service port | `5432` |
| `postgres.persistence.size` | PVC size | `10Gi` |
| `postgres.config.user` | Database user | `postgres` |
| `postgres.config.password` | Database password | `postgres` |
| `postgres.config.database` | Database name | `sourcedb` |

### MinIO

| Parameter | Description | Default |
|-----------|-------------|---------|
| `minio.enabled` | Enable MinIO | `true` |
| `minio.image.repository` | Image repository | `minio/minio` |
| `minio.service.apiPort` | API port | `9000` |
| `minio.service.consolePort` | Console port | `9001` |
| `minio.persistence.size` | PVC size | `20Gi` |
| `minio.config.rootUser` | Root user | `admin` |
| `minio.config.rootPassword` | Root password | `password` |
| `minio.buckets` | Buckets to create | `[bronze, silver, gold]` |

### Spark

| Parameter | Description | Default |
|-----------|-------------|---------|
| `spark.enabled` | Enable Spark | `true` |
| `spark.master.image.repository` | Image repository | `apache/spark` |
| `spark.master.image.tag` | Image tag | `3.5.3` |
| `spark.master.replicas` | Master replicas | `1` |
| `spark.worker.replicas` | Worker replicas | `2` |
| `spark.worker.env.sparkWorkerCores` | Worker cores | `2` |
| `spark.worker.env.sparkWorkerMemory` | Worker memory | `2g` |

### Elasticsearch

| Parameter | Description | Default |
|-----------|-------------|---------|
| `elasticsearch.enabled` | Enable Elasticsearch | `true` |
| `elasticsearch.image.tag` | Image tag | `8.11.4` |
| `elasticsearch.service.httpPort` | HTTP port | `9200` |
| `elasticsearch.persistence.size` | PVC size | `10Gi` |
| `elasticsearch.config.javaOpts` | Java options | `-Xms1024m -Xmx1024m` |

See [values.yaml](values.yaml) for complete list of parameters.

## Accessing Services

### Port Forwarding

```bash
# PostgreSQL
kubectl port-forward -n dqops svc/postgres 5432:5432

# MinIO Console
kubectl port-forward -n dqops svc/minio 9001:9001

# Spark Master UI
kubectl port-forward -n dqops svc/spark-master 8080:8080

# Elasticsearch
kubectl port-forward -n dqops svc/elasticsearch 9200:9200
```

### Inside the Cluster

Services are accessible via DNS:

- PostgreSQL: `postgres.dqops.svc.cluster.local:5432`
- MinIO: `minio.dqops.svc.cluster.local:9000`
- Spark Master: `spark://spark-master.dqops.svc.cluster.local:7077`
- Elasticsearch: `elasticsearch.dqops.svc.cluster.local:9200`

## Upgrading

```bash
helm upgrade bdsp ./bdsp-pipeline
```

With new values:

```bash
helm upgrade bdsp ./bdsp-pipeline -f new-values.yaml
```

## Uninstalling

```bash
helm uninstall bdsp
kubectl delete namespace dqops
```

Or using the script:

```bash
./helm-deploy.sh uninstall
```

## Testing

Validate the chart before deploying:

```bash
# Lint
helm lint ./bdsp-pipeline

# Dry run
helm install bdsp ./bdsp-pipeline --dry-run --debug

# Template rendering
helm template bdsp ./bdsp-pipeline
```

## Troubleshooting

### Check pod status
```bash
kubectl get pods -n dqops
kubectl describe pod <pod-name> -n dqops
kubectl logs <pod-name> -n dqops
```

### Check services
```bash
kubectl get svc -n dqops
```

### Check PVCs
```bash
kubectl get pvc -n dqops
```

### View Helm release
```bash
helm status bdsp
helm get values bdsp
helm get manifest bdsp
```

## Examples

### Minimal installation (only PostgreSQL and MinIO)

```yaml
# minimal-values.yaml
postgres:
  enabled: true

minio:
  enabled: true

spark:
  enabled: false

elasticsearch:
  enabled: false
```

```bash
helm install bdsp ./bdsp-pipeline -f minimal-values.yaml
```

### Production with larger resources

```bash
helm install bdsp ./bdsp-pipeline -f values-production.yaml
```

## License

This chart is provided as-is for the BDSP project.
