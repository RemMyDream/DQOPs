# OpenMetadata Deployment Guide

## Overview

OpenMetadata components added to your Helm chart:
- **PostgreSQL Metadata** - Separate database for OpenMetadata and Airflow
- **OpenMetadata Server** - Data catalog and metadata management
- **OpenMetadata Migration Job** - Database schema migration
- **OpenMetadata Ingestion** - Airflow-based metadata ingestion

## Architecture

```
┌─────────────────────────────────────────────────┐
│         OpenMetadata Server (Port 8585)         │
│      Data Catalog & Metadata Management         │
└──────────────┬──────────────────┬────────────────┘
               │                  │
               ▼                  ▼
┌──────────────────────┐  ┌────────────────────┐
│  PostgreSQL Metadata │  │   Elasticsearch    │
│  (Stores metadata)   │  │  (Search index)    │
│     Port: 5432       │  │    Port: 9200      │
└──────────────────────┘  └────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────┐
│    OpenMetadata Ingestion (Airflow)              │
│     Automated Metadata Collection                │
│              Port: 8080                          │
└──────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────┐
│         Your Data Sources                        │
│  (PostgreSQL, MinIO, Spark, Kafka, etc.)        │
└──────────────────────────────────────────────────┘
```

## Deployment Steps

### Step 1: Enable OpenMetadata in values.yaml

```bash
vim bdsp-pipeline/values.yaml
```

Change:
```yaml
openmetadata:
  enabled: true  # Enable OpenMetadata deployment
```

### Step 2: Deploy with Helm

```bash
# Deploy everything including OpenMetadata
./helm-deploy.sh deploy --set openmetadata.enabled=true

# Or create custom values file
cat > openmetadata-values.yaml << EOF
openmetadata:
  enabled: true
EOF

./helm-deploy.sh deploy -f openmetadata-values.yaml
```

### Step 3: Verify Deployment

```bash
# Check all pods
kubectl get pods -n dqops

# Expected OpenMetadata pods:
# - postgres-metadata-xxx
# - openmetadata-server-xxx
# - ingestion-xxx
# - openmetadata-migration-xxx (Job - will complete and show "Completed")

# Check services
kubectl get svc -n dqops | grep -E "metadata|ingestion"
```

### Step 4: Wait for Migration Job

The migration job must complete before the server starts properly:

```bash
# Watch migration job
kubectl get jobs -n dqops -w

# Check migration logs
kubectl logs -n dqops job/openmetadata-migration -f

# Wait for "Completed" status
```

### Step 5: Access OpenMetadata UI

```bash
# Port forward to access OpenMetadata UI
kubectl port-forward -n dqops svc/openmetadata-server 8585:8585

# Access in browser:
# http://localhost:8585

# Default credentials:
# Username: admin
# Password: admin
```

## Configuration

### PostgreSQL Metadata Settings

```yaml
openmetadata:
  postgresMetadata:
    enabled: true
    config:
      user: openmetadata_user
      password: openmetadata_password
      database: openmetadata_db
      airflowUser: airflow_user
      airflowPassword: airflow_pass
      airflowDatabase: airflow_db
    persistence:
      size: 10Gi  # Adjust based on needs
```

### OpenMetadata Server Settings

```yaml
openmetadata:
  server:
    enabled: true
    replicas: 1
    config:
      clusterName: "openmetadata"
      logLevel: "INFO"
      heapOpts: "-Xmx1G -Xms1G"
      authenticationProvider: "basic"  # or "google", "okta", etc.
    resources:
      requests:
        memory: "1Gi"
        cpu: "200m"
      limits:
        memory: "2Gi"
        cpu: "1000m"
```

### Ingestion (Airflow) Settings

```yaml
openmetadata:
  ingestion:
    enabled: true
    config:
      executor: "LocalExecutor"  # or "CeleryExecutor" for production
    persistence:
      dagAirflow:
        size: 5Gi
      dags:
        size: 5Gi
      tmp:
        size: 2Gi
    resources:
      requests:
        memory: "1Gi"
        cpu: "200m"
      limits:
        memory: "2Gi"
        cpu: "1000m"
```

## Using OpenMetadata

### 1. First Login

```bash
# Access OpenMetadata UI
kubectl port-forward -n dqops svc/openmetadata-server 8585:8585

# Browser: http://localhost:8585
# Login: admin / admin
```

### 2. Configure Data Sources

#### Add PostgreSQL Connection

1. Go to **Settings** → **Services** → **Databases** → **Add Database Service**
2. Select **PostgreSQL**
3. Configure:
   ```
   Name: postgres-source
   Host: postgres.dqops.svc.cluster.local
   Port: 5432
   Username: postgres
   Password: postgres
   Database: sourcedb
   ```

#### Add MinIO (S3) Connection

1. Go to **Settings** → **Services** → **Storage** → **Add Storage Service**
2. Select **S3**
3. Configure:
   ```
   Name: minio-storage
   Endpoint: http://minio.dqops.svc.cluster.local:9000
   Access Key: admin
   Secret Key: password
   ```

#### Add Kafka Connection

1. Go to **Settings** → **Services** → **Messaging** → **Add Messaging Service**
2. Select **Kafka**
3. Configure:
   ```
   Name: kafka-broker
   Bootstrap Servers: kafka-node-1.dqops.svc.cluster.local:9092
   Schema Registry: http://schema-registry.dqops.svc.cluster.local:8081
   ```

### 3. Create Ingestion Pipelines

After adding services, create ingestion pipelines to automatically collect metadata:

1. Go to your service (e.g., PostgreSQL)
2. Click **Ingestion** → **Add Ingestion**
3. Configure ingestion schedule
4. Save and deploy

### 4. Explore Your Data Assets

- **Tables**: Browse database tables with schema, columns, and lineage
- **Topics**: Kafka topics with schema and message details
- **Pipelines**: Spark and Airflow pipelines
- **Dashboards**: Analytics dashboards
- **ML Models**: Machine learning models

## Integration with Your Pipeline

### Data Lineage

OpenMetadata can track data lineage across your pipeline:

```
Raw Data (PostgreSQL) 
  → Bronze Layer (MinIO) 
  → Spark Processing 
  → Silver Layer (MinIO) 
  → Gold Layer (MinIO)
  → Analytics (Elasticsearch)
```

### API Integration

Use OpenMetadata API to programmatically access metadata:

```python
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)

server_config = OpenMetadataConnection(
    hostPort="http://openmetadata-server.dqops.svc.cluster.local:8585/api"
)
metadata = OpenMetadata(server_config)

# List all tables
tables = metadata.list_entities(entity=Table)
for table in tables.entities:
    print(f"Table: {table.name}")
```

## Deployment Scenarios

### Scenario 1: Development (Minimal)

```yaml
openmetadata:
  enabled: true
  server:
    replicas: 1
    resources:
      limits:
        memory: "1Gi"
  ingestion:
    enabled: false  # Disable Airflow for simple dev
  postgresMetadata:
    persistence:
      size: 5Gi
```

### Scenario 2: Production (Full)

```yaml
openmetadata:
  enabled: true
  server:
    replicas: 2  # High availability
    resources:
      requests:
        memory: "2Gi"
        cpu: "500m"
      limits:
        memory: "4Gi"
        cpu: "2000m"
  ingestion:
    enabled: true
    config:
      executor: "CeleryExecutor"  # Distributed execution
  postgresMetadata:
    persistence:
      size: 50Gi
```

### Scenario 3: Without Ingestion

If you only want the metadata server without Airflow ingestion:

```yaml
openmetadata:
  enabled: true
  server:
    enabled: true
  ingestion:
    enabled: false  # Disable Airflow
  migration:
    enabled: true
```

## Troubleshooting

### Migration Job Fails

```bash
# Check migration job logs
kubectl logs -n dqops job/openmetadata-migration

# Common issues:
# 1. PostgreSQL not ready - wait and rerun
# 2. Elasticsearch not ready - wait and rerun

# Delete and recreate job
kubectl delete job -n dqops openmetadata-migration
helm upgrade bdsp ./bdsp-pipeline
```

### Server Won't Start

```bash
# Check server logs
kubectl logs -n dqops -l app=openmetadata-server -f

# Check if migration completed
kubectl get jobs -n dqops

# Check PostgreSQL connection
kubectl exec -n dqops -it <openmetadata-server-pod> -- curl postgres-metadata:5432
```

### Ingestion Not Working

```bash
# Check Airflow logs
kubectl logs -n dqops -l app=ingestion -f

# Check Airflow web UI
kubectl port-forward -n dqops svc/ingestion 8080:8080
# Access: http://localhost:8080

# Check PostgreSQL Airflow database
kubectl exec -n dqops -it <postgres-metadata-pod> -- psql -U airflow_user -d airflow_db
```

### Can't Connect to Data Sources

Make sure all services are in the same namespace:
- PostgreSQL: `postgres.dqops.svc.cluster.local`
- MinIO: `minio.dqops.svc.cluster.local`
- Kafka: `kafka-node-1.dqops.svc.cluster.local`
- Elasticsearch: `elasticsearch.dqops.svc.cluster.local`

## Production Considerations

### 1. Security

```yaml
openmetadata:
  server:
    config:
      authenticationProvider: "google"  # Use OAuth
      # Configure JWT tokens
      # Enable SSL/TLS
```

### 2. Backup

```bash
# Backup PostgreSQL metadata
kubectl exec -n dqops postgres-metadata-xxx -- pg_dump -U openmetadata_user openmetadata_db > backup.sql

# Restore
kubectl exec -n dqops postgres-metadata-xxx -- psql -U openmetadata_user openmetadata_db < backup.sql
```

### 3. Monitoring

- Enable Prometheus metrics
- Monitor server health: `http://openmetadata-server:8586/healthcheck`
- Check resource usage: `kubectl top pods -n dqops`

### 4. Scaling

```yaml
openmetadata:
  server:
    replicas: 3  # Scale horizontally
  ingestion:
    config:
      executor: "CeleryExecutor"  # For distributed processing
```

## Disable OpenMetadata

If you want to deploy without OpenMetadata:

```bash
# Deploy without OpenMetadata
./helm-deploy.sh deploy --set openmetadata.enabled=false

# Or edit values.yaml
openmetadata:
  enabled: false
```

## Reference

- OpenMetadata Docs: https://docs.open-metadata.org/
- Configuration: `bdsp-pipeline/values.yaml` (openmetadata section)
- Templates: `bdsp-pipeline/templates/openmetadata.yaml`
- ConfigMaps: `bdsp-pipeline/templates/configmap.yaml`
- Original YAMLs: `k8s/openmetadata-*.yaml`, `k8s/postgres-metadata.yaml`

## Access URLs

After deployment with port-forwarding:

- **OpenMetadata UI**: http://localhost:8585
- **OpenMetadata API**: http://localhost:8585/api
- **Airflow UI**: http://localhost:8080 (if ingestion enabled)
- **Health Check**: http://localhost:8586/healthcheck

## Complete Deployment Example

```bash
# 1. Enable OpenMetadata
cat > my-values.yaml << EOF
# Base infrastructure
postgres:
  enabled: true
minio:
  enabled: true
spark:
  enabled: true
elasticsearch:
  enabled: true

# OpenMetadata stack
openmetadata:
  enabled: true
  
# Optional: Kafka
kafka:
  enabled: false
EOF

# 2. Deploy
./helm-deploy.sh deploy -f my-values.yaml

# 3. Wait for pods
kubectl get pods -n dqops -w

# 4. Access OpenMetadata
kubectl port-forward -n dqops svc/openmetadata-server 8585:8585

# 5. Open browser
# http://localhost:8585
# Login: admin / admin
```

Enjoy your data catalog! 🎉
