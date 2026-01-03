# DQOPs Kubernetes Deployment with KinD

This directory contains Kubernetes manifests and scripts to deploy the DQOPs data pipeline on Kubernetes using KinD (Kubernetes in Docker).

## Prerequisites

1. **Docker** - KinD requires Docker to be installed and running
2. **KinD (Kubernetes in Docker)** - Install from https://kind.sigs.k8s.io/docs/user/quick-start/#installation
3. **kubectl** - Kubernetes command-line tool

### Installing Prerequisites

#### Install KinD (Windows)
```powershell
# Using Chocolatey
choco install kind

# Or using Go (if you have Go installed)
go install sigs.k8s.io/kind@latest
```

#### Install kubectl (Windows)
```powershell
# Using Chocolatey
choco install kubernetes-cli

# Or download from: https://kubernetes.io/docs/tasks/tools/install-kubectl-windows/
```

## Quick Start

### 1. Create and Deploy to KinD Cluster

```bash
#change docker config
#step 1 run admin
.\configure_wsl.ps1
#step 2
wsl --shutdown
#step 3
# Restart docker desktop

#deploy for window powershell
.\deploy_final.ps1
```

If error because of Execution_Policies when run on window, run powershell as Admin and try
```bash
Set-ExecutionPolicy RemoteSigned
```

### 2. Check Deployment Status

```bash
# View all pods in the dqops namespace
kubectl get pods -n dqops

# View services
kubectl get svc -n dqops

# View persistent volume claims
kubectl get pvc -n dqops
```

### 3. Access Services

#### Port Forwarding

```bash
# OpenMetadata Server (default: http://localhost:8585)
kubectl port-forward -n dqops svc/openmetadata-server 8585:8585

# MinIO Console (default: http://localhost:9001)
# Login: admin / password
kubectl port-forward -n dqops svc/minio 9001:9001

# MinIO API (default: http://localhost:9000)
kubectl port-forward -n dqops svc/minio 9000:9000

# Spark Master Web UI (default: http://localhost:8080)
kubectl port-forward -n dqops svc/spark-master 8080:8080

# Spark Worker 1 Web UI (default: http://localhost:8081)
kubectl port-forward -n dqops svc/spark-workers 8081:8081

# Elasticsearch (default: http://localhost:9200)
kubectl port-forward -n dqops svc/elasticsearch 9200:9200

# PostgreSQL (source) - for testing
kubectl port-forward -n dqops svc/postgres 5432:5432

# OpenMetadata Ingestion (Airflow) - default: http://localhost:8080
kubectl port-forward -n dqops svc/ingestion 8090:8080
```

#### View Logs

```bash
# View logs for a specific pod
kubectl logs -f -n dqops <pod-name>

# View logs for all pods in a deployment
kubectl logs -f -n dqops deployment/<deployment-name>

# View logs for a job
kubectl logs -n dqops job/openmetadata-migration
kubectl logs -n dqops job/setup-airflow-user
kubectl logs -n dqops job/minio-init
```

### 4. Clean Up

```bash
# Delete the entire KinD cluster
./undeploy.sh

# Or manually delete the cluster
kind delete cluster --name dqops-cluster
```

## Deployment Architecture

The deployment includes the following components:

### Core Services

1. **PostgreSQL (Source)** - Source database
   - Service: `postgres:5432`
   - PVC: `postgres-pvc`

2. **MinIO** - Object storage for data lake
   - API: `minio:9000`
   - Console: `minio:9001`
   - PVC: `minio-pvc`
   - Buckets initialized: `warehouse/`

3. **Spark Cluster**
   - Master: `spark-master:7077` (Web UI: 8080)
   - Workers: 2 workers (Web UI: 8081)
   - Connected to: `spark://spark-master:7077`

4. **PostgreSQL Metadata** - OpenMetadata database
   - Service: `postgres-metadata:5432`
   - PVC: `postgres-metadata-pvc`
   - Database: `openmetadata_db`

5. **Elasticsearch** - OpenMetadata search engine
   - Service: `elasticsearch:9200`
   - PVC: `elasticsearch-pvc`

6. **OpenMetadata Server**
   - Service: `openmetadata-server:8585` (API: 8585, Admin: 8586)
   - Depends on: PostgreSQL Metadata, Elasticsearch, Migration Job

7. **OpenMetadata Ingestion (Airflow)**
   - Service: `ingestion:8080`
   - PVCs: `ingestion-dag-airflow-pvc`, `ingestion-dags-pvc`, `ingestion-tmp-pvc`
   - Database: Uses `airflow_db` in PostgreSQL Metadata

### Initialization Jobs

1. **minio-init** - Creates MinIO buckets
2. **setup-airflow-user** - Sets up Airflow database and user
3. **openmetadata-migration** - Runs OpenMetadata database migrations

## Deployment Order

The `deploy.sh` script applies manifests in the correct order:

1. Namespace creation
2. ConfigMaps and Secrets
3. PostgreSQL (source)
4. PostgreSQL Metadata
5. Elasticsearch
6. MinIO
7. Spark Cluster
8. Wait for databases
9. Setup Airflow user (Job)
10. OpenMetadata migration (Job)
11. OpenMetadata server
12. OpenMetadata ingestion

## Configuration

### Environment Variables

All configuration is managed through ConfigMaps and Secrets in `configmaps.yaml` and `secrets.yaml`.

### Persistent Storage

All stateful services use PersistentVolumeClaims:
- `postgres-pvc` (10Gi)
- `postgres-metadata-pvc` (10Gi)
- `minio-pvc` (20Gi)
- `elasticsearch-pvc` (10Gi)
- `ingestion-dag-airflow-pvc` (5Gi)
- `ingestion-dags-pvc` (5Gi)
- `ingestion-tmp-pvc` (2Gi)

Note: In KinD, storage is ephemeral by default. For persistent storage across cluster restarts, you may need to configure storage classes or use hostPath volumes.

## Troubleshooting

### Pods not starting

```bash
# Check pod status
kubectl describe pod <pod-name> -n dqops

# Check events
kubectl get events -n dqops --sort-by='.lastTimestamp'
```

### Jobs failing

```bash
# Check job status
kubectl describe job <job-name> -n dqops

# View job logs
kubectl logs -n dqops job/<job-name>
```

### Database connection issues

```bash
# Check if PostgreSQL is ready
kubectl exec -n dqops deployment/postgres-metadata -- pg_isready -U openmetadata_user

# Test connection
kubectl exec -it -n dqops deployment/postgres-metadata -- psql -U openmetadata_user -d openmetadata_db
```

### View resource usage

```bash
# View resource usage
kubectl top pods -n dqops
kubectl top nodes
```

### Restart a deployment

```bash
# Restart a deployment
kubectl rollout restart deployment/<deployment-name> -n dqops
```

## Manual Deployment

If you prefer to deploy manually:

```bash
# 1. Create namespace
kubectl apply -f namespace.yaml

# 2. Create ConfigMaps and Secrets
kubectl apply -f configmaps.yaml
kubectl apply -f secrets.yaml

# 3. Deploy infrastructure
kubectl apply -f postgres.yaml
kubectl apply -f postgres-metadata.yaml
kubectl apply -f elasticsearch.yaml
kubectl apply -f minio.yaml
kubectl apply -f spark.yaml

# 4. Wait for databases to be ready
kubectl wait --for=condition=ready pod -l app=postgres-metadata -n dqops --timeout=300s
kubectl wait --for=condition=ready pod -l app=elasticsearch -n dqops --timeout=300s

# 5. Setup and migrate
kubectl apply -f setup-airflow-user.yaml
kubectl wait --for=condition=complete job/setup-airflow-user -n dqops --timeout=120s
kubectl apply -f openmetadata-migration.yaml
kubectl wait --for=condition=complete job/openmetadata-migration -n dqops --timeout=300s

# 6. Deploy OpenMetadata
kubectl apply -f openmetadata-server.yaml
kubectl apply -f openmetadata-ingestion.yaml
```

## Notes

- **KinD Limitations**: KinD is designed for local development and testing. For production, use a proper Kubernetes cluster (GKE, EKS, AKS, etc.)
- **Resource Requirements**: Ensure Docker has enough resources allocated (recommended: 8GB RAM, 4 CPU cores)
- **Storage**: Data persists as long as the KinD cluster exists. Deleting the cluster will delete all data
- **Networking**: Services are accessible only within the cluster. Use port-forwarding for local access
- **Image Pulling**: First deployment may take time as images are downloaded

## Next Steps

1. Configure ingress for external access (optional)
2. Set up monitoring and logging
3. Configure backup strategies for persistent data
4. Set up CI/CD pipelines for deployment
5. Configure resource limits and requests based on workload

