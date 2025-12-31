# PowerShell deployment script for Windows
# DQOPs Kubernetes Deployment Script

$ErrorActionPreference = "Stop"

Write-Host "=== DQOPs Kubernetes Deployment Script ===" -ForegroundColor Cyan
Write-Host ""

# Check if kubectl is installed
try {
    kubectl version --client | Out-Null
} catch {
    Write-Host "Error: kubectl is not installed" -ForegroundColor Red
    Write-Host "Install kubectl from: https://kubernetes.io/docs/tasks/tools/install-kubectl-windows/" -ForegroundColor Yellow
    exit 1
}

# Check if kind is installed
try {
    kind version | Out-Null
} catch {
    Write-Host "Error: kind is not installed" -ForegroundColor Red
    Write-Host "Install KinD from: https://kind.sigs.k8s.io/docs/user/quick-start/#installation" -ForegroundColor Yellow
    exit 1
}

# Check if kind cluster exists
$CLUSTER_NAME = "dqops-cluster"
$existingClusters = kind get clusters 2>&1
if ($existingClusters -notmatch $CLUSTER_NAME) {
    Write-Host "Creating KinD cluster: $CLUSTER_NAME" -ForegroundColor Yellow
    if (Test-Path "kind-config.yaml") {
        kind create cluster --name $CLUSTER_NAME --config kind-config.yaml
    } else {
        kind create cluster --name $CLUSTER_NAME
    }
    Write-Host "KinD cluster created successfully" -ForegroundColor Green
} else {
    Write-Host "KinD cluster $CLUSTER_NAME already exists" -ForegroundColor Yellow
}

# Set kubectl context
kubectl config use-context "kind-$CLUSTER_NAME"

Write-Host ""
Write-Host "Applying Kubernetes manifests..." -ForegroundColor Yellow

# Apply manifests in order
Write-Host "1. Creating namespace..." -ForegroundColor Cyan
kubectl apply -f namespace.yaml

Write-Host "2. Creating ConfigMaps and Secrets..." -ForegroundColor Cyan
kubectl apply -f configmaps.yaml
kubectl apply -f secrets.yaml

Write-Host "3. Deploying PostgreSQL (source)..." -ForegroundColor Cyan
kubectl apply -f postgres.yaml

Write-Host "4. Deploying PostgreSQL Metadata..." -ForegroundColor Cyan
kubectl apply -f postgres-metadata.yaml

Write-Host "5. Deploying Elasticsearch..." -ForegroundColor Cyan
kubectl apply -f elasticsearch.yaml

Write-Host "6. Deploying MinIO..." -ForegroundColor Cyan
kubectl apply -f minio.yaml

Write-Host "7. Deploying Spark cluster..." -ForegroundColor Cyan
kubectl apply -f spark.yaml

Write-Host "8. Waiting for databases to be ready..." -ForegroundColor Cyan
kubectl wait --for=condition=ready pod -l app=postgres-metadata -n dqops --timeout=300s 2>&1 | Out-Null
kubectl wait --for=condition=ready pod -l app=elasticsearch -n dqops --timeout=300s 2>&1 | Out-Null

Write-Host "9. Setting up Airflow user..." -ForegroundColor Cyan
kubectl apply -f setup-airflow-user.yaml
kubectl wait --for=condition=complete job/setup-airflow-user -n dqops --timeout=120s 2>&1 | Out-Null

Write-Host "10. Running OpenMetadata migration..." -ForegroundColor Cyan
kubectl apply -f openmetadata-migration.yaml
kubectl wait --for=condition=complete job/openmetadata-migration -n dqops --timeout=300s 2>&1 | Out-Null

Write-Host "11. Deploying OpenMetadata server..." -ForegroundColor Cyan
kubectl apply -f openmetadata-server.yaml

Write-Host "12. Deploying OpenMetadata ingestion..." -ForegroundColor Cyan
kubectl apply -f openmetadata-ingestion.yaml

Write-Host ""
Write-Host "=== Deployment Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Waiting for all pods to be ready..." -ForegroundColor Yellow
kubectl wait --for=condition=ready pod --all -n dqops --timeout=600s 2>&1 | Out-Null

Write-Host ""
Write-Host "All services are being deployed. Check status with:" -ForegroundColor Green
Write-Host "  kubectl get pods -n dqops"
Write-Host ""
Write-Host "To access services via port-forward:" -ForegroundColor Green
Write-Host "  # OpenMetadata Server"
Write-Host "  kubectl port-forward -n dqops svc/openmetadata-server 8585:8585"
Write-Host ""
Write-Host "  # MinIO Console"
Write-Host "  kubectl port-forward -n dqops svc/minio 9001:9001"
Write-Host ""
Write-Host "  # Spark Master Web UI"
Write-Host "  kubectl port-forward -n dqops svc/spark-master 8080:8080"
Write-Host ""
Write-Host "To view logs:" -ForegroundColor Green
Write-Host "  kubectl logs -f -n dqops <pod-name>"
Write-Host ""
Write-Host "To delete the cluster:" -ForegroundColor Green
Write-Host "  kind delete cluster --name $CLUSTER_NAME"

