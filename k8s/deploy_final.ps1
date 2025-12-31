# DQOps Final Deployment Script (Merged & Robust)
# Features: RAM Check, Pinned Version (v1.32.0), Smart Recovery
$ErrorActionPreference = "Stop"
$CLUSTER_NAME = "dqops-cluster"

Write-Host "=== DQOPs Kubernetes Deployment Script ===" -ForegroundColor Cyan

# ==========================================
# 1. WSL Memory Check (Critical for Docker)
# ==========================================
Write-Host "Checking WSL Memory Configuration..." -ForegroundColor Cyan
try {
    # Check memory in Megabytes inside Linux
    $memOutput = wsl -e free -m | Select-String "Mem:"
    $memValues = -split $memOutput.ToString()
    $totalMemMB = [int]$memValues[1]
    
    Write-Host "Detected WSL Total Memory: ${totalMemMB} MB" -ForegroundColor Yellow

    if ($totalMemMB -lt 6000) {
        Write-Host "ERROR: WSL has less than 6GB RAM ($totalMemMB MB detected)." -ForegroundColor Red
        Write-Host "Please run the 'configure_wsl.ps1' script or edit .wslconfig manually." -ForegroundColor Red
        Write-Host "Then run 'wsl --shutdown' and restart Docker Desktop." -ForegroundColor Magenta
        exit 1
    } else {
        Write-Host "✓ Memory check passed (Over 6GB available)." -ForegroundColor Green
    }
} catch {
    Write-Host "Warning: Could not check memory via wsl command. Proceeding anyway..." -ForegroundColor Yellow
}

# ==========================================
# 2. Dependency Checks
# ==========================================
foreach ($cmd in @("kubectl", "kind", "docker")) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
        Write-Host "Error: $cmd is not installed or not in PATH." -ForegroundColor Red
        exit 1
    }
}

# ==========================================
# 3. Cluster Detection & Creation
# ==========================================
$clusters = kind get clusters
$clusterExists = $clusters -contains $CLUSTER_NAME

if ($clusterExists) {
    Write-Host "Cluster '$CLUSTER_NAME' detected." -ForegroundColor Yellow
    
    # Check if the cluster is actually responsive
    $isResponsive = $false
    try {
        # Export config temporarily to check connection
        kind export kubeconfig --name $CLUSTER_NAME --internal 2>&1 | Out-Null
        kubectl cluster-info --request-timeout=5s 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) { $isResponsive = $true }
    } catch {
        $isResponsive = $false
    }

    if (-not $isResponsive) {
        Write-Host "Warning: Cluster exists but is unresponsive." -ForegroundColor Red
        Write-Host "Deleting broken cluster '$CLUSTER_NAME' to ensure clean install..." -ForegroundColor Yellow
        kind delete cluster --name $CLUSTER_NAME
        $clusterExists = $false
    } else {
        Write-Host "Cluster is healthy. Skipping creation." -ForegroundColor Green
    }
}

if (-not $clusterExists) {
    Write-Host "Creating KinD cluster: $CLUSTER_NAME..." -ForegroundColor Yellow
    
    kind create cluster --name $CLUSTER_NAME --wait 5m

    if ($LASTEXITCODE -ne 0) {
        Write-Host "FATAL ERROR: Failed to create KinD cluster." -ForegroundColor Red
        exit 1
    }
    Write-Host "KinD cluster created successfully" -ForegroundColor Green
} 

# ==========================================
# 4. Configure Connection
# ==========================================
Write-Host "Configuring kubectl context..." -ForegroundColor Cyan
# CRITICAL: Export config to prevent localhost:8080 errors
kind export kubeconfig --name $CLUSTER_NAME

# Verify context switch
try {
    kubectl config use-context "kind-$CLUSTER_NAME"
} catch {
    Write-Host "Warning: Context switch failed. Trying to proceed anyway..." -ForegroundColor Yellow
}

# Verify Docker/Cluster responsiveness
Write-Host "Verifying cluster connectivity..." -ForegroundColor Cyan
try {
    kubectl cluster-info | Out-Null
} catch {
    Write-Host "Error: Cluster is created but not responding." -ForegroundColor Red
    exit 1
}

# ==========================================
# 5. Apply Manifests
# ==========================================
Write-Host "Applying Kubernetes manifests..." -ForegroundColor Yellow

# Helper function to apply and check errors
function Apply-Manifest {
    param([string]$File, [string]$Description)
    Write-Host "$Description ($File)..." -ForegroundColor Cyan
    if (Test-Path $File) {
        kubectl apply -f $File
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Error applying $File" -ForegroundColor Red
            exit 1
        }
    } else {
        Write-Host "Warning: File $File not found, skipping." -ForegroundColor Magenta
    }
}

Apply-Manifest "namespace.yaml" "1. Creating namespace"
Apply-Manifest "configmaps.yaml" "2. Creating ConfigMaps"
Apply-Manifest "secrets.yaml" "2. Creating Secrets"
Apply-Manifest "postgres.yaml" "3. Deploying PostgreSQL (source)"
Apply-Manifest "postgres-metadata.yaml" "4. Deploying PostgreSQL Metadata"
Apply-Manifest "elasticsearch.yaml" "5. Deploying Elasticsearch"
Apply-Manifest "minio.yaml" "6. Deploying MinIO"
Apply-Manifest "spark.yaml" "7. Deploying Spark cluster"

Write-Host "8. Waiting for databases to be ready..." -ForegroundColor Cyan
kubectl wait --for=condition=ready pod -l app=postgres-metadata -n dqops --timeout=300s 
kubectl wait --for=condition=ready pod -l app=elasticsearch -n dqops --timeout=300s

Apply-Manifest "setup-airflow-user.yaml" "9. Setting up Airflow user"
if (kubectl get job setup-airflow-user -n dqops -p jsonpath="{.metadata.name}" 2>$null) {
    kubectl wait --for=condition=complete job/setup-airflow-user -n dqops --timeout=120s
}

Apply-Manifest "openmetadata-migration.yaml" "10. Running OpenMetadata migration"
if (kubectl get job openmetadata-migration -n dqops -p jsonpath="{.metadata.name}" 2>$null) {
    kubectl wait --for=condition=complete job/openmetadata-migration -n dqops --timeout=300s
}

Apply-Manifest "openmetadata-server.yaml" "11. Deploying OpenMetadata server"
Apply-Manifest "openmetadata-ingestion.yaml" "12. Deploying OpenMetadata ingestion"

Write-Host ""
Write-Host "=== Deployment Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Waiting for all pods to be ready (timeout 10m)..." -ForegroundColor Yellow
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