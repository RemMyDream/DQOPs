# DQOps Final Deployment Script (Fixed & Improved)
# Features: RAM Check, Pinned Version (v1.32.0), Smart Recovery
$ErrorActionPreference = "Stop"
$CLUSTER_NAME = "dqops-cluster"
$NAMESPACE = "dqops"
$MIN_MEMORY_MB = 6000
$SCRIPT_VERSION = "1.0.1"

Write-Host "=== DQOps Kubernetes Deployment Script v$SCRIPT_VERSION ===" -ForegroundColor Cyan

# ==========================================
# UTILITY FUNCTIONS
# ==========================================

function Write-Step {
    param([string]$Message, [string]$Color = "Cyan")
    Write-Host "`n>>> $Message" -ForegroundColor $Color
}

function Write-Success {
    param([string]$Message)
    Write-Host "[OK] $Message" -ForegroundColor Green
}

function Write-Error-Message {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

function Write-Warning-Message {
    param([string]$Message)
    Write-Host "[WARNING] $Message" -ForegroundColor Yellow
}

function Test-Command {
    param([string]$CommandName)
    return $null -ne (Get-Command $CommandName -ErrorAction SilentlyContinue)
}

function Test-StorageClass {
    param([string]$Name)
    try {
        $null = kubectl get storageclass $Name -o name 2>&1
        return $LASTEXITCODE -eq 0
    } catch {
        return $false
    }
}

function Wait-ForPodReady {
    param(
        [string]$Namespace,
        [string]$LabelSelector,
        [int]$TimeoutSeconds = 300,
        [string]$Description = "pod"
    )
    
    Write-Host "Waiting for $Description to be ready (timeout: ${TimeoutSeconds}s)..." -ForegroundColor Yellow
    
    try {
        kubectl wait --for=condition=ready pod -l $LabelSelector -n $Namespace --timeout="${TimeoutSeconds}s"
        if ($LASTEXITCODE -eq 0) {
            Write-Success "$Description is ready"
            return $true
        } else {
            Write-Warning-Message "$Description did not become ready within timeout"
            return $false
        }
    } catch {
        Write-Warning-Message "Error waiting for $Description : $_"
        return $false
    }
}

function Diagnose-StuckPods {
    param([string]$Namespace, [string]$LabelSelector, [string]$Description)
    
    Write-Host "`nDiagnosing pods: $Description" -ForegroundColor Yellow
    
    try {
        $pods = kubectl get pods -n $Namespace -l $LabelSelector -o jsonpath='{.items[*].metadata.name}' 2>&1
        
        if (-not $pods -or $pods.Trim() -eq "") {
            Write-Host "  No pods found with selector: $LabelSelector" -ForegroundColor Gray
            return
        }
        
        foreach ($podName in ($pods -split '\s+')) {
            if (-not $podName) { continue }
            
            $status = kubectl get pod $podName -n $Namespace -o jsonpath='{.status.phase}' 2>&1
            $ready = kubectl get pod $podName -n $Namespace -o jsonpath='{.status.conditions[?(@.type==\"Ready\")].status}' 2>&1
            
            Write-Host "  Pod: $podName" -ForegroundColor Cyan
            Write-Host "    Phase: $status | Ready: $ready" -ForegroundColor Gray
            
            if ($status -eq "Pending" -or $ready -eq "False") {
                Write-Host "    Recent Events:" -ForegroundColor Yellow
                $events = kubectl get events -n $Namespace --field-selector involvedObject.name=$podName --sort-by='.lastTimestamp' -o custom-columns=TIME:.lastTimestamp,REASON:.reason,MESSAGE:.message --no-headers 2>&1 | Select-Object -Last 5
                if ($events) {
                    $events | ForEach-Object { Write-Host "      $_" -ForegroundColor Gray }
                }
            }
        }
    } catch {
        Write-Warning-Message "Failed to diagnose pods: $_"
    }
}

function Apply-Manifest {
    param(
        [string]$File,
        [string]$Description,
        [switch]$Required = $true
    )
    
    Write-Step "$Description"
    
    if (-not (Test-Path $File)) {
        if ($Required) {
            Write-Error-Message "Required file not found: $File"
            throw "Missing required manifest: $File"
        } else {
            Write-Warning-Message "File not found: $File (skipping)"
            return $false
        }
    }
    
    Write-Host "Applying: $File" -ForegroundColor Gray
    
    try {
        kubectl apply -f $File
        if ($LASTEXITCODE -ne 0) {
            throw "kubectl apply failed with exit code: $LASTEXITCODE"
        }
        Write-Success "Applied $File"
        return $true
    } catch {
        Write-Error-Message "Failed to apply $File : $_"
        if ($Required) {
            throw
        }
        return $false
    }
}

# ==========================================
# 1. WSL MEMORY CHECK
# ==========================================
Write-Step "Checking WSL Memory Configuration"

try {
    $memOutput = wsl -e free -m 2>&1 | Select-String "Mem:"
    
    if ($memOutput) {
        $memValues = -split $memOutput.ToString()
        $totalMemMB = [int]$memValues[1]
        
        Write-Host "Detected WSL Total Memory: $totalMemMB MB" -ForegroundColor Gray
        
        if ($totalMemMB -lt $MIN_MEMORY_MB) {
            Write-Error-Message "WSL has less than 6GB RAM ($totalMemMB MB detected)"
            Write-Host ""
            Write-Host "Actions required:" -ForegroundColor Yellow
            Write-Host "  1. Run 'configure_wsl.ps1' or edit .wslconfig manually"
            Write-Host "  2. Run 'wsl --shutdown'"
            Write-Host "  3. Restart Docker Desktop"
            Write-Host "  4. Run this script again"
            exit 1
        }
        
        Write-Success "Memory check passed ($totalMemMB MB available)"
    } else {
        Write-Warning-Message "Could not parse WSL memory output, proceeding anyway"
    }
} catch {
    Write-Warning-Message "Could not check WSL memory (is WSL running?): $_"
    Write-Host "Proceeding with deployment..." -ForegroundColor Gray
}

# ==========================================
# 2. DEPENDENCY CHECKS
# ==========================================
Write-Step "Checking Dependencies"

$missingCommands = @()
foreach ($cmd in @("kubectl", "kind", "docker")) {
    if (Test-Command $cmd) {
        Write-Host "  [OK] $cmd" -ForegroundColor Green
    } else {
        Write-Host "  [MISSING] $cmd" -ForegroundColor Red
        $missingCommands += $cmd
    }
}

if ($missingCommands.Count -gt 0) {
    Write-Error-Message "Missing required commands: $($missingCommands -join ', ')"
    Write-Host ""
    Write-Host "Installation instructions:" -ForegroundColor Yellow
    Write-Host "  - kubectl: https://kubernetes.io/docs/tasks/tools/"
    Write-Host "  - kind: https://kind.sigs.k8s.io/docs/user/quick-start/#installation"
    Write-Host "  - docker: https://docs.docker.com/get-docker/"
    exit 1
}

Write-Success "All dependencies present"

# ==========================================
# 3. DOCKER STATUS CHECK
# ==========================================
Write-Step "Checking Docker Status"

try {
    docker ps > $null 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Docker is running"
    } else {
        Write-Error-Message "Docker daemon is not responding"
        Write-Host "Please start Docker Desktop and try again" -ForegroundColor Yellow
        exit 1
    }
} catch {
    Write-Error-Message "Cannot connect to Docker: $_"
    exit 1
}

# ==========================================
# 4. CLUSTER MANAGEMENT
# ==========================================
Write-Step "Managing KinD Cluster: $CLUSTER_NAME"

try {
    $clusters = kind get clusters 2>&1
    if ($clusters -is [System.Array]) {
        $clusterExists = $clusters -contains $CLUSTER_NAME
    } elseif ($clusters -is [string]) {
        $clusterExists = ($clusters.Trim() -eq $CLUSTER_NAME)
    } else {
        $clusterExists = $false
    }
} catch {
    Write-Warning-Message "Failed to retrieve existing KinD clusters: $_"
    $clusters = @()
    $clusterExists = $false
}

if ($clusterExists) {
    Write-Host "Cluster '$CLUSTER_NAME' found, verifying health..." -ForegroundColor Gray
    
    $isHealthy = $false
    try {
        kind export kubeconfig --name $CLUSTER_NAME --internal 2>&1 | Out-Null
        kubectl cluster-info --request-timeout=5s 2>&1 | Out-Null
        $isHealthy = ($LASTEXITCODE -eq 0)
    } catch {
        $isHealthy = $false
    }
    
    if ($isHealthy) {
        Write-Success "Cluster is healthy, skipping creation"
        $clusterExists = $true
    } else {
        Write-Warning-Message "Cluster exists but is unresponsive"
        Write-Host "Deleting and recreating cluster..." -ForegroundColor Yellow
        
        kind delete cluster --name $CLUSTER_NAME 2>&1 | Out-Null
        $clusterExists = $false
    }
}

if (-not $clusterExists) {
    Write-Host "Creating KinD cluster: $CLUSTER_NAME (this may take a few minutes)..." -ForegroundColor Yellow
    
    kind create cluster --name $CLUSTER_NAME --wait 5m
    
    if ($LASTEXITCODE -ne 0) {
        Write-Error-Message "Failed to create KinD cluster"
        Write-Host "Check Docker Desktop is running and has sufficient resources" -ForegroundColor Yellow
        exit 1
    }
    
    Write-Success "KinD cluster created successfully"
}

# ==========================================
# 5. CONFIGURE KUBECTL CONTEXT
# ==========================================
Write-Step "Configuring kubectl Context"

kind export kubeconfig --name $CLUSTER_NAME

try {
    kubectl config use-context "kind-$CLUSTER_NAME"
    
    # Verify connectivity
    kubectl cluster-info --request-timeout=10s 2>&1 | Out-Null
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Connected to cluster"
    } else {
        Write-Error-Message "Cluster not responding"
        exit 1
    }
} catch {
    Write-Error-Message "Failed to configure kubectl context: $_"
    exit 1
}

# ==========================================
# 6. INSTALL STORAGE PROVISIONER
# ==========================================
Write-Step "Installing Storage Provisioner"

if (Test-StorageClass -Name "local-path") {
    Write-Success "Storage provisioner already installed"
} else {
    Write-Host "Installing local-path-provisioner..." -ForegroundColor Gray
    
    $manifestUrl = "https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.24/deploy/local-path-storage.yaml"
    $installed = $false
    
    # Try direct application first
    try {
        kubectl apply -f $manifestUrl 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            $installed = $true
        }
    } catch {
        Write-Host "Direct installation failed, trying download method..." -ForegroundColor Gray
    }
    
    # Fallback: download then apply
    if (-not $installed) {
        $tempFile = Join-Path $env:TEMP "local-path-storage.yaml"
        try {
            Invoke-WebRequest -Uri $manifestUrl -OutFile $tempFile -UseBasicParsing
            kubectl apply -f $tempFile 2>&1 | Out-Null
            $installed = ($LASTEXITCODE -eq 0)
            Remove-Item $tempFile -ErrorAction SilentlyContinue
        } catch {
            Write-Warning-Message "Failed to download provisioner manifest: $_"
        }
    }
    
    if ($installed) {
        Write-Host "Waiting for provisioner to be ready..." -ForegroundColor Gray
        Start-Sleep -Seconds 10
        
        # Wait for storage class to appear
        $maxWait = 60
        $elapsed = 0
        while ($elapsed -lt $maxWait) {
            if (Test-StorageClass -Name "local-path") {
                Write-Success "Storage provisioner installed"
                break
            }
            Start-Sleep -Seconds 5
            $elapsed += 5
        }
        
        if (-not (Test-StorageClass -Name "local-path")) {
            Write-Warning-Message "Storage class not detected after installation"
        }
    } else {
        Write-Warning-Message "Failed to install storage provisioner"
        Write-Host "PVCs may not work correctly. You can install manually with:" -ForegroundColor Yellow
        Write-Host "  kubectl apply -f $manifestUrl" -ForegroundColor Gray
    }
}

# Configure as default storage class
if (Test-StorageClass -Name "local-path") {
    Write-Host "Setting local-path as default storage class..." -ForegroundColor Gray
    
    # Remove default annotation from other storage classes
    $storageClasses = kubectl get storageclass -o jsonpath='{.items[*].metadata.name}' 2>&1
    if ($storageClasses) {
        foreach ($sc in ($storageClasses -split '\s+')) {
            if ($sc -and $sc -ne "local-path") {
                kubectl annotate storageclass $sc storageclass.kubernetes.io/is-default-class=false --overwrite 2>&1 | Out-Null
            }
        }
    }
    
    # Set local-path as default
    kubectl annotate storageclass local-path storageclass.kubernetes.io/is-default-class=true --overwrite 2>&1 | Out-Null
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Default storage class configured"
    }
}

# ==========================================
# 7. APPLY KUBERNETES MANIFESTS
# ==========================================
Write-Step "Deploying Kubernetes Resources"

# Apply manifests in order
Apply-Manifest "namespace.yaml" "Creating namespace"
Apply-Manifest "configmaps.yaml" "Creating ConfigMaps"
Apply-Manifest "secrets.yaml" "Creating Secrets"

# Deploy databases
Apply-Manifest "postgres.yaml" "Deploying PostgreSQL (source)"
Apply-Manifest "postgres-metadata.yaml" "Deploying PostgreSQL Metadata"
Apply-Manifest "elasticsearch.yaml" "Deploying Elasticsearch"
Apply-Manifest "minio.yaml" "Deploying MinIO"
Apply-Manifest "spark.yaml" "Deploying Spark cluster"

# Check PVC status
Write-Host "`nChecking PVC status..." -ForegroundColor Gray
Start-Sleep -Seconds 100

try {
    $pvcList = kubectl get pvc -n $NAMESPACE --no-headers 2>&1
    if ($pvcList) {
        Write-Host $pvcList
        
        # Check for pending PVCs
        $allPVCs = kubectl get pvc -n $NAMESPACE -o jsonpath='{.items[*].metadata.name}' 2>&1
        $pendingCount = 0
        
        if ($allPVCs) {
            foreach ($pvcName in ($allPVCs -split '\s+')) {
                if (-not $pvcName) { continue }
                
                $phase = kubectl get pvc $pvcName -n $NAMESPACE -o jsonpath='{.status.phase}' 2>&1
                if ($phase -eq "Pending") {
                    $pendingCount++
                }
            }
        }
        
        if ($pendingCount -gt 0) {
            Write-Warning-Message "$pendingCount PVC(s) are pending - pods may be stuck"
            Diagnose-StuckPods -Namespace $NAMESPACE -LabelSelector "app in (postgres,postgres-metadata,elasticsearch,minio)" -Description "Database pods"
        }
    }
} catch {
    Write-Warning-Message "Could not check PVC status: $_"
}

Start-Sleep -Seconds 180

# Wait for critical databases
Write-Step "Waiting for Databases"

Wait-ForPodReady -Namespace $NAMESPACE -LabelSelector "app=postgres-metadata" -Description "PostgreSQL Metadata" -TimeoutSeconds 300
Wait-ForPodReady -Namespace $NAMESPACE -LabelSelector "app=elasticsearch" -Description "Elasticsearch" -TimeoutSeconds 300

# Setup jobs
# docker build -t custom-debezium:latest
# kind load docker-image custom-debezium:latest
Apply-Manifest "kafka.yaml" "Deploying Kafka cluster"

Start-Sleep -Seconds 300

if (Apply-Manifest "setup-airflow-user.yaml" "Setting up Airflow user" -Required $false) {
    Write-Host "Waiting for Airflow setup job..." -ForegroundColor Gray
    kubectl wait --for=condition=complete job/setup-airflow-user -n $NAMESPACE --timeout=120s 2>&1 | Out-Null
}

if (Apply-Manifest "openmetadata-migration.yaml" "Running OpenMetadata migration" -Required $false) {
    Write-Host "Waiting for migration job..." -ForegroundColor Gray
    kubectl wait --for=condition=complete job/openmetadata-migration -n $NAMESPACE --timeout=300s 2>&1 | Out-Null
}

# Deploy OpenMetadata services
Apply-Manifest "openmetadata-server.yaml" "Deploying OpenMetadata server"
Apply-Manifest "openmetadata-ingestion.yaml" "Deploying OpenMetadata ingestion"

Apply-Manifest "airflow.yaml" "Deploying Airflow"

Apply-Manifest "backend.yaml" "Deploying Backend API"


# ==========================================
# 8. FINAL STATUS CHECK
# ==========================================
Write-Step "Checking Deployment Status" "Green"

Write-Host "Waiting for all pods to be ready (this may take several minutes)..." -ForegroundColor Yellow
kubectl wait --for=condition=ready pod --all -n $NAMESPACE --timeout=600s

Write-Host ""
Write-Host "Current pod status:" -ForegroundColor Cyan
kubectl get pods -n $NAMESPACE

Write-Host ""
Write-Host "=== Deployment Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Quick Start Commands:" -ForegroundColor Cyan
Write-Host ""
Write-Host "Check status:" -ForegroundColor White
Write-Host "  kubectl get pods -n $NAMESPACE" -ForegroundColor Gray
Write-Host "  kubectl get svc -n $NAMESPACE" -ForegroundColor Gray
Write-Host ""
Write-Host "Access services (port-forward):" -ForegroundColor White
Write-Host "  kubectl port-forward -n $NAMESPACE svc/openmetadata-server 8585:8585" -ForegroundColor Gray
Write-Host "  kubectl port-forward -n $NAMESPACE svc/minio 9001:9001" -ForegroundColor Gray
Write-Host "  kubectl port-forward -n $NAMESPACE svc/spark-master 8080:8080" -ForegroundColor Gray
Write-Host ""
Write-Host "View logs:" -ForegroundColor White
Write-Host "  kubectl logs -f -n $NAMESPACE <pod-name>" -ForegroundColor Gray
Write-Host ""
Write-Host "Delete cluster:" -ForegroundColor White
Write-Host "  kind delete cluster --name $CLUSTER_NAME" -ForegroundColor Gray
Write-Host ""
Write-Success "Deployment script finished"