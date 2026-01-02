#!/bin/bash
# DQOps Final Deployment Script (Bash/WSL2 Version)
# Features: RAM Check, Pinned Version (v1.32.0), Smart Recovery

set -e  # Exit on error

CLUSTER_NAME="dqops-cluster"
NAMESPACE="dqops"
MIN_MEMORY_MB=6000
SCRIPT_VERSION="1.0.1"

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
GRAY='\033[0;37m'
NC='\033[0m' # No Color

echo -e "${CYAN}=== DQOps Kubernetes Deployment Script v${SCRIPT_VERSION} ===${NC}"

# ==========================================
# UTILITY FUNCTIONS
# ==========================================

write_step() {
    local message=$1
    local color=${2:-$CYAN}
    echo -e "\n${color}>>> ${message}${NC}"
}

write_success() {
    echo -e "${GREEN}[OK] $1${NC}"
}

write_error() {
    echo -e "${RED}[ERROR] $1${NC}"
}

write_warning() {
    echo -e "${YELLOW}[WARNING] $1${NC}"
}

command_exists() {
    command -v "$1" >/dev/null 2>&1
}

test_storage_class() {
    kubectl get storageclass "$1" -o name >/dev/null 2>&1
}

wait_for_pod_ready() {
    local namespace=$1
    local label_selector=$2
    local timeout_seconds=${3:-300}
    local description=${4:-"pod"}
    
    echo -e "${YELLOW}Waiting for ${description} to be ready (timeout: ${timeout_seconds}s)...${NC}"
    
    if kubectl wait --for=condition=ready pod -l "$label_selector" -n "$namespace" --timeout="${timeout_seconds}s" 2>/dev/null; then
        write_success "${description} is ready"
        return 0
    else
        write_warning "${description} did not become ready within timeout"
        return 1
    fi
}

diagnose_stuck_pods() {
    local namespace=$1
    local label_selector=$2
    local description=$3
    
    echo -e "\n${YELLOW}Diagnosing pods: ${description}${NC}"
    
    local pods
    pods=$(kubectl get pods -n "$namespace" -l "$label_selector" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
    
    if [[ -z "$pods" ]]; then
        echo -e "${GRAY}  No pods found with selector: ${label_selector}${NC}"
        return
    fi
    
    for pod_name in $pods; do
        [[ -z "$pod_name" ]] && continue
        
        local status
        local ready
        status=$(kubectl get pod "$pod_name" -n "$namespace" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
        ready=$(kubectl get pod "$pod_name" -n "$namespace" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "Unknown")
        
        echo -e "${CYAN}  Pod: ${pod_name}${NC}"
        echo -e "${GRAY}    Phase: ${status} | Ready: ${ready}${NC}"
        
        if [[ "$status" == "Pending" ]] || [[ "$ready" == "False" ]]; then
            echo -e "${YELLOW}    Recent Events:${NC}"
            kubectl get events -n "$namespace" --field-selector involvedObject.name="$pod_name" \
                --sort-by='.lastTimestamp' -o custom-columns=TIME:.lastTimestamp,REASON:.reason,MESSAGE:.message \
                --no-headers 2>/dev/null | tail -5 | while IFS= read -r line; do
                    echo -e "${GRAY}      ${line}${NC}"
                done
        fi
    done
}

apply_manifest() {
    local file=$1
    local description=$2
    local required=${3:-true}
    
    write_step "$description"
    
    if [[ ! -f "$file" ]]; then
        if [[ "$required" == "true" ]]; then
            write_error "Required file not found: $file"
            exit 1
        else
            write_warning "File not found: $file (skipping)"
            return 1
        fi
    fi
    
    echo -e "${GRAY}Applying: ${file}${NC}"
    
    if kubectl apply -f "$file"; then
        write_success "Applied ${file}"
        return 0
    else
        write_error "Failed to apply ${file}"
        if [[ "$required" == "true" ]]; then
            exit 1
        fi
        return 1
    fi
}

# ==========================================
# 1. WSL MEMORY CHECK
# ==========================================
write_step "Checking WSL Memory Configuration"

if mem_output=$(free -m 2>/dev/null | grep "Mem:"); then
    total_mem_mb=$(echo "$mem_output" | awk '{print $2}')
    
    echo -e "${GRAY}Detected WSL Total Memory: ${total_mem_mb} MB${NC}"
    
    if [[ $total_mem_mb -lt $MIN_MEMORY_MB ]]; then
        write_error "WSL has less than 6GB RAM (${total_mem_mb} MB detected)"
        echo ""
        echo -e "${YELLOW}Actions required:${NC}"
        echo "  1. Edit ~/.wslconfig (Windows path: %USERPROFILE%\.wslconfig)"
        echo "  2. Add or update:"
        echo "     [wsl2]"
        echo "     memory=8GB"
        echo "  3. Run 'wsl --shutdown' in PowerShell"
        echo "  4. Restart Docker Desktop"
        echo "  5. Run this script again"
        exit 1
    fi
    
    write_success "Memory check passed (${total_mem_mb} MB available)"
else
    write_warning "Could not check WSL memory, proceeding anyway"
fi

# ==========================================
# 2. DEPENDENCY CHECKS
# ==========================================
write_step "Checking Dependencies"

missing_commands=()
for cmd in kubectl kind docker; do
    if command_exists "$cmd"; then
        echo -e "${GREEN}  [OK] ${cmd}${NC}"
    else
        echo -e "${RED}  [MISSING] ${cmd}${NC}"
        missing_commands+=("$cmd")
    fi
done

if [[ ${#missing_commands[@]} -gt 0 ]]; then
    write_error "Missing required commands: ${missing_commands[*]}"
    echo ""
    echo -e "${YELLOW}Installation instructions:${NC}"
    echo "  - kubectl: https://kubernetes.io/docs/tasks/tools/"
    echo "  - kind: https://kind.sigs.k8s.io/docs/user/quick-start/#installation"
    echo "  - docker: https://docs.docker.com/get-docker/"
    exit 1
fi

write_success "All dependencies present"

# ==========================================
# 3. DOCKER STATUS CHECK
# ==========================================
write_step "Checking Docker Status"

if docker ps >/dev/null 2>&1; then
    write_success "Docker is running"
else
    write_error "Docker daemon is not responding"
    echo -e "${YELLOW}Please start Docker Desktop and try again${NC}"
    exit 1
fi

# ==========================================
# 4. CLUSTER MANAGEMENT
# ==========================================
write_step "Managing KinD Cluster: ${CLUSTER_NAME}"

cluster_exists=false
if clusters=$(kind get clusters 2>/dev/null); then
    if echo "$clusters" | grep -q "^${CLUSTER_NAME}$"; then
        cluster_exists=true
    fi
fi

if [[ "$cluster_exists" == "true" ]]; then
    echo -e "${GRAY}Cluster '${CLUSTER_NAME}' found, verifying health...${NC}"
    
    is_healthy=false
    if kind export kubeconfig --name "$CLUSTER_NAME" --internal >/dev/null 2>&1 && \
       kubectl cluster-info --request-timeout=5s >/dev/null 2>&1; then
        is_healthy=true
    fi
    
    if [[ "$is_healthy" == "true" ]]; then
        write_success "Cluster is healthy, skipping creation"
    else
        write_warning "Cluster exists but is unresponsive"
        echo -e "${YELLOW}Deleting and recreating cluster...${NC}"
        
        kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
        cluster_exists=false
    fi
fi

if [[ "$cluster_exists" == "false" ]]; then
    echo -e "${YELLOW}Creating KinD cluster: ${CLUSTER_NAME} (this may take a few minutes)...${NC}"
    
    if ! kind create cluster --name "$CLUSTER_NAME" --image kindest/node:v1.31.1 --wait 5m; then
        write_error "Failed to create KinD cluster"
        echo -e "${YELLOW}Check Docker Desktop is running and has sufficient resources${NC}"
        exit 1
    fi
    
    write_success "KinD cluster created successfully"
fi

# ==========================================
# 5. CONFIGURE KUBECTL CONTEXT
# ==========================================
write_step "Configuring kubectl Context"

kind export kubeconfig --name "$CLUSTER_NAME"

if kubectl config use-context "kind-${CLUSTER_NAME}" && \
   kubectl cluster-info --request-timeout=10s >/dev/null 2>&1; then
    write_success "Connected to cluster"
else
    write_error "Cluster not responding"
    exit 1
fi

# ==========================================
# 6. INSTALL STORAGE PROVISIONER
# ==========================================
write_step "Installing Storage Provisioner"

if test_storage_class "local-path"; then
    write_success "Storage provisioner already installed"
else
    echo -e "${GRAY}Installing local-path-provisioner...${NC}"
    
    manifest_url="https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.24/deploy/local-path-storage.yaml"
    installed=false
    
    # Try direct application
    if kubectl apply -f "$manifest_url" >/dev/null 2>&1; then
        installed=true
    else
        echo -e "${GRAY}Direct installation failed, trying download method...${NC}"
        
        # Fallback: download then apply
        temp_file="/tmp/local-path-storage.yaml"
        if wget -q -O "$temp_file" "$manifest_url" 2>/dev/null || \
           curl -s -o "$temp_file" "$manifest_url" 2>/dev/null; then
            if kubectl apply -f "$temp_file" >/dev/null 2>&1; then
                installed=true
            fi
            rm -f "$temp_file"
        fi
    fi
    
    if [[ "$installed" == "true" ]]; then
        echo -e "${GRAY}Waiting for provisioner to be ready...${NC}"
        sleep 10
        
        # Wait for storage class to appear
        max_wait=60
        elapsed=0
        while [[ $elapsed -lt $max_wait ]]; do
            if test_storage_class "local-path"; then
                write_success "Storage provisioner installed"
                break
            fi
            sleep 5
            elapsed=$((elapsed + 5))
        done
        
        if ! test_storage_class "local-path"; then
            write_warning "Storage class not detected after installation"
        fi
    else
        write_warning "Failed to install storage provisioner"
        echo -e "${YELLOW}PVCs may not work correctly. You can install manually with:${NC}"
        echo -e "${GRAY}  kubectl apply -f ${manifest_url}${NC}"
    fi
fi

# Configure as default storage class
if test_storage_class "local-path"; then
    echo -e "${GRAY}Setting local-path as default storage class...${NC}"
    
    # Remove default annotation from other storage classes
    if storage_classes=$(kubectl get storageclass -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); then
        for sc in $storage_classes; do
            if [[ -n "$sc" ]] && [[ "$sc" != "local-path" ]]; then
                kubectl annotate storageclass "$sc" storageclass.kubernetes.io/is-default-class=false --overwrite >/dev/null 2>&1 || true
            fi
        done
    fi
    
    # Set local-path as default
    if kubectl annotate storageclass local-path storageclass.kubernetes.io/is-default-class=true --overwrite >/dev/null 2>&1; then
        write_success "Default storage class configured"
    fi
fi

# ==========================================
# 7. APPLY KUBERNETES MANIFESTS
# ==========================================
write_step "Deploying Kubernetes Resources"

# Apply manifests in order
apply_manifest "namespace.yaml" "Creating namespace"
apply_manifest "configmaps.yaml" "Creating ConfigMaps"
apply_manifest "secrets.yaml" "Creating Secrets"

# Deploy databases
apply_manifest "postgres.yaml" "Deploying PostgreSQL (source)"
apply_manifest "postgres-metadata.yaml" "Deploying PostgreSQL Metadata"
apply_manifest "elasticsearch.yaml" "Deploying Elasticsearch"
apply_manifest "minio.yaml" "Deploying MinIO"
apply_manifest "spark.yaml" "Deploying Spark cluster"

# Check PVC status
echo -e "\n${GRAY}Checking PVC status...${NC}"
sleep 100

if pvc_list=$(kubectl get pvc -n "$NAMESPACE" --no-headers 2>/dev/null); then
    echo "$pvc_list"
    
    # Check for pending PVCs
    pending_count=0
    if all_pvcs=$(kubectl get pvc -n "$NAMESPACE" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); then
        for pvc_name in $all_pvcs; do
            [[ -z "$pvc_name" ]] && continue
            
            phase=$(kubectl get pvc "$pvc_name" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
            if [[ "$phase" == "Pending" ]]; then
                pending_count=$((pending_count + 1))
            fi
        done
    fi
    
    if [[ $pending_count -gt 0 ]]; then
        write_warning "${pending_count} PVC(s) are pending - pods may be stuck"
        diagnose_stuck_pods "$NAMESPACE" "app in (postgres,postgres-metadata,elasticsearch,minio)" "Database pods"
    fi
fi

sleep 180

# Wait for critical databases
write_step "Waiting for Databases"

wait_for_pod_ready "$NAMESPACE" "app=postgres-metadata" 300 "PostgreSQL Metadata"
wait_for_pod_ready "$NAMESPACE" "app=elasticsearch" 300 "Elasticsearch"

# Setup jobs
if apply_manifest "setup-airflow-user.yaml" "Setting up Airflow user" "false"; then
    echo -e "${GRAY}Waiting for Airflow setup job...${NC}"
    kubectl wait --for=condition=complete job/setup-airflow-user -n "$NAMESPACE" --timeout=120s >/dev/null 2>&1 || true
fi

if apply_manifest "openmetadata-migration.yaml" "Running OpenMetadata migration" "false"; then
    echo -e "${GRAY}Waiting for migration job...${NC}"
    kubectl wait --for=condition=complete job/openmetadata-migration -n "$NAMESPACE" --timeout=300s >/dev/null 2>&1 || true
fi

# Deploy OpenMetadata services
apply_manifest "openmetadata-server.yaml" "Deploying OpenMetadata server"
apply_manifest "openmetadata-ingestion.yaml" "Deploying OpenMetadata ingestion"

# ==========================================
# 8. FINAL STATUS CHECK
# ==========================================
write_step "Checking Deployment Status" "$GREEN"

echo -e "${YELLOW}Waiting for all pods to be ready (this may take several minutes)...${NC}"
kubectl wait --for=condition=ready pod --all -n "$NAMESPACE" --timeout=600s || true

echo ""
echo -e "${CYAN}Current pod status:${NC}"
kubectl get pods -n "$NAMESPACE"

echo ""
echo -e "${GREEN}=== Deployment Complete ===${NC}"
echo ""
echo -e "${CYAN}Quick Start Commands:${NC}"
echo ""
echo -e "${NC}Check status:${NC}"
echo -e "${GRAY}  kubectl get pods -n ${NAMESPACE}${NC}"
echo -e "${GRAY}  kubectl get svc -n ${NAMESPACE}${NC}"
echo ""
echo -e "${NC}Access services (port-forward):${NC}"
echo -e "${GRAY}  kubectl port-forward -n ${NAMESPACE} svc/openmetadata-server 8585:8585${NC}"
echo -e "${GRAY}  kubectl port-forward -n ${NAMESPACE} svc/minio 9001:9001${NC}"
echo -e "${GRAY}  kubectl port-forward -n ${NAMESPACE} svc/spark-master 8080:8080${NC}"
echo ""
echo -e "${NC}View logs:${NC}"
echo -e "${GRAY}  kubectl logs -f -n ${NAMESPACE} <pod-name>${NC}"
echo ""
echo -e "${NC}Delete cluster:${NC}"
echo -e "${GRAY}  kind delete cluster --name ${CLUSTER_NAME}${NC}"
echo ""
write_success "Deployment script finished"
