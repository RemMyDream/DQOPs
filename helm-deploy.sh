#!/bin/bash

# BDSP Pipeline Helm Deployment Script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="$SCRIPT_DIR/bdsp-pipeline"
RELEASE_NAME="bdsp"
NAMESPACE="dqops"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if helm is installed
if ! command -v helm &> /dev/null; then
    print_error "Helm is not installed. Please install Helm first."
    echo "Run: curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash"
    exit 1
fi

# Check if kubectl is installed
if ! command -v kubectl &> /dev/null; then
    print_error "kubectl is not installed. Please install kubectl first."
    exit 1
fi

# Check Kubernetes connection
if ! kubectl cluster-info &> /dev/null; then
    print_error "Cannot connect to Kubernetes cluster. Please check your connection."
    exit 1
fi

print_info "Starting BDSP Pipeline deployment..."

# Function to install/upgrade
deploy() {
    print_info "Validating Helm chart..."
    helm lint "$CHART_DIR"
    
    print_info "Deploying $RELEASE_NAME..."
    helm upgrade --install "$RELEASE_NAME" "$CHART_DIR" \
        --create-namespace \
        --wait \
        --timeout 5m \
        "$@"
    
    print_info "Deployment completed successfully!"
    
    # Show release status
    helm status "$RELEASE_NAME"
    
    print_info "Getting pod status..."
    kubectl get pods -n "$NAMESPACE"
}

# Function to uninstall
uninstall() {
    print_warning "Uninstalling $RELEASE_NAME..."
    helm uninstall "$RELEASE_NAME" || true
    
    print_warning "Deleting namespace $NAMESPACE..."
    kubectl delete namespace "$NAMESPACE" --ignore-not-found=true
    
    print_info "Uninstallation completed!"
}

# Function to show status
status() {
    print_info "Helm release status:"
    helm status "$RELEASE_NAME" || print_error "Release not found"
    
    print_info "Pods status:"
    kubectl get pods -n "$NAMESPACE" || print_error "Namespace not found"
    
    print_info "Services:"
    kubectl get svc -n "$NAMESPACE" || print_error "Namespace not found"
}

# Function to dry-run
dry_run() {
    print_info "Running dry-run..."
    helm upgrade --install "$RELEASE_NAME" "$CHART_DIR" \
        --dry-run \
        --debug \
        "$@"
}

# Function to show values
show_values() {
    print_info "Current values:"
    helm get values "$RELEASE_NAME" || print_error "Release not found"
}

# Main script
case "${1:-deploy}" in
    deploy)
        shift
        deploy "$@"
        ;;
    install)
        shift
        deploy "$@"
        ;;
    uninstall)
        uninstall
        ;;
    status)
        status
        ;;
    dry-run)
        shift
        dry_run "$@"
        ;;
    values)
        show_values
        ;;
    *)
        echo "Usage: $0 {deploy|install|uninstall|status|dry-run|values} [additional-helm-options]"
        echo ""
        echo "Commands:"
        echo "  deploy|install  - Install or upgrade the Helm chart"
        echo "  uninstall       - Uninstall the Helm chart and delete namespace"
        echo "  status          - Show deployment status"
        echo "  dry-run         - Test the deployment without applying"
        echo "  values          - Show current values"
        echo ""
        echo "Examples:"
        echo "  $0 deploy"
        echo "  $0 deploy --set postgres.persistence.size=20Gi"
        echo "  $0 deploy -f custom-values.yaml"
        echo "  $0 dry-run"
        echo "  $0 status"
        echo "  $0 uninstall"
        exit 1
        ;;
esac
