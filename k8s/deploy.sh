#!/bin/bash
set -e

echo "=== DQOPs Kubernetes Deployment Script ==="
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if kubectl is installed
if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}Error: kubectl is not installed${NC}"
    exit 1
fi

# Check if kind is installed
if ! command -v kind &> /dev/null; then
    echo -e "${RED}Error: kind is not installed${NC}"
    echo "Install KinD from: https://kind.sigs.k8s.io/docs/user/quick-start/#installation"
    exit 1
fi

# Check if kind cluster exists
CLUSTER_NAME="dqops-cluster"
if ! kind get clusters | grep -q "^${CLUSTER_NAME}$"; then
    echo -e "${YELLOW}Creating KinD cluster: ${CLUSTER_NAME}${NC}"
    kind create cluster --name ${CLUSTER_NAME} --config kind-config.yaml 2>/dev/null || \
    kind create cluster --name ${CLUSTER_NAME}
    echo -e "${GREEN}KinD cluster created successfully${NC}"
else
    echo -e "${YELLOW}KinD cluster ${CLUSTER_NAME} already exists${NC}"
fi

# Set kubectl context
kubectl config use-context kind-${CLUSTER_NAME}

echo ""
echo -e "${YELLOW}Applying Kubernetes manifests...${NC}"

# Apply manifests in order
echo "1. Creating namespace..."
kubectl apply -f namespace.yaml

echo "2. Creating ConfigMaps and Secrets..."
kubectl apply -f configmaps.yaml
kubectl apply -f secrets.yaml

echo "3. Deploying PostgreSQL (source)..."
kubectl apply -f postgres.yaml

echo "4. Deploying PostgreSQL Metadata..."
kubectl apply -f postgres-metadata.yaml

echo "5. Deploying Elasticsearch..."
kubectl apply -f elasticsearch.yaml

echo "6. Deploying MinIO..."
kubectl apply -f minio.yaml

echo "7. Deploying Spark cluster..."
kubectl apply -f spark.yaml

echo "8. Waiting for databases to be ready..."
kubectl wait --for=condition=ready pod -l app=postgres-metadata -n dqops --timeout=300s || true
kubectl wait --for=condition=ready pod -l app=elasticsearch -n dqops --timeout=300s || true

echo "9. Setting up Airflow user..."
kubectl apply -f setup-airflow-user.yaml
kubectl wait --for=condition=complete job/setup-airflow-user -n dqops --timeout=120s || true

echo "10. Running OpenMetadata migration..."
kubectl apply -f openmetadata-migration.yaml
kubectl wait --for=condition=complete job/openmetadata-migration -n dqops --timeout=300s || true

echo "11. Deploying OpenMetadata server..."
kubectl apply -f openmetadata-server.yaml

echo "12. Deploying OpenMetadata ingestion..."
kubectl apply -f openmetadata-ingestion.yaml

echo ""
echo -e "${GREEN}=== Deployment Complete ===${NC}"
echo ""
echo "Waiting for all pods to be ready..."
kubectl wait --for=condition=ready pod --all -n dqops --timeout=600s || true

echo ""
echo -e "${GREEN}All services are being deployed. Check status with:${NC}"
echo "  kubectl get pods -n dqops"
echo ""
echo -e "${GREEN}To access services via port-forward:${NC}"
echo "  # OpenMetadata Server"
echo "  kubectl port-forward -n dqops svc/openmetadata-server 8585:8585"
echo ""
echo "  # MinIO Console"
echo "  kubectl port-forward -n dqops svc/minio 9001:9001"
echo ""
echo "  # Spark Master Web UI"
echo "  kubectl port-forward -n dqops svc/spark-master 8080:8080"
echo ""
echo -e "${GREEN}To view logs:${NC}"
echo "  kubectl logs -f -n dqops <pod-name>"
echo ""
echo -e "${GREEN}To delete the cluster:${NC}"
echo "  kind delete cluster --name ${CLUSTER_NAME}"

