#!/bin/bash

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}   Stock Prediction Training - Setup Script${NC}"
echo -e "${BLUE}============================================================${NC}\n"

# Step 1: Check prerequisites
echo -e "${YELLOW}Step 1: Checking prerequisites...${NC}"
command -v kubectl >/dev/null 2>&1 || { echo -e "${RED}✗ kubectl is required but not installed. Aborting.${NC}" >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo -e "${RED}✗ python3 is required but not installed. Aborting.${NC}" >&2; exit 1; }
echo -e "${GREEN}✓ kubectl found${NC}"
echo -e "${GREEN}✓ python3 found${NC}\n"

# Step 2: Check if K8s pods are running
echo -e "${YELLOW}Step 2: Checking Kubernetes pods...${NC}"
NAMESPACE="dqops"

check_pod() {
    local pod_name=$1
    kubectl get pods -n $NAMESPACE 2>/dev/null | grep -q "$pod_name.*Running"
    return $?
}

if ! kubectl get namespace $NAMESPACE >/dev/null 2>&1; then
    echo -e "${RED}✗ Namespace '$NAMESPACE' not found${NC}"
    echo -e "${YELLOW}  Deploy the Helm chart first:${NC}"
    echo -e "${YELLOW}  helm install bdsp-pipeline ./chart -n dqops --create-namespace${NC}"
    exit 1
fi

if check_pod "minio"; then
    echo -e "${GREEN}✓ MinIO pod is running${NC}"
else
    echo -e "${RED}✗ MinIO pod is not running${NC}"
    exit 1
fi

if check_pod "mlflow-server"; then
    echo -e "${GREEN}✓ MLflow server pod is running${NC}"
else
    echo -e "${RED}✗ MLflow server pod is not running${NC}"
    exit 1
fi

if check_pod "mlflow-postgres"; then
    echo -e "${GREEN}✓ MLflow PostgreSQL pod is running${NC}"
else
    echo -e "${RED}✗ MLflow PostgreSQL pod is not running${NC}"
    exit 1
fi

echo ""

# Step 3: Set up port forwarding
echo -e "${YELLOW}Step 3: Setting up port forwarding...${NC}"
echo -e "This will run in the background. Use ${BLUE}'pkill -f port-forward'${NC} to stop."

# Kill existing port-forwards
pkill -f "port-forward.*minio.*9000" 2>/dev/null
pkill -f "port-forward.*mlflow.*5000" 2>/dev/null
sleep 2

# Start new port-forwards in background
kubectl port-forward -n $NAMESPACE svc/minio 9000:9000 >/dev/null 2>&1 &
MINIO_PF_PID=$!
sleep 3

kubectl port-forward -n $NAMESPACE svc/mlflow 5000:5000 >/dev/null 2>&1 &
MLFLOW_PF_PID=$!
sleep 3

# Verify port-forwards are working
if curl -s http://localhost:9000/minio/health/live >/dev/null 2>&1; then
    echo -e "${GREEN}✓ MinIO port-forward active (PID: $MINIO_PF_PID)${NC}"
    echo -e "  Access at: ${BLUE}http://localhost:9000${NC}"
else
    echo -e "${YELLOW}⚠ MinIO port-forward may not be ready yet${NC}"
fi

if curl -s http://localhost:5000/health >/dev/null 2>&1; then
    echo -e "${GREEN}✓ MLflow port-forward active (PID: $MLFLOW_PF_PID)${NC}"
    echo -e "  Access at: ${BLUE}http://localhost:5000${NC}"
else
    echo -e "${YELLOW}⚠ MLflow port-forward may not be ready yet${NC}"
fi

echo ""

# Step 4: Set environment variables
echo -e "${YELLOW}Step 4: Setting environment variables...${NC}"
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password

echo -e "${GREEN}✓ Environment variables set (for data loading)${NC}"
echo -e "  ${BLUE}AWS_ACCESS_KEY_ID${NC}=$AWS_ACCESS_KEY_ID"
echo -e "  ${BLUE}AWS_SECRET_ACCESS_KEY${NC}=********"

echo ""
echo -e "${BLUE}ℹ Note: MLFLOW_S3_ENDPOINT_URL is NOT needed${NC}"
echo -e "${BLUE}  MLflow server (in K8s) handles artifact storage${NC}"
echo ""

# Step 5: Install Python dependencies
echo -e "${YELLOW}Step 5: Checking Python dependencies...${NC}"
if [ -f "requirements.txt" ]; then
    echo -e "Installing from requirements.txt..."
    python -m venv .venv
    .venv\Scripts\activate
    pip install -q -r requirements.txt
    echo -e "${GREEN}✓ Dependencies installed${NC}\n"
else
    echo -e "${YELLOW}⚠ requirements.txt not found, skipping${NC}\n"
fi

# Step 6: Run verification
echo -e "${YELLOW}Step 6: Running verification checks...${NC}"
if [ -f "verify_setup.py" ]; then
    python3 verify_setup.py
    VERIFY_STATUS=$?
else
    echo -e "${YELLOW}⚠ verify_setup.py not found, skipping verification${NC}"
    VERIFY_STATUS=0
fi

echo ""

# Step 7: Print summary and next steps
echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}   Setup Complete!${NC}"
echo -e "${BLUE}============================================================${NC}\n"

if [ $VERIFY_STATUS -eq 0 ]; then
    echo -e "${GREEN}✓ All systems ready for training${NC}\n"
else
    echo -e "${YELLOW}⚠ Some checks failed, but you may be able to proceed${NC}\n"
fi

echo -e "${BLUE}Quick Start Commands:${NC}\n"

echo -e "${GREEN}# Test with mock data (no MinIO credentials needed):${NC}"
echo -e "unset AWS_ACCESS_KEY_ID && unset AWS_SECRET_ACCESS_KEY"
echo -e "python trainer.py --stock NVDA --mlflow-tracking-uri http://localhost:5000 --max-trials 3 --epochs 10"
echo ""

echo -e "${GREEN}# Full training with real data from MinIO:${NC}"
echo -e "export AWS_ACCESS_KEY_ID=admin"
echo -e "export AWS_SECRET_ACCESS_KEY=password"
echo -e "python trainer.py \\"
echo -e "  --stock NVDA \\"
echo -e "  --bucket gold \\"
echo -e "  --prefix ml_features/data \\"
echo -e "  --mlflow-tracking-uri http://localhost:5000 \\"
echo -e "  --mlflow-experiment stock-prediction \\"
echo -e "  --look-back 45 \\"
echo -e "  --max-trials 10 \\"
echo -e "  --epochs 50 \\"
echo -e "  --register-model"
echo ""

echo -e "${BLUE}Useful Links:${NC}"
echo -e "  MLflow UI:     ${BLUE}http://localhost:5000${NC}"
echo -e "  MinIO Console: ${BLUE}http://localhost:9001${NC} (need to port-forward 9001)"
echo ""

echo -e "${BLUE}Port Forwarding:${NC}"
echo -e "  MinIO PID:  $MINIO_PF_PID"
echo -e "  MLflow PID: $MLFLOW_PF_PID"
echo -e "  Stop all:   ${YELLOW}pkill -f port-forward${NC}"
echo ""

echo -e "${BLUE}Architecture:${NC}"
echo -e "  Trainer → MinIO (load training data from gold/ml_features/data)"
echo -e "  Trainer → MLflow Server (log metrics/models)"
echo -e "  MLflow Server → MinIO (store artifacts in mlflow-artifacts)"
echo ""

echo -e "${GREEN}Ready to train! Run one of the commands above.${NC}\n"