#!/bin/bash
# Example: Deploy BDSP Pipeline using Helm

echo "=========================================="
echo "Example Helm Deployment for BDSP Pipeline"
echo "=========================================="

# Step 1: Ensure Helm is installed
echo -e "\n1. Checking Helm installation..."
if ! command -v helm &> /dev/null; then
    echo "❌ Helm not found. Installing Helm..."
    curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
else
    echo "✅ Helm is installed: $(helm version --short)"
fi

# Step 2: Verify Kubernetes connection
echo -e "\n2. Checking Kubernetes connection..."
if kubectl cluster-info &> /dev/null; then
    echo "✅ Connected to Kubernetes cluster"
else
    echo "❌ Cannot connect to Kubernetes. Starting kind cluster..."
    kind create cluster --name dqops-cluster
fi

# Step 3: Validate Helm chart
echo -e "\n3. Validating Helm chart..."
helm lint bdsp-pipeline/
if [ $? -eq 0 ]; then
    echo "✅ Chart validation passed"
else
    echo "❌ Chart validation failed"
    exit 1
fi

# Step 4: Show what will be deployed (dry-run)
echo -e "\n4. Dry-run deployment (preview)..."
echo "This shows what would be deployed without actually deploying..."
read -p "Press Enter to see the dry-run output, or Ctrl+C to skip..."
helm install bdsp-test bdsp-pipeline/ --dry-run --debug | head -50

# Step 5: Deploy
echo -e "\n5. Ready to deploy?"
echo "Options:"
echo "  1) Deploy with default values"
echo "  2) Deploy with development values (smaller resources)"
echo "  3) Deploy with production values (larger resources)"
echo "  4) Skip deployment"
read -p "Enter your choice (1-4): " choice

case $choice in
    1)
        echo "Deploying with default values..."
        helm install bdsp bdsp-pipeline/ --create-namespace --wait
        ;;
    2)
        echo "Deploying with development values..."
        helm install bdsp bdsp-pipeline/ -f bdsp-pipeline/values-dev.yaml --create-namespace --wait
        ;;
    3)
        echo "Deploying with production values..."
        helm install bdsp bdsp-pipeline/ -f bdsp-pipeline/values-production.yaml --create-namespace --wait
        ;;
    4)
        echo "Skipping deployment."
        exit 0
        ;;
    *)
        echo "Invalid choice. Exiting."
        exit 1
        ;;
esac

# Step 6: Check deployment status
echo -e "\n6. Checking deployment status..."
helm status bdsp

echo -e "\n7. Checking pods..."
kubectl get pods -n dqops

echo -e "\n8. Waiting for all pods to be ready..."
kubectl wait --for=condition=ready pod --all -n dqops --timeout=5m

# Step 9: Show access instructions
echo -e "\n=========================================="
echo "🎉 Deployment Complete!"
echo "=========================================="
echo -e "\nTo access services, run these in separate terminals:"
echo ""
echo "PostgreSQL:"
echo "  kubectl port-forward -n dqops svc/postgres 5432:5432"
echo "  psql -h localhost -p 5432 -U postgres -d sourcedb"
echo ""
echo "MinIO Console:"
echo "  kubectl port-forward -n dqops svc/minio 9001:9001"
echo "  Browser: http://localhost:9001 (admin/password)"
echo ""
echo "Spark Master UI:"
echo "  kubectl port-forward -n dqops svc/spark-master 8080:8080"
echo "  Browser: http://localhost:8080"
echo ""
echo "Elasticsearch:"
echo "  kubectl port-forward -n dqops svc/elasticsearch 9200:9200"
echo "  curl http://localhost:9200"
echo ""
echo "=========================================="
echo "Useful commands:"
echo "  helm list                           # List releases"
echo "  helm status bdsp                    # Check status"
echo "  helm upgrade bdsp bdsp-pipeline/    # Update deployment"
echo "  helm rollback bdsp                  # Rollback to previous"
echo "  helm uninstall bdsp                 # Remove deployment"
echo "=========================================="
