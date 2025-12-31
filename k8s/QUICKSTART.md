# Quick Start Guide - Deploy DQOPs on KinD

## Prerequisites Check

```powershell
# Check if Docker is running
docker ps

# Check if KinD is installed
kind version

# Check if kubectl is installed
kubectl version --client
```

## Deploy Everything (Windows PowerShell)

```powershell
cd k8s
.\deploy.ps1
```

## Deploy Everything (Linux/WSL/Bash)

```bash
cd k8s
chmod +x deploy.sh
./deploy.sh
```

## Verify Deployment

```bash
# Check all pods
kubectl get pods -n dqops

# Watch pods until they're all ready
kubectl get pods -n dqops -w
```

## Access Services

### OpenMetadata Server
```bash
kubectl port-forward -n dqops svc/openmetadata-server 8585:8585
# Open: http://localhost:8585
```

### MinIO Console
```bash
kubectl port-forward -n dqops svc/minio 9001:9001
# Open: http://localhost:9001
# Login: admin / password
```

### Spark Master Web UI
```bash
kubectl port-forward -n dqops svc/spark-master 8080:8080
# Open: http://localhost:8080
```

## Common Commands

```bash
# View logs
kubectl logs -f -n dqops <pod-name>

# Check service status
kubectl get svc -n dqops

# Check persistent volumes
kubectl get pvc -n dqops

# Restart a deployment
kubectl rollout restart deployment/<name> -n dqops

# Delete everything
kind delete cluster --name dqops-cluster
```

## Troubleshooting

### Pods stuck in Pending
```bash
kubectl describe pod <pod-name> -n dqops
```

### Check resource usage
```bash
kubectl top pods -n dqops
```

### View events
```bash
kubectl get events -n dqops --sort-by='.lastTimestamp'
```

