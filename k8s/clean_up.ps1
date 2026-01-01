# Simple script to clean up DQOps cluster
$ErrorActionPreference = "SilentlyContinue"
$CLUSTER_NAME = "dqops-cluster"

Write-Host "Removing KinD cluster: $CLUSTER_NAME" -ForegroundColor Yellow
kind delete cluster --name $CLUSTER_NAME

Write-Host "Cleaning up docker system (pruning unused networks/volumes)..." -ForegroundColor Yellow
# Be careful with prune in shared environments, but useful for local resets
docker container prune -f
docker network prune -f

Write-Host "Cleanup complete. Please restart Docker Desktop if you encountered timeout errors." -ForegroundColor Green