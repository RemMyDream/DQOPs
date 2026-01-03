#!/bin/sh
set -e

echo "Starting MinIO..."
minio server /data --console-address ":9001" &
MINIO_PID=$!

echo "Waiting for MinIO to be ready..."
until mc alias set myminio http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" 2>/dev/null; do
  sleep 1
done

echo "Creating buckets..."
mc mb myminio/bronze --ignore-existing
mc mb myminio/silver --ignore-existing
mc mb myminio/gold --ignore-existing
mc mb myminio/mlflow-artifacts --ignore-existing

echo "Buckets created:"
mc ls myminio

echo "MinIO is ready."
wait $MINIO_PID
