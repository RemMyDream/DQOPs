#!/bin/sh

minio server /data --console-address ":9001" &

echo "Waiting for MinIO to start..."
sleep 5

mc alias set myminio http://localhost:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}

echo "Creating buckets..."
mc mb myminio/bronze --ignore-existing
mc mb myminio/silver --ignore-existing
mc mb myminio/gold --ignore-existing

echo "Buckets created successfully:"
mc ls myminio

wait