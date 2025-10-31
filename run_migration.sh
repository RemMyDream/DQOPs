#!/bin/bash

set -e

echo "========================================="
echo "Running PostgreSQL to Iceberg Migration"
echo "========================================="

# Check if containers are running
if ! docker ps | grep -q spark-master; then
    echo "Error: Spark master is not running!"
    echo "Please start the cluster first: docker-compose up -d"
    exit 1
fi

if ! docker ps | grep -q postgres; then
    echo "Error: PostgreSQL is not running!"
    echo "Please start the cluster first: docker-compose up -d"
    exit 1
fi

if ! docker ps | grep -q minio; then
    echo "Error: MinIO is not running!"
    echo "Please start the cluster first: docker-compose up -d"
    exit 1
fi

echo ""
echo "All services are running. Starting migration..."
echo ""

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1g \
  --jars /opt/spark/jars-extra/postgresql-42.7.3.jar,/opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars-extra/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hadoop \
  --conf spark.sql.catalog.lakehouse.warehouse=s3a://lakehouse/warehouse \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  /opt/spark-apps/postgres_to_iceberg.py

echo ""
echo "========================================="
echo "Migration completed!"
echo "========================================="
echo ""
echo "Access your data:"
echo "- MinIO Console: http://localhost:9001"
echo "  Username: minioadmin"
echo "  Password: minioadmin"
echo ""
echo "- Spark UI: http://localhost:8080"
echo ""
echo "Buckets created:"
echo "- lakehouse: Iceberg warehouse"
echo "- warehouse: Direct Parquet files"
echo ""