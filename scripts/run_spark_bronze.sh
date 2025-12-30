#!/bin/bash

echo "=========================================="
echo "Running Spark Job: PostgreSQL to Bronze"
echo "=========================================="

# Run Spark job in the Spark master container
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1g \
  --jars /opt/spark/jars/extra/postgresql-42.6.0.jar,/opt/spark/jars/extra/hadoop-aws-3.3.4.jar,/opt/spark/jars/extra/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/extra/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hadoop \
  --conf spark.sql.catalog.lakehouse.warehouse=s3a://bronze \
  --conf spark.sql.warehouse.dir=s3a://bronze \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=password \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  /opt/spark/jobs/pycode-spark-bronze.py


echo "=========================================="
echo "Spark job completed!"
echo "=========================================="