#!/bin/bash

echo "========================================="
echo "Starting PySpark Shell with Iceberg"
echo "========================================="
echo ""
echo "Available tables:"
echo "  - lakehouse.gdelt_events"
echo "  - lakehouse.gdelt_mentions"
echo "  - lakehouse.squad_data"
echo ""
echo "Example queries:"
echo "  spark.sql('SELECT * FROM lakehouse.gdelt_events LIMIT 5').show()"
echo "  spark.sql('SELECT * FROM lakehouse.gdelt_events.snapshots').show()"
echo ""
echo "Type 'exit()' to quit"
echo "========================================="
echo ""

docker exec -it spark-master /opt/spark/bin/pyspark \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars-extra/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hadoop \
  --conf spark.sql.catalog.lakehouse.warehouse=s3a://lakehouse/warehouse \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false