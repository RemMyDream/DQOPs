#!/bin/bash

set -e

echo "========================================="
echo "Spark + MinIO + PostgreSQL Setup"
echo "========================================="

# Create directories
echo "Creating directories..."
mkdir -p spark-data spark-apps spark-jars init-db

# Download JARs
echo ""
echo "Downloading required JARs..."
cd spark-jars

if [ ! -f "postgresql-42.7.3.jar" ]; then
    echo "Downloading PostgreSQL driver..."
    curl -L -o postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
fi

if [ ! -f "hadoop-aws-3.3.4.jar" ]; then
    echo "Downloading Hadoop AWS..."
    curl -L -o hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
fi

if [ ! -f "aws-java-sdk-bundle-1.12.262.jar" ]; then
    echo "Downloading AWS SDK..."
    curl -L -o aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
fi

if [ ! -f "iceberg-spark-runtime-3.5_2.12-1.5.2.jar" ]; then
    echo "Downloading Iceberg..."
    curl -L -o iceberg-spark-runtime-3.5_2.12-1.5.2.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar
fi

cd ..

echo ""
echo "JARs downloaded successfully!"
echo ""
echo "========================================="
echo "Next steps:"
echo "========================================="
echo "1. Make sure you have the following files:"
echo "   - init-db/init-postgres.sql"
echo "   - spark-apps/postgres_to_iceberg.py"
echo ""
echo "2. Start the cluster:"
echo "   docker-compose up -d"
echo ""
echo "3. Wait about 30 seconds for services to be ready"
echo ""
echo "4. Run the migration:"
echo "   ./run_migration.sh"
echo ""
echo "5. Access the UIs:"
echo "   - Spark Master: http://localhost:8080"
echo "   - MinIO Console: http://localhost:9001"
echo "========================================="