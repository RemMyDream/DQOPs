#!/bin/bash

echo "=========================================="
echo "Data Pipeline Setup Script"
echo "=========================================="

# Create necessary directories
echo "Creating project directories..."
mkdir -p spark/jars
mkdir -p spark/jobs
# mkdir -p data/postgres
# mkdir -p data/minio

# Download required Spark JARs
echo "Downloading Spark dependencies..."
cd spark/jars

# PostgreSQL JDBC Driver
if [ ! -f "postgresql-42.6.0.jar" ]; then
    wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
fi

# Iceberg Spark Runtime
if [ ! -f "iceberg-spark-runtime-3.5_2.12-1.4.2.jar" ]; then
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar
fi

# Hadoop AWS
if [ ! -f "hadoop-aws-3.3.4.jar" ]; then
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
fi

# AWS Java SDK Bundle (dependency for Hadoop AWS)
if [ ! -f "aws-java-sdk-bundle-1.12.262.jar" ]; then
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
fi

cd ../..

# Copy Spark job to jobs directory
echo "Copying Spark job..."
cp src/pycode-spark-bronze.py spark/jobs/
cp src/pycode-spark-silver.py spark/jobs/
cp src/pycode-spark-gold.py spark/jobs/
cp src/spark_utils.py spark/jobs/


# uncomment the following section if you want to set up a Python virtual environment
# and install dependencies

# # install Python virtual environment
# echo "Setting up Python virtual environment..."
# sudo apt update
# sudo apt install python3-venv
# python3 -m venv .venv
# source .venv/bin/activate

# # Install Python dependencies
# echo "Installing Python dependencies..."
# pip install -r requirements.txt

echo "=========================================="
echo "Setup completed successfully!"
echo "=========================================="
echo ""