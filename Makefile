.PHONY: help setup start stop restart clean ingest bronze health logs pyspark check-pg check-bronze install-deps rebuild

# Default target
help:
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "  Data Lakehouse Pipeline - Available Commands"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo ""
	@echo "  make setup          - Initial setup (download JARs)"
	@echo "  make start          - Start all Docker services"
	@echo "  make stop           - Stop all Docker services"
	@echo "  make restart        - Restart all Docker services"
	@echo "  make ingest         - Run data ingestion from APIs"
	@echo "  make bronze         - Process data to Bronze layer"
	@echo "  make pipeline       - Run full pipeline (ingest + bronze)"
	@echo "  make health         - Check health of all services"
	@echo "  make logs           - Show logs for all services"
	@echo "  make logs-<service> - Show logs for specific service"
	@echo "  make clean          - Stop services and remove volumes (⚠️  deletes data)"
	@echo "  make psql           - Connect to PostgreSQL"
	@echo "  make pyspark        - Connect to PySpark shell"
	@echo "  make check-pg       - Check PostgreSQL data summary"
	@echo "  make check-bronze   - Check Bronze layer tables"
	@echo "  make install-deps   - Install Python dependencies"
	@echo "  make rebuild        - Clean and rebuild everything"
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Initial setup
setup:
	@echo "🚀 Starting initial setup..."
	@chmod +x setup.sh
	@./setup.sh

# Start services
start:
	@echo "🟢 Starting Docker services..."
	@docker-compose up -d
	@echo "✅ Services started!"
	@echo "   - PostgreSQL: localhost:5432"
	@echo "   - MinIO Console: http://localhost:9001"
	@echo "   - MinIO API: http://localhost:9000"
	@echo "   - Spark Master UI: http://localhost:8080"
	@echo "   - Spark Worker 1 UI: http://localhost:8081"
	@echo "   - Spark Worker 2 UI: http://localhost:8082"
	@echo "   - Spark Application UI: http://localhost:4040"

# Stop services
stop:
	@echo "🔴 Stopping Docker services..."
	@docker-compose stop
	@echo "✅ Services stopped!"

# Restart services
restart:
	@echo "🔄 Restarting Docker services..."
	@docker-compose restart
	@echo "✅ Services restarted!"

# Run data ingestion
ingest:
	@echo "📥 Running data ingestion..."
	@python3 pycode-data-ingestion.py
	@echo "✅ Data ingestion completed!"

# Process to Bronze layer
bronze:
	@echo "🔨 Processing data to Bronze layer..."
	@chmod +x run_spark_job.sh
	@./run_spark_job.sh
	@echo "✅ Bronze layer processing completed!"

# Run full pipeline
pipeline: setup start ingest
	@echo "✅ Full pipeline completed!"

# Check health
health:
	@echo "🏥 Checking service health..."
	@python3 pycode-health-check.py

# Show logs
logs:
	@docker-compose logs -f --tail=100

# Show logs for specific service
logs-%:
	@docker-compose logs -f --tail=100 $*

# Clean everything (removes data!)
clean:
	@echo "⚠️  WARNING: This will delete all data!"
	@read -p "Are you sure? (yes/no): " confirm; \
	if [ "$$confirm" = "yes" ]; then \
		echo "🗑️  Cleaning up..."; \
		docker-compose down -v; \
		echo "✅ Cleanup completed!"; \
	else \
		echo "❌ Cleanup cancelled."; \
	fi

# Connect to PostgreSQL
psql:
	@echo "🐘 Connecting to PostgreSQL..."
	@docker exec -it postgres-source psql -U postgres -d sourcedb

# Connect to PySpark
pyspark:
	@echo "⚡ Connecting to PySpark..."
	@docker exec -it spark-master /opt/spark/bin/pyspark \
		--master spark://spark-master:7077 \
		--jars /opt/spark/jars/extra/postgresql-42.6.0.jar,/opt/spark/jars/extra/hadoop-aws-3.3.4.jar,/opt/spark/jars/extra/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/extra/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
		--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
		--conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
		--conf spark.sql.catalog.lakehouse.type=hadoop \
		--conf spark.sql.catalog.lakehouse.warehouse=s3a://warehouse/ \
		--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
		--conf spark.hadoop.fs.s3a.access.key=admin \
		--conf spark.hadoop.fs.s3a.secret.key=password \
		--conf spark.hadoop.fs.s3a.path.style.access=true \
		--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
		--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
		--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider

# Check PostgreSQL data
check-pg:
	@echo "📊 PostgreSQL Data Summary:"
	@docker exec postgres-source psql -U postgres -d sourcedb -c "\
		SELECT 'gdelt_events' as table_name, COUNT(*) as record_count FROM gdelt_events \
		UNION ALL \
		SELECT 'gdelt_gkg', COUNT(*) FROM gdelt_gkg \
		UNION ALL \
		SELECT 'finnhub_stock_prices', COUNT(*) FROM finnhub_stock_prices;"

# Check Bronze layer
check-bronze:
	@echo "📊 Bronze Layer Summary:"
	@docker exec spark-master /opt/spark/bin/spark-sql \
		--jars /opt/spark/jars/extra/hadoop-aws-3.3.4.jar,/opt/spark/jars/extra/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/extra/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
		--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
		--conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
		--conf spark.sql.catalog.lakehouse.type=hadoop \
		--conf spark.sql.catalog.lakehouse.warehouse=s3a://warehouse/ \
		--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
		--conf spark.hadoop.fs.s3a.access.key=admin \
		--conf spark.hadoop.fs.s3a.secret.key=password \
		--conf spark.hadoop.fs.s3a.path.style.access=true \
		--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
		--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
		--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
		-e "SHOW NAMESPACES IN lakehouse; SHOW TABLES IN lakehouse.bronze;"

# Install Python dependencies
install-deps:
	@echo "📦 Installing Python dependencies..."
	@pip install -r requirements.txt
	@echo "✅ Dependencies installed!"

# Build and start everything from scratch
rebuild: clean setup start
	@echo "✅ Rebuild completed!"