from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'sourcedb')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'admin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'password')

WAREHOUSE_PATH = os.getenv('WAREHOUSE_PATH', 's3a://warehouse/')

if not all([POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD,
            MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, WAREHOUSE_PATH]):
    logger.error("One or more required environment variables are missing.")
    raise EnvironmentError("Missing environment variables for configuration.")


def create_spark_session():
    """Create Spark session with Iceberg and MinIO configuration"""
    
    spark = SparkSession.builder \
        .appName("PostgreSQL to Bronze Layer") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hadoop") \
        .config("spark.sql.catalog.lakehouse.warehouse", WAREHOUSE_PATH) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully")
    
    return spark


def read_from_postgres(spark, table_name, partition_column="id", num_partitions=10):
    """Read data from PostgreSQL table with dynamic partitioning for big data
    
    Args:
        spark: SparkSession
        table_name: Name of the PostgreSQL table
        partition_column: Column to use for partitioning (default: 'id')
        num_partitions: Number of partitions for parallel reading (default: 10)
    """
    
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    
    logger.info(f"Reading table {table_name} from PostgreSQL with partitioning on {partition_column}")
    
    # First, get the min and max values for the partition column
    bounds_query = f"(SELECT MIN({partition_column}) as min_val, MAX({partition_column}) as max_val FROM {table_name}) as bounds"
    
    try:
        bounds_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", bounds_query) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        bounds = bounds_df.collect()[0]
        lower_bound = bounds['min_val']
        upper_bound = bounds['max_val']
        
        logger.info(f"Partition bounds for {table_name}: lower={lower_bound}, upper={upper_bound}")
        
        # Read with partitioning
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .option("partitionColumn", partition_column) \
            .option("lowerBound", lower_bound) \
            .option("upperBound", upper_bound) \
            .option("numPartitions", num_partitions) \
            .load()
        
        logger.info(f"Successfully read {df.count()} records from {table_name} using {num_partitions} partitions")
        
    except Exception as e:
        logger.warning(f"Could not partition on {partition_column}, falling back to non-partitioned read: {str(e)}")
        # Fallback to non-partitioned read if partition column doesn't exist or other issues
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        logger.info(f"Successfully read {df.count()} records from {table_name}")
    
    return df


def write_to_bronze_iceberg(df, table_name, mode="append"):
    """Write data to Bronze layer in Iceberg format
    
    Note: Iceberg table partitioning should be defined during table creation.
    This function handles the write operation. Spark will distribute the data
    across partitions, and Iceberg will organize it according to the table's
    partition spec if one exists.
    """
    
    iceberg_table = f"lakehouse.bronze.{table_name}"
    
    logger.info(f"Writing data to Bronze layer: {iceberg_table}")
    
    # Add processing timestamp
    df = df.withColumn("bronze_load_timestamp", current_timestamp())
    
    try:
        # Write to Iceberg table
        df.write \
            .format("iceberg") \
            .mode(mode) \
            .saveAsTable(iceberg_table)
        
        logger.info(f"Successfully wrote data to {iceberg_table}")
        
    except Exception as e:
        logger.error(f"Failed to write to Iceberg table {iceberg_table}: {str(e)}")
        raise


def process_table_to_bronze(spark, table_name, partition_column="id", num_partitions=10, mode="append"):
    """Generic function to process any table from PostgreSQL to Bronze layer
    
    Args:
        spark: SparkSession
        table_name: Name of the PostgreSQL table to process
        partition_column: Column to use for partitioning during read (default: 'id')
        num_partitions: Number of partitions for parallel reading (default: 10)
        mode: Write mode for Iceberg table (default: 'append')
    
    Note: The function assumes MinIO buckets are already created:
    - bronze-gdelt-events
    - bronze-gdelt-gkg  
    - bronze-finhub
    """
    try:
        logger.info(f"Processing {table_name} to Bronze layer")
        
        # Read from PostgreSQL with partitioning
        df = read_from_postgres(spark, table_name, partition_column, num_partitions)
        
        # Write to Bronze layer (Iceberg)
        write_to_bronze_iceberg(df, table_name, mode=mode)
        
        logger.info(f"{table_name} processed successfully to Bronze layer")
        
    except Exception as e:
        logger.error(f"Error processing {table_name}: {str(e)}")
        raise


def verify_bronze_tables(spark, tables=['gdelt_events', 'gdelt_gkg', 'finnhub_stock_prices']):
    """Verify Bronze layer tables
    
    Args:
        spark: SparkSession
        tables: List of table names to verify
    """
    try:
        logger.info("\n=== Bronze Layer Tables ===")
        
        for table in tables:
            iceberg_table = f"lakehouse.bronze.{table}"
            try:
                count = spark.sql(f"SELECT COUNT(*) as count FROM {iceberg_table}").collect()[0]['count']
                logger.info(f"{table}: {count} records")
                
                # Show sample data
                logger.info(f"\nSample data from {table}:")
                spark.sql(f"SELECT * FROM {iceberg_table} LIMIT 5").show(truncate=False)
                
            except Exception as e:
                logger.warning(f"Could not verify {table}: {str(e)}")
        
    except Exception as e:
        logger.error(f"Error verifying tables: {str(e)}")


def main():
    """Main Spark job"""
    logger.info("Starting Spark job: PostgreSQL to Bronze Layer")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Define tables to process with their configurations
        # These correspond to MinIO buckets: bronze-gdelt-events, bronze-gdelt-gkg, bronze-finhub
        tables_config = [
            {"table_name": "gdelt_events", "partition_column": "globaleventid", "num_partitions": 10},
            {"table_name": "gdelt_gkg", "partition_column": "id", "num_partitions": 10},
            {"table_name": "finnhub_stock_prices", "partition_column": "id", "num_partitions": 10}
        ]
        
        # Process each table using the generic function
        for config in tables_config:
            process_table_to_bronze(
                spark=spark,
                table_name=config["table_name"],
                partition_column=config["partition_column"],
                num_partitions=config["num_partitions"],
                mode="append"
            )
        
        # Verify tables
        #verify_bronze_tables(spark, tables=[c["table_name"] for c in tables_config])
        
        logger.info("Spark job completed successfully")
        
    except Exception as e:
        logger.error(f"Spark job failed: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()