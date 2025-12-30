"""
Finnhub Data Transfer: PostgreSQL to MinIO Bronze Layer
Reads Finnhub stock price data from PostgreSQL and writes to MinIO Bronze layer
Can be run independently whenever needed
"""
import logging
import os
from dotenv import load_dotenv
from spark_utils import create_spark_session, write_to_bronze_iceberg

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PostgreSQL Configuration
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'sourcedb')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')


def read_finnhub_from_postgres(spark):
    """Read Finnhub stock prices from PostgreSQL"""
    
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    table_name = "finnhub_stock_prices"
    
    logger.info(f"Reading {table_name} from PostgreSQL...")
    
    # Simple read without partitioning since Finnhub table is small
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    record_count = df.count()
    logger.info(f"Successfully read {record_count} records from PostgreSQL")
    
    return df


def main():
    """Main job to transfer Finnhub data from PostgreSQL to MinIO"""
    logger.info("=" * 60)
    logger.info("Starting Finnhub PostgreSQL → MinIO Transfer")
    logger.info("=" * 60)
    
    # Create Spark session using utility function
    spark = create_spark_session(app_name="Finnhub PostgreSQL to MinIO")
    
    try:
        # Read from PostgreSQL
        df = read_finnhub_from_postgres(spark)
        
        if df.count() == 0:
            logger.warning("No data found in PostgreSQL. Skipping write to MinIO.")
        else:
            # Write to MinIO Bronze layer using utility function
            # Using 'overwrite' mode to replace existing data
            # Change to 'append' if you want to add to existing data
            write_to_bronze_iceberg(df, 'finnhub_stock_prices', mode='overwrite')
            
            # Verify the write
            count = spark.sql("SELECT COUNT(*) as count FROM lakehouse.bronze.finnhub_stock_prices").collect()[0]['count']
            logger.info(f"Verification: lakehouse.bronze.finnhub_stock_prices now contains {count} records")
        
        logger.info("=" * 60)
        logger.info("Finnhub PostgreSQL → MinIO Transfer Completed Successfully!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Transfer job failed: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
