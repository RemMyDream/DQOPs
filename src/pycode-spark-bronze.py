import logging
from spark_utils import (
    create_spark_session,
    ingest_gdelt_events_to_minio,
    ingest_gdelt_gkg_to_minio,
    ingest_stooq_to_minio,
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main Bronze layer processing job"""
    logger.info("Starting Bronze Layer Processing Job")
    
    # Create Spark session
    spark = create_spark_session(elt_layer='bronze')
    
    try:
        # Configuration for data ingestion
        GDELT_DAYS = 1
        GDELT_MAX_RECORDS_PER_SPAMP = 1
        STOOQ_DAYS = GDELT_DAYS
        STOCK_SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'META']
        
        logger.info("=== Direct API to MinIO Ingestion ===")
        
        # 1. Ingest GDELT Events directly to MinIO
        logger.info("Step 1: Ingesting GDELT Events directly to MinIO...")
        ingest_gdelt_events_to_minio(
            spark=spark,
            days=GDELT_DAYS,
            max_records_per_timespamp=GDELT_MAX_RECORDS_PER_SPAMP
        )
        
        # 2. Ingest GDELT GKG directly to MinIO
        logger.info("Step 2: Ingesting GDELT GKG directly to MinIO...")
        ingest_gdelt_gkg_to_minio(
            spark=spark,
            days=GDELT_DAYS,
            max_records_per_timespamp=GDELT_MAX_RECORDS_PER_SPAMP
        )
        
        # 3. Ingest Stooq stock data directly to MinIO
        logger.info("Step 3: Ingesting Stooq stock data directly to MinIO...")
        ingest_stooq_to_minio(
            spark=spark,
            symbols=STOCK_SYMBOLS,
            days=STOOQ_DAYS
        )
        
        
        logger.info("Bronze Layer Processing Job completed successfully!")
        
    except Exception as e:
        logger.error(f"Bronze Layer Processing Job failed: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
