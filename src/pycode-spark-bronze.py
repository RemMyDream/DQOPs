import logging
from datetime import datetime, timedelta
from spark_utils import (
    create_spark_session,
    ingest_gdelt_events_to_minio,
    ingest_gdelt_gkg_to_minio,
    ingest_stooq_to_minio,
    ingest_gdelt_gkg_to_minio_v2,
    ingest_gdelt_gkg_to_minio_v2_test,
    ingest_stooq_to_minio_v2,
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def bronze_pipeline_v1():
    """
    Bronze Layer Pipeline V1: GDELT API + Stooq ingestion
    Uses the original GDELT python library for events and GKG data
    """
    logger.info("Starting Bronze Layer Pipeline V1")
    
    # Create Spark session
    spark = create_spark_session(elt_layer='bronze')
    
    try:
        # Configuration for data ingestion
        GDELT_DAYS = 1
        GDELT_MAX_RECORDS_PER_SPAMP = 1
        STOOQ_DAYS = GDELT_DAYS
        STOCK_SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'META']
        
        logger.info("=== V1: Direct API to MinIO Ingestion ===")
        
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
        
        logger.info("Bronze Layer Pipeline V1 completed successfully!")
        
    except Exception as e:
        logger.error(f"Bronze Layer Pipeline V1 failed: {str(e)}")
        raise
    
    finally:
        spark.stop()


def bronze_pipeline_v2(start_date=None, end_date=None, stock_config=None):
    """
    Bronze Layer Pipeline V2: BigQuery + Stooq ingestion
    Uses BigQuery for GDELT GKG data with stock-specific filtering
    
    Args:
        start_date: Start date (YYYY-MM-DD string or datetime). Default: 7 days ago
        end_date: End date (YYYY-MM-DD string or datetime). Default: today
        stock_config: Dict with structure {ticker: {'aliases': [...], 'blacklist': []}}
    """
    logger.info("Starting Bronze Layer Pipeline V2 (BigQuery)")
    
    # Create Spark session
    spark = create_spark_session(elt_layer='bronze')
    
    try:
        # Default configuration
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        elif isinstance(start_date, datetime):
            start_date = start_date.strftime('%Y-%m-%d')
            
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        elif isinstance(end_date, datetime):
            end_date = end_date.strftime('%Y-%m-%d')
        
        # Default stock configuration
        if stock_config is None:
            stock_config = {
                'NVDA': {
                    'aliases': ['nvidia', 'jensen huang', 'geforce', 'rtx', 'nvda'],
                    'blacklist': []
                },
                'MSFT': {
                    'aliases': ['microsoft', 'satya nadella', 'azure', 'msft'],
                    'blacklist': []
                }
            }
        
        logger.info(f"=== V2: BigQuery to MinIO Ingestion ({start_date} to {end_date}) ===")
        
        # 1. Ingest GDELT GKG from BigQuery
        logger.info("Step 1: Ingesting GDELT GKG from BigQuery...")
        ingest_gdelt_gkg_to_minio_v2(
            spark=spark,
            start_date=start_date,
            end_date=end_date,
            stock_config=stock_config,
            gcp_project='bdsp-test'
        )
        
        # 2. Ingest Stooq stock data (starting from 2020-01-01)
        tickers = list(stock_config.keys())
        logger.info(f"Step 2: Ingesting Stooq stock data for {tickers}...")
        ingest_stooq_to_minio_v2(
            spark=spark,
            tickers=tickers,
            start_date='2020-01-01',
            end_date=end_date
        )
        
        logger.info("Bronze Layer Pipeline V2 completed successfully!")
        
    except Exception as e:
        logger.error(f"Bronze Layer Pipeline V2 failed: {str(e)}")
        raise
    
    finally:
        spark.stop()


def bronze_pipeline_v2_test(csv_path=None, stock_config=None):
    """
    Bronze Layer Pipeline V2 TEST: CSV + Stooq ingestion
    Uses local CSV file instead of BigQuery for GDELT GKG data
    Perfect for testing when you don't have BigQuery access
    
    Args:
        csv_path: Path to GDELT CSV file from Google Colab. If None, auto-detects from ./notebooks/data_pipeline/bronze/
        stock_config: Dict with structure {ticker: {'aliases': [...], 'blacklist': []}}
    """
    logger.info("Starting Bronze Layer Pipeline V2 TEST (CSV - no BigQuery required)")
    
    # Create Spark session
    spark = create_spark_session(elt_layer='bronze')
    
    try:
        # Default stock configuration
        if stock_config is None:
            stock_config = {
                'NVDA': {
                    'aliases': ['nvidia', 'jensen huang', 'geforce', 'rtx', 'nvda'],
                    'blacklist': []
                },
                'MSFT': {
                    'aliases': ['microsoft', 'satya nadella', 'azure', 'msft'],
                    'blacklist': []
                }
            }
        
        logger.info("=== V2 TEST: CSV to MinIO Ingestion (BigQuery bypass) ===")
        
        # 1. Ingest GDELT GKG from CSV
        logger.info("Step 1: Ingesting GDELT GKG from local CSV...")
        ingest_gdelt_gkg_to_minio_v2_test(
            spark=spark,
            csv_path=csv_path
        )
        
        # 2. Ingest Stooq stock data (starting from 2020-01-01)
        tickers = list(stock_config.keys())
        logger.info(f"Step 2: Ingesting Stooq stock data for {tickers}...")
        ingest_stooq_to_minio_v2(
            spark=spark,
            tickers=tickers,
            start_date='2020-01-01',
            end_date=None
        )
        
        logger.info("Bronze Layer Pipeline V2 TEST completed successfully!")
        logger.info("You can now proceed to Silver and Gold layers!")
        
    except Exception as e:
        logger.error(f"Bronze Layer Pipeline V2 TEST failed: {str(e)}")
        raise
    
    finally:
        spark.stop()


def main():    
    bronze_pipeline_v2_test()


if __name__ == "__main__":
    main()
