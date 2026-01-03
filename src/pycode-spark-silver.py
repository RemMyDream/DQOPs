import logging
from spark_utils import (
    create_spark_session,
    process_gdelt_silver_from_iceberg,
    process_stooq_silver_from_iceberg,
    generate_nlp_input_and_score,
    aggregate_daily_signals
)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def silver_pipeline_v2(stock_config=None):
    """
    Silver Layer Pipeline V2: Transform Bronze data to Silver with NLP processing
    
    Args:
        stock_config: Dict with structure {ticker: {'aliases': [...], 'blacklist': []}}
    """
    logger.info("Starting Silver Layer Pipeline V2")
    
    spark = create_spark_session(elt_layer='silver')
    
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
        
        logger.info("=== V2: Bronze to Silver Transformation ===")
        
        # 1. Process GDELT news data
        logger.info("Processing GDELT news data...")
        process_gdelt_silver_from_iceberg(spark, stock_config)
        
        # 2. Process Stooq stock data
        logger.info("Processing Stooq stock data...")
        process_stooq_silver_from_iceberg(spark)
        
        # 3. NLP Sentiment Scoring
        logger.info("Running NLP sentiment analysis...")
        generate_nlp_input_and_score(spark)
        
        # 4. Aggregate to daily signals
        logger.info("Aggregating daily signals...")
        aggregate_daily_signals(spark)
        
        logger.info("✓ Silver Layer Pipeline V2 completed successfully!")
        
    except Exception as e:
        logger.error(f"Silver Layer Pipeline V2 failed: {str(e)}")
        raise
    
    finally:
        spark.stop()


def main():
    """Main entry point"""
    silver_pipeline_v2()


if __name__ == "__main__":
    main()

