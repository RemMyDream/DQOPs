import logging
from spark_utils import (
    create_spark_session,
    create_gold_layer_from_iceberg,
    generate_analytics_summary
)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def gold_pipeline_v2():
    """
    Gold Layer Pipeline V2: Create ML-ready features and dashboard data
    """
    logger.info("Starting Gold Layer Pipeline V2")
    
    spark = create_spark_session(elt_layer='gold')
    
    try:
        logger.info("=== V2: Silver to Gold Aggregation ===")
        
        # 1. Create Gold layer tables
        logger.info("Creating Gold layer tables...")
        create_gold_layer_from_iceberg(spark)
        
        # 2. Generate analytics insights
        logger.info("Generating analytics insights...")
        generate_analytics_summary(spark)
        
        logger.info("✓ Gold Layer Pipeline V2 completed successfully!")
        logger.info("Data is ready for:")
        logger.info("  - Machine Learning (ml_training_data)")
        logger.info("  - BI Dashboards (superset_dashboard_data)")
        
    except Exception as e:
        logger.error(f"Gold Layer Pipeline V2 failed: {str(e)}")
        raise
    
    finally:
        spark.stop()


def main():
    """Main entry point"""
    gold_pipeline_v2()


if __name__ == "__main__":
    main()

