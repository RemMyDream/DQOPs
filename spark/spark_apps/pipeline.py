import argparse
import sys
import os
from typing import List, Dict, Any

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from create_spark_connection import get_or_create_spark, stop_spark
from utils.helpers import create_logger, load_cfg

logger = create_logger("PipelineRunner")

PIPELINES = {
    'full': ['bronze', 'silver', 'gold'],
    'bronze': ['bronze'],
    'silver': ['silver'],
    'gold': ['gold'],
    'silver_stooq': ['silver_stooq'],
    'silver_gdelt': ['silver_gdelt'],
    'gold_raw_stooq': ['gold_raw_stooq'],
    'gold_sentiment': ['gold_sentiment'],
    'gold_ml_features': ['gold_ml_features'],
}


def run_bronze_job(spark, **kwargs):
    from ingestion.gdelt_stooq_to_bronze import ingest_stooq, ingest_gdelt_gkg
    
    stock_config = load_cfg("utils/config.yaml")['stock_config']
    
    # Stooq ingestion
    if not kwargs.get('skip_stooq', False):
        logger.info("--- Stooq Ingestion ---")
        stooq_count = ingest_stooq(
            spark=spark,
            tickers=kwargs.get('stooq_tickers', ['NVDA', 'MSFT']),
            start_date=kwargs.get('stooq_start', '2020-01-02'),
            end_date=kwargs.get('stooq_end', '2025-12-22')
        )
        logger.info(f"Stooq: {stooq_count} rows")
    
    # GDELT ingestion
    if not kwargs.get('skip_gdelt', False):
        logger.info("--- GDELT Ingestion ---")
        gdelt_count = ingest_gdelt_gkg(
            spark=spark,
            start_date=kwargs.get('gdelt_start', '2019-12-31'),
            end_date=kwargs.get('gdelt_end', '2025-12-23'),
            stock_config=stock_config
        )
        logger.info(f"GDELT: {gdelt_count} rows")


def run_silver_job(spark, **kwargs):
    """Run all silver layer jobs"""
    logger.info("=== Starting Silver Stooq ===")
    run_silver_stooq_job(spark, **kwargs)
    logger.info("=== Silver Stooq Done ===")
    
    logger.info("=== Starting Silver GDELT ===")
    run_silver_gdelt_job(spark, **kwargs)
    logger.info("=== Silver GDELT Done ===")


def run_silver_stooq_job(spark, **kwargs):
    from processor.stooq_to_silver import process_stooq_to_silver
    
    logger.info("--- Stooq: Bronze → Silver ---")
    tickers = kwargs.get('stooq_tickers', ['NVDA', 'MSFT'])
    row_count = process_stooq_to_silver(
        spark=spark,
        tickers=tickers
    )
    logger.info(f"Stooq Silver: {row_count} rows")


def run_silver_gdelt_job(spark, **kwargs):
    from processor.gdelt_to_silver import process_gdelt_to_silver
    
    logger.info("--- GDELT: Bronze → Silver ---")
    stock_config = load_cfg("utils/config.yaml")['stock_config']
    row_count = process_gdelt_to_silver(
        spark=spark,
        stock_config=stock_config
    )
    logger.info(f"GDELT Silver: {row_count} rows")


def run_gold_job(spark, **kwargs):
    """Run all gold layer jobs in order"""
    run_gold_raw_stooq_job(spark, **kwargs)
    run_gold_sentiment_job(spark, **kwargs)
    run_gold_ml_features_job(spark, **kwargs)


def run_gold_raw_stooq_job(spark, **kwargs):
    from processor.raw_stooq_to_gold import process_bronze_to_gold_stooq
    
    logger.info("--- Stooq: Bronze → Gold (Raw) ---")
    row_count = process_bronze_to_gold_stooq(spark=spark)
    logger.info(f"Gold Stooq Raw: {row_count} rows")


def run_gold_sentiment_job(spark, **kwargs):
    from processor.gdelt_to_gold import process_silver_to_gold
    
    logger.info("--- GDELT: Silver → Gold (Sentiment) ---")
    row_count = process_silver_to_gold(spark=spark)
    logger.info(f"Gold Sentiment: {row_count} rows")


def run_gold_ml_features_job(spark, **kwargs):
    from processor.build_ml_features import build_ml_features
    
    logger.info("--- Building ML Features ---")
    row_count = build_ml_features(spark=spark)
    logger.info(f"Gold ML Features: {row_count} rows")


# Job registry
JOBS = {
    'bronze': run_bronze_job,
    'silver': run_silver_job,
    'silver_stooq': run_silver_stooq_job,
    'silver_gdelt': run_silver_gdelt_job,
    'gold': run_gold_job,
    'gold_raw_stooq': run_gold_raw_stooq_job,
    'gold_sentiment': run_gold_sentiment_job,
    'gold_ml_features': run_gold_ml_features_job,
}


def run_jobs(job_names: List[str], **kwargs) -> Dict[str, Any]:
    results = {}
    
    logger.info("=" * 60)
    logger.info(f"PIPELINE START: {job_names}")
    logger.info(f"Parameters: {kwargs}")
    logger.info("=" * 60)
    
    spark = get_or_create_spark()
    
    try:
        for job_name in job_names:
            logger.info("-" * 50)
            logger.info(f"RUNNING: {job_name}")
            logger.info("-" * 50)
            
            job_func = JOBS.get(job_name)
            
            if job_func is None:
                logger.error(f"Job not found: {job_name}")
                logger.error(f"Available jobs: {list(JOBS.keys())}")
                results[job_name] = {'success': False, 'error': 'Job not found'}
                continue
            
            try:
                job_func(spark, **kwargs)
                results[job_name] = {'success': True}
                logger.info(f"✓ {job_name} completed successfully")
                
            except Exception as e:
                logger.error(f"✗ {job_name} failed: {e}")
                results[job_name] = {'success': False, 'error': str(e)}
                raise
        
        return results
        
    finally:
        logger.info("=" * 60)
        logger.info("Stopping Spark session...")
        stop_spark()
        logger.info("PIPELINE FINISHED")
        logger.info("=" * 60)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Spark Pipeline Runner',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--jobs', type=str, 
                       help='Comma-separated job names: bronze,silver,gold,silver_stooq,silver_gdelt,gold_raw_stooq,gold_sentiment,gold_ml_features')
    group.add_argument('--pipeline', type=str, choices=list(PIPELINES.keys()), 
                       help='Predefined pipeline: full, bronze, silver, gold, etc.')
    
    # Stooq parameters
    parser.add_argument('--stooq-tickers', type=str, default='NVDA,MSFT',
                        help='Comma-separated stock tickers (default: NVDA,MSFT)')
    parser.add_argument('--stooq-start', type=str, default='2020-01-02',
                        help='Stooq start date YYYY-MM-DD (default: 2020-01-02)')
    parser.add_argument('--stooq-end', type=str, default='2025-12-22',
                        help='Stooq end date YYYY-MM-DD (default: 2025-12-22)')
    
    # GDELT parameters
    parser.add_argument('--gdelt-start', type=str, default='2019-12-31',
                        help='GDELT start date YYYY-MM-DD (default: 2019-12-31)')
    parser.add_argument('--gdelt-end', type=str, default='2025-12-23',
                        help='GDELT end date YYYY-MM-DD (default: 2025-12-23)')
    
    # Skip options
    parser.add_argument('--skip-stooq', type=str, default='false',
                        help='Skip Stooq ingestion (default: false)')
    parser.add_argument('--skip-gdelt', type=str, default='false',
                        help='Skip GDELT ingestion (default: false)')
   
    return parser.parse_args()


def parse_bool(val: str) -> bool:
    return str(val).lower() in ('true', '1', 'yes')


def main():
    args = parse_args()
    
    # Determine jobs to run
    if args.pipeline:
        job_names = PIPELINES[args.pipeline]
    else:
        job_names = [j.strip() for j in args.jobs.split(',')]
    
    # Build kwargs
    kwargs = {
        'stooq_tickers': [t.strip() for t in args.stooq_tickers.split(',')],
        'stooq_start': args.stooq_start,
        'stooq_end': args.stooq_end,
        'gdelt_start': args.gdelt_start,
        'gdelt_end': args.gdelt_end,
        'skip_stooq': parse_bool(args.skip_stooq),
        'skip_gdelt': parse_bool(args.skip_gdelt),
    }
    
    # Run pipeline
    results = run_jobs(job_names, **kwargs)
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)
    for job_name, result in results.items():
        status = "SUCCESS" if result.get('success') else f"FAILED: {result.get('error', 'Unknown')}"
        logger.info(f"  {job_name}: {status}")
    
    # Exit code
    failed = [k for k, v in results.items() if not v.get('success')]
    if failed:
        logger.error(f"\nFailed jobs: {failed}")
        return 1
    
    logger.info("\nAll jobs completed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())