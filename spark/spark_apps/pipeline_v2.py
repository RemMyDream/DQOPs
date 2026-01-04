import argparse
import sys
import os
from typing import List, Dict, Any

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from create_spark_connection import get_or_create_spark, stop_spark
from utils.helpers import create_logger, load_cfg

logger = create_logger("PipelineRunner")

PIPELINES = {
    'full': ['bronze', 'silver'],
    'bronze': ['bronze'],
    'silver': ['silver'],
}

def run_bronze_job(spark, **kwargs):
    from ingestion.gdelt_stooq_to_bronze_v2 import ingest_stooq, ingest_gdelt_gkg
        
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
        stock_config = load_cfg("utils/config.yaml")['stock_config']
        gdelt_count = ingest_gdelt_gkg(
            spark=spark,
            start_date=kwargs.get('gdelt_start', '2019-12-31'),
            end_date=kwargs.get('gdelt_end', '2025-12-23'),
            stock_config=stock_config
        )
        logger.info(f"GDELT: {gdelt_count} rows")
    
def run_silver_job(spark, **kwargs):
    from processor.stooq_to_silver_v2 import process_stooq_to_silver
        
    logger.info("--- Stooq: Bronze → Silver ---")
    row_count = process_stooq_to_silver(spark=spark)
    logger.info(f"Process {row_count} rows")
    
# Job registry
JOBS = {
    'bronze': run_bronze_job,
    'silver': run_silver_job
}

def run_jobs(job_names: List[str], **kwargs) -> Dict[str, Any]:
    results = {}
    
    logger.info(f"PIPELINE START: {job_names}")
    logger.info(f"Parameters: {kwargs}")
    
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
                
            except Exception as e:
                logger.error(f"{job_name} failed: {e}")
                results[job_name] = {'success': False, 'error': str(e)}
                raise
        
        return results
        
    finally:
        logger.info("Stopping Spark session...")
        stop_spark()


def parse_args():
    parser = argparse.ArgumentParser(description='Spark Pipeline Runner')
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--jobs', type=str, help='Comma-separated job names: bronze,silver')
    group.add_argument('--pipeline', type=str, choices=list(PIPELINES.keys()), 
                       help='Predefined pipeline: full, bronze, silver')
    
    # Stooq parameters
    parser.add_argument('--stooq-tickers', type=str, default='NVDA,MSFT',
                        help='Comma-separated stock tickers')
    parser.add_argument('--stooq-start', type=str, default='2020-01-02',
                        help='Stooq start date (YYYY-MM-DD)')
    parser.add_argument('--stooq-end', type=str, default='2025-12-22',
                        help='Stooq end date (YYYY-MM-DD)')
    
    # GDELT parameters
    parser.add_argument('--gdelt-start', type=str, default='2019-12-31',
                        help='GDELT start date (YYYY-MM-DD)')
    parser.add_argument('--gdelt-end', type=str, default='2025-12-23',
                        help='GDELT end date (YYYY-MM-DD)')
    
    # Skip options
    parser.add_argument('--skip-stooq', type=str, default='false')
    parser.add_argument('--skip-gdelt', type=str, default='false')
   
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
        'skip_gdelt': parse_bool(args.skip_gdelt) 
    }
    
    # Run pipeline
    results = run_jobs(job_names, **kwargs)
    
    # Exit code
    failed = [k for k, v in results.items() if not v.get('success')]
    if failed:
        logger.error(f"Failed jobs: {failed}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())