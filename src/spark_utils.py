"""
Utility functions for Spark Bronze Layer Processing
Contains helper functions for ingesting data directly to MinIO
"""
import os
import logging
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pandas_datareader import data as pdr
import gdelt

from bs4 import BeautifulSoup
import time
import requests

def get_article_title(url, timeout=15, max_retries=3):
    """
    Fetch the title of an article from a URL with retry logic
    
    Args:
        url: The URL to fetch the title from
        timeout: Request timeout in seconds
        max_retries: Maximum number of retry attempts
    
    Returns:
        The article title or None if not found after all retries
    """
    for attempt in range(max_retries):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Try different methods to find the title
            # 1. Look for <title> tag
            if soup.title and soup.title.string:
                return soup.title.string.strip()
            
            # 2. Look for og:title meta tag
            og_title = soup.find('meta', property='og:title')
            if og_title and og_title.get('content'):
                return og_title.get('content').strip()
            
            # 3. Look for twitter:title meta tag
            twitter_title = soup.find('meta', attrs={'name': 'twitter:title'})
            if twitter_title and twitter_title.get('content'):
                return twitter_title.get('content').strip()
            
            # 4. Look for h1 tag
            h1 = soup.find('h1')
            if h1:
                return h1.get_text().strip()
            
            return None
            
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}. Retrying...")
                time.sleep(2 * (attempt + 1))  # Exponential backoff
            else:
                logger.error(f"Failed to fetch title from {url} after {max_retries} attempts: {str(e)}")
                return None

# Add title column to dataframe
def crawl_titles(df, url_column='sourceurl', delay=2, timeout = 15, max_retries = 3):
    """
    Crawl titles for all URLs in the dataframe
    
    Args:
        df: The dataframe containing URLs
        url_column: Name of the column containing URLs
        delay: Delay between requests in seconds (to be polite)
    
    Returns:
        DataFrame with added 'title' column
    """
    titles = []
    
    for idx, row in df.iterrows():
        url = row[url_column]
        logger.info(f"Fetching title {idx+1}/{len(df)} from {url}")
        
        title = get_article_title(url=url, timeout=timeout, max_retries=max_retries)
        titles.append(title)
        
        # Add delay to avoid overwhelming servers
        if idx < len(df) - 1:
            time.sleep(delay)
    
    df['title'] = titles
    return df

from dotenv import load_dotenv
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MinIO Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'admin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'password')


def create_spark_session(elt_layer):
    """Create Spark session with Iceberg and MinIO configuration"""

    warehouse_path = f's3a://{elt_layer}'

    app_name = f"{elt_layer.capitalize()} Layer Processing"
    
    # Stop any existing Spark session to ensure clean config
    from pyspark import SparkContext
    try:
        SparkContext.getOrCreate().stop()
    except:
        pass
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hadoop") \
        .config("spark.sql.catalog.lakehouse.warehouse", warehouse_path) \
        .config("spark.sql.warehouse.dir", warehouse_path) \
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
    logger.info(f"Spark session '{app_name}' created successfully")
    logger.info(f"Warehouse path: {warehouse_path}")
    
    return spark


def write_in_iceberg(df, elt_layer, table_name, mode="append"):
    """Write DataFrame to Iceberg table in specified ELT layer"""
    iceberg_table = f"lakehouse.{table_name}"
    
    df_with_timestamp = df.withColumn("ingestion_timestamp", current_timestamp())
    
    try:
        df_with_timestamp.write \
            .format("iceberg") \
            .mode(mode) \
            .saveAsTable(iceberg_table)
        logger.info(f"Data written to Iceberg table {iceberg_table} successfully in {mode} mode.")
    except Exception as e:
        logger.error(f"Error writing to Iceberg table {iceberg_table}: {str(e)}")
        raise


# =============================
# Shared helpers (refactoring)
# =============================
def _generate_quarter_hour_timestamps(day_date):
    """Generate 96 quarter-hour timestamps (UTC) for a given date."""
    for minutes in range(0, 24 * 60, 15):
        dt = datetime.combine(day_date, datetime.min.time()) + timedelta(minutes=minutes)
        yield dt.strftime('%Y%m%d%H%M%S')


def _normalize_columns_to_lower(df_pd: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with all column names lower-cased."""
    df_pd.columns = [c.lower() for c in df_pd.columns]
    return df_pd


def _write_pandas_to_iceberg(spark: SparkSession, df_pd: pd.DataFrame, table_name: str, layer: str = 'bronze', mode: str = 'append'):
    """Convert pandas DataFrame to Spark and write to Iceberg using write_in_iceberg."""
    if df_pd is None or df_pd.empty:
        logger.warning("No data to write; skipping Iceberg write.")
        return
    spark_df = spark.createDataFrame(df_pd)
    write_in_iceberg(spark_df, layer, table_name, mode=mode)


def _fetch_gdelt_table_as_pandas(table: str, days: int = 2, max_records_per_timespamp=None) -> Optional[pd.DataFrame]:
    """Fetch a GDELT table (events or gkg) over the past N days, concatenated into one pandas DataFrame.

    Args:
        table: 'events' or 'gkg'
        days: number of days to pull (each day split by 15-minute intervals)
        max_records_per_timespamp: optional head limit per timestamp batch
    Returns:
        pandas DataFrame or None if no data collected
    """
    gd = gdelt.gdelt(version=2)
    all_batches: List[pd.DataFrame] = []

    for i in range(days):
        day = datetime.now(timezone.utc).date() - timedelta(days=i + 1)
        logger.info(f"Fetching GDELT {table} for day: {day}")
        for ts in _generate_quarter_hour_timestamps(day):
            try:
                results = gd.Search(ts, table=table)
                if results is not None and not results.empty:
                    df_part = results.head(max_records_per_timespamp) if max_records_per_timespamp else results
                    all_batches.append(df_part)
                    logger.info(f"Fetched {len(df_part)} rows for {table} at {ts}")
            except Exception as e:
                logger.warning(f"No data or error for {table} at {ts}: {e}")
                continue

    if not all_batches:
        logger.warning(f"No GDELT {table} data found for the specified period.")
        return None

    combined = pd.concat(all_batches, ignore_index=True)
    return _normalize_columns_to_lower(combined)


def _ingest_gdelt_table(spark: SparkSession, *, table: str, url_column: str, bronze_table: str, days: int, max_records_per_timespamp=None):
    """Shared ingestion for GDELT 'events' and 'gkg'."""
    logger.info(f"Starting GDELT {table} ingestion directly to MinIO for the last {days} days")
    combined_df = _fetch_gdelt_table_as_pandas(table=table, days=days, max_records_per_timespamp=max_records_per_timespamp)
    if combined_df is None:
        return

    combined_df = crawl_titles(df=combined_df, url_column=url_column, delay=2, timeout=15, max_retries=3)
    crawled = int(combined_df['title'].notna().sum()) if 'title' in combined_df.columns else 0
    logger.info(f"Crawled {crawled} titles for GDELT {table}")

    _write_pandas_to_iceberg(spark, combined_df, bronze_table, layer='bronze', mode='append')
    logger.info(f"Successfully ingested {len(combined_df)} rows from GDELT {table} to MinIO Bronze layer.")

# MAIN INGESTION FUNCTIONS

def ingest_gdelt_events_to_minio(spark, days=2, max_records_per_timespamp=None):
    """Ingest GDELT Events data directly to MinIO Bronze layer."""
    try:
        _ingest_gdelt_table(
            spark,
            table='events',
            url_column='sourceurl',
            bronze_table='gdelt_events',
            days=days,
            max_records_per_timespamp=max_records_per_timespamp,
        )
    except Exception as e:
        logger.error(f"Error ingesting GDELT events: {str(e)}")
        raise


def ingest_gdelt_gkg_to_minio(spark, days=2, max_records_per_timespamp=None):
    """Ingest GDELT GKG data directly to MinIO Bronze layer."""
    try:
        _ingest_gdelt_table(
            spark,
            table='gkg',
            url_column='documentidentifier',
            bronze_table='gdelt_gkg',
            days=days,
            max_records_per_timespamp=max_records_per_timespamp,
        )
    except Exception as e:
        logger.error(f"Error ingesting GDELT GKG: {str(e)}")
        raise


def ingest_stooq_to_minio(spark, symbols=['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN'], days=30):
    """
    Ingest Stooq stock data directly to MinIO Bronze layer
    
    Args:
        spark: SparkSession
        symbols: List of stock symbols to fetch
        days: Number of days of historical data to fetch (default: 30)
    """
    try:
        logger.info(f"Starting Stooq stock data ingestion directly to MinIO for symbols: {symbols}")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        all_data = []
        
        for symbol in symbols:
            try:
                logger.info(f"Fetching historical data for {symbol} from {start_date.date()} to {end_date.date()}")
                
                # Fetch historical data from Stooq
                df = pdr.DataReader(symbol, 'stooq', start_date, end_date)
                
                if not df.empty:
                    # Reset index to make Date a column
                    df = df.reset_index()
                    
                    # Add symbol column
                    df['symbol'] = symbol
                    
                    # Rename columns to lowercase and more descriptive names
                    df.columns = [col.lower() for col in df.columns]
                    df = df.rename(columns={'date': 'timestamp'})
                    
                    all_data.append(df)
                    logger.info(f"Fetched {len(df)} records for {symbol}")
                else:
                    logger.warning(f"No data available for {symbol}")
                        
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {str(e)}")
                continue
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = _normalize_columns_to_lower(combined_df)
            _write_pandas_to_iceberg(spark, combined_df, 'stooq_stock_prices', layer='bronze', mode='append')
            
            logger.info(f"Successfully ingested {len(combined_df)} stock price records from Stooq to MinIO Bronze layer")
        else:
            logger.warning("No Stooq stock data to ingest")
            
    except Exception as e:
        logger.error(f"Error ingesting Stooq stock data: {str(e)}")
        raise

