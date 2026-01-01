"""
Utility functions for Spark Layer Processing
Contains helper functions.
"""
import os
import logging
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql import functions as F
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
    
    builder = SparkSession.builder \
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
        .master("spark://spark-master:7077")
    
    # Add cross-layer catalog access
    if elt_layer == 'silver':
        # Silver needs to read from Bronze
        builder = builder \
            .config("spark.sql.catalog.bronze", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.bronze.type", "hadoop") \
            .config("spark.sql.catalog.bronze.warehouse", "s3a://bronze")
    elif elt_layer == 'gold':
        # Gold needs to read from Silver (and potentially Bronze)
        builder = builder \
            .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.silver.type", "hadoop") \
            .config("spark.sql.catalog.silver.warehouse", "s3a://silver") \
            .config("spark.sql.catalog.bronze", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.bronze.type", "hadoop") \
            .config("spark.sql.catalog.bronze.warehouse", "s3a://bronze")
    
    spark = builder.getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark session '{app_name}' created successfully")
    logger.info(f"Warehouse path: {warehouse_path}")
    
    return spark


def write_in_iceberg(df, elt_layer, table_name, mode="append"):
    """Write DataFrame to Iceberg table in specified ELT layer"""
    iceberg_table = f"lakehouse.{table_name}"
    
    # Drop ingestion_timestamp if it already exists, then add a fresh one
    if "ingestion_timestamp" in df.columns:
        df = df.drop("ingestion_timestamp")
    
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


# =============================
# V2 Functions (BigQuery-based)
# =============================
import re


def _generate_bq_filter(stock_config):
    """
    Constructs a SQL WHERE clause for BigQuery GDELT filtering.
    Based on stock aliases and blacklist patterns.
    
    Args:
        stock_config: Dict with structure {ticker: {'aliases': [...], 'blacklist': [...]}}
    
    Returns:
        SQL WHERE clause string
    """
    stock_conditions = []
    
    for ticker, rules in stock_config.items():
        # Escape special regex characters in aliases
        aliases = [re.escape(a) for a in rules['aliases']]
        alias_str = "|".join(aliases)
        
        condition = f"REGEXP_CONTAINS(DocumentIdentifier, r'(?i){alias_str}')"
        
        if rules.get('blacklist'):
            blacklist = [re.escape(b) for b in rules['blacklist']]
            blk_str = "|".join(blacklist)
            condition += f" AND NOT REGEXP_CONTAINS(DocumentIdentifier, r'(?i){blk_str}')"
        
        stock_conditions.append(f"({condition})")
    
    return " OR ".join(stock_conditions)


def ingest_gdelt_gkg_to_minio_v2(spark, start_date, end_date, stock_config, gcp_project='bdsp-test'):
    """
    Ingest GDELT GKG data from BigQuery to MinIO Bronze layer.
    Filters by stock aliases using regex matching on DocumentIdentifier.
    
    Args:
        spark: SparkSession
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)
        stock_config: Dict with structure {ticker: {'aliases': [...], 'blacklist': [...]}}
        gcp_project: GCP project ID for BigQuery (default: 'bdsp-test')
    
    Example stock_config:
        {
            'NVDA': {
                'aliases': ['nvidia', 'jensen huang', 'geforce', 'rtx', 'nvda'],
                'blacklist': []
            },
            'MSFT': {
                'aliases': ['microsoft', 'satya nadella', 'azure', 'msft'],
                'blacklist': []
            }
        }
    """
    try:
        from google.cloud import bigquery
        
        logger.info(f"Starting GDELT GKG BigQuery ingestion from {start_date} to {end_date}")
        
        client = bigquery.Client(project=gcp_project)
        filter_logic = _generate_bq_filter(stock_config)
        
        # Select specific columns to minimize data transfer costs
        query = f"""
            SELECT
                DATE,
                DocumentIdentifier as source_url,
                Themes as themes,
                Organizations as organizations,
                V2Tone as tone,
                Extras as extras,
                TranslationInfo as translation_info
            FROM `gdelt-bq.gdeltv2.gkg_partitioned`
            WHERE _PARTITIONDATE BETWEEN DATE('{start_date}') AND DATE('{end_date}')
              AND (TranslationInfo IS NULL OR LENGTH(TranslationInfo) = 0)
              AND ({filter_logic})
        """
        
        logger.info("Running BigQuery job (this may take time for large date ranges)...")
        
        query_job = client.query(query)
        df_pd = query_job.to_dataframe()
        
        if df_pd.empty:
            logger.warning(f"BigQuery returned 0 rows for {start_date} to {end_date}")
            return
        
        logger.info(f"Downloaded {len(df_pd)} rows from BigQuery")
        
        # Normalize columns and write to Iceberg
        df_pd = _normalize_columns_to_lower(df_pd)
        _write_pandas_to_iceberg(spark, df_pd, 'gdelt_gkg_bq', layer='bronze', mode='append')
        
        logger.info(f"Successfully ingested {len(df_pd)} rows from GDELT GKG (BigQuery) to MinIO Bronze layer")
        
    except Exception as e:
        logger.error(f"Error ingesting GDELT GKG from BigQuery: {str(e)}")
        raise


def ingest_stooq_to_minio_v2(spark, tickers, start_date='2020-01-01', end_date=None):
    """
    Ingest Stooq stock data directly to MinIO Bronze layer with explicit date range.
    Enhanced version with configurable date range and ticker list.
    
    Args:
        spark: SparkSession
        tickers: List of stock tickers (e.g., ['NVDA', 'MSFT'])
        start_date: Start date string (YYYY-MM-DD) or datetime object
        end_date: End date string (YYYY-MM-DD) or datetime object (default: now)
    """
    try:
        logger.info(f"Starting Stooq stock data ingestion (v2) for tickers: {tickers}")
        
        # Parse dates
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        
        if end_date is None:
            end_date = datetime.now()
        elif isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
        all_data = []
        
        for ticker in tickers:
            logger.info(f"Downloading Stooq data for {ticker}: {start_date.date()} to {end_date.date()}")
            
            try:
                # Fetch data from Stooq - provides OHLCV data aligned with US Market Close
                df = pdr.DataReader(ticker, 'stooq', start=start_date, end=end_date)
                
                if not df.empty:
                    # Reset index to make Date a column
                    df = df.reset_index()
                    
                    # Add ticker column
                    df['ticker'] = ticker
                    
                    # Normalize column names
                    df.columns = [col.lower() for col in df.columns]
                    
                    all_data.append(df)
                    logger.info(f"✓ Fetched {len(df)} rows for {ticker}")
                else:
                    logger.warning(f"No data available for {ticker}")
                    
            except Exception as e:
                logger.error(f"Stooq failed for {ticker}: {str(e)}")
                continue
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = _normalize_columns_to_lower(combined_df)
            _write_pandas_to_iceberg(spark, combined_df, 'stooq_stock_prices_v2', layer='bronze', mode='append')
            
            logger.info(f"Successfully ingested {len(combined_df)} stock price records from Stooq (v2) to MinIO Bronze layer")
        else:
            logger.warning("No Stooq stock data to ingest")
            
    except Exception as e:
        logger.error(f"Error ingesting Stooq stock data (v2): {str(e)}")
        raise


def ingest_gdelt_gkg_to_minio_v2_test(spark, csv_path=None):
    """
    TEST VERSION: Ingest GDELT GKG data from local CSV file to MinIO Bronze layer.
    Use this when you can't access BigQuery but have the CSV from Google Colab.
    
    Args:
        spark: SparkSession
        csv_path: Path to the CSV file. If None, will search in notebooks/data_pipeline/bronze/
                 for files matching pattern 'gdelt_bronze_bq_*.csv'
    
    Example:
        ingest_gdelt_gkg_to_minio_v2_test(spark, '/app/notebooks/data_pipeline/bronze/gdelt_bronze_bq_2025-12-01_2026-01-01.csv')
    """
    try:
        logger.info("Starting GDELT GKG CSV ingestion (TEST MODE - no BigQuery required)")
        
        # Auto-detect CSV if not provided
        if csv_path is None:
            # Try multiple possible paths (local dev vs docker)
            possible_paths = [
                '/app/notebooks/data_pipeline/bronze',  # Docker path
                './notebooks/data_pipeline/bronze',      # Relative from project root
                '../notebooks/data_pipeline/bronze',     # Relative from src/
            ]
            
            bronze_dir = None
            for path in possible_paths:
                if os.path.exists(path):
                    bronze_dir = path
                    logger.info(f"Found bronze directory at: {bronze_dir}")
                    break
            
            if bronze_dir is None:
                logger.error(f"Bronze directory not found. Tried: {possible_paths}")
                logger.error("Please provide csv_path parameter explicitly or ensure notebooks/data_pipeline/bronze/ exists")
                return
            
            csv_files = [f for f in os.listdir(bronze_dir) if f.startswith('gdelt_bronze_bq_') and f.endswith('.csv')]
            if csv_files:
                csv_path = os.path.join(bronze_dir, csv_files[0])
                logger.info(f"Auto-detected CSV file: {csv_path}")
            else:
                logger.error(f"No GDELT CSV files found in {bronze_dir}")
                return
        
        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found: {csv_path}")
            return
        
        logger.info(f"Reading CSV from: {csv_path}")
        df_pd = pd.read_csv(csv_path, low_memory=False)
        
        if df_pd.empty:
            logger.warning(f"CSV file is empty: {csv_path}")
            return
        
        logger.info(f"Loaded {len(df_pd)} rows from CSV")
        
        # Normalize columns and write to Iceberg
        df_pd = _normalize_columns_to_lower(df_pd)
        _write_pandas_to_iceberg(spark, df_pd, 'gdelt_gkg_bq', layer='bronze', mode='append')
        
        logger.info(f"Successfully ingested {len(df_pd)} rows from GDELT GKG CSV to MinIO Bronze layer")
        
    except Exception as e:
        logger.error(f"Error ingesting GDELT GKG from CSV: {str(e)}")
        raise


# =============================
# Silver Layer Utilities
# =============================

def assign_trading_date(row):
    """
    Assign trading date based on 9:00 AM cutoff (US/Eastern).
    - News before 09:00 AM on day T -> Belongs to trading date T (used to predict T).
    - News after 09:00 AM on day T -> Belongs to trading date T+1 (too late for T).
    """
    dt_est = row['timestamp_et']
    if pd.isnull(dt_est):
        return None

    # Cutoff at 9:00 AM
    if dt_est.hour < 9:
        return dt_est.normalize()
    else:
        return (dt_est + timedelta(days=1)).normalize()


def process_gdelt_silver_from_iceberg(spark, stock_config, output_table='news_silver'):
    """
    Process GDELT GKG data from Bronze Iceberg table to Silver layer using PySpark.
    
    Args:
        spark: SparkSession
        stock_config: Dict with structure {ticker: {'aliases': [...], 'blacklist': [...]}}
        output_table: Name of output Silver table
    """
    try:
        from pyspark.sql.functions import (
            col, when, length, to_timestamp, 
            from_utc_timestamp, date_format, lower, hour, lit, 
            to_date, coalesce
        )
        from pyspark.sql.types import StringType
        from pyspark.sql.functions import udf
        import re
        
        # Define UDF functions inline to avoid serialization issues
        def extract_xml_title(extras_str):
            """Extract title from GDELT Extras XML field"""
            if not isinstance(extras_str, str):
                return None
            try:
                match = re.search(r'<PAGE_TITLE>(.*?)</PAGE_TITLE>', extras_str)
                if match:
                    return match.group(1).strip()
            except:
                pass
            return None
        
        def extract_url_title(url):
            """Extract title from URL slug as fallback"""
            if not isinstance(url, str):
                return ""
            try:
                slug = re.search(r'([^/]+)(?:\.html|\?|$)', url.strip('/'))
                if slug:
                    text = slug.group(1)
                    text = re.sub(r'\d+', '', text)
                    text = text.replace('-', ' ').replace('_', ' ').replace('+', ' ')
                    return text.strip().title()
            except:
                pass
            return ""
        
        logger.info("Reading GDELT GKG data from Bronze Iceberg table...")
        # Read from Bronze catalog
        df = spark.read.format("iceberg").load("bronze.gdelt_gkg_bq")
        
        row_count = df.count()
        logger.info(f"Loaded {row_count} rows from Bronze")
        
        if row_count == 0:
            logger.warning("No data found in Bronze layer")
            return
        
        # Step 1: Extract titles using UDFs for clarity
        logger.info("Step 1: Extracting titles...")
        
        # Register UDFs
        extract_xml_title_udf = udf(extract_xml_title, StringType())
        extract_url_title_udf = udf(extract_url_title, StringType())
        
        # Extract XML title
        df = df.withColumn("xml_title", extract_xml_title_udf(col("extras")))
        
        # Extract URL slug title
        df = df.withColumn("url_title", extract_url_title_udf(col("source_url")))
        
        # Use XML title if available, otherwise URL title
        df = df.withColumn(
            "extracted_title",
            when(col("xml_title").isNotNull(), col("xml_title"))
            .otherwise(col("url_title"))
        )
        
        # Filter titles with length > 10
        df = df.filter(length(col("extracted_title")) > 10)
        logger.info(f"After title extraction: {df.count()} rows")
        
        # Step 2: Timezone processing
        logger.info("Step 2: Processing timestamps and timezone conversion...")
        
        # Convert date column to timestamp (format: YYYYMMDDHHmmSS)
        df = df.withColumn(
            "timestamp_utc",
            to_timestamp(col("date").cast("string"), "yyyyMMddHHmmss")
        )
        df = df.filter(col("timestamp_utc").isNotNull())
        
        # Convert to US/Eastern timezone
        df = df.withColumn(
            "timestamp_et",
            from_utc_timestamp(col("timestamp_utc"), "America/New_York")
        )
        
        # Assign trading date: before 9 AM -> same day, after 9 AM -> next day
        df = df.withColumn(
            "trading_date",
            when(hour(col("timestamp_et")) < 9, to_date(col("timestamp_et")))
            .otherwise(date_format(col("timestamp_et") + F.expr("INTERVAL 1 DAY"), "yyyy-MM-dd"))
        )
        df = df.filter(col("trading_date").isNotNull())
        logger.info(f"After timezone processing: {df.count()} rows")
        
        # Step 3: Filter by stock aliases using Spark SQL
        logger.info("Step 3: Filtering by stock aliases...")
        
        all_ticker_dfs = []
        
        for ticker, rules in stock_config.items():
            aliases = rules['aliases']
            alias_pattern = '|'.join([re.escape(a) for a in aliases])
            
            # Filter by aliases (case-insensitive)
            ticker_df = df.filter(
                lower(col("extracted_title")).rlike(f"(?i){alias_pattern}")
            )
            
            # Apply blacklist if present
            if rules.get('blacklist'):
                blk_pattern = '|'.join([re.escape(b) for b in rules['blacklist']])
                ticker_df = ticker_df.filter(
                    ~(lower(col("extracted_title")).rlike(f"(?i){blk_pattern}") |
                      lower(col("source_url")).rlike(f"(?i){blk_pattern}"))
                )
            
            # Add ticker column
            ticker_df = ticker_df.withColumn("ticker", lit(ticker))
            
            ticker_count = ticker_df.count()
            if ticker_count > 0:
                all_ticker_dfs.append(ticker_df)
                logger.info(f"Found {ticker_count} rows for {ticker}")
        
        if all_ticker_dfs:
            # Union all ticker dataframes
            from functools import reduce
            df_silver = reduce(lambda df1, df2: df1.union(df2), all_ticker_dfs)
            
            # Select and rename columns
            df_silver = df_silver.select(
                col("trading_date").alias("date"),
                col("ticker"),
                col("extracted_title"),
                col("source_url"),
                col("timestamp_et")
            )
            
            # Remove duplicates
            df_silver = df_silver.dropDuplicates(["date", "ticker", "extracted_title"])
            
            final_count = df_silver.count()
            logger.info(f"Total Silver news: {final_count} rows")
            
            # Write to Iceberg
            write_in_iceberg(df_silver, 'silver', output_table, mode='append')
            logger.info(f"✓ Saved Silver News to Iceberg: {output_table}")
        else:
            logger.warning("No relevant news found after filtering")
            
    except Exception as e:
        logger.error(f"Error processing GDELT Silver: {str(e)}")
        raise


def process_stooq_silver_from_iceberg(spark, output_table='stocks_silver'):
    """
    Process Stooq stock data from Bronze Iceberg table to Silver layer using PySpark.
    
    Args:
        spark: SparkSession
        output_table: Name of output Silver table
    """
    try:
        from pyspark.sql.functions import col, to_date
        
        logger.info("Reading Stooq data from Bronze Iceberg table...")
        # Read from Bronze catalog
        df = spark.read.format("iceberg").load("bronze.stooq_stock_prices_v2")
        
        row_count = df.count()
        logger.info(f"Loaded {row_count} rows from Bronze")
        
        if row_count == 0:
            logger.warning("No stock data found in Bronze layer")
            return
        
        # Check if date column exists
        if 'date' not in df.columns:
            logger.error("Date column not found in stock data")
            return
        
        # Convert date to proper format
        df = df.withColumn("date", to_date(col("date")))
        
        # Select silver layer columns
        silver_cols = ['date', 'ticker', 'close', 'open', 'high', 'low', 'volume']
        existing_cols = [c for c in silver_cols if c in df.columns]
        
        df_silver = df.select(*existing_cols)
        
        # Write to Iceberg
        write_in_iceberg(df_silver, 'silver', output_table, mode='append')
        logger.info(f"✓ Saved {df_silver.count()} stock records to Iceberg: {output_table}")
            
    except Exception as e:
        logger.error(f"Error processing Stooq Silver: {str(e)}")
        raise


def generate_nlp_input_and_score(spark, silver_news_table='news_silver', output_table='nlp_output_scored'):
    """
    Generate NLP input from silver news and simulate sentiment scoring using PySpark.
    In production, replace simulate logic with actual NLP model.
    
    Args:
        spark: SparkSession
        silver_news_table: Name of silver news table
        output_table: Name of output table with sentiment scores
    """
    try:
        from pyspark.sql.functions import col, rand
        
        logger.info("NLP Processing - Generating sentiment scores...")
        # Read from Silver's own lakehouse catalog
        df = spark.read.format("iceberg").load(f"lakehouse.{silver_news_table}")
        
        row_count = df.count()
        if row_count == 0:
            logger.warning("No news data found for NLP processing")
            return
        
        logger.info(f"Processing {row_count} news articles for sentiment analysis")
        
        # Select and rename columns for NLP input
        nlp_input = df.select(
            col("date"),
            col("ticker").alias("entity"),
            col("extracted_title").alias("title")
        )
        
        # SIMULATE NLP OUTPUT (Replace with actual model in production)
        # Generate random sentiment scores between -1 and 1
        scored_df = nlp_input.withColumn(
            "sentiment_score",
            (rand() * 2) - 1  # Maps rand() from [0,1] to [-1,1]
        )
        logger.info("✓ Sentiment scoring completed (simulated)")
        
        # Write to Iceberg
        write_in_iceberg(scored_df, 'silver', output_table, mode='append')
        logger.info(f"✓ Saved sentiment scores to Iceberg: {output_table}")
        
    except Exception as e:
        logger.error(f"Error in NLP processing: {str(e)}")
        raise


def aggregate_daily_signals(spark, scored_table='nlp_output_scored', output_table='daily_signals_advanced'):
    """
    Aggregate sentiment scores to daily signals per ticker using PySpark.
    
    Args:
        spark: SparkSession
        scored_table: Name of table with sentiment scores
        output_table: Name of output table with daily aggregates
    """
    try:
        from pyspark.sql.functions import col, avg, sum as spark_sum, count, when, coalesce, lit
        
        logger.info("Aggregating sentiment scores to daily signals...")
        # Read from Silver's own lakehouse catalog
        df = spark.read.format("iceberg").load(f"lakehouse.{scored_table}")
        
        row_count = df.count()
        if row_count == 0:
            logger.warning("No scored data found for aggregation")
            return
        
        # Aggregate sentiment scores by date and entity (ticker)
        daily_signals = df.groupBy("date", "entity").agg(
            avg("sentiment_score").alias("sentiment_avg"),
            spark_sum("sentiment_score").alias("sentiment_sum"),
            count("*").alias("news_count"),
            spark_sum(when(col("sentiment_score") > 0, 1).otherwise(0)).alias("positive_count")
        )
        
        # Calculate polarity ratio (positive / total)
        daily_signals = daily_signals.withColumn(
            "polarity_ratio",
            coalesce((col("positive_count") / col("news_count")), lit(0.5))
        )
        
        # Rename entity to ticker
        daily_signals = daily_signals.withColumnRenamed("entity", "ticker")
        
        # Select final columns (drop positive_count as it's only needed for calculation)
        daily_signals = daily_signals.select(
            "date", "ticker", "sentiment_avg", "sentiment_sum", "news_count", "polarity_ratio"
        )
        
        signal_count = daily_signals.count()
        logger.info(f"Generated {signal_count} daily signal records")
        
        # Write to Iceberg
        write_in_iceberg(daily_signals, 'silver', output_table, mode='append')
        logger.info(f"✓ Saved daily signals to Iceberg: {output_table}")
        
    except Exception as e:
        logger.error(f"Error aggregating daily signals: {str(e)}")
        raise


# =============================
# Gold Layer Utilities
# =============================


def create_gold_layer_from_iceberg(spark, ml_output_table='ml_training_data', dashboard_output_table='superset_dashboard_data'):
    """
    Create Gold layer by merging stocks and sentiment signals from Silver Iceberg tables using PySpark.
    Generates ML-ready features and dashboard data.
    
    Args:
        spark: SparkSession
        ml_output_table: Name of ML training data output table
        dashboard_output_table: Name of dashboard data output table
    """
    try:
        from pyspark.sql.functions import (
            col, coalesce, lit, when, lag, lead, avg as spark_avg, stddev,
            row_number
        )
        from pyspark.sql.window import Window
        
        logger.info("Starting Gold Layer aggregation...")
        logger.info("Step 1: Loading data from Silver layer...")
        
        # Read from Silver catalog
        stocks = spark.read.format("iceberg").load("silver.stocks_silver")
        news_signals = spark.read.format("iceberg").load("silver.daily_signals_advanced")
        
        stocks_count = stocks.count()
        news_count = news_signals.count()
        logger.info(f"Loaded {stocks_count} stock records and {news_count} news signal records")
        
        # Standardize ticker names in news_signals
        news_signals = news_signals.withColumn(
            "ticker",
            when(col("ticker").isin(["Nvidia", "NVIDIA"]), "NVDA")
            .when(col("ticker").isin(["Microsoft", "MICROSOFT"]), "MSFT")
            .otherwise(col("ticker"))
        )
        
        # Rename news_count to news_volume if it exists
        if "news_count" in news_signals.columns:
            news_signals = news_signals.withColumnRenamed("news_count", "news_volume")
        
        # Get valid tickers from stocks
        valid_tickers = [row.ticker for row in stocks.select("ticker").distinct().collect()]
        news_signals = news_signals.filter(col("ticker").isin(valid_tickers))
        logger.info(f"Processing tickers: {valid_tickers}")
        
        # Merge stocks with news signals (left join)
        logger.info("Step 2: Merging stocks with news signals...")
        gold_df = stocks.join(news_signals, on=["date", "ticker"], how="left")
        
        # Fill nulls from left join
        gold_df = gold_df.withColumn("sentiment_avg", coalesce(col("sentiment_avg"), lit(0)))
        gold_df = gold_df.withColumn("sentiment_sum", coalesce(col("sentiment_sum"), lit(0)))
        gold_df = gold_df.withColumn("polarity_ratio", coalesce(col("polarity_ratio"), lit(0.5)))
        gold_df = gold_df.withColumn("news_volume", coalesce(col("news_volume"), lit(0)))
        
        # Feature engineering using window functions
        logger.info("Step 3: Engineering features...")
        
        # Define window partitioned by ticker, ordered by date
        window_spec = Window.partitionBy("ticker").orderBy("date")
        window_spec_rows = Window.partitionBy("ticker").orderBy("date").rowsBetween(-6, 0)
        
        # Calculate returns and moving averages
        gold_df = gold_df.withColumn(
            "prev_close",
            lag("close", 1).over(window_spec)
        )
        gold_df = gold_df.withColumn(
            "return_t",
            (col("close") - col("prev_close")) / col("prev_close")
        )
        
        # 7-day moving average
        gold_df = gold_df.withColumn(
            "ma_7",
            spark_avg("close").over(window_spec_rows)
        )
        
        # 7-day volatility (standard deviation of returns)
        gold_df = gold_df.withColumn(
            "volatility_7",
            stddev("return_t").over(window_spec_rows)
        )
        
        # Target: next day's return (lead return by -1)
        gold_df = gold_df.withColumn(
            "next_return",
            lead("return_t", 1).over(window_spec)
        )
        gold_df = gold_df.withColumnRenamed("next_return", "target_next_return")
        
        # Drop helper column
        gold_df = gold_df.drop("prev_close")
        
        # Log processing per ticker
        for ticker in valid_tickers:
            ticker_count = gold_df.filter(col("ticker") == ticker).count()
            logger.info(f"Processed {ticker_count} days for {ticker}")
        
        # Save tables
        logger.info("Step 4: Saving Gold tables...")
        
        # A. ML training data: filter out rows with null targets
        ml_cols = [
            'date', 'ticker', 'close', 'open', 'high', 'low', 'volume',
            'sentiment_avg', 'sentiment_sum', 'polarity_ratio', 'news_volume',
            'return_t', 'ma_7', 'volatility_7', 'target_next_return'
        ]
        existing_ml_cols = [c for c in ml_cols if c in gold_df.columns]
        
        ml_data = gold_df.filter(col("target_next_return").isNotNull()).select(*existing_ml_cols)
        
        ml_count = ml_data.count()
        write_in_iceberg(ml_data, 'gold', ml_output_table, mode='append')
        logger.info(f"✓ Saved ML training data: {ml_output_table} ({ml_count} rows)")
        
        # B. Dashboard data: keep all rows
        write_in_iceberg(gold_df, 'gold', dashboard_output_table, mode='append')
        dashboard_count = gold_df.count()
        logger.info(f"✓ Saved dashboard data: {dashboard_output_table} ({dashboard_count} rows)")
        
        # Validation
        logger.info("\n=== Sample Data Validation ===")
        sample_with_news = ml_data.filter(col("news_volume") > 0).limit(5)
        sample_count = sample_with_news.count()
        
        if sample_count > 0:
            logger.info("Sample rows with news (first 5):")
            for row in sample_with_news.collect():
                logger.info(f"  {row['date']} | {row['ticker']} | Sentiment: {row['sentiment_avg']:.3f} | News: {int(row['news_volume'])}")
        else:
            logger.warning("WARNING: No rows with news found. Check if sentiment data exists in Silver layer.")
        
    except Exception as e:
        logger.error(f"Error creating Gold layer: {str(e)}")
        raise


def generate_analytics_summary(spark):
    """
    Generate analytics summary from Gold layer data using PySpark aggregations.
    This replaces the EDA/visualization with logging-based insights.
    
    Args:
        spark: SparkSession
    """
    try:
        from pyspark.sql.functions import (
            col, min as spark_min, max as spark_max, count, avg as spark_avg,
            stddev, sum as spark_sum, when
        )
        
        logger.info("\n" + "="*60)
        logger.info("GENERATING ANALYTICS SUMMARY")
        logger.info("="*60)
        
        # Read from Gold's own lakehouse catalog
        df = spark.read.format("iceberg").load("lakehouse.superset_dashboard_data")
        
        row_count = df.count()
        if row_count == 0:
            logger.warning("No data found in Gold layer")
            return
        
        # Calculate sentiment_sum if missing
        if 'sentiment_sum' not in df.columns:
            if 'news_volume' in df.columns and 'sentiment_avg' in df.columns:
                df = df.withColumn("sentiment_sum", col("sentiment_avg") * col("news_volume"))
            else:
                df = df.withColumn("sentiment_sum", col("sentiment_avg"))
        
        # Dataset Summary
        logger.info("\n--- Dataset Summary ---")
        logger.info(f"Total records: {row_count}")
        
        date_range = df.agg(
            spark_min("date").alias("min_date"),
            spark_max("date").alias("max_date")
        ).collect()[0]
        logger.info(f"Date range: {date_range['min_date']} to {date_range['max_date']}")
        
        tickers = [row.ticker for row in df.select("ticker").distinct().collect()]
        logger.info(f"Tickers: {tickers}")
        
        # Per-ticker statistics
        for ticker in tickers:
            ticker_df = df.filter(col("ticker") == ticker)
            
            stats = ticker_df.agg(
                count("*").alias("total_records"),
                spark_min("close").alias("min_price"),
                spark_max("close").alias("max_price"),
                spark_sum(when(col("news_volume") > 0, 1).otherwise(0)).alias("days_with_news"),
                spark_avg(when(col("news_volume") > 0, col("news_volume"))).alias("avg_news_volume"),
                spark_avg("sentiment_avg").alias("avg_sentiment"),
                stddev("sentiment_avg").alias("std_sentiment")
            ).collect()[0]
            
            logger.info(f"\n--- {ticker} Statistics ---")
            logger.info(f"  Records: {stats['total_records']}")
            logger.info(f"  Price range: ${stats['min_price']:.2f} - ${stats['max_price']:.2f}")
            
            if stats['days_with_news'] > 0:
                news_pct = (stats['days_with_news'] / stats['total_records']) * 100
                logger.info(f"  Days with news: {stats['days_with_news']} ({news_pct:.1f}%)")
                logger.info(f"  Avg news per day (when present): {stats['avg_news_volume']:.1f}")
                logger.info(f"  Sentiment avg: {stats['avg_sentiment']:.3f}")
                logger.info(f"  Sentiment std: {stats['std_sentiment']:.3f}")
            else:
                logger.info(f"  Days with news: 0 (No news data)")
        
        # Sentiment Distribution
        logger.info("\n--- Sentiment Distribution ---")
        for ticker in tickers:
            ticker_df = df.filter((col("ticker") == ticker) & (col("news_volume") > 0))
            
            sentiment_stats = ticker_df.agg(
                count("*").alias("total_with_news"),
                spark_avg("sentiment_avg").alias("mean"),
                stddev("sentiment_avg").alias("std"),
                spark_min("sentiment_avg").alias("min"),
                spark_max("sentiment_avg").alias("max"),
                spark_sum(when(col("sentiment_avg") > 0.1, 1).otherwise(0)).alias("positive"),
                spark_sum(when((col("sentiment_avg") >= -0.1) & (col("sentiment_avg") <= 0.1), 1).otherwise(0)).alias("neutral"),
                spark_sum(when(col("sentiment_avg") < -0.1, 1).otherwise(0)).alias("negative")
            ).collect()[0]
            
            if sentiment_stats['total_with_news'] > 0:
                logger.info(f"{ticker}:")
                logger.info(f"  Mean: {sentiment_stats['mean']:.3f}")
                logger.info(f"  Std: {sentiment_stats['std']:.3f}")
                logger.info(f"  Min: {sentiment_stats['min']:.3f}")
                logger.info(f"  Max: {sentiment_stats['max']:.3f}")
                
                total = sentiment_stats['total_with_news']
                positive_pct = (sentiment_stats['positive'] / total) * 100
                neutral_pct = (sentiment_stats['neutral'] / total) * 100
                negative_pct = (sentiment_stats['negative'] / total) * 100
                
                logger.info(f"  Positive (>0.1): {sentiment_stats['positive']} ({positive_pct:.1f}%)")
                logger.info(f"  Neutral (-0.1 to 0.1): {sentiment_stats['neutral']} ({neutral_pct:.1f}%)")
                logger.info(f"  Negative (<-0.1): {sentiment_stats['negative']} ({negative_pct:.1f}%)")
        
        logger.info("\n" + "="*60)
        logger.info("✓ Analytics summary completed")
        logger.info("="*60 + "\n")
        
    except Exception as e:
        logger.error(f"Error generating analytics summary: {str(e)}")
        raise


