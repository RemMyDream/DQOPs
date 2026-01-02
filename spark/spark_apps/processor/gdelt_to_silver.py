import sys
import os
import re
from datetime import timedelta
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from create_spark_connection import create_spark_connection
from utils.helpers import create_logger, load_cfg
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, when, length, to_timestamp, from_utc_timestamp, 
    hour, to_date, date_format, lower, lit, current_timestamp
)
from pyspark.sql.types import StringType, DateType

logger = create_logger("bronze_to_silver_gdelt")

def extract_xml_title(extras_str):
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


def process_gdelt_to_silver(spark: SparkSession, stock_config: dict):
    logger.info("Starting GDELT Bronze to Silver transformation")
    
    extract_xml_udf = udf(extract_xml_title, StringType())
    extract_url_udf = udf(extract_url_title, StringType())
    
    logger.info("Reading GDELT GKG from Bronze")
    df = spark.read.format("iceberg").load("bronze.gdelt_gkg")
    
    row_count = df.count()
    logger.info(f"Loaded {row_count} rows from Bronze")
    
    logger.info("Step 1: Extracting titles")
    df = df.withColumn("xml_title", extract_xml_udf(col("extras")))
    df = df.withColumn("url_title", extract_url_udf(col("source_url")))
    df = df.withColumn(
        "extracted_title",
        when(col("xml_title").isNotNull(), col("xml_title"))
        .otherwise(col("url_title"))
    )
    
    df = df.filter(length(col("extracted_title")) > 10)
    logger.info(f"After title extraction: {df.count()} rows")
    
    logger.info("Step 2: Timezone conversion and trading date assignment")
    df = df.withColumn(
        "timestamp_utc",
        col("date")
    )
    df = df.filter(col("timestamp_utc").isNotNull())
    
    df = df.withColumn(
        "timestamp_et",
        from_utc_timestamp(col("timestamp_utc"), "America/New_York")
    )
    
    df = df.withColumn(
        "trading_date",
        when(hour(col("timestamp_et")) < 9, to_date(col("timestamp_et")))
        .otherwise(to_date(col("timestamp_et")) + 1)
    )
    df = df.filter(col("trading_date").isNotNull())
    logger.info(f"After timezone processing: {df.count()} rows")
    
    logger.info("Step 3: Filtering by stock aliases")
    all_ticker_dfs = []
    
    for ticker, rules in stock_config.items():
        aliases = rules['aliases']
        alias_pattern = '|'.join([re.escape(a) for a in aliases])
        
        ticker_df = df.filter(
            lower(col("extracted_title")).rlike(f"(?i){alias_pattern}")
        )
        
        if rules.get('blacklist'):
            blk_pattern = '|'.join([re.escape(b) for b in rules['blacklist']])
            ticker_df = ticker_df.filter(
                ~(lower(col("extracted_title")).rlike(f"(?i){blk_pattern}") |
                  lower(col("source_url")).rlike(f"(?i){blk_pattern}"))
            )
        
        ticker_df = ticker_df.withColumn("ticker", lit(ticker))
        
        ticker_count = ticker_df.count()
        if ticker_count > 0:
            all_ticker_dfs.append(ticker_df)
            logger.info(f"Found {ticker_count} rows for {ticker}")
    
    if all_ticker_dfs:
        from functools import reduce
        df_silver = reduce(lambda df1, df2: df1.union(df2), all_ticker_dfs)
        
        df_silver = df_silver.select(
            col("trading_date").alias("date"),
            col("ticker"),
            col("extracted_title"),
            col("source_url"),
            col("timestamp_et")
        )
        
        df_silver = df_silver.dropDuplicates(["date", "ticker", "extracted_title"])
        df_silver = df_silver.withColumn("ingestion_timestamp", current_timestamp())
        
        final_count = df_silver.count()
        logger.info(f"Total Silver news: {final_count} rows")
        
        logger.info("Writing to Silver layer")
        df_silver.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable("silver.gdelt_news")
        
        logger.info("Silver layer created successfully")
    else:
        logger.warning("No relevant news found after filtering")


def main():
    logger.info("Starting GDELT Bronze to Silver Pipeline")
    
    stock_config = load_cfg("utils/config.yaml")['stock_config']
    spark = create_spark_connection()
    
    try:
        process_gdelt_to_silver(spark, stock_config)
        logger.info("Pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    spark = create_spark_connection()
    df = spark.read.parquet("s3a://silver/stock_indicators/data")
    df.printSchema()
    print("Row count:", df.count())
