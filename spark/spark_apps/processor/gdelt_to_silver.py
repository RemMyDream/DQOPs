import sys
import os
import re
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, udf, when, length, from_utc_timestamp, 
    hour, to_date, lower, lit, current_timestamp
)
from pyspark.sql.types import StringType
from typing import List
from functools import reduce


def extract_xml_title(extras_str):
    if not isinstance(extras_str, str):
        return None
    match = re.search(r'<PAGE_TITLE>(.*?)</PAGE_TITLE>', extras_str)
    return match.group(1).strip() if match else None


def extract_url_title(url):
    if not isinstance(url, str):
        return ""
    slug = re.search(r'([^/]+)(?:\.html|\?|$)', url.strip('/'))
    if slug:
        text = slug.group(1)
        text = re.sub(r'\d+', '', text)
        text = text.replace('-', ' ').replace('_', ' ').replace('+', ' ')
        return text.strip().title()
    return ""


class GdeltSilverService:
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def _table_exists(self, table_name: str) -> bool:
        try:
            self.spark.sql(f"SELECT 1 FROM {table_name} LIMIT 1")
            return True
        except:
            return False
    
    def _merge_into_table(self, source_df: DataFrame, target_table: str, primary_keys: List[str]) -> None:
        temp_view = "source_temp_view"
        source_df.createOrReplaceTempView(temp_view)
        
        merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])
        columns = source_df.columns
        update_set = ", ".join([f"target.{c} = source.{c}" for c in columns])
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f"source.{c}" for c in columns])
        
        merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING {temp_view} AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        self.spark.sql(merge_sql)
    
    def process_gdelt_to_silver(self, stock_config: dict) -> int:
        extract_xml_udf = udf(extract_xml_title, StringType())
        extract_url_udf = udf(extract_url_title, StringType())
        
        df = self.spark.read.format("iceberg").load("bronze.gdelt_gkg")
        
        if df.count() == 0:
            return 0
        
        # Extract titles
        df = df.withColumn("xml_title", extract_xml_udf(col("extras")))
        df = df.withColumn("url_title", extract_url_udf(col("source_url")))
        df = df.withColumn(
            "extracted_title",
            when(col("xml_title").isNotNull(), col("xml_title")).otherwise(col("url_title"))
        )
        df = df.filter(length(col("extracted_title")) > 10)
        
        # Date is already TimestampType
        df = df.withColumn("timestamp_utc", col("date"))
        df = df.filter(col("timestamp_utc").isNotNull())
        
        # Convert to ET and calculate trading date
        df = df.withColumn("timestamp_et", from_utc_timestamp(col("timestamp_utc"), "America/New_York"))
        df = df.withColumn(
            "trading_date",
            when(hour(col("timestamp_et")) < 9, to_date(col("timestamp_et")))
            .otherwise(to_date(col("timestamp_et")) + 1)
        )
        df = df.filter(col("trading_date").isNotNull())
        
        # Process each ticker
        all_ticker_dfs = []
        for ticker, rules in stock_config.items():
            aliases = rules['aliases']
            alias_pattern = '|'.join([re.escape(a) for a in aliases])
            
            ticker_df = df.filter(lower(col("extracted_title")).rlike(f"(?i){alias_pattern}"))
            
            if rules.get('blacklist'):
                blk_pattern = '|'.join([re.escape(b) for b in rules['blacklist']])
                ticker_df = ticker_df.filter(
                    ~(lower(col("extracted_title")).rlike(f"(?i){blk_pattern}") |
                      lower(col("source_url")).rlike(f"(?i){blk_pattern}"))
                )
            
            ticker_df = ticker_df.withColumn("ticker", lit(ticker))
            if ticker_df.count() > 0:
                all_ticker_dfs.append(ticker_df)
        
        if not all_ticker_dfs:
            return 0
        
        # Union all ticker dataframes
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
        
        # Write to silver
        silver_table = "silver.gdelt_news"
        if not self._table_exists(silver_table):
            df_silver.write.format("iceberg").mode("overwrite").saveAsTable(silver_table)
        else:
            self._merge_into_table(df_silver, silver_table, ["date", "ticker", "extracted_title"])
        
        return self.spark.table(silver_table).count()


def process_gdelt_to_silver(spark: SparkSession, stock_config: dict):
    service = GdeltSilverService(spark)
    return service.process_gdelt_to_silver(stock_config)