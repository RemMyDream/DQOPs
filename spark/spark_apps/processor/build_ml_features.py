import sys
import os
from typing import List
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, lead, current_timestamp, when
from pyspark.sql.types import NumericType

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from create_spark_connection import create_spark_connection
from utils.helpers import create_logger

logger = create_logger("build_ml_features")


class MLFeatureBuilder:
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def build_features(self) -> DataFrame:
        stock_indicators = self.spark.table("silver.stock_indicators")
        sentiment_features = self.spark.table("gold.sentiment_features")
        
        stock_indicators = stock_indicators.drop("ingestion_timestamp")
        stock_indicators = stock_indicators.withColumnRenamed("Date", "date")  
        
        sentiment_features = sentiment_features.withColumn(
            "ticker",
            when(col("ticker") == "Nvidia", "NVDA")
            .when(col("ticker") == "Microsoft", "MSFT")
            .otherwise(col("ticker"))
        )

        df = stock_indicators.join(
            sentiment_features.select("date", "ticker", "sentiment_sum", "polarity_ratio"),
            on=["date", "ticker"],
            how="left"
        )
        
        df = df.fillna(0.0, subset=["sentiment_sum", "polarity_ratio"])
        
        window_spec = Window.partitionBy("ticker").orderBy("date")
        df = df.withColumn("next_close", lead("Return", 1).over(window_spec))
        df = df.withColumn("next_day_return", col("next_close"))
        df = df.drop("next_close")
                
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        
        return df

class GoldIngestionService:
    
    def __init__(self, spark: SparkSession, target_catalog: str = "gold"):
        self.spark = spark
        self.target_catalog = target_catalog
        self.builder = MLFeatureBuilder(spark)
    
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
    
    def process_ml_features(self) -> int:
        features_df = self.builder.build_features()
        
        gold_table = f"{self.target_catalog}.ml_features"
        table_exists = self._table_exists(gold_table)
        
        if not table_exists:
            features_df.write.format("iceberg").mode("overwrite").saveAsTable(gold_table)
        else:
            self._merge_into_table(features_df, gold_table, ["date", "ticker"])
        
        return self.spark.table(gold_table).count()


def build_ml_features(target_catalog: str = "gold") -> int:
    spark = create_spark_connection()
    try:
        service = GoldIngestionService(spark, target_catalog)
        return service.process_ml_features()
    finally:
        spark.stop()


def main():
    row_count = build_ml_features()
    logger.info(f"Processing completed: {row_count} rows in ml_features table")

if __name__ == "__main__":
    main()