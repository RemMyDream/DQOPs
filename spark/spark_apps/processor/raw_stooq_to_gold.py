import sys
import os
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, col, to_date 

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from create_spark_connection import create_spark_connection
from utils.helpers import create_logger

logger = create_logger("bronze_to_gold_stooq")


class StooqGoldService:
    
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
    
    def process_bronze_to_gold(self) -> int:
        bronze_stooq = self.spark.table("bronze.stooq")
        
        gold_df = bronze_stooq.select(
            to_date(col("date")).alias("date"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("ticker")
        )
        
        gold_df = gold_df.withColumn("ingestion_timestamp", current_timestamp())
        
        gold_table = "gold.stooq"
        table_exists = self._table_exists(gold_table)
        
        if not table_exists:
            gold_df.write.format("iceberg").mode("overwrite").saveAsTable(gold_table)
        else:
            self._merge_into_table(gold_df, gold_table, ["date", "ticker"])
        
        return self.spark.table(gold_table).count()


def process_bronze_to_gold_stooq() -> int:
    spark = create_spark_connection()
    try:
        service = StooqGoldService(spark)
        return service.process_bronze_to_gold()
    finally:
        spark.stop()

def main():
    row_count = process_bronze_to_gold_stooq()
    logger.info(f"Processing completed: {row_count} rows in gold.stooq")

if __name__ == "__main__":
    main()
