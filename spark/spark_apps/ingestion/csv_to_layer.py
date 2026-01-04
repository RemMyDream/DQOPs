# csv_to_iceberg.py

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import TimestampType

from create_spark_connection import create_spark_connection, stop_spark
from utils.helpers import create_logger

logger = create_logger("csv_to_iceberg")


class CSVToIcebergService:
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def _table_exists(self, table_name: str) -> bool:
        try:
            self.spark.sql(f"SELECT 1 FROM {table_name} LIMIT 1")
            return True
        except:
            return False
    
    def _merge_into_table(self, source_df, target_table: str, primary_keys: list):
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
    
    def ingest(
        self,
        csv_path: str,
        table_name: str,
        catalog: str = "bronze",
        primary_keys: list = None,
        mode: str = "overwrite"
    ) -> int:
        logger.info(f"Reading CSV: {csv_path}")
        
        df = self.spark.read.csv(csv_path, header=True, inferSchema=True)
        logger.info(f"Read {df.count()} rows")
        
        if 'date' in df.columns:
            df = df.withColumn("date", col("date").cast(TimestampType()))
        
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        
        iceberg_table = f"{catalog}.{table_name}"
        
        if mode == "overwrite" or not self._table_exists(iceberg_table):
            logger.info(f"Creating table: {iceberg_table}")
            df.write.format("iceberg").mode("overwrite").saveAsTable(iceberg_table)
        elif mode == "append":
            logger.info(f"Appending to: {iceberg_table}")
            df.write.format("iceberg").mode("append").saveAsTable(iceberg_table)
        elif mode == "merge" and primary_keys:
            logger.info(f"Merging into: {iceberg_table}")
            self._merge_into_table(df, iceberg_table, primary_keys)
        
        final_count = self.spark.table(iceberg_table).count()
        logger.info(f"✓ {iceberg_table}: {final_count} rows")
        
        return final_count


def ingest_csv(
    csv_path: str,
    table_name: str,
    catalog: str = "bronze",
    primary_keys: list = None,
    mode: str = "overwrite"
) -> int:
    """
    Đẩy CSV lên Iceberg table.
    
    Args:
        csv_path: Đường dẫn file CSV
        table_name: Tên bảng
        catalog: bronze / silver / gold
        primary_keys: List primary keys cho merge mode
        mode: overwrite / append / merge
    """
    spark = create_spark_connection()
    try:
        service = CSVToIcebergService(spark)
        return service.ingest(csv_path, table_name, catalog, primary_keys, mode)
    finally:
        stop_spark()


if __name__ == "__main__":
    # Overwrite
    ingest_csv(
        csv_path="/opt/spark/data/sentiment.csv",
        table_name="sentiment_features",
        catalog="gold"
    )
    