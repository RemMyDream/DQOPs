# spark_apps/ingestion/postgres_ingestion.py
import sys
import os
from pyspark.sql import SparkSession, DataFrame
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("PostgresIngestion")


def get_partition_bounds(spark: SparkSession, jdbc_url: str, properties: dict, 
                         schema_name: str, table_name: str, partition_column: str) -> tuple:
    """Lấy min/max của partition column"""
    query = f"""
        (SELECT MIN("{partition_column}") AS lower, MAX("{partition_column}") AS upper 
         FROM "{schema_name}"."{table_name}") AS bounds
    """
    
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
    row = df.first()
    
    return row["lower"], row["upper"]


def read_data_from_postgres(
    spark: SparkSession,
    conn_config: dict,
    schema_name: str,
    table_name: str,
    primary_keys: list,
    num_partitions: int = 8
) -> DataFrame:
    """
    Đọc data từ PostgreSQL
    
    Args:
        spark: SparkSession
        conn_config: Dict chứa jdbc_url, username, password
        schema_name: Schema name
        table_name: Table name
        primary_keys: List primary keys
        num_partitions: Số partitions
    """
    jdbc_url = conn_config["jdbc_url"]
    properties = {
        "user": conn_config["username"],
        "password": conn_config["password"],
        "driver": "org.postgresql.Driver"
    }
    
    partition_column = primary_keys[0] if primary_keys else None
    
    if partition_column:
        try:
            lower_bound, upper_bound = get_partition_bounds(
                spark, jdbc_url, properties,
                schema_name, table_name, partition_column
            )
            
            logger.info(f"Reading with partition on '{partition_column}' [{lower_bound}, {upper_bound}]")
            
            df = spark.read.jdbc(
                url=jdbc_url,
                table=f'"{schema_name}"."{table_name}"',
                column=partition_column,
                lowerBound=lower_bound,
                upperBound=upper_bound,
                numPartitions=num_partitions,
                properties=properties
            )
        except Exception as e:
            logger.warning(f"Cannot partition on '{partition_column}': {e}. Reading without partition.")
            partition_column = None
    
    if not partition_column:
        logger.info(f"Reading without partition from {schema_name}.{table_name}")
        df = spark.read.jdbc(
            url=jdbc_url,
            table=f'"{schema_name}"."{table_name}"',
            properties=properties
        )
    
    row_count = df.count()
    logger.info(f"Successfully read {row_count} rows from {schema_name}.{table_name}")
    
    return df