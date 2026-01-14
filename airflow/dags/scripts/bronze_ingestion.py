#!/usr/bin/env python3

import sys
import os
import json
import argparse
import logging
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BronzeIngestion")


def create_spark_session(app_name: str = "BronzeIngestion") -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("SparkSession created successfully")
    return spark

def get_partition_bounds(
    spark: SparkSession,
    jdbc_url: str,
    properties: dict,
    schema_name: str,
    table_name: str,
    partition_column: str
) -> tuple:
    query = f"""
        (SELECT MIN("{partition_column}") AS lower, MAX("{partition_column}") AS upper 
         FROM "{schema_name}"."{table_name}") AS bounds
    """
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
    row = df.first()
    return row["lower"], row["upper"]

def read_from_postgres(
    spark: SparkSession,
    jdbc_url: str,
    username: str,
    password: str,
    schema_name: str,
    table_name: str,
    primary_keys: List[str],
    num_partitions: int = 8
) -> DataFrame:
    properties = {
        "user": username,
        "password": password,
        "driver": "org.postgresql.Driver"
    }
    
    full_table = f'"{schema_name}"."{table_name}"'
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
                table=full_table,
                column=partition_column,
                lowerBound=lower_bound,
                upperBound=upper_bound,
                numPartitions=num_partitions,
                properties=properties
            )
            return df
        except Exception as e:
            logger.warning(f"Cannot partition on '{partition_column}': {e}")
    
    logger.info(f"Reading without partition from {full_table}")
    return spark.read.jdbc(url=jdbc_url, table=full_table, properties=properties)


def write_to_iceberg(
    spark: SparkSession,
    df: DataFrame,
    catalog: str,
    database: str,
    table_name: str,
    primary_keys: List[str],
    partition_cols: Optional[List[str]] = None,
    mode: str = "merge"
) -> int:
    
    warehouse = spark.conf.get("spark.sql.catalog.bronze.warehouse")
    logger.info(f"DEBUG - Warehouse config: {warehouse}")
    logger.info(f"DEBUG - Full table: {catalog}.{database}.{table_name}")
    
    full_table_name = f"{catalog}.{database}.{table_name}"
    
    # db_location = f"{warehouse.rstrip('/')}/{database}/"
    # spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database} LOCATION '{db_location}'")    
    # logger.info(f"CREATE DATABASE SUCCESSFULLY")
    # Check if table exists
    try:
        spark.table(full_table_name)
        logger.info(f"Table exists: {full_table_name}")
        table_exists = True
    except:
        table_exists = False
    
    if mode == "overwrite" or not table_exists:
        writer = df.writeTo(full_table_name).using("iceberg")
        
        if partition_cols:
            writer = writer.partitionedBy(*partition_cols)
        
        if table_exists:
            writer.overwritePartitions()
            logger.info(f"Overwrote partitions in {full_table_name}")
        else:
            writer.createOrReplace()
            logger.info(f"Created table {full_table_name}")
    
    elif mode == "append":
        df.writeTo(full_table_name).append()
        logger.info(f"Appended to {full_table_name}")
    
    elif mode == "merge" and primary_keys:
        df.createOrReplaceTempView("source_data")
        
        merge_condition = " AND ".join([
            f"target.{col} = source.{col}" for col in primary_keys
        ])
        
        columns = df.columns
        update_cols = [col for col in columns if col not in primary_keys]
        
        if update_cols:
            update_set = ", ".join([f"target.{col} = source.{col}" for col in update_cols])
        else:
            update_set = ", ".join([f"target.{col} = source.{col}" for col in columns])
        
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f"source.{col}" for col in columns])
        
        merge_sql = f"""
            MERGE INTO {full_table_name} AS target
            USING source_data AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        
        spark.sql(merge_sql)
        logger.info(f"Merged into {full_table_name}")
    
    row_count = spark.table(full_table_name).count()
    logger.info(f"{full_table_name}: {row_count} rows")
    return row_count


def process_table(
    spark: SparkSession,
    config: Dict[str, Any],
    table_info: Dict[str, Any]
) -> Dict[str, Any]:
    """Process single table ingestion."""
    schema_name = table_info['schema_name']
    table_name = table_info['table_name']
    primary_keys = table_info.get('primary_keys', [])
    partition_cols = table_info.get('partition_cols', None)
    mode = table_info.get('mode', 'merge')
    
    logger.info(f"Processing: {schema_name}.{table_name}")
    
    try:
        df = read_from_postgres(
            spark=spark,
            jdbc_url=config['jdbc_url'],
            username=os.environ.get('POSTGRES_USER', 'postgres'),
            password=os.environ.get('POSTGRES_PASSWORD', 'postgres'),
            schema_name=schema_name,
            table_name=table_name,
            primary_keys=primary_keys
        )
        
        if df is None:
            raise Exception("Failed to read data from PostgreSQL")
        
        row_count = write_to_iceberg(
            spark=spark,
            df=df,
            catalog="bronze",
            database=config.get('database', 'default'),
            table_name=table_name,
            primary_keys=primary_keys,
            partition_cols=partition_cols,
            mode=mode
        )
        
        return {
            "table": f"{schema_name}.{table_name}",
            "status": "success",
            "rows": row_count
        }
        
    except Exception as e:
        logger.error(f"Failed {schema_name}.{table_name}: {e}")
        return {
            "table": f"{schema_name}.{table_name}",
            "status": "error",
            "message": str(e)
        }


def parse_args():
    parser = argparse.ArgumentParser(description='Bronze Ingestion Job')
    parser.add_argument('--config', required=True, help='Path to config JSON file')
    return parser.parse_args()


def main():
    args = parse_args()
    
    with open(args.config, 'r') as f:
        config = json.load(f)
    
    tables = config.get('tables', [])
    logger.info(f"Starting ingestion: {len(tables)} table(s)")
    
    spark = None
    try:
        spark = create_spark_session("BronzeIngestion")
        
        results = [process_table(spark, config, t) for t in tables]
        
        success = sum(1 for r in results if r['status'] == 'success')
        failed = sum(1 for r in results if r['status'] == 'error')
        
        logger.info(f"Completed: {success} success, {failed} failed")
        
        if failed > 0:
            failed_tables = [r['table'] for r in results if r['status'] == 'error']
            logger.error(f"Failed tables: {failed_tables}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Job failed: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()