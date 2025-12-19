import sys
import os
import json
import argparse
from typing import List
from minio import Minio
from minio.error import S3Error
sys.path.append(os.path.join(os.path.dirname(__file__), "..")) 
from utils.helpers import create_logger, parse_config_args
from create_spark_connection import create_spark_connection
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from read_from_postgres import read_data_from_postgres

logger = create_logger(name = "BronzeIngestionRunner")

def ensure_bucket_exists(spark: SparkSession, path: str):
    """
    Ensure the S3/MinIO bucket exists before writing data.
    Extract bucket name from s3a:// path and create if not exists.
    
    :param spark: SparkSession object
    :param path: S3 path in format s3a://bucket-name/path/to/data
    """
    try:
        if path.startswith("s3a://"):
            bucket_name = path.replace("s3a://", "").split("/")[0]
            
            access_key = spark.conf.get("spark.hadoop.fs.s3a.access.key")
            secret_key = spark.conf.get("spark.hadoop.fs.s3a.secret.key")
            endpoint = spark.conf.get("spark.hadoop.fs.s3a.endpoint")
            
            endpoint = endpoint.replace("http://", "")
            
            # Create MinIO client
            client = Minio(
                endpoint=endpoint,       
                access_key=access_key,
                secret_key=secret_key,
                secure=False                 
            )
            
            if not client.bucket_exists(bucket_name=bucket_name):
                client.make_bucket(bucket_name=bucket_name) 
                logger.info(f"Created bucket: {bucket_name}")
            else:
                logger.info(f"Bucket already exists: {bucket_name}")
                
    except S3Error as e:
        logger.error(f"S3 Error when ensuring bucket exists: {e}")
        raise
    except Exception as e:
        logger.error(f"Error when ensuring bucket exists: {e}")
        raise

def write_to_layer(
        spark: SparkSession,
        df: DataFrame,
        table_name: str, 
        primary_keys: List[str],
        layer: str,
        path: str,
        format: str = "delta"
):
    """
    Load raw data to Delta Table with Hive Metastore as catalog.
    """
    logger.info(f"Loading to Layer {layer} ...")
    ensure_bucket_exists(spark, path)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {layer}")
    
    full_table_name = f"`{layer}`.`{table_name}`"

    table_exists = False
    try:
        DeltaTable.forName(spark, full_table_name)
        table_exists = True
    except:
        table_exists = False

    if table_exists:
        logger.info(f"Table {full_table_name} exists, performing merge...")
        delta_table = DeltaTable.forName(spark, full_table_name)
        
        merge_condition = " AND ".join([f"target.`{col}` = source.`{col}`" for col in primary_keys])

        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        logger.info(f"Merge completed for {full_table_name}")
    else:
        logger.info(f"Creating new table {full_table_name}")
        df.write.format("delta")\
            .mode("overwrite")\
            .option("path", path)\
            .option("overwriteSchema", "true")\
            .saveAsTable(full_table_name)
        logger.info(f"Table {full_table_name} created and data loaded successfully")

    # Log row count
    row_count = spark.table(full_table_name).count()
    logger.info(f"Load successfully: {row_count} rows in {full_table_name}")

    return row_count

def main():
    """Main entry point"""
    config = parse_config_args()
    
    # Extract configuration
    schema_name = config.get('schema_name')
    table_name = config.get('table_name')
    primary_keys = config.get('primary_keys', [])
    layer = config.get('layer', 'bronze')
    format = config.get('format', 'delta')
    path = config.get('path')

    # Validate required fields
    if not all([schema_name, table_name, path]):
        logger.error("Missing required fields: schema_name, table_name, path")
        sys.exit(1)
    
    logger.info(f"Starting ingestion: {schema_name}.{table_name} -> {layer}")
    
    try:
        # Initialize Spark
        spark = create_spark_connection()
        if layer == 'bronze':
            conn_config = {
                "jdbc_url": config["jdbc_url"],
                "username": config["username"],
                "password": config["password"]
            }
            # Read from source
            df = read_data_from_postgres(
                spark=spark,
                conn_config=conn_config,
                schema_name=schema_name,
                table_name=table_name,
                primary_keys=primary_keys
            )
            
        if df is None:
            logger.error("Failed to read data from source")
            sys.exit(1)
        
        # Write to bronze
        row_count = write_to_layer(
            spark=spark,
            df=df,
            table_name=table_name,
            primary_keys=primary_keys,
            layer=layer,
            path=path,
            format = format
        )
        
        logger.info(f"Ingestion completed successfully: {row_count} rows")
        
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()
    # path = config.get('path', f"s3a://{pc.database}/{layer}/{schema_name}/{table_name}")
