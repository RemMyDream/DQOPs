import sys
import os
from typing import List
from minio import Minio
from minio.error import S3Error

sys.path.append(os.path.join(os.path.dirname(__file__), "..")) 
from utils.helpers import create_logger, parse_config_args
from create_spark_connection import create_spark_connection
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from read_from_postgres import read_data_from_postgres

logger = create_logger(name="BronzeIngestionRunner")


def ensure_bucket_exists(spark: SparkSession, path: str):
    """Ensure the S3/MinIO bucket exists before writing data."""
    try:
        if path.startswith("s3a://"):
            bucket_name = path.replace("s3a://", "").split("/")[0]
            
            access_key = spark.conf.get("spark.hadoop.fs.s3a.access.key")
            secret_key = spark.conf.get("spark.hadoop.fs.s3a.secret.key")
            endpoint = spark.conf.get("spark.hadoop.fs.s3a.endpoint").replace("http://", "")
            
            client = Minio(
                endpoint=endpoint,       
                access_key=access_key,
                secret_key=secret_key,
                secure=False                 
            )
            
            if not client.bucket_exists(bucket_name=bucket_name):
                client.make_bucket(bucket_name=bucket_name) 
                logger.info(f"Created bucket: {bucket_name}")
                
    except Exception as e:
        logger.error(f"Error ensuring bucket exists: {e}")
        raise


def write_to_layer(
    spark: SparkSession,
    df: DataFrame,
    table_name: str, 
    primary_keys: List[str],
    layer: str,
    path: str
):
    """Load data to Delta Table."""
    logger.info(f"Writing to {layer}.{table_name}...")
    ensure_bucket_exists(spark, path)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {layer}")
    
    full_table_name = f"`{layer}`.`{table_name}`"

    try:
        delta_table = DeltaTable.forName(spark, full_table_name)
        
        # Table exists -> merge
        merge_condition = " AND ".join([f"target.`{col}` = source.`{col}`" for col in primary_keys])
        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
        logger.info(f"Merged into {full_table_name}")
    except:
        # Table not exists -> create
        df.write.format("delta")\
            .mode("overwrite")\
            .option("path", path)\
            .option("overwriteSchema", "true")\
            .saveAsTable(full_table_name)
        logger.info(f"Created {full_table_name}")

    row_count = spark.table(full_table_name).count()
    logger.info(f"{full_table_name}: {row_count} rows")
    return row_count


def process_table(spark: SparkSession, config: dict, table_info: dict):
    """Process single table ingestion."""
    schema_name = table_info['schema_name']
    table_name = table_info['table_name']
    primary_keys = table_info.get('primary_keys', [])
    layer = config['layer']
    database = config['database']
    path = f"s3a://{database}/{layer}/{schema_name}/{table_name}"
    
    logger.info(f"Processing: {schema_name}.{table_name}")
    
    try:
        df = read_data_from_postgres(
            spark=spark,
            conn_config={
                "jdbc_url": config["jdbc_url"],
                "username": config["username"],
                "password": config["password"]
            },
            schema_name=schema_name,
            table_name=table_name,
            primary_keys=primary_keys
        )
        
        if df is None:
            raise Exception("Failed to read data")
        
        row_count = write_to_layer(spark, df, table_name, primary_keys, layer, path)
        return {"table": f"{schema_name}.{table_name}", "status": "success", "rows": row_count}
        
    except Exception as e:
        logger.error(f"Failed {schema_name}.{table_name}: {e}")
        return {"table": f"{schema_name}.{table_name}", "status": "error", "message": str(e)}


def main():
    config = parse_config_args()
    tables = config['tables']
    
    logger.info(f"Starting ingestion: {len(tables)} table(s)")
    
    try:
        spark = create_spark_connection()
        
        results = [process_table(spark, config, t) for t in tables]
        
        success = sum(1 for r in results if r['status'] == 'success')
        failed = sum(1 for r in results if r['status'] == 'error')
        
        logger.info(f"Completed: {success} success, {failed} failed")
        
        if failed > 0:
            logger.error(f"Failed: {[r['table'] for r in results if r['status'] == 'error']}")
            sys.exit(1)
            
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()