import sys
import os
import json
import argparse
from typing import List

sys.path.append(os.path.join(os.path.dirname(__file__), "..")) 
from utils.helpers import create_logger, parse_args
from utils.create_spark_connection import create_spark_connection
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from postgres_connection import read_data_from_postgre
logger = create_logger(name = "BronzeIngestionRunner")

def write_to_layer(
        spark: SparkSession,
        df: DataFrame,
        tableName: str, 
        primaryKeys: List[str],
        layer: str,
        path: str,
        format: str = "delta"
):
    """
    Load raw data to Delta Table with Hive Metastore as catalog.
    """
    logger.info(f"Loading to Layer {layer} ...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {layer}")
    
    full_table_name = f"`{layer}`.`{tableName}`"

    table_exists = False
    try:
        DeltaTable.forName(spark, full_table_name)
        table_exists = True
    except:
        table_exists = False

    if table_exists:
        logger.info(f"Table {full_table_name} exists, performing merge...")
        delta_table = DeltaTable.forName(spark, full_table_name)
        
        merge_condition = " AND ".join([f"target.`{col}` = source.`{col}`" for col in primaryKeys])

        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        logger.info(f"Merge completed for {full_table_name}")
    else:
        # path = f"s3a://{pc.database}/{layer}/{schemaName}/{tableName}"

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
    args = parse_args()
    
    # Parse JSON config from Airflow
    try:
        config = json.loads(args.config)
        logger.info(f"Received config: {json.dumps(config, indent=2)}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse config JSON: {e}")
        sys.exit(1)
    
    # Extract configuration
    schema_name = config.get('schemaName')
    table_name = config.get('tableName')
    primary_keys = config.get('primaryKeys', [])
    layer = config.get('layer', 'bronze')
    path = config.get('path')
    format = config.get('format', 'delta')
    connection_name = config.get('connectionName')
    
    # Validate required fields
    if not all([schema_name, table_name, path]):
        logger.error("Missing required fields: schema_name, table_name, path")
        sys.exit(1)
    
    logger.info(f"Starting ingestion: {schema_name}.{table_name} -> {layer}")
    
    try:
        # Initialize Spark
        spark = create_spark_connection()
        if layer == 'bronze':
            from backend.routers.dependencies import get_postgres_service
            service = get_postgres_service()
            pc = service.get_postgres_client(connection_name)
        
            # Read from source
            df = read_data_from_postgre(spark, pc, schema_name, table_name, primary_keys)

        if df is None:
            logger.error("Failed to read data from source")
            sys.exit(1)
        
        # Write to bronze
        row_count = write_to_layer(
            spark=spark,
            df=df,
            tableName=table_name,
            primaryKeys=primary_keys,
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