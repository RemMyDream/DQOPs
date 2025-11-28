import sys
from pyspark.sql import SparkSession
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../..")) 
from utils.helpers import create_logger
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from sqlalchemy import text
from postgres_connection import read_data_from_postgre
from utils.postgresql_client import PostgresSQLClient
import argparse

logger = create_logger(name = "Bronze_Ingestion")

parser = argparse.ArgumentParser()
parser.add_argument("--spark", required=True)
parser.add_argument("--pc", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument("--table", required=True)
parser.add_argument("--primary_keys", default = None)
args = parser.parse_args()

def ingest_to_bronze(
    schema_name: str,
    table_name: str, 
    primary_keys: list,
    spark: SparkSession,
    pc: PostgresSQLClient,
    layer: str = 'bronze', 
):
    """
    Load raw data to Delta Table with Hive Metastore as catalog.
    """
    try:
        df = read_data_from_postgre(spark, pc, schema_name, table_name, primary_keys)
        logger.info(f"Loading to Layer {layer} ...")
        
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
            path = f"s3a://{pc.database}/{layer}/{schema_name}/{table_name}"

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

    except Exception as e:
        logger.error(f"Error when ingest to bronze: {e}")
        raise

ingest_to_bronze(args.schema, args.table, args.primary_keys, args.spark, args.pc)
