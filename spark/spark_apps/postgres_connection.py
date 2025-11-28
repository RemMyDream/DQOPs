import sys
from pyspark.sql import SparkSession
import os
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), "../..")) 
from utils.postgresql_client import PostgresSQLClient
from utils.helpers import create_logger
from pyspark.sql import SparkSession, DataFrame

logger = create_logger(name = "Postgres_Connection")

def cal_upper_and_lower_bound(pc, schema: str, table_name: str, partition_column: str):
    """
    Tính lower và upper bound cho partition column.
    """
    with pc.engine.connect() as conn:
        query = f'''
            SELECT 
                MIN("{partition_column}") AS lower, 
                MAX("{partition_column}") AS upper 
            FROM "{schema}"."{table_name}"
        '''
        result = pd.read_sql_query(query, con=conn)
        lower_bound = result['lower'].iloc[0]
        upper_bound = result['upper'].iloc[0]
        return lower_bound, upper_bound 

def read_data_from_postgre(
    spark: SparkSession,
    pc: PostgresSQLClient,
    schema: str,
    table_name: str,
    primary_keys: list,
    num_partitions: int = 8
) -> DataFrame:

    jdbc_url = pc.get_jdbc_url()
    properties = pc.get_jdbc_properties()

    partition_column = primary_keys[0] if primary_keys else None

    if partition_column:
        try:
            lower_bound, upper_bound = cal_upper_and_lower_bound(
                pc, schema, table_name, partition_column
            )

            properties.update({
                "partitionColumn": partition_column,
                "lowerBound": str(lower_bound),
                "upperBound": str(upper_bound),
                "numPartitions": str(num_partitions)
            })

            logger.info(f"Reading data with partition on '{partition_column}' from {schema}.{table_name}")
        except Exception as e:
            logger.warning(f"Cannot partition on '{partition_column}': {e}. Reading without partition.")
            partition_column = None

    if not partition_column:
        logger.info(f"Reading data without partition from {schema}.{table_name}")

    try:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=f'"{schema}"."{table_name}"',   
            properties=properties
        )
        row_count = df.count()
        logger.info(f"Successfully read {row_count} rows from {schema}.{table_name}")
        return df

    except Exception as e:
        logger.error(f"Error connecting Spark to {schema}.{table_name}: {e}")
        return None