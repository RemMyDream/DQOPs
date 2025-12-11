import sys
import os
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from backend.domain.entity.postgres_client import PostgresConnectionClient
from utils.helpers import create_logger
from pyspark.sql import SparkSession, DataFrame

logger = create_logger(name = "PostgresIngestion")


def read_data_from_postgre(
    spark: SparkSession,
    pc: PostgresConnectionClient,
    schemaName: str,
    tableName: str,
    primaryKeys: list,
    num_partitions: int = 8
) -> DataFrame:

    jdbc_url = pc.get_jdbc_url()
    properties = pc.get_jdbc_properties()

    partition_column = primaryKeys[0] if primaryKeys else None

    if partition_column:
        try:
            lower_bound, upper_bound = pc.cal_upper_and_lower_bound(
                schemaName, tableName, partition_column
            )

            properties.update({
                "partitionColumn": partition_column,
                "lowerBound": str(lower_bound),
                "upperBound": str(upper_bound),
                "numPartitions": str(num_partitions)
            })

            logger.info(f"Reading data with partition on '{partition_column}' from {schemaName}.{tableName}")
        except Exception as e:
            logger.warning(f"Cannot partition on '{partition_column}': {e}. Reading without partition.")
            partition_column = None

    if not partition_column:
        logger.info(f"Reading data without partition from {schemaName}.{tableName}")

    try:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=f'"{schemaName}"."{tableName}"',   
            properties=properties
        )
        row_count = df.count()
        logger.info(f"Successfully read {row_count} rows from {schemaName}.{tableName}")
        return df

    except Exception as e:
        logger.error(f"Error connecting Spark to {schemaName}.{tableName}: {e}")
        return None