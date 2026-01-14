#!/usr/bin/env python3

import sys
import os
import json
import argparse
import logging
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("SilverTransformation")


def create_spark_session(app_name: str = "SilverTransformation") -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("SparkSession created successfully")
    return spark


def read_from_iceberg(
    spark: SparkSession,
    catalog: str,
    database: str,
    table_name: str
) -> DataFrame:
    """Read data from Iceberg table."""
    full_table_name = f"{catalog}.{database}.{table_name}"
    logger.info(f"Reading from {full_table_name}")
    return spark.table(full_table_name)


def apply_transformations(
    df: DataFrame,
    transformations: Dict[str, Any]
) -> DataFrame:
    
    # 1. Apply log1p transformations
    log1p_configs = transformations.get('log1p', [])
    for config in log1p_configs:
        col_name = config['column']
        output_col = config.get('output', f"{col_name}_log")
        df = df.withColumn(output_col, F.log1p(F.col(col_name).cast(DoubleType())))
        logger.info(f"Applied log1p: {col_name} -> {output_col}")
    
    # 2. Apply quantile clip transformations
    clip_configs = transformations.get('clip', [])
    for config in clip_configs:
        col_name = config['column']
        low = config.get('low', 0.01)
        high = config.get('high', 0.99)
        
        quantiles = df.approxQuantile(col_name, [low, high], 0.01)
        if len(quantiles) == 2:
            q_low, q_high = quantiles[0], quantiles[1]
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name) < q_low, q_low)
                 .when(F.col(col_name) > q_high, q_high)
                 .otherwise(F.col(col_name))
            )
            logger.info(f"Clipped {col_name} to [{q_low:.4f}, {q_high:.4f}]")
    
    # 3. Apply fixed value clip transformations
    clip_value_configs = transformations.get('clip_value', [])
    for config in clip_value_configs:
        col_name = config['column']
        lower = config.get('lower')
        upper = config.get('upper')
        
        if lower is not None:
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name) < lower, lower).otherwise(F.col(col_name))
            )
        if upper is not None:
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name) > upper, upper).otherwise(F.col(col_name))
            )
        logger.info(f"Clipped {col_name}: lower={lower}, upper={upper}")
    
    # 4. Apply IQR clip transformations
    iqr_configs = transformations.get('iqr_clip', [])
    for config in iqr_configs:
        col_name = config['column']
        output_col = config.get('output', f"{col_name}_iqr")
        
        quantiles = df.approxQuantile(col_name, [0.25, 0.75], 0.01)
        if len(quantiles) == 2:
            q1, q3 = quantiles[0], quantiles[1]
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            df = df.withColumn(
                output_col,
                F.when(
                    (F.col(col_name) >= lower_bound) & (F.col(col_name) <= upper_bound),
                    F.col(col_name)
                ).otherwise(F.lit(None))
            )
            logger.info(f"IQR filter: {col_name} -> {output_col} [{lower_bound:.4f}, {upper_bound:.4f}]")
    
    # 5. Drop columns 
    drop_cols = transformations.get('drop_columns', [])
    if drop_cols:
        existing_cols = [c for c in drop_cols if c in df.columns]
        if existing_cols:
            df = df.drop(*existing_cols)
            logger.info(f"Dropped columns: {existing_cols}")
        
        missing_cols = set(drop_cols) - set(existing_cols)
        if missing_cols:
            logger.warning(f"Columns not found (skipped): {missing_cols}")
    
    return df


def write_to_iceberg(
    spark: SparkSession,
    df: DataFrame,
    catalog: str,
    database: str,
    table_name: str,
    primary_keys: List[str],
    partition_cols: Optional[List[str]] = None,
    mode: str = "overwrite"
) -> int:
    """Write data to Iceberg table."""
    full_table_name = f"{catalog}.{database}.{table_name}"
        
    try:
        spark.table(full_table_name)
        table_exists = True
        logger.info(f"Table exists: {full_table_name}")
    except:
        table_exists = False
    
    if mode == "overwrite" or not table_exists:
        writer = df.writeTo(full_table_name).using("iceberg")
        
        if partition_cols:
            writer = writer.partitionedBy(*partition_cols)
        
        writer.createOrReplace()
        logger.info(f"Created/Replaced table {full_table_name}")
    
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


def process_transformation(
    spark: SparkSession,
    transform_info: Dict[str, Any]
) -> Dict[str, Any]:
    """Process single transformation."""
    source = transform_info['source']
    target = transform_info['target']
    transformations = transform_info.get('transformations', {})
    
    source_table = f"{source['catalog']}.{source['database']}.{source['table']}"
    target_table = f"{target['catalog']}.{target['database']}.{target['table']}"
    
    logger.info(f"Transforming: {source_table} -> {target_table}")
    
    try:
        df = read_from_iceberg(
            spark=spark,
            catalog=source['catalog'],
            database=source['database'],
            table_name=source['table']
        )
        
        logger.info(f"Source columns: {df.columns}")
        
        df = apply_transformations(df, transformations)
        
        logger.info(f"Output columns: {df.columns}")
        
        row_count = write_to_iceberg(
            spark=spark,
            df=df,
            catalog=target['catalog'],
            database=target['database'],
            table_name=target['table'],
            primary_keys=target.get('primary_keys', []),
            partition_cols=target.get('partition_cols'),
            mode=target.get('mode', 'overwrite')
        )
        
        return {
            "source": source_table,
            "target": target_table,
            "status": "success",
            "rows": row_count
        }
        
    except Exception as e:
        logger.error(f"Failed {source_table} -> {target_table}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            "source": source_table,
            "target": target_table,
            "status": "error",
            "message": str(e)
        }


def parse_args():
    parser = argparse.ArgumentParser(description='Silver Transformation Job')
    parser.add_argument('--config', required=True, help='Path to config JSON file')
    return parser.parse_args()


def main():
    args = parse_args()
    
    with open(args.config, 'r') as f:
        config = json.load(f)
    
    transforms = config.get('transformations', [])
    logger.info(f"Starting transformation: {len(transforms)} job(s)")
    
    spark = None
    try:
        spark = create_spark_session("SilverTransformation")
        
        results = [process_transformation(spark, t) for t in transforms]
        
        success = sum(1 for r in results if r['status'] == 'success')
        failed = sum(1 for r in results if r['status'] == 'error')
        
        logger.info(f"Completed: {success} success, {failed} failed")
        
        if failed > 0:
            failed_jobs = [
                f"{r['source']} -> {r['target']}" 
                for r in results if r['status'] == 'error'
            ]
            logger.error(f"Failed jobs: {failed_jobs}")
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