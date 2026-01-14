#!/usr/bin/env python3
"""
Gold Transformation: Silver to Gold layer
Prepares ML-ready features for churn prediction model
"""
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
logger = logging.getLogger("GoldTransformation")


def create_spark_session(app_name: str = "GoldTransformation") -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
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


def apply_feature_engineering(
    df: DataFrame,
    feature_config: Dict[str, Any]
) -> DataFrame:
    """
    Apply feature engineering transformations for ML pipeline.
    
    Config structure:
    {
        "drop_columns": ["col1", "col2"],           # Columns to drop
        "log1p_features": ["col1", "col2"],         # Apply log1p transformation
        "clip_quantile_features": {                  # Clip by quantile
            "col1": {"low": 0.01, "high": 0.99}
        },
        "clip_value_features": {                     # Clip by fixed value
            "col1": {"upper": 5}
        },
        "iqr_features": ["col1"],                   # IQR outlier handling
        "drop_after_transform": ["col1_original"],  # Drop after creating derived cols
        "target_column": "churn_label"              # Target column name
    }
    """
    
    # Step 1: Drop initial columns (user_id, age, country, city, etc.)
    drop_cols = feature_config.get('drop_columns', [])
    if drop_cols:
        existing_cols = [c for c in drop_cols if c in df.columns]
        if existing_cols:
            df = df.drop(*existing_cols)
            logger.info(f"Dropped columns: {existing_cols}")
    
    # Step 2: Apply log1p transformations
    log1p_features = feature_config.get('log1p_features', [])
    for col_name in log1p_features:
        if col_name in df.columns:
            output_col = f"{col_name}_log"
            df = df.withColumn(output_col, F.log1p(F.col(col_name).cast(DoubleType())))
            logger.info(f"Applied log1p: {col_name} -> {output_col}")
    
    # Step 3: Apply quantile clipping
    clip_quantile = feature_config.get('clip_quantile_features', {})
    for col_name, params in clip_quantile.items():
        if col_name in df.columns:
            low = params.get('low', 0.01)
            high = params.get('high', 0.99)
            
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
    
    # Step 4: Apply fixed value clipping
    clip_value = feature_config.get('clip_value_features', {})
    for col_name, params in clip_value.items():
        if col_name in df.columns:
            lower = params.get('lower')
            upper = params.get('upper')
            
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
    
    # Step 5: Apply IQR outlier handling
    iqr_features = feature_config.get('iqr_features', [])
    for col_name in iqr_features:
        if col_name in df.columns:
            output_col = f"{col_name}_iqr"
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
    
    # Step 6: Drop columns after transformation
    drop_after = feature_config.get('drop_after_transform', [])
    if drop_after:
        existing_cols = [c for c in drop_after if c in df.columns]
        if existing_cols:
            df = df.drop(*existing_cols)
            logger.info(f"Dropped after transform: {existing_cols}")
    
    return df


def prepare_ml_dataset(
    df: DataFrame,
    target_column: str,
    feature_columns: Optional[List[str]] = None
) -> DataFrame:
    """
    Prepare final ML dataset with features and target.
    
    Returns DataFrame with:
    - feature columns (all columns except target if not specified)
    - target column
    """
    if feature_columns:
        # Use specified feature columns
        all_cols = feature_columns + [target_column]
        df = df.select(*[c for c in all_cols if c in df.columns])
    
    # Drop rows with null values in important columns
    df = df.dropna()
    
    logger.info(f"Final dataset columns: {df.columns}")
    logger.info(f"Target column: {target_column}")
    
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
    
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")
    
    try:
        spark.table(full_table_name)
        table_exists = True
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


def process_gold_transformation(
    spark: SparkSession,
    transform_info: Dict[str, Any]
) -> Dict[str, Any]:
    """Process gold layer transformation."""
    source = transform_info['source']
    target = transform_info['target']
    feature_config = transform_info.get('feature_engineering', {})
    
    source_table = f"{source['catalog']}.{source['database']}.{source['table']}"
    target_table = f"{target['catalog']}.{target['database']}.{target['table']}"
    
    logger.info(f"Gold Transformation: {source_table} -> {target_table}")
    
    try:
        # Read from silver
        df = read_from_iceberg(
            spark=spark,
            catalog=source['catalog'],
            database=source['database'],
            table_name=source['table']
        )
        
        logger.info(f"Source row count: {df.count()}")
        logger.info(f"Source columns: {df.columns}")
        
        # Apply feature engineering
        df = apply_feature_engineering(df, feature_config)
        
        # Prepare ML dataset
        target_column = feature_config.get('target_column', 'churn_label')
        feature_columns = feature_config.get('final_feature_columns')
        
        df = prepare_ml_dataset(df, target_column, feature_columns)
        
        # Write to gold
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
            "rows": row_count,
            "columns": df.columns
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
    parser = argparse.ArgumentParser(description='Gold Transformation Job')
    parser.add_argument('--config', required=True, help='Path to config JSON file')
    return parser.parse_args()


def main():
    args = parse_args()
    
    with open(args.config, 'r') as f:
        config = json.load(f)
    
    transformations = config.get('transformations', [])
    logger.info(f"Starting gold transformation: {len(transformations)} job(s)")
    
    spark = None
    try:
        spark = create_spark_session("GoldTransformation")
        
        results = [process_gold_transformation(spark, t) for t in transformations]
        
        success = sum(1 for r in results if r['status'] == 'success')
        failed = sum(1 for r in results if r['status'] == 'error')
        
        logger.info(f"Completed: {success} success, {failed} failed")
        
        for r in results:
            if r['status'] == 'success':
                logger.info(f"  {r['target']}: {r['rows']} rows, columns: {r.get('columns', [])}")
        
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