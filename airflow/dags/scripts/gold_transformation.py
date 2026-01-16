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
logger = logging.getLogger("GoldTransformation")


def create_spark_session(app_name: str = "GoldTransformation") -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    spark._jvm.org.apache.log4j.Logger.getLogger("org.apache.iceberg").setLevel(
        spark._jvm.org.apache.log4j.Level.ERROR
    )
    spark._jvm.org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(
        spark._jvm.org.apache.log4j.Level.ERROR
    )
    
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
    
    # 3. Apply clip transformations
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
            
            # SỬA: Clip về boundary thay vì set NULL
            df = df.withColumn(
                output_col,
                F.when(F.col(col_name) < lower_bound, lower_bound)
                .when(F.col(col_name) > upper_bound, upper_bound)
                .otherwise(F.col(col_name))
            )
            logger.info(f"IQR clip: {col_name} -> {output_col} [{lower_bound:.4f}, {upper_bound:.4f}]")
    
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
    
    row_count = spark.table(full_table_name).count()
    logger.info(f"{full_table_name}: {row_count} rows")
    return row_count


def process_transformation(
    spark: SparkSession,
    transform_info: Dict[str, Any]
) -> Dict[str, Any]:
    
    source = transform_info.get('source', {})
    target = transform_info.get('target', {})
    transformations = transform_info.get('transformations', {})
    output_config = transform_info.get('output', {})
    source_table = f"{source['catalog']}.{source['database']}.{source['table']}"
    
    logger.info(f"Processing: {source_table}")
    
    try:
        # Read source
        df = read_from_iceberg(
            spark=spark,
            catalog=source['catalog'],
            database=source['database'],
            table_name=source['table']
        )
        
        logger.info(f"Source row count: {df.count()}")
        
        # Apply transformations
        df = apply_transformations(df, transformations)
        
        # Get output config
        feature_columns = output_config.get('feature_columns', [])
        target_column = output_config.get('target_column', 'churn_label')
        drop_nulls = output_config.get('drop_nulls', True)
        
        # Select và drop nulls
        all_cols = feature_columns + [target_column]
        existing_cols = [c for c in all_cols if c in df.columns]
        df_clean = df.select(*existing_cols)
        
        if drop_nulls:
            before_count = df_clean.count()
            df_clean = df_clean.dropna()
            after_count = df_clean.count()
            dropped = before_count - after_count
            if dropped > 0:
                logger.info(f"Dropped {dropped} rows with nulls ({dropped/before_count*100:.2f}%)")
        
        # Split into features and label
        df_features = df_clean.select(*[c for c in feature_columns if c in df_clean.columns])
        df_label = df_clean.select(target_column)
        
        # Write features table
        features_table = target.get('features_table', 'features')
        features_count = write_to_iceberg(
            spark=spark,
            df=df_features,
            catalog=target['catalog'],
            database=target['database'],
            table_name=features_table,
            partition_cols=target.get('partition_cols'),
            mode=target.get('mode', 'overwrite')
        )
        
        # Write label table
        label_table = target.get('label_table', 'label')
        label_count = write_to_iceberg(
            spark=spark,
            df=df_label,
            catalog=target['catalog'],
            database=target['database'],
            table_name=label_table,
            partition_cols=target.get('partition_cols'),
            mode=target.get('mode', 'overwrite')
        )
        
        return {
            "source": source_table,
            "status": "success",
            "features_table": f"{target['catalog']}.{target['database']}.{features_table}",
            "features_rows": features_count,
            "features_columns": df_features.columns,
            "label_table": f"{target['catalog']}.{target['database']}.{label_table}",
            "label_rows": label_count
        }
        
    except Exception as e:
        logger.error(f"Failed {source_table}: {e}")
        return {
            "source": source_table,
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
    
    transforms = config.get('transformations', [])
    logger.info(f"Starting transformation: {len(transforms)} job(s)")
    
    spark = None
    try:
        spark = create_spark_session("GoldTransformation")
        
        results = [process_transformation(spark, t) for t in transforms]
        
        success = sum(1 for r in results if r['status'] == 'success')
        failed = sum(1 for r in results if r['status'] == 'error')
        
        logger.info(f"Completed: {success} success, {failed} failed")
        
        for r in results:
            if r['status'] == 'success':
                logger.info(f"  Features: {r['features_table']} ({r['features_rows']} rows, {len(r['features_columns'])} cols)")
                logger.info(f"  Label: {r['label_table']} ({r['label_rows']} rows)")
        
        if failed > 0:
            failed_jobs = [r['source'] for r in results if r['status'] == 'error']
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