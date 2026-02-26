#!/usr/bin/env python3
"""
DQ Profiling Spark Job
- Reads Bronze Iceberg table schema
- Renders sensor SQL per column per dimension via Jinja2 templates
- Executes SQL against Iceberg tables
- Writes profiling_results rows to internal PostgreSQL via JDBC
"""
import sys
import os
import json
import argparse
import uuid
import time
import re
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

from pyspark.sql import SparkSession, Row
from jinja2 import Environment, FileSystemLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DQProfiling")


# ============================================================
# SENSOR MAPPING: dimension -> { metric_name -> template_path }
# ============================================================
COLUMN_SENSORS = {
    "completeness": {
        "null_record_count": "column/null/null_record_count/spark.sql.jinja2",
        "null_record_percent": "column/null/null_record_percent/spark.sql.jinja2",
        "not_null_record_count": "column/null/not_null_record_count/spark.sql.jinja2",
        "not_null_record_percent": "column/null/not_null_record_percent/spark.sql.jinja2",
    },
    "uniqueness": {
        "distinct_record_count": "column/uniqueness/distinct_record_count/spark.sql.jinja2",
        "distinct_record_percent": "column/uniqueness/distinct_record_percent/spark.sql.jinja2",
        "duplicate_record_count": "column/uniqueness/duplicate_record_count/spark.sql.jinja2",
        "duplicate_record_percent": "column/uniqueness/duplicate_record_percent/spark.sql.jinja2",
    },
    "validity": {
        "min": "column/numeric/min/spark.sql.jinja2",
        "max": "column/numeric/max/spark.sql.jinja2",
        "mean": "column/numeric/mean/spark.sql.jinja2",
        "text_min_length": "column/text/text_min_length/spark.sql.jinja2",
        "text_max_length": "column/text/text_max_length/spark.sql.jinja2",
        "text_mean_length": "column/text/text_mean_length/spark.sql.jinja2",
    },
}

TABLE_SENSORS = {
    "volume": {
        "row_count": "table/volume/row_count/spark.sql.jinja2",
    }
}

NUMERIC_TYPES = {
    "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT",
    "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL", "LONG",
}
TEXT_TYPES = {"STRING", "VARCHAR", "CHAR", "TEXT"}


def create_spark_session() -> SparkSession:
    builder = SparkSession.builder.appName("DQProfiling")

    # Set S3/MinIO credentials from env vars (injected by K8s Secret)
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT")

    if minio_access_key:
        builder = builder.config("spark.hadoop.fs.s3a.access.key", minio_access_key)
    if minio_secret_key:
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
    if minio_endpoint:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("SparkSession created successfully")
    return spark


def create_template_env(template_dir: str) -> Environment:
    """Create Jinja2 environment matching TemplateEngine config"""
    return Environment(
        loader=FileSystemLoader(template_dir),
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=False,
        extensions=['jinja2.ext.do']
    )


def get_table_columns(
    spark: SparkSession, catalog: str, database: str, table_name: str
) -> Dict[str, str]:
    """Get column name -> type from Iceberg table schema"""
    full_name = f"{catalog}.{database}.{table_name}"
    logger.info(f"Reading schema from: {full_name}")
    df = spark.table(full_name)
    return {
        field.name: field.dataType.simpleString().upper()
        for field in df.schema.fields
    }


def build_column_context(
    catalog: str,
    database: str,
    table_name: str,
    column_name: str,
    all_columns: Dict[str, str],
) -> Dict[str, Any]:
    """
    Build context dict matching the template context structure.

    render_target_table() produces `schema_name`.`table_name` (2-part).
    For Iceberg 3-part names, we pass schema_name='catalog.database'
    and fix the quoting in run_sensor() after rendering.
    """
    columns_dict = {}
    for col, ctype in all_columns.items():
        columns_dict[col] = {
            'type_snapshot': {'column_type': ctype},
            'sql_expression': None,
        }

    return {
        'target_table': {
            'schema_name': f"{catalog}.{database}",
            'table_name': table_name,
        },
        'table': {
            'filter': None,
            'columns': columns_dict,
        },
        'column_name': column_name,
        'error_sampling': {
            'samples_limit': 10,
            'total_samples_limit': 1000,
            'id_columns': [],
        },
        'parameters': {},
        'additional_filters': [],
    }


def build_table_context(
    catalog: str,
    database: str,
    table_name: str,
    all_columns: Dict[str, str],
) -> Dict[str, Any]:
    """Build context for table-level sensors (volume)"""
    first_col = next(iter(all_columns))
    return build_column_context(catalog, database, table_name, first_col, all_columns)


def select_validity_sensors(column_type: str) -> Dict[str, str]:
    """Pick numeric or text sensors based on column type"""
    upper_type = column_type.upper()

    if any(t in upper_type for t in NUMERIC_TYPES):
        return {
            k: COLUMN_SENSORS["validity"][k]
            for k in ("min", "max", "mean")
        }

    if any(t in upper_type for t in TEXT_TYPES):
        return {
            k: COLUMN_SENSORS["validity"][k]
            for k in ("text_min_length", "text_max_length", "text_mean_length")
        }

    return {}


def run_sensor(
    spark: SparkSession,
    template_env: Environment,
    template_path: str,
    context: Dict[str, Any],
) -> Tuple[Optional[float], str, int]:
    """Render and execute a sensor SQL. Returns (value, sql, time_ms)."""
    template = template_env.get_template(template_path)
    rendered = template.render(**context)

    # Clean up whitespace (same as TemplateEngine.render)
    lines = [line.rstrip() for line in rendered.split('\n')]
    sql = '\n'.join(lines).strip()
    while '\n\n\n' in sql:
        sql = sql.replace('\n\n\n', '\n\n')

    # Fix Iceberg 3-part quoting: `catalog.database` -> `catalog`.`database`
    sql = re.sub(r'`(\w+)\.(\w+)`', r'`\1`.`\2`', sql)

    start = time.time()
    df = spark.sql(sql)
    rows = df.collect()
    elapsed = int((time.time() - start) * 1000)

    if rows and rows[0]:
        val = rows[0][0]
        return (float(val) if val is not None else None, sql, elapsed)
    return (None, sql, elapsed)


def make_result(
    run_id: str,
    schema_name: str,
    table_name: str,
    column_name: Optional[str],
    dimension: str,
    metric_name: str,
    actual_value: Optional[float] = None,
    executed_sql: Optional[str] = None,
    execution_time_ms: Optional[int] = None,
    error_message: Optional[str] = None,
    column_type: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a single result dict"""
    return {
        "profile_run_id": run_id,
        "schema_name": schema_name,
        "table_name": table_name,
        "column_name": column_name,
        "dimension": dimension,
        "metric_name": metric_name,
        "actual_value": actual_value,
        "executed_sql": executed_sql,
        "execution_time_ms": execution_time_ms,
        "error_message": error_message,
        "column_type": column_type,
    }


def profile_table(
    spark: SparkSession,
    template_env: Environment,
    config: Dict[str, Any],
    table_info: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Profile a single table across requested dimensions"""
    schema_name = table_info["schema_name"]
    table_name = table_info["table_name"]
    requested_columns = table_info.get("columns")  # None = all
    dimensions = config.get("dimensions",
                            ["completeness", "uniqueness", "validity", "volume"])
    catalog = config.get("iceberg_catalog", "bronze")
    run_id = config["profile_run_id"]

    all_columns = get_table_columns(spark, catalog, schema_name, table_name)
    logger.info(f"Table {catalog}.{schema_name}.{table_name}: {len(all_columns)} columns")

    if requested_columns:
        profile_columns = {
            k: v for k, v in all_columns.items() if k in requested_columns
        }
    else:
        profile_columns = all_columns

    results = []

    # --- Table-level sensors (volume) ---
    if "volume" in dimensions:
        table_ctx = build_table_context(catalog, schema_name, table_name, all_columns)
        for metric_name, template_path in TABLE_SENSORS["volume"].items():
            try:
                value, sql, elapsed = run_sensor(
                    spark, template_env, template_path, table_ctx
                )
                results.append(make_result(
                    run_id, schema_name, table_name, None,
                    "volume", metric_name, value, sql, elapsed,
                ))
            except Exception as e:
                logger.error(f"volume/{metric_name} failed: {e}")
                results.append(make_result(
                    run_id, schema_name, table_name, None,
                    "volume", metric_name, error_message=str(e),
                ))

        # column_count from schema metadata (no SQL needed)
        results.append(make_result(
            run_id, schema_name, table_name, None,
            "volume", "column_count", float(len(all_columns)),
        ))

    # --- Column-level sensors ---
    for col_name, col_type in profile_columns.items():
        ctx = build_column_context(
            catalog, schema_name, table_name, col_name, all_columns
        )

        for dim in ("completeness", "uniqueness", "validity"):
            if dim not in dimensions:
                continue

            if dim == "validity":
                sensors = select_validity_sensors(col_type)
            else:
                sensors = COLUMN_SENSORS.get(dim, {})

            for metric_name, template_path in sensors.items():
                try:
                    value, sql, elapsed = run_sensor(
                        spark, template_env, template_path, ctx
                    )
                    results.append(make_result(
                        run_id, schema_name, table_name, col_name,
                        dim, metric_name, value, sql, elapsed,
                        column_type=col_type,
                    ))
                except Exception as e:
                    logger.error(f"{dim}/{metric_name} for {col_name} failed: {e}")
                    results.append(make_result(
                        run_id, schema_name, table_name, col_name,
                        dim, metric_name, error_message=str(e),
                        column_type=col_type,
                    ))

    return results


def write_results_to_postgres(
    spark: SparkSession,
    results: List[Dict[str, Any]],
):
    """Write profiling results to internal PostgreSQL via Spark JDBC.
    Reads connection details from env vars (injected by K8s Secret).
    """
    if not results:
        logger.warning("No results to write")
        return

    host = os.environ.get("INTERNAL_DB_HOST", "fastapi-postgres")
    port = os.environ.get("INTERNAL_DB_PORT", "5432")
    database = os.environ.get("INTERNAL_DB_NAME", "internal_database")
    username = os.environ.get("INTERNAL_DB_USER", "postgres")
    password = os.environ.get("INTERNAL_DB_PASSWORD", "postgres")

    rows = [Row(**r) for r in results]
    df = spark.createDataFrame(rows)

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
    props = {
        "user": username,
        "password": password,
        "driver": "org.postgresql.Driver",
    }

    df.write.jdbc(
        url=jdbc_url,
        table="profiling_results",
        mode="append",
        properties=props,
    )
    logger.info(f"Wrote {len(results)} profiling results to PostgreSQL")


def parse_args():
    parser = argparse.ArgumentParser(description='DQ Profiling Job')
    parser.add_argument('--config', required=True, help='Path to config JSON')
    return parser.parse_args()


def main():
    args = parse_args()

    with open(args.config, 'r') as f:
        config = json.load(f)

    if "profile_run_id" not in config:
        config["profile_run_id"] = (
            f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            f"_{uuid.uuid4().hex[:8]}"
        )

    tables = config.get("tables", [])
    logger.info(f"Starting profiling: {len(tables)} table(s)")

    spark = None
    try:
        spark = create_spark_session()

        # Jinja2 templates - accessible via shared airflow-dags volume
        template_dir = "/opt/airflow/dags/scripts/templates/sensors"
        template_env = create_template_env(template_dir)
        logger.info(f"Template engine initialized: {template_dir}")

        all_results = []
        for table_info in tables:
            logger.info(
                f"Profiling {table_info['schema_name']}.{table_info['table_name']}"
            )
            results = profile_table(spark, template_env, config, table_info)
            all_results.extend(results)

        # Write all results to internal PostgreSQL
        write_results_to_postgres(spark, all_results)

        success = sum(1 for r in all_results if r["error_message"] is None)
        errors = sum(1 for r in all_results if r["error_message"] is not None)
        logger.info(f"Profiling complete: {success} metrics OK, {errors} errors")

    except Exception as e:
        logger.error(f"Profiling job failed: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
