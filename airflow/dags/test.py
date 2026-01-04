from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.param import Param

default_args = {
    'owner': 'Chien',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

SPARK_APPS_PATH = '/opt/spark/apps'

with DAG(
    'test',
    default_args=default_args,
    description='Full stock data pipeline: Bronze → Silver',
    schedule_interval='0 6 * * *',
    catchup=False,
    tags=['full'],
    render_template_as_native_obj=True,
) as dag_full:

    full_pipeline = SparkSubmitOperator(
        task_id='chien',
        application=f'{SPARK_APPS_PATH}/pipeline_v1.py',
        conn_id='spark_default',
        deploy_mode="client",
        # Thêm hadoop-aws JARs
        jars='/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar,'
             '/opt/spark/jars/iceberg-aws-bundle-1.7.1.jar,'
             '/opt/spark/jars/hadoop-aws-3.3.4.jar,'
             '/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar',
        verbose=True,
        conf={
            # S3/MinIO configs
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.access.key': 'minioadmin',
            'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            
            # Iceberg extension
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            
            # Bronze catalog
            'spark.sql.catalog.bronze': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.bronze.type': 'hadoop',
            'spark.sql.catalog.bronze.warehouse': 's3a://bronze',
            
            # Silver catalog
            'spark.sql.catalog.silver': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.silver.type': 'hadoop',
            'spark.sql.catalog.silver.warehouse': 's3a://silver',
            
            # Gold catalog
            'spark.sql.catalog.gold': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.gold.type': 'hadoop',
            'spark.sql.catalog.gold.warehouse': 's3a://gold',
        }
    )