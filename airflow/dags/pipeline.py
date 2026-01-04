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

SPARK_CONF = {
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
    
    # Python path for workers
    'spark.executorEnv.PYTHONPATH': '/opt/spark/apps',
    'spark.yarn.appMasterEnv.PYTHONPATH': '/opt/spark/apps',
}

JARS = (
    '/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar,'
    '/opt/spark/jars/iceberg-aws-bundle-1.7.1.jar,'
    '/opt/spark/jars/hadoop-aws-3.3.4.jar,'
    '/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar'
)

# DAG 1: Ingest (Bronze + Silver) - Daily
with DAG(
    'stock_gdelt_ingest',
    default_args=default_args,
    description='Daily ingestion: Bronze → Silver',
    schedule_interval='0 6 * * *',
    catchup=False,
    tags=['ingest', 'bronze', 'silver'],
    params={
        'stooq_tickers': Param(default='NVDA,MSFT', type='string'),
        'stooq_start': Param(default='2025-12-10', type='string'),
        'stooq_end': Param(default='2025-12-22', type='string'),
        'gdelt_start': Param(default='2025-12-10', type='string'),
        'gdelt_end': Param(default='2025-12-23', type='string'),
        'skip_stooq': Param(default='false', type='string', enum=['true', 'false']),
        'skip_gdelt': Param(default='false', type='string', enum=['true', 'false'])
    },
    render_template_as_native_obj=True,
) as dag_ingest:

    bronze_task = SparkSubmitOperator(
        task_id='bronze_ingestion',
        application=f'{SPARK_APPS_PATH}/pipeline.py',
        conn_id='spark_default',
        application_args=[
            '--pipeline', 'bronze',
            '--stooq-tickers', '{{ params.stooq_tickers }}',
            '--stooq-start', '{{ params.stooq_start }}',
            '--stooq-end', '{{ params.stooq_end }}',
            '--gdelt-start', '{{ params.gdelt_start }}',
            '--gdelt-end', '{{ params.gdelt_end }}',
            '--skip-stooq', '{{ params.skip_stooq }}',
            '--skip-gdelt', '{{ params.skip_gdelt }}'
        ],
        deploy_mode="client",
        verbose=True,
        jars=JARS,
        
        conf=SPARK_CONF
    )

    silver_stooq_task = SparkSubmitOperator(
        task_id='silver_stooq',
        application=f'{SPARK_APPS_PATH}/pipeline.py',
        conn_id='spark_default',
        application_args=[
            '--pipeline', 'silver_stooq',
            '--stooq-tickers', '{{ params.stooq_tickers }}',
        ],
        deploy_mode="client",
        verbose=True,
        jars=JARS,
        
        conf=SPARK_CONF
    )

    silver_gdelt_task = SparkSubmitOperator(
        task_id='silver_gdelt',
        application=f'{SPARK_APPS_PATH}/pipeline.py',
        conn_id='spark_default',
        application_args=[
            '--pipeline', 'silver_gdelt',
        ],
        deploy_mode="client",
        verbose=True,
        jars=JARS,
        
        conf=SPARK_CONF
    )

    bronze_task >> [silver_stooq_task, silver_gdelt_task]


# DAG 2: Gold Processing - Monthly
with DAG(
    'gold_processing',
    default_args=default_args,
    description='Monthly gold processing: Sentiment → ML Features',
    schedule_interval='0 0 1 * *',
    catchup=False,
    tags=['gold', 'sentiment', 'ml'],
    render_template_as_native_obj=True,
) as dag_gold:

    gold_raw_stooq_task = SparkSubmitOperator(
        task_id='gold_raw_stooq',
        application=f'{SPARK_APPS_PATH}/pipeline.py',
        conn_id='spark_default',
        application_args=[
            '--pipeline', 'gold_raw_stooq',
        ],
        deploy_mode="client",
        verbose=True,
        jars=JARS,
        
        conf=SPARK_CONF
    )

    gold_sentiment_task = SparkSubmitOperator(
        task_id='gold_sentiment',
        application=f'{SPARK_APPS_PATH}/pipeline.py',
        conn_id='spark_default',
        application_args=[
            '--pipeline', 'gold_sentiment',
        ],
        deploy_mode="client",
        verbose=True,
        jars=JARS,
        
        conf=SPARK_CONF
    )

    gold_ml_features_task = SparkSubmitOperator(
        task_id='gold_ml_features',
        application=f'{SPARK_APPS_PATH}/pipeline.py',
        conn_id='spark_default',
        application_args=[
            '--pipeline', 'gold_ml_features',
        ],
        deploy_mode="client",
        verbose=True,
        jars=JARS,
        
        conf=SPARK_CONF
    )

    gold_raw_stooq_task >> gold_sentiment_task >> gold_ml_features_task