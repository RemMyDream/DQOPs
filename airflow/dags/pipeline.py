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

# DAG 1: Full Pipeline
with DAG(
    'full_pipeline',
    default_args=default_args,
    description='Full stock data pipeline: Bronze → Silver',
    schedule_interval='0 6 * * *',
    catchup=False,
    tags=['full'],
    params={
        'stooq_tickers': Param(default='NVDA,MSFT', type='string'),
        'stooq_start': Param(default='2020-01-02', type='string'),
        'stooq_end': Param(default='2025-12-22', type='string'),
        'gdelt_start': Param(default='2019-12-31', type='string'),
        'gdelt_end': Param(default='2025-12-23', type='string')
    },
    render_template_as_native_obj=True,
) as dag_full:

    full_pipeline = SparkSubmitOperator(
        task_id='run_full_pipeline',
        application=f'{SPARK_APPS_PATH}/pipeline.py',
        conn_id='spark_default',
        application_args=[
            '--pipeline', 'full',
            '--stooq-tickers', '{{ params.stooq_tickers }}',
            '--stooq-start', '{{ params.stooq_start }}',
            '--stooq-end', '{{ params.stooq_end }}',
            '--gdelt-start', '{{ params.gdelt_start }}',
            '--gdelt-end', '{{ params.gdelt_end }}',
        ],
        deploy_mode="cluster",
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        verbose=True
    )

# DAG 2: Bronze Only
with DAG(
    'stock_bronze_ingestion',
    default_args=default_args,
    description='Ingest data to Bronze layer',
    schedule_interval='0 5 * * *',
    catchup=False,
    tags=['bronze'],
    params={
        'stooq_tickers': Param(default='NVDA,MSFT', type='string'),
        'stooq_start': Param(default='2020-01-02', type='string'),
        'stooq_end': Param(default='2025-12-22', type='string'),
        'gdelt_start': Param(default='2019-12-31', type='string'),
        'gdelt_end': Param(default='2025-12-23', type='string'),
        'skip_stooq': Param(default='false', type='string', enum=['true', 'false']),
        'skip_gdelt': Param(default='false', type='string', enum=['true', 'false'])
    },
    render_template_as_native_obj=True,
) as dag_bronze:
    
    bronze_ingestion = SparkSubmitOperator(
        task_id='ingest_bronze',
        application=f'{SPARK_APPS_PATH}/pipeline_v2.py',
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
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        verbose=True,
    )


# DAG 3: Silver Processing
with DAG(
    'stock_silver_processing',
    default_args=default_args,
    description='Process Bronze → Silver',
    schedule_interval='0 7 * * *',
    catchup=False,
    tags=['silver'],
    params={
        'stooq_tickers': Param(default='NVDA,MSFT', type='string'),
    },
    render_template_as_native_obj=True,
) as dag_silver:
    
    silver_processing = SparkSubmitOperator(
        task_id='process_silver',
        application=f'{SPARK_APPS_PATH}/pipeline.py',
        conn_id='spark_default',
        application_args=[
            '--pipeline', 'silver',
        ],
        deploy_mode="client",
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        verbose=True
    )