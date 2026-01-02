from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
import json

default_args = {
    'owner': 'admin',
    'start_date': days_ago(0),
    'retries': 0
}

def prepare_spark_config(**context):
    conf = context["dag_run"].conf
    connection_name = conf["connection_name"]
    tables = conf["tables"]
    layer = conf.get("layer", "bronze")
    
    conn = BaseHook.get_connection(connection_name)
    
    spark_config = {
        "tables": tables,
        "layer": layer,
        "jdbc_url": f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}",
        "database": conn.schema,
        "username": conn.login,
        "password": conn.password
    }
    
    return json.dumps(spark_config)

with DAG(
    dag_id='ingest_to_bronze',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=5,
    tags=['ingest', 'bronze']
) as dag:
    
    prepare_config = PythonOperator(
        task_id='prepare_config',
        python_callable=prepare_spark_config
    )
    
    ingest_task = SparkSubmitOperator(
        task_id='spark_ingest',
        application='/opt/airflow/spark_apps/write_to_layer.py',
        conn_id='spark_default',
        packages='io.delta:delta-spark_2.12:3.2.0,'
                 'org.apache.hadoop:hadoop-aws:3.3.4,'
                 'com.amazonaws:aws-java-sdk-bundle:1.12.262,'
                 'org.postgresql:postgresql:42.7.3',
        application_args=[
            '--config', '{{ ti.xcom_pull(task_ids="prepare_config") }}'
        ],
        verbose=True,
        execution_timeout=timedelta(minutes=60),
    )
    
    prepare_config >> ingest_task