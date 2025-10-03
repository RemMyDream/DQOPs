from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'chien',
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    'streaming_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    kafka_task = BashOperator(
        task_id='kafka_streaming',
        bash_command='python /opt/airflow/scripts/kafka_event_streaming.py',
        dag=dag
    )

    spark_submit_task = SparkSubmitOperator(
        task_id='spark_task',
        application='/opt/spark/apps/spark_processing.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,' \
        'org.apache.hadoop:hadoop-aws:3.3.4,'\
        'com.amazonaws:aws-java-sdk-bundle:1.12.262',
        dag=dag
    )
