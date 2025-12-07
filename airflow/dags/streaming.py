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
    'data_quality_app',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # postgres_connect_task = SparkSubmitOperator(
    #     task_id='postgres_connection',
    #     application='/opt/spark/apps/postgres_connection.py',
    #     conn_id='spark_default',
    #     packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,' \
    #     'org.apache.hadoop:hadoop-aws:3.3.4,'\
    #     'com.amazonaws:aws-java-sdk-bundle:1.12.262',
    #     dag=dag
    # )

    # ingest_bronze_task = SparkSubmitOperator(
    #     task_id='bronze_ingestion',
    #     application='/opt/spark/apps/bronze_ingestion.py',
    #     conn_id='spark_default',
    #     packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,' \
    #     'org.apache.hadoop:hadoop-aws:3.3.4,'\
    #     'com.amazonaws:aws-java-sdk-bundle:1.12.262',
    #     dag=dag
    # )


    # postgres_connect_task >> ingest_bronze_task

    task = BashOperator(
        task_id='test',
        bash_command='python /opt/airflow/dags/test1.py',
        dag=dag
    )
    task