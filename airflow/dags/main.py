from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.operators.python import PythonOperator

SparkKubernetesOperator.template_ext = ()

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 0
}

with DAG(
    dag_id='spark_postgres_to_iceberg',
    default_args=default_args,
    description='Ingest data from PostgreSQL to Minio',
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'iceberg']
) as dag:


    # bronze_ingestion = SparkKubernetesOperator(
    #     task_id='bronze_ingestion',
    #     namespace='data-pipeline',
    #     application_file="/opt/airflow/dags/spark-apps/bronze_ingestion.yaml",
    #     kubernetes_conn_id='kubernetes_default',
    #     base_container_name='spark-kubernetes-driver',
    #     do_xcom_push=False,
    #     get_logs=True
    # )

    gold_transformation = SparkKubernetesOperator(
        task_id='gold_transformation',
        namespace='data-pipeline',
        application_file="/opt/airflow/dags/spark-apps/gold_transformation.yaml",
        kubernetes_conn_id='kubernetes_default',
        base_container_name='spark-kubernetes-driver',
        do_xcom_push=False,
        get_logs=True
    )

    # bronze_ingestion >> gold_transformation
    gold_transformation

