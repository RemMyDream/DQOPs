from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

SparkKubernetesOperator.template_ext = ()

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 0
}

with DAG(
    dag_id='dq_profiling',
    default_args=default_args,
    description='Data Quality Profiling for Bronze Iceberg tables',
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'dq', 'profiling']
) as dag:

    run_profiling = SparkKubernetesOperator(
        task_id='run_dq_profiling',
        namespace='data-pipeline',
        application_file="/opt/airflow/dags/spark-apps/dq_profiling.yaml",
        kubernetes_conn_id='kubernetes_default',
        base_container_name='spark-kubernetes-driver',
        do_xcom_push=False,
        get_logs=True
    )

    run_profiling
