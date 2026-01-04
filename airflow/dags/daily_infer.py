# """
# Airflow DAG for Daily Stock Inference
# Runs predictions using the trained model in MLflow
# """
# from airflow import DAG
# from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
# from airflow.kubernetes.secret import Secret
# from airflow.utils.dates import days_ago
# from datetime import timedelta
# from kubernetes.client import models as k8s

# # Default arguments
# default_args = {
#     'owner': 'Hanh',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 2,
#     'retry_delay': timedelta(minutes=5),
# }

# # Define the DAG
# with DAG(
#     'daily_stock_inference',
#     default_args=default_args,
#     description='Runs daily stock prediction inference on KinD',
#     schedule_interval='0 9 * * *',  # Run daily at 9 AM
#     start_date=days_ago(1),
#     catchup=False,
#     tags=['ml', 'stock', 'inference'],
# ) as dag:

#     # Environment variables
#     env_vars = {
#         'MLFLOW_TRACKING_URI': 'http://mlflow-service.default.svc.cluster.local:5000',
#         'TF_CPP_MIN_LOG_LEVEL': '3',
#         'TF_ENABLE_ONEDNN_OPTS': '0'
#     }

#     # Secrets for MinIO Access
#     # Assumes a K8s secret named 'minio-credentials' exists
#     secret_aws_key = Secret(
#         deploy_type='env',
#         deploy_target='AWS_ACCESS_KEY_ID',
#         secret='minio-credentials',  # Your K8s secret name
#         key='access_key'
#     )
    
#     secret_aws_secret = Secret(
#         deploy_type='env',
#         deploy_target='AWS_SECRET_ACCESS_KEY',
#         secret='minio-credentials',  # Your K8s secret name
#         key='secret_key'
#     )

#     # Task: Run Inference for NVDA
#     predict_nvda = KubernetesPodOperator(
#         task_id='predict_nvda',
#         name='stock-inference-nvda-{{ ts_nodash | lower }}',  # Unique pod name
#         namespace='default',
#         image='stock-inference:latest',
#         image_pull_policy='IfNotPresent',
#         cmds=["python", "infer.py"],  # FIXED: Correct filename
#         arguments=[
#             "--stock", "NVDA",
#             "--model-name", "stock-gru-model",
#             "--look-back", "45",
#             "--minio-endpoint", "http://minio-service.default.svc.cluster.local:9000",
#             # Add --no-sentiment if your model was trained without sentiment
#         ],
#         env_vars=env_vars,
#         secrets=[secret_aws_key, secret_aws_secret],
#         container_resources=k8s.V1ResourceRequirements(
#             requests={"cpu": "500m", "memory": "1Gi"},
#             limits={"cpu": "2000m", "memory": "2Gi"}
#         ),
#         get_logs=True,
#         log_events_on_failure=True,
#         is_delete_operator_pod=True,  # Clean up pods after completion
#         in_cluster=True,  # Running inside K8s
#     )

#     # Optional: Add more stocks
#     predict_aapl = KubernetesPodOperator(
#         task_id='predict_aapl',
#         name='stock-inference-aapl-{{ ts_nodash | lower }}',
#         namespace='default',
#         image='stock-inference:latest',
#         image_pull_policy='IfNotPresent',
#         cmds=["python", "infer.py"],
#         arguments=[
#             "--stock", "AAPL",
#             "--model-name", "stock-gru-model",
#             "--look-back", "45",
#             "--minio-endpoint", "http://minio-service.default.svc.cluster.local:9000",
#         ],
#         env_vars=env_vars,
#         secrets=[secret_aws_key, secret_aws_secret],
#         container_resources=k8s.V1ResourceRequirements(
#             requests={"cpu": "500m", "memory": "1Gi"},
#             limits={"cpu": "2000m", "memory": "2Gi"}
#         ),
#         get_logs=True,
#         is_delete_operator_pod=True,
#         in_cluster=True,
#     )

#     # Run predictions in parallel
#     [predict_nvda, predict_aapl]
