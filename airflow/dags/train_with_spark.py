# from datetime import datetime, timedelta

# from airflow import DAG
# from airflow.models import Variable
# from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (SparkKubernetesOperator)
# from kubernetes.client import models as k8s


# # ======================
# # Default args
# # ======================
# default_args = {
#     "owner": "data-engineer",
#     "depends_on_past": False,
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 0,
#     "retry_delay": timedelta(minutes=5),
# }

# # ======================
# # Variables
# # ======================
# NAMESPACE = Variable.get("namespace", default_var="data-pipeline")
# TRAINER_IMAGE = Variable.get("trainer_image", default_var="remmydream/model:v3.1")

# FEATURES_PATH = Variable.get(
#     "features_path", default_var="gold.data_source.churn_features"
# )
# LABELS_PATH = Variable.get(
#     "labels_path", default_var="gold.data_source.churn_label"
# )

# MINIO_ENDPOINT = Variable.get(
#     "minio_endpoint", default_var="http://minio-svc:9000"
# )
# MLFLOW_URI = Variable.get(
#     "mlflow_uri", default_var="http://mlflow-svc:5000"
# )
# MLFLOW_EXPERIMENT = Variable.get(
#     "mlflow_experiment", default_var="churn-prediction3"
# )

# N_TRIALS = "1"
# N_SPLITS = "5"

# # Git
# GIT_REPO = Variable.get("git_repo", default_var="https://github.com/RemMyDream/DQOPs.git")
# GIT_BRANCH = Variable.get("git_branch", default_var="model")

# spark_env = [
#     {
#         "name": "AWS_ACCESS_KEY_ID",
#         "valueFrom": {
#             "secretKeyRef": {
#                 "name": "minio-credential",
#                 "key": "MINIO_ROOT_USER",
#             }
#         },
#     },
#     {
#         "name": "AWS_SECRET_ACCESS_KEY",
#         "valueFrom": {
#             "secretKeyRef": {
#                 "name": "minio-credential",
#                 "key": "MINIO_ROOT_PASSWORD",
#             }
#         },
#     },
#     {"name": "MLFLOW_TRACKING_URI", "value": MLFLOW_URI},
#     {"name": "MLFLOW_S3_ENDPOINT_URL", "value": MINIO_ENDPOINT},
#     {"name": "MLFLOW_S3_IGNORE_TLS", "value": "true"},
# ]

# # ======================
# # Base SparkApplication spec
# # ======================
# def spark_app_spec(app_name: str, model_name: str):
#     return {
#         "apiVersion": "sparkoperator.k8s.io/v1beta2",
#         "kind": "SparkApplication",
#         "metadata": {
#             "name": app_name,
#             "namespace": NAMESPACE,
#         },
#         "spec": {
#             "type": "Python",
#             "pythonVersion": "3",
#             "mode": "cluster",
#             "image": TRAINER_IMAGE,
#             "imagePullPolicy": "IfNotPresent",
#             "mainApplicationFile": "local:///app/trainer.py",
#             "sparkVersion": "3.5.1",
#             "arguments": [
#                 "--features-table", FEATURES_PATH,
#                 "--labels-table", LABELS_PATH,
#                 "--mlflow-experiment", MLFLOW_EXPERIMENT,
#                 "--models", model_name,
#                 "--n-trials", N_TRIALS,
#                 "--n-splits", N_SPLITS,
#                 "--register-models",
#             ],
#             "sparkConf": {
#                 # ---------- MinIO ----------
#                 "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
#                 "spark.hadoop.fs.s3a.path.style.access": "true",
#                 "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
#                 "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",

#                 # ---------- Iceberg ----------
#                 "spark.sql.extensions": (
#                     "org.apache.iceberg.spark.extensions."
#                     "IcebergSparkSessionExtensions"
#                 ),
#                 "spark.sql.catalog.bronze": (
#                     "org.apache.iceberg.spark.SparkCatalog"
#                 ),
#                 "spark.sql.catalog.bronze.type": "hadoop",
#                 "spark.sql.catalog.bronze.warehouse": "s3a://bronze",

#                 # ---------- MLflow ----------
#                 "spark.executorEnv.MLFLOW_TRACKING_URI": MLFLOW_URI,
#                 "spark.executorEnv.MLFLOW_S3_ENDPOINT_URL": MINIO_ENDPOINT,
#             },
#             "driver": {
#                 "cores": 1,
#                 "memory": "2g",
#                 "serviceAccount": "spark-operator-spark",
#                 "env": spark_env,
#                 "volumeMounts": [
#                     {"name": "code", "mountPath": "/app"}
#                 ],
#                 "initContainers": [
#                     {
#                         "name": "git-sync",
#                         "image": "alpine/git:latest",
#                         "command": ["sh", "-c"],
#                         "args": [
#                             f"git clone --branch {GIT_BRANCH} "
#                             f"--single-branch --depth 1 "
#                             f"{GIT_REPO} /app"
#                         ],
#                         "volumeMounts": [
#                             {"name": "code", "mountPath": "/app"}
#                         ],
#                     }
#                 ],
#             },
#             "executor": {
#                 "cores": 1,
#                 "instances": 2,
#                 "memory": "4g",
#                 "env": spark_env,
#                 "volumeMounts": [
#                     {"name": "code", "mountPath": "/app"}
#                 ],
#             },
#             "volumes": [
#                 {"name": "code", "emptyDir": {}}
#             ],
#         },
#     }


# # ======================
# # DAG
# # ======================
# with DAG(
#     dag_id="churn_model_training",
#     default_args=default_args,
#     description="Train churn models with Spark Iceberg + MLflow",
#     schedule_interval=None,
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     tags=["ml", "spark", "iceberg", "churn"],
# ) as dag:

#     # -------- XGBoost --------
#     train_xgb = SparkKubernetesOperator(
#         task_id="train_xgboost",
#         namespace=NAMESPACE,
#         application=spark_app_spec(
#             app_name="churn-train-xgb",
#             model_name="xgb",
#         ),
#         do_xcom_push=False,
#     )

#     wait_xgb = SparkKubernetesSensor(
#         task_id="wait_train_xgb",
#         namespace=NAMESPACE,
#         application_name="churn-train-xgb",
#     )

#     # -------- LightGBM --------
#     train_lgbm = SparkKubernetesOperator(
#         task_id="train_lightgbm",
#         namespace=NAMESPACE,
#         application=spark_app_spec(
#             app_name="churn-train-lightgbm",
#             model_name="lightgbm",
#         ),
#         do_xcom_push=False,
#     )

#     wait_lgbm = SparkKubernetesSensor(
#         task_id="wait_train_lightgbm",
#         namespace=NAMESPACE,
#         application_name="churn-train-lightgbm",
#     )

#     # Parallel execution
#     train_xgb >> wait_xgb
#     train_lgbm >> wait_lgbm
