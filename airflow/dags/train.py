from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.models import Variable
from kubernetes.client import models as k8s

# Default args
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

NAMESPACE = Variable.get("namespace", default_var="data-pipeline")
TRAINER_IMAGE = Variable.get("trainer_image", default_var="remmydream/model:v3.1")

# Git config
GIT_REPO = Variable.get("git_repo", default_var="https://github.com/RemMyDream/DQOPs.git")
GIT_BRANCH = Variable.get("git_branch", default_var="model")

# Resource config
RESOURCE_REQUESTS = {"cpu": "2", "memory": "4Gi"}
RESOURCE_LIMITS = {"cpu": "4", "memory": "8Gi"}

code_volume = k8s.V1Volume(
    name="code",
    empty_dir=k8s.V1EmptyDirVolumeSource()
)

code_volume_mount = k8s.V1VolumeMount(
    name="code",
    mount_path="/app"
)

git_sync_init = k8s.V1Container(
    name="git-sync",
    image="alpine/git:latest",
    command=["sh", "-c"],
    args=[
        f"git clone --branch {GIT_BRANCH} --single-branch --depth 1 {GIT_REPO} /app && ls -la /app"
    ],
    volume_mounts=[code_volume_mount]
)

def get_env_vars(minio_endpoint):
    return [
        k8s.V1EnvVar(
            name="AWS_ACCESS_KEY_ID",
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name="minio-credential",
                    key="MINIO_ROOT_USER"
                )
            )
        ),
        k8s.V1EnvVar(
            name="AWS_SECRET_ACCESS_KEY",
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name="minio-credential",
                    key="MINIO_ROOT_PASSWORD"
                )
            )
        ),
        k8s.V1EnvVar(name="MLFLOW_S3_ENDPOINT_URL", value=minio_endpoint),
        k8s.V1EnvVar(name="MLFLOW_S3_IGNORE_TLS", value="true"),
        k8s.V1EnvVar(name="GIT_PYTHON_REFRESH", value="quiet"),
    ]

with DAG(
    dag_id='churn_model_training',
    default_args=default_args,
    description='Train churn prediction models (XGBoost, LightGBM)',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ml', 'training', 'churn'],
    params={
        "features_path": "gold/data_source/churn_features/data",
        "labels_path": "gold/data_source/churn_label/data",
        "minio_endpoint": "http://minio-svc:9000",
        "mlflow_uri": "http://mlflow-svc:5000",
        "mlflow_experiment": "churn-prediction",
        "n_trials": "1",
        "n_splits": "5",
    }
) as dag:
    
    # Train XGBoost
    train_xgb = KubernetesPodOperator(
        task_id='train_xgboost',
        name='train-xgboost',
        namespace=NAMESPACE,
        image=TRAINER_IMAGE,
        cmds=["python", "/app/trainer.py"],
        arguments=[
            "--features-path", "{{ params.features_path }}",
            "--labels-path", "{{ params.labels_path }}",
            "--minio-endpoint", "{{ params.minio_endpoint }}",
            "--mlflow-tracking-uri", "{{ params.mlflow_uri }}",
            "--mlflow-experiment", "{{ params.mlflow_experiment }}",
            "--models", "xgb",
            "--n-trials", "{{ params.n_trials }}",
            "--n-splits", "{{ params.n_splits }}",
            "--register-models",
        ],
        init_containers=[git_sync_init],
        volumes=[code_volume],
        volume_mounts=[code_volume_mount],
        env_vars=get_env_vars("{{ params.minio_endpoint }}"),
        container_resources=k8s.V1ResourceRequirements(
            requests=RESOURCE_REQUESTS,
            limits=RESOURCE_LIMITS
        ),
        is_delete_operator_pod=False,
        get_logs=True,
        startup_timeout_seconds=600,
    )
    
    # Train LightGBM
    train_lightgbm = KubernetesPodOperator(
        task_id='train_lightgbm',
        name='train-lightgbm',
        namespace=NAMESPACE,
        image=TRAINER_IMAGE,
        cmds=["python", "/app/trainer.py"],
        arguments=[
            "--features-path", "{{ params.features_path }}",
            "--labels-path", "{{ params.labels_path }}",
            "--minio-endpoint", "{{ params.minio_endpoint }}",
            "--mlflow-tracking-uri", "{{ params.mlflow_uri }}",
            "--mlflow-experiment", "{{ params.mlflow_experiment }}",
            "--models", "lightgbm",
            "--n-trials", "{{ params.n_trials }}",
            "--n-splits", "{{ params.n_splits }}",
            "--register-models",
        ],
        init_containers=[git_sync_init],
        volumes=[code_volume],
        volume_mounts=[code_volume_mount],
        env_vars=get_env_vars("{{ params.minio_endpoint }}"),
        container_resources=k8s.V1ResourceRequirements(
            requests=RESOURCE_REQUESTS,
            limits=RESOURCE_LIMITS
        ),
        is_delete_operator_pod=False,
        get_logs=True,
        startup_timeout_seconds=600,
    )
    
    [train_xgb, train_lightgbm]