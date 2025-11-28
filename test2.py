import requests
from typing import Dict

airflow_user = "admin"
airflow_password = "admin"
airflow_url = "http://localhost:8080"  # nếu chạy từ máy host
dag_id = "my_test_dag1"


def trigger_airflow_dag(dag_name: str, dag_run_id: str, dag_conf: Dict) -> Dict:
    response = requests.patch(
    f"{airflow_url}/api/v1/dags/{dag_id}",
    json={"is_paused": False},
    auth=(airflow_user, airflow_password),
    headers={"Content-Type": "application/json"}
)   
    print(response.status_code, response.json())

    response = requests.post(
        f"{airflow_url}/api/v1/dags/{dag_name}/dagRuns",
        json={
            "dag_run_id": dag_run_id,
            "conf": dag_conf
        },
        auth=(airflow_user, airflow_password),
        headers={"Content-Type": "application/json"}
    )
    print("Status:", response.status_code)
    return response.json()

result = trigger_airflow_dag(
    dag_id,
    "manual__001",
    {"param1": 14563, "param2": 234 }
)

print(result)
