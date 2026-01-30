"""
Service: Job Trigger
Simplified service - trigger directly from request, save to DB is optional
"""
from typing import Dict, Any, Optional
import requests

from domain.entity.airflow_client import Airflow
from domain.entity.job_client import JobType
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class JobTriggerService:
    """Simplified Job Trigger Service"""
    
    DAG_MAPPING = {
    JobType.INGEST: {
        "bronze": "ingest_to_bronze",
        "silver": "ingest_to_silver",
        "default": "ingest_to_bronze"
    },
    JobType.TRANSFORM: {
        "silver": "transform_to_silver",
        "gold": "transform_to_gold",
        "default": "transform_to_silver"
    },
    JobType.EXPORT: {
        "postgres": "export_to_postgres",
        "s3": "export_to_s3",
        "default": "export_to_postgres"
    }
}
    def __init__(self, airflow: Airflow):
        self.airflow = airflow

    def trigger(
        self,
        job_type: JobType,
        dag_conf: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Core trigger method - triggers DAG directly from config"""
        dag_id = self._get_dag_id(job_type, dag_conf)
        logger.info(f"Triggering {job_type.value} job: DAG={dag_id}")
        return self._trigger_dag(dag_id, dag_conf)
    
    def _get_dag_id(self, job_type: JobType, dag_conf: Dict[str, Any]) -> str:
        """Determine DAG ID based on job type and config"""
        type_mapping = self.DAG_MAPPING.get(job_type, {})
        
        if job_type == JobType.INGEST:
            target_key = dag_conf.get('layer', 'bronze')
        else:
            target_key = 'default'
        
        return type_mapping.get(target_key, type_mapping.get('default', 'unknown_dag'))
    
    def _unpause_dag(self, dag_id: str) -> bool:
        """Unpause DAG before triggering"""
        try:
            response = requests.patch(
                f"{self.airflow.url}/api/v1/dags/{dag_id}",
                json={"is_paused": False},
                auth=self.airflow.auth,
                headers=self.airflow.headers,
                timeout=30
            )
            response.raise_for_status()
            return True
        except requests.RequestException as e:
            logger.error(f"Failed to unpause DAG {dag_id}: {e}")
            return False
    
    def _trigger_dag(
        self,
        dag_id: str,
        dag_conf: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Internal method to trigger Airflow DAG"""
        if not self._unpause_dag(dag_id):
            return {
                "status": "error",
                "message": f"Failed to unpause DAG {dag_id}",
                "dag_id": dag_id,
            }
        
        try:
            response = requests.post(
                f"{self.airflow.url}/api/v1/dags/{dag_id}/dagRuns",
                json={"conf": dag_conf},
                auth=self.airflow.auth,
                headers=self.airflow.headers,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"Triggered DAG {dag_id} with run_id {result['dag_run_id']}")
            
            return {
                "status": "success",
                "message": f"DAG {dag_id} triggered successfully",
            } | result
            
        except requests.HTTPError as e:
            error_detail = str(e)
            if e.response is not None:
                try:
                    error_detail = e.response.json().get("detail", str(e))
                except:
                    error_detail = e.response.text
            
            logger.error(f"HTTP error: {error_detail}")
            return {
                "status": "error",
                "message": f"HTTP error: {error_detail}",
                "dag_id": dag_id,
            }
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return {
                "status": "error",
                "message": str(e),
                "dag_id": dag_id,
            }
    
    def get_dag_run_info(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """Get status of a DAG run"""
        try:
            response = requests.get(
                f"{self.airflow.url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}",
                auth=self.airflow.auth,
                headers=self.airflow.headers,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            return {
                "status": "success",
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "state": result.get("state"),
                "execution_date": result.get("execution_date"),
                "start_date": result.get("start_date"),
                "end_date": result.get("end_date")
            }
        except requests.RequestException as e:
            return {
                "status": "error",
                "message": str(e),
                "dag_id": dag_id,
                "dag_run_id": dag_run_id
            }
    
    def get_dag_runs(self, dag_id: str, limit: int = 10, state: Optional[str] = None) -> Dict[str, Any]:
        """Get recent DAG runs"""
        try:
            params = {"limit": limit, "order_by": "-execution_date"}
            if state:
                params["state"] = state
            
            response = requests.get(
                f"{self.airflow.url}/api/v1/dags/{dag_id}/dagRuns",
                params=params,
                auth=self.airflow.auth,
                headers=self.airflow.headers,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            return {
                "status": "success",
                "dag_id": dag_id,
                "total_entries": result.get("total_entries", 0),
                "dag_runs": [
                    {
                        "dag_run_id": run.get("dag_run_id"),
                        "state": run.get("state"),
                        "execution_date": run.get("execution_date"),
                        "start_date": run.get("start_date"),
                        "end_date": run.get("end_date")
                    }
                    for run in result.get("dag_runs", [])
                ]
            }
        except requests.RequestException as e:
            return {
                "status": "error",
                "message": str(e),
                "dag_id": dag_id,
                "total_entries": 0,
                "runs": []
            }