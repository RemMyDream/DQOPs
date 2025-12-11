"""
Ingest Job Trigger Service
Handles triggering Airflow DAGs for data ingestion jobs
"""
from typing import Dict, Any, Optional
from datetime import datetime
import requests

from utils.helpers import create_logger
from domain.entity.airflow_client import Airflow
from domain.request.ingest_job_request import IngestJobCreateRequest

logger = create_logger("IngestJobTriggerService")


class IngestJobTriggerService:
    """Service layer for triggering Ingest Jobs via Airflow"""
    
    DAG_ID = "ingest_to_bronze"  # Default DAG for ingestion
    
    def __init__(self, airflow: Airflow):
        self.airflow = airflow
        self.base_url = airflow.airflow_url
        self.auth = (airflow.airflow_user, airflow.airflow_password)
        self.headers = {"Content-Type": "application/json"}
    
    def _unpause_dag(self, dag_id: str) -> bool:
        """Unpause a DAG before triggering"""
        try:
            response = requests.patch(
                f"{self.base_url}/api/v1/dags/{dag_id}",
                json={"is_paused": False},
                auth=self.auth,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            logger.info(f"Unpaused DAG: {dag_id}")
            return True
        except requests.RequestException as e:
            logger.error(f"Failed to unpause DAG {dag_id}: {e}")
            return False
    
    def _generate_dag_run_id(self, job_name: str) -> str:
        """Generate unique dag_run_id"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"manual__{job_name}__{timestamp}"
    
    def trigger_ingest_job(
        self,
        request: IngestJobCreateRequest,
        dag_id: Optional[str] = None,
        dag_run_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Trigger an ingest job by calling Airflow DAG
        
        Args:
            request: IngestJobCreateRequest with all job configurations
            dag_id: Optional custom DAG ID (defaults to self.DAG_ID)
            dag_run_id: Optional custom run ID (auto-generated if not provided)
            
        Returns:
            Dict with trigger result and status
        """
        dag_id = dag_id or self.DAG_ID
        dag_run_id = dag_run_id or self._generate_dag_run_id(request.jobName)
        
        # Build DAG conf from request - truyền trực tiếp config
        dag_conf = {
            "job_name": request.jobName,
            "connection_name": request.connectionName,
            "schema_name": request.source.schemaName,
            "table_name": request.source.tableName,
            "primary_keys": request.source.primaryKeys,
            "layer": request.target.layer,
            "path": request.target.path,
            "format": request.target.format,
            "created_by": request.createdBy
        }
        
        return self.trigger_dag(dag_id, dag_run_id, dag_conf)
    
    def trigger_dag(
        self,
        dag_id: str,
        dag_run_id: str,
        dag_conf: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generic method to trigger any Airflow DAG with config
        
        Args:
            dag_id: The DAG to trigger
            dag_run_id: Unique identifier for this run
            dag_conf: Configuration dictionary to pass to DAG
            
        Returns:
            Dict with status, dag_run_id, and response data
        """
        try:
            # Unpause DAG first
            if not self._unpause_dag(dag_id):
                return {
                    "status": "error",
                    "message": f"Failed to unpause DAG {dag_id}",
                    "dag_id": dag_id,
                    "dag_run_id": dag_run_id
                }
            
            # Trigger DAG run
            response = requests.post(
                f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns",
                json={
                    "dag_run_id": dag_run_id,
                    "conf": dag_conf
                },
                auth=self.auth,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"Triggered DAG {dag_id} with run_id {dag_run_id}")
            
            return {
                "status": "success",
                "message": f"DAG {dag_id} triggered successfully",
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "state": result.get("state"),
                "execution_date": result.get("execution_date"),
                "conf": dag_conf
            }
            
        except requests.HTTPError as e:
            error_detail = ""
            if e.response is not None:
                try:
                    error_detail = e.response.json().get("detail", str(e))
                except:
                    error_detail = e.response.text
            
            logger.error(f"HTTP error triggering DAG {dag_id}: {error_detail}")
            return {
                "status": "error",
                "message": f"HTTP error: {error_detail}",
                "dag_id": dag_id,
                "dag_run_id": dag_run_id
            }
            
        except requests.RequestException as e:
            logger.error(f"Request error triggering DAG {dag_id}: {e}")
            return {
                "status": "error",
                "message": f"Request failed: {str(e)}",
                "dag_id": dag_id,
                "dag_run_id": dag_run_id
            }
        
        except Exception as e:
            logger.error(f"Unexpected error triggering DAG {dag_id}: {e}")
            return {
                "status": "error",
                "message": f"Unexpected error: {str(e)}",
                "dag_id": dag_id,
                "dag_run_id": dag_run_id
            }
    
    def get_dag_run_status(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """Get status of a specific DAG run"""
        try:
            response = requests.get(
                f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}",
                auth=self.auth,
                headers=self.headers,
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
            logger.error(f"Failed to get DAG run status: {e}")
            return {
                "status": "error",
                "message": str(e),
                "dag_id": dag_id,
                "dag_run_id": dag_run_id
            }