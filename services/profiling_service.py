"""
Profiling Service: Trigger DQ profiling jobs and retrieve results
"""
from typing import Dict, Any, List, Optional
from domain.entity.job_client import JobType
from services.job_trigger_service import JobTriggerService
from repositories.profiling_repository import ProfilingRepository
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProfilingService:
    """Service for DQ profiling operations"""

    def __init__(
        self,
        profiling_repo: ProfilingRepository,
        trigger_service: JobTriggerService,
    ):
        self.profiling_repo = profiling_repo
        self.trigger_service = trigger_service

    def trigger_profiling(self, dag_conf: Dict[str, Any]) -> Dict[str, Any]:
        """Trigger profiling DAG via Airflow"""
        logger.info(f"Triggering profiling for tables: {dag_conf.get('tables', [])}")
        return self.trigger_service.trigger(
            job_type=JobType.QUALITY,
            dag_conf=dag_conf
        )

    def get_results_by_run(self, run_id: str) -> List[Dict[str, Any]]:
        """Get all profiling results for a specific run"""
        return self.profiling_repo.get_results_by_run_id(run_id)

    def get_results_by_table(
        self, schema_name: str, table_name: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get recent profiling results for a table"""
        return self.profiling_repo.get_results_by_table(schema_name, table_name, limit)

    def get_latest_profile(
        self, schema_name: str, table_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get the latest profiling run for a table, grouped by dimension.

        get_results_by_run_id returns list[dict] because SELECT * has many columns,
        each dict has keys: result_id, profile_run_id, schema_name, table_name,
        column_name, dimension, metric_name, actual_value, ...
        """
        run_id = self.profiling_repo.get_latest_run_id(schema_name, table_name)
        if not run_id:
            return None

        results = self.profiling_repo.get_results_by_run_id(run_id)

        grouped = {}
        for r in results:
            dim = r["dimension"]
            if dim not in grouped:
                grouped[dim] = []
            grouped[dim].append(r)

        return {
            "profile_run_id": run_id,
            "schema_name": schema_name,
            "table_name": table_name,
            "dimensions": grouped,
            "total_metrics": len(results),
        }

    def get_run_history(
        self, schema_name: str, table_name: str, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get historical profiling run summaries"""
        return self.profiling_repo.get_run_history(schema_name, table_name, limit)
