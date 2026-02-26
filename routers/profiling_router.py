from fastapi import APIRouter, HTTPException, Depends, Query, status
from domain.request.profiling_job_create_request import ProfilingJobCreateRequest
from services.profiling_service import ProfilingService
from .dependencies import get_profiling_service
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/profiling",
    tags=["Data Quality Profiling"],
    responses={404: {"description": "Not found"}},
)


@router.post("/trigger",
             summary="Trigger DQ profiling for tables",
             response_description="DAG trigger result")
def trigger_profiling(
    request: ProfilingJobCreateRequest,
    profiling_service: ProfilingService = Depends(get_profiling_service),
):
    """Trigger DQ profiling on Bronze Iceberg tables via Airflow Spark job"""
    try:
        dag_conf = request.to_dag_conf()
        table_names = [f"{t.schema_name}.{t.table_name}" for t in request.tables]
        logger.info(f"Triggering profiling for: {table_names}")

        result = profiling_service.trigger_profiling(dag_conf)

        if result.get("status") == "error":
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.get("message", "Failed to trigger profiling")
            )
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error triggering profiling: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/results/{run_id}",
            summary="Get profiling results by run ID",
            response_description="Profiling results for a run")
def get_profiling_results_by_run(
    run_id: str,
    profiling_service: ProfilingService = Depends(get_profiling_service),
):
    """Get all profiling metric results for a specific run"""
    results = profiling_service.get_results_by_run(run_id)
    return {"profile_run_id": run_id, "results": results, "count": len(results)}


@router.get("/table/{schema_name}/{table_name}",
            summary="Get profiling results for a table",
            response_description="Recent profiling results")
def get_profiling_results_by_table(
    schema_name: str,
    table_name: str,
    limit: int = Query(default=100, le=1000),
    profiling_service: ProfilingService = Depends(get_profiling_service),
):
    """Get recent profiling results for a specific table"""
    results = profiling_service.get_results_by_table(schema_name, table_name, limit)
    return {
        "schema_name": schema_name,
        "table_name": table_name,
        "results": results,
        "count": len(results)
    }


@router.get("/table/{schema_name}/{table_name}/latest",
            summary="Get latest profiling run grouped by dimension",
            response_description="Latest profiling results")
def get_latest_profile(
    schema_name: str,
    table_name: str,
    profiling_service: ProfilingService = Depends(get_profiling_service),
):
    """Get the latest profiling run results, grouped by dimension for visualization"""
    result = profiling_service.get_latest_profile(schema_name, table_name)
    if not result:
        return {
            "profile_run_id": None,
            "schema_name": schema_name,
            "table_name": table_name,
            "dimensions": {},
            "total_metrics": 0,
        }
    return result


@router.get("/table/{schema_name}/{table_name}/history",
            summary="Get profiling run history",
            response_description="Historical run summaries")
def get_profiling_history(
    schema_name: str,
    table_name: str,
    limit: int = Query(default=20, le=100),
    profiling_service: ProfilingService = Depends(get_profiling_service),
):
    """Get historical profiling run summaries for a table"""
    return profiling_service.get_run_history(schema_name, table_name, limit)
