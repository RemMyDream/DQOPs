"""
Router: Job Trigger Endpoints
"""
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends, Query, status
from pydantic import BaseModel

from domain.entity.job_client import JobType
from domain.request.ingest_job_create_request import IngestJobCreateRequest

from services.job_trigger_service import JobTriggerService
from services.job_service import JobService
from routers.dependencies import get_job_trigger_service, get_job_service
from utils.helpers import create_logger

logger = create_logger("JobTriggerRouter")

router = APIRouter(
    prefix="/trigger",
    tags=["Job Trigger"],
    responses={404: {"description": "Not found"}},
)

# ==================== Response Models ====================

class StatusResponse(BaseModel):
    status: str
    dag_id: str
    dag_run_id: str
    state: Optional[str] = None
    message: Optional[str] = None
    execution_date: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

# ==================== TRIGGER ENDPOINTS ====================

@router.post("/ingest", response_model=StatusResponse)
def trigger_ingest(
    request: IngestJobCreateRequest,
    trigger_service: JobTriggerService = Depends(get_job_trigger_service),
    job_service: JobService = Depends(get_job_service)
):
    """Trigger ingest for one or more tables"""
    try:
        dag_conf = request.to_dag_conf()
        
        table_names = [f"{t.schema_name}.{t.table_name}" for t in request.tables]
        logger.info(f"Triggering INGEST: {len(request.tables)} table(s) - {table_names}")
        
        # 1. TRIGGER DAG
        trigger_result = trigger_service.trigger(
            job_type=JobType.INGEST,
            dag_conf=dag_conf,
        )
        
        if trigger_result["status"] == "error":
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=trigger_result["message"]
            )
        
        # 2. SAVE JOB TO DB
        try:
            job_result = job_service.create_job(request)
            logger.info(f"Saved job to DB: job_id={job_result.get('job_id')}")
        except Exception as e:
            logger.warning(f"Trigger succeeded but failed to save job: {e}")
        
        return StatusResponse(**trigger_result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error triggering ingest: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

# ==================== STATUS ENDPOINTS ====================

@router.get("/status/{dag_id}/{dag_run_id}", response_model=StatusResponse)
def get_run_status(
    dag_id: str,
    dag_run_id: str,
    trigger_service: JobTriggerService = Depends(get_job_trigger_service)
):
    """Get status of a DAG run"""
    try:
        result = trigger_service.get_dag_run_info(dag_id, dag_run_id)
        return StatusResponse(**result)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.get("/runs/{dag_id}")
def get_dag_runs(
    dag_id: str,
    limit: int = Query(default=10, le=100),
    state: Optional[str] = Query(default=None),
    trigger_service: JobTriggerService = Depends(get_job_trigger_service)
):
    """Get recent DAG runs"""
    try:
        return trigger_service.get_dag_runs(dag_id, limit=limit, state=state)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )