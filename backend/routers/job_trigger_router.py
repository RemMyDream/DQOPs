"""
Router: Job Trigger Endpoints
Simplified flow: Trigger first, save to DB is optional

Endpoints:
- POST /trigger/ingest     → Trigger INGEST job (optionally save)
- POST /trigger/transform  → Trigger TRANSFORM job (optionally save)  
- POST /trigger/job/{id}   → Re-trigger existing job from DB
- GET  /trigger/status/... → Check status
"""
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, HTTPException, Depends, Query, status
from pydantic import BaseModel, Field

from domain.entity.job_client import JobType
from domain.request.ingest_job_create_request import IngestSourceConfig, IngestTargetConfig, IngestJobCreateRequest

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

# ==================== Request/Response Models ====================

class TriggerResponse(BaseModel):
    status: str
    message: str
    job_type: Optional[str] = None
    dag_id: str
    dag_run_id: str
    state: Optional[str] = None
    execution_date: Optional[str] = None
    conf: Optional[Dict[str, Any]] = None
    job_id: Optional[int] = None
    job_name: Optional[str] = None

# ==================== TRIGGER ENDPOINTS ====================

@router.post("/ingest")
def trigger_ingest(
    request: IngestJobCreateRequest,
    trigger_service: JobTriggerService = Depends(get_job_trigger_service),
    job_service: JobService = Depends(get_job_service)
):
    """
    Trigger an INGEST job
    
    Flow:
    1. Trigger Airflow DAG immediately
    2. Save job to database
    
    Use saveJob=True if you want to:
    - Re-run this job later
    - Track job history
    - Add scheduling later
    """
    try:
        dag_conf = request.to_dag_conf()
        
        # 1. TRIGGER FIRST
        logger.info(f"Triggering INGEST: {request.source.schema_name}.{request.source.table_name}")
        
        trigger_result = trigger_service.trigger(
            job_type=JobType.INGEST,
            dag_conf=dag_conf,
        )
        
        if trigger_result["status"] == "error":
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=trigger_result["message"]
            )
        
        job_id = None
        
        # 2. SAVE TO DB
        if not request.job_name:
            request.job_name = f"ingest_{request.source.schema_name}_{request.source.table_name}"
        
        try:
            job_result = job_service.create_job(request)
            job_id = job_result.get("job_id")
            logger.info(f"Saved job to DB: job_id={job_id}")
            
        except Exception as e:
            logger.warning(f"Trigger succeeded but failed to save job: {e}")
    
        return { 
            "status" : trigger_result['status'],
            "message" : trigger_result['message']
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error triggering ingest: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

# ==================== RE-TRIGGER EXISTING JOB ====================

# @router.post("/job/{job_id}", response_model=TriggerResponse)
# async def retrigger_job(
#     job_id: int,
#     dag_run_id: Optional[str] = Query(default=None, alias="dagRunId"),
#     trigger_service: JobTriggerService = Depends(get_job_trigger_service),
#     job_service: JobService = Depends(get_job_service)
# ) -> TriggerResponse:
#     """
#     Re-trigger an existing job from database
    
#     Use this for:
#     - Re-running failed jobs
#     - Manual re-runs of scheduled jobs
#     """
#     try:
#         logger.info(f"Re-triggering job: {job_id}")
        
#         result = trigger_service.retrigger_job(
#             job_id=job_id,
#             job_service=job_service,
#             dag_run_id=dag_run_id
#         )
        
#         if result["status"] == "error":
#             raise HTTPException(
#                 status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#                 detail=result["message"]
#             )
        
#         return TriggerResponse(**result, jobId=job_id)
        
#     except ValueError as e:
#         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Error re-triggering job {job_id}: {e}")
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=str(e)
#         )


# # ==================== STATUS ENDPOINTS ====================

# @router.get("/status/{dag_id}/{dag_run_id}", response_model=StatusResponse)
# async def get_run_status(
#     dag_id: str,
#     dag_run_id: str,
#     trigger_service: JobTriggerService = Depends(get_job_trigger_service)
# ) -> StatusResponse:
#     """Get status of a DAG run"""
#     try:
#         result = trigger_service.get_dag_run_status(dag_id, dag_run_id)
#         return StatusResponse(**result)
#     except Exception as e:
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=str(e)
#         )


# @router.get("/runs/{dag_id}")
# async def get_dag_runs(
#     dag_id: str,
#     limit: int = Query(default=10, le=100),
#     state: Optional[str] = Query(default=None),
#     trigger_service: JobTriggerService = Depends(get_job_trigger_service)
# ):
#     """Get recent DAG runs"""
#     try:
#         return trigger_service.get_dag_runs(dag_id, limit=limit, state=state)
#     except Exception as e:
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=str(e)
#         )