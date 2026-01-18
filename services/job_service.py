"""
Service: Job Business Logic Layer
Supports multiple job types (Ingest, Transform, Export, etc.)
"""
from typing import List, Dict, Any, Optional, Union
from datetime import datetime

from repositories.job_repository import JobRepository
from repositories.job_version_repository import JobVersionRepository
from factories.job_factory import JobFactory

from domain.entity.job_client import Job, JobType, JobStatus
from domain.entity.job_schemas import JobVersion, JobSummary, ScheduleType

from domain.request.job_request import JobCreateRequest
from domain.request.ingest_job_create_request import IngestJobCreateRequest

logger = create_logger("JobService")

JobRequestType = Union[JobCreateRequest, IngestJobCreateRequest]


class JobService:
    """Service layer for Job operations"""
    
    def __init__(self, job_repo: JobRepository, version_repo: JobVersionRepository):
        self.job_repo = job_repo
        self.version_repo = version_repo
    
    def _convert_to_generic(self, request: JobRequestType) -> JobCreateRequest:
        """Convert any specific request type to generic JobCreateRequest"""
        if isinstance(request, JobCreateRequest):
            return request
        
        if hasattr(request, 'to_generic') and callable(request.to_generic):
            logger.debug(f"Converting {type(request).__name__} to generic request")
            return request.to_generic()
        
        raise ValueError(f"Unsupported request type: {type(request).__name__}")
    
    def _build_response(self, job: Job, version: JobVersion) -> Dict[str, Any]:
        """Build standardized response dict"""
        return job.to_dict() | version.to_dict()

    def create_job(self, request: JobRequestType) -> Dict[str, Any]:
        """Create new job with initial version"""
        generic_request = self._convert_to_generic(request)
        logger.info(f"Creating job: {generic_request.job_name} (type: {generic_request.job_type})")

        if self.job_repo.exists(generic_request.job_name):
            logger.warning(f"Job name already exists: {generic_request.job_name}")
            raise ValueError(f"Job with name '{generic_request.job_name}' already exists")    

        job = JobFactory.create_job(
            job_name=generic_request.job_name,
            job_type=generic_request.job_type,
            config=generic_request.config,
            created_by=generic_request.created_by            
        )
        
        job.validate()
        job.status = JobStatus.ACTIVE
        job.created_at = datetime.now()
        job.updated_at = datetime.now()
        
        job_id = self.job_repo.insert_job(job)
        job.job_id = job_id
        logger.debug(f"Job inserted with job_id: {job_id}")

        version = JobVersion(
            job_id=job_id,
            version_id=1,
            is_active=True,
            connection_name=generic_request.connection_name,
            config=generic_request.config,
            schedule_type=generic_request.schedule_type,
            schedule_cron=generic_request.schedule_cron,
            created_by=generic_request.created_by,
            created_at=datetime.now()
        )
        
        self.version_repo.insert_version(version)
        logger.info(f"Successfully created job '{generic_request.job_name}' with job_id: {job_id}")
        
        return {
            "job_id": job_id,
            "job_name": job.job_name,
            "job_type": job.job_type.value if isinstance(job.job_type, JobType) else job.job_type,
            "version_id": 1,
            "status": job.status.value,
            "created_at": job.created_at.isoformat()
        }
    
    def get_job_by_id(self, job_id: int) -> Dict[str, Any]:
        """Get complete job information with active version"""
        logger.debug(f"Getting details for job_id: {job_id}")

        data = self.job_repo.get_active_job_by_id(job_id)
        if not data:
            logger.warning(f"Job not found: {job_id}")
            raise ValueError(f"Job with id {job_id} not found")
        
        job = JobFactory.create_job(
            job_name=data['job_name'], 
            job_type=data["job_type"], 
            config=data["config"],
            created_by=data["created_by"]
        )
        version = JobVersion.from_dict(data)
        
        return self._build_response(job, version)
    
    def list_jobs(self, 
                  status: Optional[JobStatus] = None,
                  job_type: Optional[JobType] = None) -> List[JobSummary]:
        """List jobs with optional filters"""
        return self.job_repo.get_all_jobs(status=status, job_type=job_type)
    
    def list_active_jobs(self) -> List[JobSummary]:
        """Get all active jobs"""
        return self.job_repo.get_all_jobs(status=JobStatus.ACTIVE)
    
    def update_job(
        self, 
        job_id: int,
        connection_name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        schedule_type: Optional[ScheduleType] = None,
        schedule_cron: Optional[str] = None,
        updated_by: str = "admin"
    ) -> Dict[str, Any]:
        """Update job by creating new version"""
        current_version = self.version_repo.get_active_version(job_id)
        
        if not current_version:
            raise ValueError(f"Job {job_id} not found or has no active version")
        
        next_version_id = self.version_repo.get_next_version_id(job_id)
        
        new_version = JobVersion(
            job_id=job_id,
            version_id=next_version_id,
            is_active=True,
            connection_name=connection_name or current_version.connection_name,
            config=config or current_version.config,
            schedule_type=schedule_type or current_version.schedule_type,
            schedule_cron=schedule_cron if schedule_cron is not None else current_version.schedule_cron,
            created_by=updated_by,
            created_at=datetime.now()
        )
        
        self.version_repo.insert_version(new_version)
        self.job_repo.update_timestamp(job_id)
        
        logger.info(f"Updated job {job_id} to version {next_version_id}")
        
        return {
            "job_id": job_id,
            "new_version_id": next_version_id,
            "updated_by": updated_by,
            "updated_at": datetime.now().isoformat()
        }
    
    def delete_job(self, job_id: int, deleted_by: str = "admin"):
        """Soft delete job"""
        self.job_repo.soft_delete(job_id)
        self.version_repo.deactivate_all_versions(job_id)
        logger.info(f"Soft deleted job {job_id} by {deleted_by}")
    
    def get_job_versions(self, job_id: int) -> List[Dict[str, Any]]:
        """Get all versions of a job"""
        versions = self.version_repo.get_all_versions(job_id)
        return [v.to_dict() if hasattr(v, 'to_dict') else v for v in versions]
    
    def activate_job(self, job_id: int) -> Dict[str, Any]:
        """Activate a paused job"""
        self.job_repo.update_status(job_id, JobStatus.ACTIVE)
        return {"job_id": job_id, "status": JobStatus.ACTIVE.value}
    
    def pause_job(self, job_id: int) -> Dict[str, Any]:
        """Pause an active job"""
        self.job_repo.update_status(job_id, JobStatus.PAUSED)
        return {"job_id": job_id, "status": JobStatus.PAUSED.value}