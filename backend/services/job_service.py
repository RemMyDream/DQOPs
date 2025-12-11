"""
Service: Job Business Logic Layer
"""
from typing import List, Dict, Any, Optional
from datetime import datetime
from repositories.job_repository import JobRepository
from repositories.job_version_repository import JobVersionRepository
from factories.job_factory import JobFactory
from backend.domain.entity.job_client import Job, JobType, JobStatus
from backend.domain.entity.job_schemas import JobVersion, JobSummary, ScheduleType
from backend.domain.request.job_request import JobCreateRequest

class JobService:
    """Service layer for Job operations"""
    
    def __init__(self, job_repo: JobRepository, version_repo: JobVersionRepository):
        self.job_repo = job_repo
        self.version_repo = version_repo
    
    def create_job(self, request: JobCreateRequest) -> Dict[str, Any]:
        """Create new job with initial version"""
        # Validate job configuration
        job = JobFactory.create_job(
            jobName=request.jobName,
            jobType=request.jobType,
            config=request.config,
            createdBy=request.createdBy            
        )
        job.validate()
        job_id = self.job_repo.insert_job(job)
        job.jobId = job_id
        job.status = JobStatus.ACTIVE
        job.c  1reatedAt = datetime.now()
        job.updatedAt = datetime.now()
        
        # Create first version
        version = JobVersion(
            jobId=job_id,
            versionId=1,
            isActive=True,
            connectionName = request.connectionName,
            config = request.config,
            scheduleType=request.scheduleType,
            scheduleCron = request.scheduleCron,
            createdby = request.createdBy,
            createdAt = datetime.now()
        )
        
        self.version_repo.insert_job_version(version)
        
        return {
            "job_id": job_id,
            "job_name": job.jobName,
            "version_id": 1
        }
    
    def get_job_detail(self, job_id: int) -> Dict[str, Any]:
        """Get complete job information"""
        job_data = self.job_repo.get_by_id(job_id)
        
        if not job_data:
            raise ValueError(f"Job {job_id} not found")
        
        # Reconstruct job object
        job = JobFactory.create_job(
            job_type=job_data['job_type'],
            job_id=job_data['job_id'],
            job_name=job_data['job_name'],
            config=job_data['job_config'],
            created_by=job_data['created_by'],
            status=JobStatus(job_data['status']),
            created_at=job_data['created_at'],
            updated_at=job_data['updated_at']
        )
        
        return {
            "job": job.to_dict(),
            "config": job.to_config(),
            "version": {
                "version_id": job_data['version_id'],
                "connection_name": job_data['connection_name'],
                "schedule_type": job_data['schedule_type'],
                "schedule_cron": job_data['schedule_cron'],
                "is_active": job_data['is_active']
            }
        }
    
    def list_active_jobs(self) -> List[JobSummary]:
        """Get all active jobs"""
        return self.job_repo.get_all_active()
    
    def update_job(
        self, 
        job_id: int,
        connection_name: Optional[str] = None,
        job_config: Optional[Dict[str, Any]] = None,
        schedule_type: Optional[str] = None,
        schedule_cron: Optional[str] = None,
        updated_by: str = "admin"
    ) -> Dict[str, Any]:
        """Update job (creates new version)"""
        
        # Get current version
        current_version = self.version_repo.get_latest(job_id)
        
        if not current_version:
            raise ValueError(f"Job {job_id} not found")
        
        # Get next version ID
        next_version = self.version_repo.get_next_version_id(job_id)
        
        # Create new version with updates
        new_version = JobVersion(
            job_id=job_id,
            version_id=next_version,
            is_active=True,
            connection_name=connection_name or current_version.connection_name,
            job_config=job_config or current_version.job_config,
            schedule_type=ScheduleType(schedule_type) if schedule_type else current_version.schedule_type,
            schedule_cron=schedule_cron or current_version.schedule_cron,
            created_by=updated_by,
            created_at=datetime.now()
        )
        
        self.version_repo.create(new_version)
        self.job_repo.update_timestamp(job_id)
        
        return {
            "job_id": job_id,
            "new_version_id": next_version
        }
    
    def delete_job(self, job_id: int):
        """Soft delete job"""
        self.job_repo.soft_delete(job_id)
        self.version_repo.deactivate_all(job_id)
    
    def get_job_versions(self, job_id: int) -> List[JobVersion]:
        """Get all versions of a job"""
        return self.version_repo.get_all_by_job_id(job_id)