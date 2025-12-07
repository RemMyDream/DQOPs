"""
Factory: Job Creation Factory
"""
from typing import Dict, Any
from domain.job_client import Job, JobType
from domain.ingest_job_client import IngestJob


class JobFactory:
    """Factory to create appropriate job type from config"""
    
    @staticmethod
    def create_job(
        jobId: int, 
        jobName: str, 
        jobType: str, 
        config: Dict[str, Any], 
        createdBy: str = "system", 
        **kwargs
    ) -> Job:
        """Create job instance based on type"""
        job_type_enum = JobType(jobType)
        
        if job_type_enum == JobType.INGEST:
            return IngestJob.from_config(jobId, jobName, config, createdBy, **kwargs)
        
        # Add more job types here as needed
        # elif job_type_enum == JobType.TRANSFORM:
        #     return TransformJob.from_config(job_id, job_name, config, created_by, **kwargs)
        # elif job_type_enum == JobType.QUALITY:
        #     return QualityJob.from_config(job_id, job_name, config, created_by, **kwargs)
        
        else:
            raise ValueError(f"Unknown job type: {jobType}")