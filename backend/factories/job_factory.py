"""
Factory: Job Creation Factory
"""
from typing import Dict, Any
from domain.entity.job_client import Job, JobType
from domain.entity.ingest_job_client import IngestJob


class JobFactory:
    """Factory to create appropriate job type from config"""
    
    @staticmethod
    def create_job(
        job_name: str, 
        job_type: str, 
        config: Dict[str, Any], 
        created_by: str = "system", 
        **kwargs
    ) -> Job:
        """Create job instance based on type"""
        job_type_enum = JobType(job_type)
        
        if job_type_enum == JobType.INGEST:
            return IngestJob.from_config(job_name, config, created_by, **kwargs)
        
        raise ValueError(f"Unknown job type: {job_type}")