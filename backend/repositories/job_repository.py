"""
Repository: Job Data Access Layer
"""
from typing import Optional, List, Dict, Any
from datetime import datetime
from domain.job_schemas import JobHistory
from domain.postgres_client import PostgresConnectionClient
from domain.job_client import Job
from utils.helpers import create_logger


logger = create_logger("JobRepository")

class JobRepository(PostgresConnectionClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args,**kwargs)

    def init_table(self):
        logger.info("Initializing jobs table")
        query = """
            CREATE TABLE IF NOT EXISTS jobs (
                job_id SERIAL PRIMARY KEY,
                job_name VARCHAR(200) UNIQUE NOT NULL,
                job_type VARCHAR(50) NOT NULL,
                status VARCHAR(20) DEFAULT 'active',
                created_by VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_jobs_name ON jobs(job_name);
            CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
            CREATE INDEX IF NOT EXISTS idx_jobs_status_type ON jobs(status, job_type);
        """
        self.execute_query(query)
        logger.info("Successfully initialize jobs table")

    def _serialize_params(self, job: Job) -> dict:
        logger.debug(f"Serializing params for job: {job.jobName}")
        data = job.to_dict()
        mapped = {
            "job_name": data.get("jobName"),
            "job_type": data.get("jobType"),
            "created_by": data.get("createdBy"),
            "status": data.get("status"),
            "job_id": data.get("jobId"),
            "created_at": data.get("createdAt"),
            "updated_at": data.get("updatedAt")
        }
        return mapped

    def insert_job(self, job: Job) -> int:
        """Create new job record in database and return job_id"""
        query = """
            INSERT INTO jobs (job_name, job_type, status, created_by, created_at)
            VALUE (:job_name, :job_type, :status, :created_by, :created_at)
            RETURN job_id
        """
        job_id = self.execute_query(query = query, params = self._serialize_params(job))[0]
        logger.info(f"Successfully insert job into database with job_id: {job_id}")
        return job_id



    