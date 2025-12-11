"""
Repository: Job Data Access Layer
"""
from typing import Optional, List, Dict, Any
from datetime import datetime

from domain.entity.postgres_client import PostgresConnectionClient
from domain.entity.job_client import Job, JobStatus, JobType
from domain.entity.job_schemas import JobSummary
from utils.helpers import create_logger

logger = create_logger("JobRepository")


class JobRepository(PostgresConnectionClient):
    """Repository for Job CRUD operations"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def init_table(self):
        """Initialize jobs table in database"""
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
            CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs(job_type);
            CREATE INDEX IF NOT EXISTS idx_jobs_status_type ON jobs(status, job_type);
        """
        self.execute_query(query)
        logger.info("Successfully initialized jobs table")

    def _serialize_job(self, job: Job) -> Dict[str, Any]:
        """
        Convert Job object to database parameters
        Maps camelCase to snake_case
        """
        logger.debug(f"Serializing job: {job.jobName}")
        
        data = job.to_dict()
        
        return {
            "job_name": data.get("jobName"),
            "job_type": data.get("jobType").value if isinstance(data.get("jobType"), JobType) else data.get("jobType"),
            "created_by": data.get("createdBy"),
            "status": data.get("status").value if isinstance(data.get("status"), JobStatus) else data.get("status"),
            "job_id": data.get("jobId"),
            "created_at": data.get("createdAt") or datetime.now(),
            "updated_at": data.get("updatedAt") or datetime.now()
        }

    def insert_job(self, job: Job) -> int:
        """
        Create new job record in database and return job_id
        
        Args:
            job: Job object to insert
            
        Returns:
            int: Generated job_id
        """
        query = """
            INSERT INTO jobs (job_name, job_type, status, created_by, created_at, updated_at)
            VALUES (:job_name, :job_type, :status, :created_by, :created_at, :updated_at)
            RETURNING job_id
        """
        
        params = self._serialize_job(job)
        result = self.execute_query(query, params)
        
        # Handle different return formats
        if isinstance(result, list) and len(result) > 0:
            job_id = result[0].get('job_id') if isinstance(result[0], dict) else result[0]
        else:
            job_id = result
        
        logger.info(f"Successfully inserted job '{job.jobName}' with job_id: {job_id}")
        return job_id

    def get_active_job_by_id(self, job_id: int) -> Optional[Dict[str, Any]]:
        """
        Get complete job info with active version
        
        Args:
            job_id: Job identifier
            
        Returns:
            Dict with job and version data, or None if not found
        """
        query = """
            SELECT 
                j.job_id, j.job_name, j.job_type, j.status,
                j.created_by, j.created_at, j.updated_at,
                jv.version_id, jv.connection_name, jv.config,
                jv.schedule_type, jv.schedule_cron, jv.is_active
            FROM jobs j
            INNER JOIN job_versions jv ON j.job_id = jv.job_id
            WHERE j.job_id = :job_id 
            AND j.status != 'deleted'
            AND jv.is_active = TRUE
        """
        
        result = self.execute_query(query, {"job_id": job_id})
        
        if result and len(result) > 0:
            logger.debug(f"Found job with job_id: {job_id}")
            return result[0] if isinstance(result, list) else result
        
        logger.warning(f"Job not found with job_id: {job_id}")
        return None
    
    def get_all_jobs(
        self, 
        status: Optional[JobStatus] = None,
        job_type: Optional[JobType] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all jobs with optional filters
        
        Args:
            status: Filter by job status
            job_type: Filter by job type
            
        Returns:
            List of job dicts
        """
        conditions = ["j.status != 'deleted'"]
        params = {}
        
        if status:
            conditions.append("j.status = :status")
            params["status"] = status.value if isinstance(status, JobStatus) else status
        
        if job_type:
            conditions.append("j.job_type = :job_type")
            params["job_type"] = job_type.value if isinstance(job_type, JobType) else job_type
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
            SELECT 
                j.job_id, j.job_name, j.job_type, j.status,
                j.created_by, j.created_at, j.updated_at
            FROM jobs j
            WHERE {where_clause}
            ORDER BY j.updated_at DESC
        """
        
        result = self.execute_query(query, params)
        logger.info(f"Found {len(result) if result else 0} jobs")
        
        return result or []
    
    def update_timestamp(self, job_id: int):
        """
        Update job's updated_at timestamp    
        
        Args:
            job_id: Job identifier
        """
        query = """
            UPDATE jobs 
            SET updated_at = CURRENT_TIMESTAMP 
            WHERE job_id = :job_id
        """
        self.execute_query(query, {"job_id": job_id})
        logger.debug(f"Updated timestamp for job_id: {job_id}")
    
    def update_status(self, job_id: int, status: JobStatus):
        """
        Update job status
        
        Args:
            job_id: Job identifier
            status: New status
        """
        query = """
            UPDATE jobs 
            SET status = :status, updated_at = CURRENT_TIMESTAMP 
            WHERE job_id = :job_id
        """
        status_value = status.value if isinstance(status, JobStatus) else status
        self.execute_query(query, {"job_id": job_id, "status": status_value})
        logger.info(f"Updated status for job_id {job_id} to {status_value}")
    
    def soft_delete(self, job_id: int):
        """
        Soft delete job by setting status to deleted
        
        Args:
            job_id: Job identifier
        """
        self.update_status(job_id, JobStatus.DELETED)
        logger.info(f"Soft deleted job with job_id: {job_id}")
    
    def exists(self, job_name: str) -> bool:
        """
        Check if a job with given name exists (not deleted)
        
        Args:
            job_name: Name to check
            
        Returns:
            bool: True if exists, False otherwise
        """
        query = """
            SELECT COUNT(*) as count
            FROM jobs
            WHERE job_name = :job_name AND status != 'deleted'
        """
        result = self.execute_query(query, {"job_name": job_name})
        
        # Handle different return formats
        if isinstance(result, list) and len(result) > 0:
            count = result[0].get('count', 0) if isinstance(result[0], dict) else result[0]
        else:
            count = result or 0
            
        return count > 0
    
    def get_job_by_name(self, job_name: str) -> Optional[Dict[str, Any]]:
        """
        Get job by name
        
        Args:
            job_name: Job name
            
        Returns:
            Dict with job data or None
        """
        query = """
            SELECT job_id, job_name, job_type, status, 
                   created_by, created_at, updated_at
            FROM jobs
            WHERE job_name = :job_name AND status != 'deleted'
        """
        
        result = self.execute_query(query, {"job_name": job_name})
        
        if result and len(result) > 0:
            return result[0] if isinstance(result, list) else result
        
        return None