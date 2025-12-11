"""
Repository: Job Version Data Access Layer
"""
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
from domain.entity.postgres_client import PostgresConnectionClient
from domain.entity.job_client import Job
from domain.entity.job_schemas import JobVersion, ScheduleType
from utils.helpers import create_logger

logger = create_logger("JobVersionRepository")


class JobVersionRepository(PostgresConnectionClient):
    """Repository for Job Version operations"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def init_table(self):
        """Initialize job_versions table in database"""
        logger.info("Initializing job_versions table")
        query = """
            CREATE TABLE IF NOT EXISTS job_versions (
                id SERIAL PRIMARY KEY,
                job_id INTEGER NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
                version_id INTEGER NOT NULL,
                is_active BOOLEAN DEFAULT FALSE,
                
                connection_name VARCHAR(100) REFERENCES postgres_connections(connection_name) ON DELETE SET NULL,
                config JSON NOT NULL,
                
                schedule_type VARCHAR(20) NOT NULL,
                schedule_cron VARCHAR(50),
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR(100),
                
                UNIQUE(job_id, version_id)
            );
            
            CREATE INDEX IF NOT EXISTS idx_job_versions_active 
            ON job_versions(job_id, is_active);
            
            CREATE INDEX IF NOT EXISTS idx_job_versions_connection 
            ON job_versions(connection_name);
        """
        
        self.execute_query(query)
        logger.info("Successfully initialized job_versions table")

    def _serialize_version(self, version: JobVersion) -> Dict[str, Any]:
        """
        Convert JobVersion object to database parameters
        Maps camelCase to snake_case
        """
        logger.debug(f"Serializing version for job_id {version.jobId}, version_id: {version.versionId}")
        
        data = version.to_dict()
        
        return {
            "job_id": data.get("jobId"),
            "version_id": data.get("versionId"),
            "connection_name": data.get("connectionName"),
            "config": json.dumps(data.get("config", {})),
            "schedule_type": data.get("scheduleType"),
            "created_at": data.get("createdAt") or datetime.now(),
            "created_by": data.get("createdBy"),
            "is_active": data.get("isActive", False),
            "schedule_cron": data.get("scheduleCron")
        }

    def deactivate_all_versions(self, job_id: int):
        """
        Deactivate all versions for a job
        
        Args:
            job_id: Job identifier
        """
        query = """
            UPDATE job_versions 
            SET is_active = FALSE 
            WHERE job_id = :job_id
        """
        self.execute_query(query, {"job_id": job_id})
        logger.info(f"Deactivated all versions for job_id {job_id}")
        
    def insert_version(self, version: JobVersion):
        """
        Insert new version and automatically deactivate previous versions
        
        Args:
            version: JobVersion object to insert
        """
        # First, deactivate all previous versions
        self.deactivate_all_versions(version.jobId)
        
        # Then insert new version
        query = """
            INSERT INTO job_versions 
            (job_id, version_id, is_active, connection_name, config, 
             schedule_type, schedule_cron, created_at, created_by)
            VALUES 
            (:job_id, :version_id, :is_active, :connection_name, 
             :config, :schedule_type, :schedule_cron, :created_at, :created_by)
        """
        
        self.execute_query(query, self._serialize_version(version))
        logger.info(f"Inserted version {version.versionId} for job_id {version.jobId}")

    def get_all_versions(self, job_id: int) -> List[JobVersion]:
        """
        Get all versions of a job
        
        Args:
            job_id: Job identifier
            
        Returns:
            List of JobVersion objects, ordered by version_id DESC
        """
        query = """
            SELECT job_id, version_id, is_active, connection_name, config,
                   schedule_type, schedule_cron, created_at, created_by
            FROM job_versions
            WHERE job_id = :job_id
            ORDER BY version_id DESC
        """
        
        versions = self.execute_query(query, {"job_id": job_id})
        logger.info(f"Retrieved {len(versions)} versions for job_id {job_id}")

        return versions
    
    def get_active_version(self, job_id: int) -> Optional[JobVersion]:
        """
        Get active version of a job
        
        Args:
            job_id: Job identifier
            
        Returns:
            JobVersion object or None if not found
        """
        query = """
            SELECT job_id, version_id, is_active, connection_name, config,
                   schedule_type, schedule_cron, created_at, created_by
            FROM job_versions
            WHERE job_id = :job_id AND is_active = TRUE
            LIMIT 1
        """
        
        result = self.execute_query(query, {"job_id": job_id})
        
        if result and len(result) > 0:
            try:
                return JobVersion.from_dict(result[0])
            except Exception as e:
                logger.error(f"Error creating JobVersion from row: {e}")
                return None
        
        logger.warning(f"No active version found for job_id {job_id}")
        return None

    def get_latest_version(self, job_id: int) -> Optional[JobVersion]:
        """
        Get latest version of a job (may not be active)
        
        Args:
            job_id: Job identifier
            
        Returns:
            JobVersion object or None if not found
        """
        query = """
            SELECT job_id, version_id, is_active, connection_name, config,
                   schedule_type, schedule_cron, created_at, created_by
            FROM job_versions
            WHERE job_id = :job_id
            ORDER BY version_id DESC
            LIMIT 1
        """
        
        result = self.execute_query(query, {"job_id": job_id})
        
        if result and len(result) > 0:
            try:
                return JobVersion.from_dict(result[0])
            except Exception as e:
                logger.error(f"Error creating JobVersion from row: {e}")
                return None
        
        logger.warning(f"No versions found for job_id {job_id}")
        return None
    
    def get_next_version_id(self, job_id: int) -> int:
        """
        Get next version ID for a job
        
        Args:
            job_id: Job identifier
            
        Returns:
            int: Next version number (starts at 1 if no versions exist)
        """
        query = """
            SELECT COALESCE(MAX(version_id), 0) + 1 as next_version
            FROM job_versions
            WHERE job_id = :job_id
        """
        result = self.execute_query(query, {"job_id": job_id})
        next_version = result[0]
        
        logger.debug(f"Next version for job_id {job_id} is {next_version}")
        return next_version
    
    def version_exists(self, job_id: int, version_id: int) -> bool:
        """
        Check if a specific version exists
        
        Args:
            job_id: Job identifier
            version_id: Version number
            
        Returns:
            bool: True if exists, False otherwise
        """
        query = """
            SELECT COUNT(*) as count
            FROM job_versions
            WHERE job_id = :job_id AND version_id = :version_id
        """
        result = self.execute_query(query, {"job_id": job_id, "version_id": version_id})
        count = result[0]
        return count > 0