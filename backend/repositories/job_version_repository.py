"""
Repository: Job Version Data Access Layer
"""
import json
from typing import Dict, List, Optional, Any
from datetime import datetime

from domain.entity.postgres_client import PostgresConnectionClient
from domain.entity.job_schemas import JobVersion, ScheduleType
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
                
                connection_name VARCHAR(100),
                config JSONB NOT NULL,
                
                schedule_type VARCHAR(20) NOT NULL,
                schedule_cron VARCHAR(50),
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR(100),
                
                UNIQUE(job_id, version_id)
            );
            
            CREATE INDEX IF NOT EXISTS idx_job_versions_job_id 
            ON job_versions(job_id);
            
            CREATE INDEX IF NOT EXISTS idx_job_versions_active 
            ON job_versions(job_id, is_active) WHERE is_active = TRUE;
            
            CREATE INDEX IF NOT EXISTS idx_job_versions_connection 
            ON job_versions(connection_name);
        """
        
        self.execute_query(query)
        logger.info("Successfully initialized job_versions table")

    def _serialize_version(self, version: JobVersion) -> Dict[str, Any]:
        """Convert JobVersion object to database parameters"""
        logger.debug(f"Serializing version for job_id {version.job_id}, version_id: {version.version_id}")
        
        config = version.config
        if isinstance(config, dict):
            config_json = json.dumps(config)
        else:
            config_json = config
        
        schedule_type = version.schedule_type
        if isinstance(schedule_type, ScheduleType):
            schedule_type = schedule_type.value
        
        return {
            "job_id": version.job_id,
            "version_id": version.version_id,
            "connection_name": version.connection_name,
            "config": config_json,
            "schedule_type": schedule_type,
            "schedule_cron": version.schedule_cron,
            "created_at": version.created_at or datetime.now(),
            "created_by": version.created_by,
            "is_active": version.is_active
        }

    def deactivate_all_versions(self, job_id: int):
        """Deactivate all versions for a job"""
        query = """
            UPDATE job_versions 
            SET is_active = FALSE 
            WHERE job_id = :job_id
        """
        self.execute_query(query, {"job_id": job_id})
        logger.info(f"Deactivated all versions for job_id {job_id}")
        
    def insert_version(self, version: JobVersion):
        """Insert new version and automatically deactivate previous versions"""
        self.deactivate_all_versions(version.job_id)
        
        query = """
            INSERT INTO job_versions 
            (job_id, version_id, is_active, connection_name, config, 
            schedule_type, schedule_cron, created_at, created_by)
            VALUES 
            (:job_id, :version_id, :is_active, :connection_name, 
            CAST(:config AS JSON), :schedule_type, :schedule_cron, :created_at, :created_by)
        """
        
        self.execute_query(query, self._serialize_version(version))
        logger.info(f"Inserted version {version.version_id} for job_id {version.job_id}")

    def get_all_versions(self, job_id: int) -> List[JobVersion]:
        """Get all versions of a job"""
        query = """
            SELECT job_id, version_id, is_active, connection_name, config,
                   schedule_type, schedule_cron, created_at, created_by
            FROM job_versions
            WHERE job_id = :job_id
            ORDER BY version_id DESC
        """
        
        result = self.execute_query(query, {"job_id": job_id})
        
        if not result:
            logger.info(f"No versions found for job_id {job_id}")
            return []
        
        versions = [JobVersion.from_dict(row) for row in result]
        logger.info(f"Retrieved {len(versions)} versions for job_id {job_id}")
        
        return versions
    
    def get_active_version(self, job_id: int) -> Optional[JobVersion]:
        """Get active version of a job"""
        query = """
            SELECT job_id, version_id, is_active, connection_name, config,
                   schedule_type, schedule_cron, created_at, created_by
            FROM job_versions
            WHERE job_id = :job_id AND is_active = TRUE
            LIMIT 1
        """
        
        result = self.execute_query(query, {"job_id": job_id})
        
        if result and len(result) > 0:
            row = result[0] if isinstance(result, list) else result
            try:
                return JobVersion.from_dict(row)
            except Exception as e:
                logger.error(f"Error deserializing version: {e}")
                return None
        
        logger.warning(f"No active version found for job_id {job_id}")
        return None

    def get_latest_version(self, job_id: int) -> Optional[JobVersion]:
        """Get latest version of a job (may not be active)"""
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
            row = result[0] if isinstance(result, list) else result
            try:
                return JobVersion.from_dict(row)
            except Exception as e:
                logger.error(f"Error deserializing version: {e}")
                return None
        
        logger.warning(f"No versions found for job_id {job_id}")
        return None
    
    def get_next_version_id(self, job_id: int) -> int:
        """Get next version ID for a job"""
        query = """
            SELECT COALESCE(MAX(version_id), 0) + 1 as next_version
            FROM job_versions
            WHERE job_id = :job_id
        """
        result = self.execute_query(query, {"job_id": job_id})
        
        if isinstance(result, list) and len(result) > 0:
            next_version = result[0].get('next_version', 1) if isinstance(result[0], dict) else result[0]
        else:
            next_version = 1
        
        logger.debug(f"Next version for job_id {job_id} is {next_version}")
        return next_version
    
    def version_exists(self, job_id: int, version_id: int) -> bool:
        """Check if a specific version exists"""
        query = """
            SELECT COUNT(*) as count
            FROM job_versions
            WHERE job_id = :job_id AND version_id = :version_id
        """
        result = self.execute_query(query, {"job_id": job_id, "version_id": version_id})
        
        if isinstance(result, list) and len(result) > 0:
            count = result[0].get('count', 0) if isinstance(result[0], dict) else result[0]
        else:
            count = 0
            
        return count > 0
    
    def activate_version(self, job_id: int, version_id: int):
        """Activate a specific version (deactivates others)"""
        self.deactivate_all_versions(job_id)
        
        query = """
            UPDATE job_versions 
            SET is_active = TRUE 
            WHERE job_id = :job_id AND version_id = :version_id
        """
        self.execute_query(query, {"job_id": job_id, "version_id": version_id})
        logger.info(f"Activated version {version_id} for job_id {job_id}")