from datetime import datetime
from utils.postgresql_client import PostgresSQLClient
import json
from typing import Dict, Optional, List
from utils.airflow_client import (
    AirflowJob, JobVersion, JobHistory, JobSummary, 
    JobType, JobStatus, RunStatus, ScheduleType,
    IngestJob, JobFactory
)

class FastAPIClient(PostgresSQLClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._connection_cache: Dict[str, PostgresSQLClient] = {}

    def init_fastapi_db(self):


        # Table 4: job_history (generic result)
        query4 = """
            CREATE TABLE IF NOT EXISTS job_history (
                history_id SERIAL PRIMARY KEY,
                job_id INTEGER NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
                version_id INTEGER NOT NULL,
                dag_run_id VARCHAR(200) UNIQUE,
                trigger_type VARCHAR(20) NOT NULL,
                triggered_by VARCHAR(100),
                
                run_at TIMESTAMP NOT NULL,
                end_at TIMESTAMP,
                duration_seconds INTEGER,
                status VARCHAR(20) NOT NULL,
                
                error_message TEXT,
                result JSON,
                
                FOREIGN KEY (job_id, version_id) 
                    REFERENCES job_versions(job_id, version_id) 
                    ON DELETE CASCADE
            );
            
            CREATE INDEX IF NOT EXISTS idx_job_history_job_version 
            ON job_history(job_id, version_id);
            
            CREATE INDEX IF NOT EXISTS idx_job_history_status 
            ON job_history(status);
            
            CREATE INDEX IF NOT EXISTS idx_job_history_run_at 
            ON job_history(run_at DESC);
            
            CREATE INDEX IF NOT EXISTS idx_job_history_trigger 
            ON job_history(trigger_type);
            
            CREATE INDEX IF NOT EXISTS idx_job_history_dag_run 
            ON job_history(dag_run_id);
        """
        
        self.execute_query(query3)
        self.execute_query(query4)

    # ============= Connection Management Endpoints =============
    

    # ==================== Job CRUD Operations ====================    
    

    def get_all_active_jobs(self) -> List[JobSummary]:
        """Get all active jobs with stats - FIXED: success_rate calculation"""
        query = """
            SELECT 
                j.job_id, j.job_name, j.job_type,
                jv.version_id, jv.connection_name, jv.schedule_type, jv.schedule_cron,
                jv.is_active, j.created_by, j.created_at,
                (SELECT run_at FROM job_history WHERE job_id = j.job_id ORDER BY run_at DESC LIMIT 1) as last_run_at,
                (SELECT status FROM job_history WHERE job_id = j.job_id ORDER BY run_at DESC LIMIT 1) as last_run_status,
                (SELECT COUNT(*) FROM job_history WHERE job_id = j.job_id) as total_runs,
                CASE 
                    WHEN (SELECT COUNT(*) FROM job_history WHERE job_id = j.job_id) > 0 THEN
                        (SELECT COUNT(*)::FLOAT FROM job_history WHERE job_id = j.job_id AND status = 'success') / 
                        (SELECT COUNT(*) FROM job_history WHERE job_id = j.job_id) * 100
                    ELSE 0
                END as success_rate
            FROM jobs j
            INNER JOIN job_versions jv ON j.job_id = jv.job_id
            WHERE jv.is_active = TRUE AND j.status = 'active'
            ORDER BY j.created_at DESC
        """
        
        rows = self.execute_query(query).mappings().all()
        
        jobs = []
        for row in rows:
            data = dict(row)
            # Ensure success_rate is float
            data['success_rate'] = float(data['success_rate']) if data['success_rate'] else 0.0
            jobs.append(JobSummary(**data))
        
        return jobs

    # ==================== Job History Operations ====================
    def create_history(self, history: JobHistory) -> int:
        """Create job run record"""
        query = """
            INSERT INTO job_history 
            (job_id, version_id, dag_run_id, trigger_type, triggered_by,
             run_at, status)
            VALUES 
            (:job_id, :version_id, :dag_run_id, :trigger_type, :triggered_by,
             :run_at, :status)
            RETURNING history_id
        """
        
        params = {
            "job_id": history.job_id,
            "version_id": history.version_id,
            "dag_run_id": history.dag_run_id,
            "trigger_type": history.trigger_type,
            "triggered_by": history.triggered_by,
            "run_at": history.run_at or datetime.now(),
            "status": history.status.value
        }
        
        return self.execute_query(query, params).scalar()
    
    def update_history(self, dag_run_id: str, status: str, 
                       end_at: datetime = None, duration: int = None,
                       error_message: str = None, result: Dict = None):
        """Update job run result"""
        query = """
            UPDATE job_history
            SET status = :status,
                end_at = :end_at,
                duration_seconds = :duration,
                error_message = :error_message,
                result = :result
            WHERE dag_run_id = :dag_run_id
        """
        
        params = {
            "dag_run_id": dag_run_id,
            "status": status,
            "end_at": end_at or datetime.now(),
            "duration": duration,
            "error_message": error_message,
            "result": result
        }
        
        self.execute_query(query, params)
    
    def get_job_history(self, job_id: int, limit: int = 20) -> List[JobHistory]:
        """Get job execution history"""
        query = """
            SELECT history_id, job_id, version_id, dag_run_id, trigger_type, 
                   triggered_by, run_at, end_at, duration_seconds, status, 
                   error_message, result
            FROM job_history
            WHERE job_id = :job_id
            ORDER BY run_at DESC
            LIMIT :limit
        """
        rows = self.execute_query(query, {"job_id": job_id, "limit": limit}).mappings().all()
        
        history = []
        for row in rows:
            data = dict(row)
            if data.get('result') and isinstance(data['result'], str):
                data['result'] = json.loads(data['result'])
            # Convert status to Enum
            data['status'] = RunStatus(data['status'])
            history.append(JobHistory(**data))