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
        """Initialize the postgres_connections table if it doesn't exist"""
        query1 = """
            CREATE TABLE IF NOT EXISTS postgres_connections (
                connection_id SERIAL PRIMARY KEY,
                connection_name VARCHAR(100) UNIQUE NOT NULL,
                host VARCHAR(100) NOT NULL,
                port VARCHAR(50) NOT NULL,
                database VARCHAR(100) NOT NULL,
                username VARCHAR(100) NOT NULL,
                password TEXT NOT NULL,
                jdbc_properties JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'active'
            );
            
            CREATE INDEX IF NOT EXISTS idx_connection_name_status 
            ON postgres_connections(connection_name, status);
        """

        # Table 2: jobs
        query2 = """
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
        
        # Table 3: job_versions (generic config)
        query3 = """
            CREATE TABLE IF NOT EXISTS job_versions (
                id SERIAL PRIMARY KEY,
                job_id INTEGER NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
                version_id INTEGER NOT NULL,
                is_active BOOLEAN DEFAULT FALSE,
                
                connection_name VARCHAR(100) REFERENCES postgres_connections(connection_name) ON DELETE SET NULL,
                job_config JSON NOT NULL,
                
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
        
        self.execute_query(query1)
        self.execute_query(query2)
        self.execute_query(query3)
        self.execute_query(query4)

    # ============= Connection Management Endpoints =============
    
    def insert_connection(self, pc_connection: PostgresSQLClient, connection_name: str):
        """Insert a new connection into the database"""
        jdbc_props_json = json.dumps(pc_connection.jdbc_properties) if pc_connection.jdbc_properties else '{}'
        
        query = """
            INSERT INTO postgres_connections 
            (connection_name, host, port, database, username, password, jdbc_properties, status)
            VALUES (:connection_name, :host, :port, :database, :username, :password, CAST(:jdbc_properties AS JSONB), 'active')
        """
        
        params = {
            "connection_name": connection_name,
            "host": pc_connection.host,
            "port": pc_connection.port,
            "database": pc_connection.database,
            "username": pc_connection.user,
            "password": pc_connection.password,
            "jdbc_properties": jdbc_props_json
        }
        
        self.execute_query(query, params)
        # Add to Cache after successful insertion
        self._connection_cache[connection_name] = pc_connection

    def get_active_connection(self, connection_name: str):
        """
        Get active connection details from database
        Returns connection info dict if found and active, None otherwise
        """
        query = """
            SELECT connection_name, host, port, database, username, password, jdbc_properties, created_at, last_update
            FROM postgres_connections
            WHERE connection_name = :connection_name AND status = 'active'
            ORDER BY last_update DESC
            LIMIT 1
        """
        
        params = {
            "connection_name": connection_name
        }

        result = self.execute_query(query, params)
        rows = result.mappings().all()
        if rows:
            return rows[0]
        else:
            return None
    
    def get_all_active_connection(self):
        """
        Get active connection details from database
        Returns connection info dict if found and active, None otherwise
        """
        query = """
            SELECT connection_name, host, port, database, username, password, jdbc_properties, created_at, last_update
            FROM postgres_connections
            WHERE status = 'active'
            ORDER BY created_at
        """
        
        result = self.execute_query(query)
        rows = result.mappings().all()
        if not rows:
            return None
        return rows

    def get_postgres_client(self, connection_name):
        """
        Get or create a PostgresSQLClient instance for the given connection name.        
        Returns:
            PostgresSQLClient instance if connection exists and is active, None otherwise
        """
        # Check cache
        if connection_name in self._connection_cache:
            cached_client = self._connection_cache[connection_name]
            try:
                cached_client.test_connection()
                return cached_client
            except:
                del self._connection_cache[connection_name]

        # Get connection_info from database
        conn_info = self.get_active_connection(connection_name)
        if not conn_info:
            return None

        # Create new client
        pc = PostgresSQLClient(database=conn_info['database'], 
                               user = conn_info['username'],
                               password = conn_info['password'],
                               jdbc_properties=conn_info.get('jdbc_properties', {}),
                               host = conn_info['host'],
                               port=conn_info['port'])
        
        # Cache the client
        self._connection_cache[connection_name] = pc
        return pc 

    def update_connection(self, connection_name: str):
        """
        Mark connection as deleted (soft delete)
        Updates status from 'active' to 'deleted' and remove from cached
        """
        query = """
            UPDATE postgres_connections
            SET status = 'deleted', last_update = CURRENT_TIMESTAMP
            WHERE connection_name = :connection_name AND status = 'active'
        """
        params = {
            "connection_name": connection_name
        }
        self.execute_query(query, params)

        self.clear_connection_cache(connection_name=connection_name)
   
    def clear_connection_cache(self, connection_name: Optional[str] = None):
        """
        Clear connection cache.
        If connection_name is provided, clear only that connection.
        """
        if connection_name:
            if connection_name in self._connection_cache:
                del self._connection_cache[connection_name]
        else:
            self._connection_cache.clear()

    # ==================== Job CRUD Operations ====================    
    
    def create_job(self, job: AirflowJob) -> int:
        """Create new job"""
        query = """
            INSERT INTO jobs (job_name, job_type, status, created_at, created_by)
            VALUES (:job_name, :job_type, :status, :created_at, :created_by)
            RETURNING job_id
        """
        
        params = {
            "job_name": job.job_name,
            "job_type": job.job_type.value,
            "status": job.status.value,
            "created_at": job.created_at,
            "created_by": job.created_by
        }
        
        result = self.execute_query(query, params)
        return result.scalar()
    
    def create_version(self, version: JobVersion):
        """Create new job version"""
        # Deactivate all previous versions
        deactivate_query = """
            UPDATE job_versions 
            SET is_active = FALSE 
            WHERE job_id = :job_id
        """
        self.execute_query(deactivate_query, {"job_id": version.job_id})
        
        # Insert new version
        insert_query = """
            INSERT INTO job_versions 
            (job_id, version_id, is_active, connection_name, job_config, 
             schedule_type, schedule_cron, created_at, created_by)
            VALUES 
            (:job_id, :version_id, :is_active, :connection_name, 
             :job_config, :schedule_type, :schedule_cron, :created_at, :created_by)
        """
        
        params = {
            "job_id": version.job_id,
            "version_id": version.version_id,
            "is_active": version.is_active,
            "connection_name": version.connection_name,
            "job_config": version.job_config,
            "schedule_type": version.schedule_type.value,
            "schedule_cron": version.schedule_cron,
            "created_at": version.created_at,
            "created_by": version.created_by
        }
        
        self.execute_query(insert_query, params)
        
        # Update job timestamp
        update_query = """
            UPDATE jobs 
            SET updated_at = CURRENT_TIMESTAMP 
            WHERE job_id = :job_id
        """
        self.execute_query(update_query, {"job_id": version.job_id})
    
    def get_job_versions(self, job_id: int) -> List[JobVersion]:
        """Get all versions of a job"""
        query = """
            SELECT job_id, version_id, is_active, connection_name, job_config,
                   schedule_type, schedule_cron, created_by, created_at
            FROM job_versions
            WHERE job_id = :job_id
            ORDER BY version_id DESC
        """
        
        rows = self.execute_query(query, {"job_id": job_id}).mappings().all()
        return [JobVersion(**row) for row in rows]

    def get_next_version_id(self, job_id: int) -> int:
        """Get next version ID"""
        query = """
            SELECT COALESCE(MAX(version_id), 0) + 1 as next_version
            FROM job_versions
            WHERE job_id = :job_id
        """
        return self.execute_query(query, {"job_id": job_id}).scalar()
    
    def get_active_job_by_id(self, job_id: int) -> Optional[Dict]:
        """Get complete job info"""
        query = """
            SELECT 
                j.job_id, j.job_name, j.job_type, j.status,
                j.created_by, j.created_at, j.updated_at,
                jv.version_id, jv.connection_name, jv.job_config,
                jv.schedule_type, jv.schedule_cron, jv.is_active,
                c.host, c.port, c.database, c.username, c.password
            FROM jobs j
            INNER JOIN job_versions jv ON j.job_id = jv.job_id
            LEFT JOIN postgres_connections c ON jv.connection_name = c.connection_name
            WHERE j.job_id = :job_id AND jv.is_active = TRUE
        """
        
        return self.execute_query(query, {"job_id": job_id}).mappings().all()
    
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

    def get_latest_job_version(self, job_id: int) -> Optional[JobVersion]:
        """Get latest version of a job - NO CHANGES"""
        query = """
            SELECT job_id, version_id, is_active, connection_name, job_config,
                   schedule_type, schedule_cron, created_at, created_by
            FROM job_versions
            WHERE job_id = :job_id
            ORDER BY version_id DESC
            LIMIT 1
        """
        
        row = self.execute_query(query, {"job_id": job_id}).mappings().one_or_none()

        if row:
            data = dict(row)
            if isinstance(data['job_config'], str):
                data['job_config'] = json.loads(data['job_config'])
            data['schedule_type'] = ScheduleType(data['schedule_type'])
            return JobVersion(**data)
        
        return None
        
    def update_job(self, job_id: int, new_config: Dict, updated_by: str):
        """Update job (creates new version)"""
        next_version = self.get_next_version_id(job_id)
        old_version = self.get_latest_job_version(job_id)

        if not old_version:
            raise ValueError(f"Job {job_id} not found")

        version = JobVersion(
            job_id=job_id,
            version_id=next_version,
            is_active=True,
            connection_name=new_config.get("connection_name") or old_version.connection_name,
            job_config=new_config.get("job_config") or old_version.job_config,
            schedule_type=ScheduleType(new_config["schedule_type"]) 
                        if new_config.get("schedule_type") else old_version.schedule_type,
            schedule_cron=new_config.get("schedule_cron") or old_version.schedule_cron,
            created_by=updated_by
        )

        self.create_version(version)
    
    def delete_job(self, job_id: int):
        """Soft delete job"""
        query = """
            UPDATE jobs 
            SET status = 'deleted', updated_at = CURRENT_TIMESTAMP 
            WHERE job_id = :job_id
        """
        self.execute_query(query, {"job_id": job_id})
        
        # Deactivate versions
        deactivate = "UPDATE job_versions SET is_active = FALSE WHERE job_id = :job_id"
        self.execute_query(deactivate, {"job_id": job_id})    
    
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