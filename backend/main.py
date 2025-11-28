from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator
from typing import Dict, List, Optional, Any
from datetime import datetime
import requests
import os
import json
from utils.postgresql_client import PostgresSQLClient
from utils.fastapi_db import FastAPIClient
from utils.helpers import create_logger, load_cfg
from utils.create_spark_connection import create_spark_connection
from utils.airflow_client import (
    JobType, JobStatus, RunStatus, ScheduleType,
    AirflowJob, IngestJob, IngestJobSource, IngestJobTarget, IngestJobOptions,
    JobFactory, JobVersion, JobHistory, JobSummary
)

logger = create_logger(name="Backend")

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== Initialize Services ====================
cfg = load_cfg("utils/sys_conf/config.yaml")

# Configure Spark
app_name = cfg['spark']['app_name']
lakehouse_cfg = cfg['lakehouse']
access_key = lakehouse_cfg['root_user']
secret_key = lakehouse_cfg['root_password']
minio_endpoint = f"http://{lakehouse_cfg['endpoint']}"

spark = create_spark_connection(
    app_name, 
    access_key=access_key, 
    secret_key=secret_key, 
    endpoint=minio_endpoint
)

# Configure Airflow
airflow_user = cfg['airflow']['user']
airflow_password = cfg['airflow']['password']
airflow_url = cfg['airflow']['url']

# Initialize FastAPIClient
fastapi_client = FastAPIClient(
    database=cfg['fastapi_db']['database'],
    user=cfg['fastapi_db']['user'],
    password=cfg['fastapi_db']['password'],
    host=cfg['fastapi_db']['host'],
    port=cfg['fastapi_db']['port']
)
fastapi_client.init_fastapi_db()

# ==================== Pydantic Models ====================
class DBConfig(BaseModel):
    connectionName: str
    host: str
    port: str
    username: str
    password: str
    database: str
    jdbcProperties: Optional[Dict[str, str]] = None
    savedAt: str

class DBCredentials(BaseModel):
    connection_name: str
    db_schema: Optional[str] = None
    table: Optional[str] = None

class ConnectionDelete(BaseModel):
    connection_name: str

class TableSubmission(BaseModel):
    connectionName: str
    tables: List[Dict[str, str]]

class CacheClear(BaseModel):
    connection_name: Optional[str] = None

class CreateJobRequest(BaseModel):
    job_name: str
    job_type: str
    connection_name: str
    config: Dict[str, Any]
    schedule_type: str
    schedule_cron: Optional[str] = None
    created_by: str = "system"

    @field_validator('schedule_cron')
    def validate_cron(cls, v, info):
        values = info.data
        if values.get('schedule_type') in ['scheduled', 'both'] and not v:
            raise ValueError('schedule_cron required for scheduled jobs')
        return v

class UpdateJobRequest(BaseModel):
    connection_name: str = None
    job_config: Dict[str, Any] = None
    schedule_type: str = None
    schedule_cron: Optional[str] = None
    updated_by: str = "admin"

class TriggerJobsRequest(BaseModel):
    """Trigger multiple jobs at once"""
    job_ids: List[int]
    triggered_by: str = "admin"

class CreateIngestJobsRequest(BaseModel):
    connection_name: str
    tables: List[Dict[str, Any]]  # [{"schema": "public", "table": "users", "primary_keys": ["id"]}]
    schedule_type: str = "on_demand"  # 'on_demand', 'scheduled', 'both'
    schedule_cron: Optional[str] = None
    created_by: str = "admin"
    
    @field_validator('tables')
    def validate_tables(cls, v):
        if not v or len(v) == 0:
            raise ValueError('At least one table is required')
        for table in v:
            if 'schema' not in table or 'table' not in table:
                raise ValueError('Each table must have schema and table name')
        return v
    
# ================ Connection Management Endpoints ==================

@app.post("/create_connection")
def connect_postgres(config: DBConfig):
    """
    Create or verify a PostgreSQL connection.
    If connection exists and unchanged, reuse it.
    If changed, mark old as deleted and create new one.
    """
    try:
        logger.info(f"Attempting to connect to database: {config.database} at {config.host}:{config.port}")
        
        # Check if connection already exists
        existing_conn = fastapi_client.get_active_connection(config.connectionName)
        
        if existing_conn:
            existing_jdbc = existing_conn.get("jdbc_properties", {})
            if isinstance(existing_jdbc, str):
                try:
                    existing_jdbc = json.loads(existing_jdbc)
                except json.JSONDecodeError:
                    existing_jdbc = {}
            
            config_jdbc = config.jdbcProperties if config.jdbcProperties else {}
            
            has_changes = (
                existing_conn['host'] != config.host or 
                existing_conn['port'] != str(config.port) or  
                existing_conn['database'] != config.database or
                existing_conn['username'] != config.username or
                existing_conn['password'] != config.password or
                existing_jdbc != config_jdbc
            )
            
            if has_changes:
                logger.info(f"Connection details changed for: {config.connectionName}, creating new entry")
                
                # Create new connection with updated details
                pc = PostgresSQLClient(
                    database=config.database,
                    user=config.username,
                    password=config.password,
                    jdbc_properties=config_jdbc,
                    host=config.host,
                    port=config.port
                )
                
                # Test connection before updating
                pc.test_connection()
                
                # Mark old connection as deleted and insert new one
                fastapi_client.update_connection(config.connectionName)
                fastapi_client.insert_connection(pc, connection_name=config.connectionName)
                
                logger.info(f"Successfully updated connection: {config.connectionName}")
                
                return {
                    "status": "success",
                    "message": "Connection updated successfully",
                    "connectionName": config.connectionName,
                    "database": config.database,
                    "host": config.host,
                    "port": config.port,
                }
            else:
                logger.info(f"Using existing connection: {config.connectionName}")
                return {
                    "status": "success",
                    "message": "Using existing connection",
                    "connectionName": config.connectionName,
                    "database": config.database,
                    "host": config.host,
                    "port": config.port,
                }
        
        # Create new connection (no existing connection found)
        pc = PostgresSQLClient(
            database=config.database,
            user=config.username,
            password=config.password,
            jdbc_properties=config.jdbcProperties if config.jdbcProperties else {},
            host=config.host,
            port=config.port
        )
       
        # Test connection before saving
        pc.test_connection()
        
        # Save to database and cache
        fastapi_client.insert_connection(pc, connection_name=config.connectionName)
        logger.info(f"Successfully created new connection: {config.connectionName}")
        
        return {
            "status": "success",
            "message": "Connection established successfully",
            "connectionName": config.connectionName,
            "database": config.database,
            "host": config.host,
            "port": config.port,
        }
        
    except Exception as e:
        logger.error(f"Connection failed: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    
@app.get("/connections")
def list_connections():
    """Get all active connections"""
    try:
        connections_data = fastapi_client.get_all_active_connection()
        return connections_data
    except Exception as e:
        logger.error(f"Failed to list connections: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    
@app.delete("/delete_connection")
def delete_connection_endpoint(connection_delete: ConnectionDelete):
    """Soft delete a connection (mark as deleted)"""
    try:
        logger.info(f"Attempting to delete connection: {connection_delete.connection_name}")
        
        # Check if connection exists and is active
        conn_info = fastapi_client.get_active_connection(connection_delete.connection_name)
        
        if not conn_info:
            raise HTTPException(
                status_code=404, 
                detail=f"No active connection found with name: {connection_delete.connection_name}"
            )
        
        # Delete (mark as deleted) the connection and clear from cache
        fastapi_client.update_connection(connection_delete.connection_name)
        
        logger.info(f"Successfully deleted connection: {connection_delete.connection_name}")
        
        return {
            "status": "success",
            "message": f"Connection '{connection_delete.connection_name}' has been deleted",
            "connectionName": connection_delete.connection_name
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete connection: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

# ==================== Schema and Table Endpoints ====================

@app.post("/get_schemas")
def get_schemas(config: DBConfig):
    """Get all schemas and tables for a connection"""
    try:
        logger.info(f"Fetching schemas for connection: {config.connectionName}")
        
        # Get active connection from fastapi_db
        pc = fastapi_client.get_postgres_client(config.connectionName)
        if not pc:
            raise HTTPException(
                status_code=404, 
                detail=f"No active connection found with name: {config.connectionName}"
            )
        
        result = pc.get_schema_and_table()
        logger.info(f"Found {len(result['schemas'])} schemas")
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch schemas: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/preview_table")
def preview_table(credentials: DBCredentials):
    """Preview table data (first 15 rows)"""
    try:
        logger.info(f"Preview table request: {credentials.model_dump()}")
        
        if not credentials.db_schema or not credentials.table:
            raise HTTPException(status_code=400, detail="Schema and table names are required")
        
        # Get PostgreSQL client from cache
        pc = fastapi_client.get_postgres_client(credentials.connection_name)
        if not pc:
            raise HTTPException(
                status_code=404, 
                detail=f"No active connection found with name: {credentials.connection_name}"
            )

        limit = 15
        logger.info(f"Previewing table: {credentials.db_schema}.{credentials.table}")
        
        query = f"""SELECT * FROM "{credentials.db_schema}"."{credentials.table}" LIMIT {limit}"""
        result = pc.execute_query(query)
        
        columns = pc.get_columns(table_name=credentials.table, schema=credentials.db_schema)
        rows = [list(row.values()) if hasattr(row, 'values') else list(row) for row in result]
        
        return {
            "schema": credentials.db_schema,
            "table": credentials.table,
            "columns": columns,
            "rows": rows,
            "rowCount": len(rows)
        }

    except HTTPException:
        raise  
    except Exception as e:
        logger.error(f"Failed to preview table: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("air/get_columns")
def get_columns(credentials: DBCredentials):
    """Get column information for a table"""
    try:
        if not credentials.db_schema or not credentials.table:
            raise HTTPException(status_code=400, detail="Schema and table names are required")
        
        logger.info(f"Getting columns for table: {credentials.db_schema}.{credentials.table}")

        # Get PostgreSQL client from cache
        pc = fastapi_client.get_postgres_client(credentials.connection_name)
        if not pc:
            raise HTTPException(
                status_code=404, 
                detail=f"No active connection found with name: {credentials.connection_name}"
            )
            
        columns = pc.get_columns(credentials.db_schema, credentials.table)
        
        return {
            "schema": credentials.db_schema,
            "table": credentials.table,
            "columns": columns,
            "count": len(columns)
        }
        
    except HTTPException:
        raise  
    except Exception as e:
        logger.error(f"Failed to get columns: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/get_primary_keys")
def get_primary_keys(credentials: DBCredentials):
    """Get primary keys for a table, with detection if none exist"""
    try:
        if not credentials.db_schema or not credentials.table:
            raise HTTPException(status_code=400, detail="Schema and table names are required")
        
        logger.info(f"Getting primary keys for table: {credentials.db_schema}.{credentials.table}")
        
        # Get PostgreSQL client from cache
        pc = fastapi_client.get_postgres_client(credentials.connection_name)
        if not pc:
            raise HTTPException(
                status_code=404, 
                detail=f"No active connection found with name: {credentials.connection_name}"
            )
        
        # Get existing primary keys
        existing_pks = pc.get_primary_keys(credentials.db_schema, credentials.table)
        has_pks = len(existing_pks) > 0
        
        # Detect potential primary keys if none exist
        detected_pks = []
        if not has_pks:
            detected_pks = pc.detect_primary_keys(credentials.db_schema, credentials.table)
        
        return {
            "schema": credentials.db_schema,
            "table": credentials.table,
            "primary_keys": existing_pks,
            "detected_keys": detected_pks,
            "has_primary_keys": has_pks
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get primary keys: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

# ==================== Airflow  ====================
def trigger_airflow_dag(dag_id: str, dag_run_id: str, dag_conf: Dict) -> Dict:
    """ Trigger Airflow DAG via REST API """
    response = requests.patch(
        f"{airflow_url}/api/v1/dags/{dag_id}",
        json={"is_paused": False},
        auth = {airflow_user, airflow_password},
        headers={"Content-Type": "application/json"}
    )
    logger.info("Patch with response: ", response.status_code)

    response = requests.post(
        f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns",
        json={
            "dag_run_id": dag_run_id,
            "conf": dag_conf
        },
        auth=(airflow_user, airflow_password),
        headers={"Content-Type": "application/json"}
    )

    if response.status_code not in [200, 201]:
        raise HTTPException(
            status_code=500,
            detail=f"Airflow API error: {response.text}"
        )
    
    return response.json()

@app.post("/job/create", tags = ["Jobs"])
def create_job(request: CreateJobRequest):
    try:
        logger.info(f"Start creating jobs: {request.job_name} with type {request.job_type}")

        # Validate connection exist
        conn_info = fastapi_client.get_active_connection(connection_name = request.connection_name)
        if not conn_info:
            raise HTTPException(
                status_code=404,
                detail=f"Connection '{request.connection_name}' not found"
            )
        
        temp_job = JobFactory.create_job(job_type = request.job_type,
                                         job_id = 0,
                                         job_name = request.job_name,
                                         config = request.config,
                                         created_by=request.created_by)

        temp_job.validate()

        # Create Job
        job = AirflowJob(job_name = request.job_name, 
                         job_type = JobType(request.job_type),
                         status = JobStatus.ACTIVE,
                         created_at = datetime.now(),
                         created_by = request.created_by)
        
        job_id = fastapi_client.create_job(job)
        if not job_id:
            raise HTTPException(status_code=500, detail="Failed to create job")

        # Create first version of job
        version = JobVersion(job_id = job_id,
                            version_id=1,
                            is_active=True,
                            connection_name=request.connection_name,
                            job_config=request.config,
                            schedule_type=ScheduleType(request.schedule_type),
                            schedule_cron=request.schedule_cron,
                            created_by=request.created_by,
                            created_at=datetime.now())
        
        fastapi_client.create_version(version)
        
        logger.info(f"Create job {job_id}")
        return {
            "status": "success",
            "message": "Job created successfully",
            "job_id": job_id,
            "job_name": request.job_name,
            "version_id": 1
        }
    except ValueError as ve:
        # Validation error
        logger.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=f"Validation error: {str(ve)}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create job: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs", tags=["Jobs"])
def list_jobs():
    """Get all active jobs"""
    try:
        jobs = fastapi_client.get_all_active_jobs()
        return {
            "status": "success",
            "jobs": [job.to_dict() for job in jobs],
            "count": len(jobs)
        }
        
    except Exception as e:
        logger.error(f"Failed to list jobs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs/{job_id}", tags=["Jobs"])
def get_job_detail(job_id: int):
    """Get detailed job information with active configuration"""
    try:
        job_data = fastapi_client.get_active_job_by_id(job_id)
        
        if not job_data or len(job_data) == 0:
            raise HTTPException(status_code=404, detail="Job not found")
        
        job_info = job_data[0]
        
        job = JobFactory.create_job(
            job_type=job_info['job_type'],
            job_id=job_info['job_id'],
            job_name=job_info['job_name'],
            config=job_info['job_config'],
            created_by=job_info['created_by'],
            status=JobStatus(job_info['status']),
            created_at=job_info['created_at'],
            updated_at=job_info['updated_at']
        )
        
        return {
            "status": "success",
            "job": job.to_dict(),
            "config": job.to_config(),
            "version": {
                "version_id": job_info['version_id'],
                "connection_name": job_info['connection_name'],
                "schedule_type": job_info['schedule_type'],
                "schedule_cron": job_info['schedule_cron'],
                "is_active": job_info['is_active']
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get job: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/jobs/{job_id}", tags=["Jobs"])
def update_job(job_id: int, request: UpdateJobRequest):
    """Update job configuration (creates new version)"""
    try:
        logger.info(f"Updating jobs {job_id}")
        current_job = fastapi_client.get_active_job_by_id(job_id=job_id)
        if len(current_job) == 0 or not current_job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        new_config = {}
        if request.connection_name:
            new_config['connection_name'] = request.connection_name
        if request.job_config:
            new_config['job_config'] = request.job_config
        if request.schedule_type:
            new_config['schedule_type'] = request.schedule_type
        if request.schedule_cron:
            new_config['schedule_cron'] = request.schedule_cron
        
        # Update
        fastapi_client.update_job(job_id = job_id, 
                                  new_config = new_config, 
                                  updated_by=request.updated_by)
        return {
            "status": "success",
            "message": "Job updated successfully",
            "job_id": job_id,
            "new_version_id": fastapi_client.get_next_version_id(job_id)-1
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update job: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/jobs/{job_id}", tags=["Jobs"])
def delete_job(job_id: int):
    """Soft delete job"""
    try:
        logger.info(f"Deleting job {job_id}")
        
        job = fastapi_client.get_active_job_by_id(job_id)
        if not job or len(job) == 0:
            raise HTTPException(status_code=404, detail="Job not found")
        
        fastapi_client.delete_job(job_id)
        
        job_info = job[0]
        
        logger.info(f"Job {job_id} deleted")
        
        return {
            "status": "success",
            "message": f"Job '{job_info['job_name']}' deleted successfully",
            "job_id": job_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete job: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs/{job_id}/versions", tags=["Jobs"])
def get_job_versions(job_id: int):
    """Get all versions of a job"""
    try:
        versions = fastapi_client.get_job_versions(job_id)
        
        return {
            "status": "success",
            "job_id": job_id,
            "versions": [v.to_dict() for v in versions],
            "count": len(versions)
        }
        
    except Exception as e:
        logger.error(f"Failed to get versions: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== Job Execution Endpoints ====================

@app.post("/jobs/{job_id}/trigger", tags=["Execution"])
def trigger_job(job_id: int, request: TriggerJobsRequest):
    """Trigger job execution immediately (on-demand)"""
    try:
        logger.info(f"Triggering job {job_id} manually")
        
        # Get job config
        job_list = fastapi_client.get_active_job_by_id(job_id)
        
        if not job_list or len(job_list) == 0:
            raise HTTPException(status_code=404, detail="Job not found")
        
        job = job_list[0] if isinstance(job_list, list) else job_list
        
        if job['status'] != 'active':
            raise HTTPException(status_code=400, detail="Job is not active")
        
        # Generate DAG run ID
        dag_run_id = f"manual__{datetime.now().strftime('%Y%m%dT%H%M%S')}_{job_id}"
        
        # Prepare DAG config
        dag_conf = {
            "job_id": job_id,
            "job_name": job['job_name'],
            "job_type": job['job_type'],
            "version_id": job['version_id'],
            "connection_name": job['connection_name'],
            "job_config": job['job_config'],
            "triggered_by": request.triggered_by,
            "triggered_at": datetime.now().isoformat()
        }
        
        # Choose DAG based on job type
        dag_name_map = {
            'ingest': 'ingest_on_demand',
            'transform': 'transform_on_demand',
            'quality': 'quality_on_demand'
        }
        
        dag_name = dag_name_map.get(job['job_type'], 'ingest_on_demand')
        
        # Trigger Airflow
        trigger_airflow_dag(dag_name, dag_run_id, dag_conf)
        
        # Record in history
        history = JobHistory(
            job_id=job_id,
            version_id=job['version_id'],
            dag_run_id=dag_run_id,
            trigger_type='manual',
            triggered_by=request.triggered_by,
            run_at=datetime.now(),
            status=RunStatus.QUEUED
        )
        
        history_id = fastapi_client.create_history(history)
                
        logger.info(f"Job {job_id} triggered: {dag_run_id}")
        
        return {
            "status": "success",
            "message": "Job triggered successfully",
            "job_id": job_id,
            "dag_run_id": dag_run_id,
            "history_id": history_id,
            "airflow_url": f"{airflow_url}/dags/{dag_name}/grid?dag_run_id={dag_run_id}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to trigger job: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs/{job_id}/history", tags=["Execution"])
def get_job_history(job_id: int, limit: int = 20):
    """Get job execution history"""
    try:
        history = fastapi_client.get_job_history(job_id, limit)
        
        return {
            "status": "success",
            "job_id": job_id,
            "history": [h.to_dict() for h in history],
            "count": len(history)
        }
        
    except Exception as e:
        logger.error(f"Failed to get history: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/jobs/callback/completed", tags=["Execution"])
def job_completion_callback(request: dict):
    """Callback from Airflow when job completes"""
    try:
        dag_run_id = request.get('dag_run_id')
        status = request.get('status')
        duration = request.get('duration_seconds')
        error_message = request.get('error_message')
        result = request.get('result', {})
        
        logger.info(f"Callback: {dag_run_id} - {status}")
        
        # Update history
        fastapi_client.update_history(
            dag_run_id=dag_run_id,
            status=status,
            duration=duration,
            error_message=error_message,
            result=result
        )
        
        return {
            "status": "success",
            "message": "Callback processed"
        }
        
    except Exception as e:
        logger.error(f"Failed to process callback: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== Batch Operations ====================

@app.post("/jobs/ingest/batch_create", tags=["Batch Operations"])
def create_ingest_jobs(request: CreateIngestJobsRequest):
    """Create multiple ingest jobs from schema selector"""
    try:
        logger.info(f"Batch creating {len(request.tables)} ingest jobs")
        
        # Validate connection
        conn_info = fastapi_client.get_active_connection(request.connection_name)
        if not conn_info:
            raise HTTPException(
                status_code=404,
                detail=f"Connection '{request.connection_name}' not found"
            )
        
        created_jobs = []
        failed_jobs = []
        
        for table_info in request.tables:
            try:
                schema = table_info['schema']
                table = table_info['table']
                primary_keys = table_info.get('primary_keys', [])
                
                # Generate job name
                job_name = f"ingest_{request.connection_name}_{schema}_{table}"
                
                # Build ingest job config
                job_config = {
                    "source": {
                        "schema": schema,
                        "table": table,
                        "primary_keys": primary_keys
                    },
                    "target": {
                        "path": f"s3a://{conn_info['database']}/bronze/{schema}/{table}",
                        "format": "delta"
                    }
                }
                
                # Create job
                job = AirflowJob(
                    job_name=job_name,
                    job_type=JobType.INGEST,
                    created_by=request.created_by
                )
                
                job_id = fastapi_client.create_job(job)
                
                if not job_id:
                    failed_jobs.append({
                        "schema": schema,
                        "table": table,
                        "reason": "Failed to create job"
                    })
                    continue
                
                # Create version
                version = JobVersion(
                    job_id=job_id,
                    version_id=1,
                    is_active=True,
                    connection_name=request.connection_name,
                    job_config=job_config,
                    schedule_type=ScheduleType(request.schedule_type),
                    schedule_cron=request.schedule_cron,
                    created_by=request.created_by
                )
                
                fastapi_client.create_version(version)
                
                created_jobs.append({
                    "job_id": job_id,
                    "job_name": job_name,
                    "schema": schema,
                    "table": table
                })
                
                logger.info(f"Created ingest job {job_id}: {job_name}")
                
            except Exception as e:
                logger.error(f"Failed to create job for {schema}.{table}: {str(e)}")
                failed_jobs.append({
                    "schema": table_info.get('schema'),
                    "table": table_info.get('table'),
                    "reason": str(e)
                })
        
        return {
            "status": "success" if len(failed_jobs) == 0 else "partial",
            "message": f"Created {len(created_jobs)} jobs, {len(failed_jobs)} failed",
            "created_jobs": created_jobs,
            "failed_jobs": failed_jobs,
            "total": len(request.tables)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Batch creation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== System Endpoints ====================

@app.get("/health", tags=["System"])
def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "Data Quality Monitoring API",
        "version": "1.0.0"
    }


@app.get("/", tags=["System"])
def root():
    """API information"""
    return {
        "service": "Data Quality Monitoring API",
        "version": "1.0.0",
        "docs_url": "/docs",
        "endpoints": {
            "jobs": {
                "POST /jobs/create": "Create new job",
                "GET /jobs": "List all jobs",
                "GET /jobs/{id}": "Get job details",
                "PUT /jobs/{id}": "Update job",
                "DELETE /jobs/{id}": "Delete job",
                "GET /jobs/{id}/versions": "Get job versions"
            },
            "execution": {
                "POST /jobs/{id}/trigger": "Trigger job execution",
                "GET /jobs/{id}/history": "Get execution history",
                "POST /jobs/callback/completed": "Airflow callback"
            },
            "batch": {
                "POST /jobs/ingest/batch_create": "Batch create ingest jobs"
            }
        }
    }
# ==================== Data Processing Endpoints ====================
@app.post("/submit_tables")
def submit_tables(submission: TableSubmission):
    """Submit tables for data quality monitoring"""
    try:
        logger.info(f"Submitting {len(submission.tables)} tables for data quality monitoring")
        
        if not submission.tables:
            raise ValueError("No tables selected")
        
        processed_tables = []
        for table_info in submission.tables:
            schema = table_info.get("schema")
            table = table_info.get("table")

            if not schema or not table:
                continue
                
            processed_tables.append({
                "schema": schema,
                "table": table,
                "full_name": f"{schema}.{table}"
            })
            
            logger.info(f"Added table: {schema}.{table} to quality monitoring")
        
        config = {
            "connectionName": submission.connectionName,
            "tables": processed_tables,
            "tableCount": len(processed_tables),
            "addedAt": datetime.now().isoformat()
        }
        
        config_file_path = f"configs/quality_tables_{submission.connectionName}.json"
        os.makedirs("configs", exist_ok=True)
        
        with open(config_file_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        logger.info(f"Configuration saved to {config_file_path}")
        
        return {
            "status": "success",
            "message": f"Successfully added {len(processed_tables)} tables to data quality system",
            "tables": processed_tables,
            "configFile": config_file_path
        }
        
    except Exception as e:
        logger.error(f"Failed to submit tables: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))



# ==================== Utility Endpoints ====================
@app.post("/clear_cache")
def clear_cache(cache_clear: CacheClear):
    """Clear connection cache for testing or maintenance"""
    try:
        connection_name = cache_clear.connection_name
        fastapi_client.clear_connection_cache(connection_name)
        
        msg = (f"Cache cleared for connection: {connection_name}" 
               if connection_name 
               else "All connection cache cleared")
        logger.info(msg)
        
        return {
            "status": "success",
            "message": msg
        }
        
    except Exception as e:
        logger.error(f"Failed to clear cache: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "FastAPI Backend",
        "version": "1.0.0"
    }

@app.get("/")
def root():
    """Root endpoint with API information"""
    return {
        "service": "Data Quality Monitoring API",
        "version": "1.0.0",
        "endpoints": {
            "connections": {
                "POST /create_connection": "Create or verify PostgreSQL connection",
                "GET /connections": "List all active connections",
                "DELETE /delete_connection": "Delete a connection"
            },
            "schema": {
                "POST /get_schemas": "Get schemas and tables",
                "POST /preview_table": "Preview table data",
                "POST /get_columns": "Get table columns",
                "POST /get_primary_keys": "Get table primary keys"
            },
            "processing": {
                "POST /bronze_ingestion": "Ingest data to bronze layer",
                "POST /submit_tables": "Submit tables for quality monitoring"
            },
            "utility": {
                "POST /clear_cache": "Clear connection cache",
                "GET /health": "Health check"
            }
        }
    }

# ==================== Error Handlers ====================
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return {
        "status": "error",
        "message": "Endpoint not found",
        "path": str(request.url)
    }

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    logger.error(f"Internal server error: {str(exc)}")
    return {
        "status": "error",
        "message": "Internal server error",
        "detail": str(exc)
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)