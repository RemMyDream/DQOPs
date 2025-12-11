"""
IngestJob Creation Requests
"""
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator
from domain.entity.job_client import JobType
from domain.entity.job_schemas import ScheduleType
from domain.request.job_request import JobCreateRequest

# ==================== Ingest Job Configs ====================

class IngestSourceConfig(BaseModel):
    """Type-safe configuration for ingest source"""
    schemaName: str = Field(..., min_length=1, description="Database schema name")
    tableName: str = Field(..., min_length=1, description="Table name to ingest")
    primaryKeys: List[str] = Field(..., min_length=1, description="Primary key columns")
    
    @field_validator('schemaName', 'tableName')
    def validate_names(cls, v):
        """Validate schema and table names"""
        if not v or not v.strip():
            raise ValueError('Name cannot be empty')
        
        # Check for SQL injection characters
        dangerous_chars = [';', '--', '/*', '*/', 'xp_', 'sp_']
        if any(char in v.lower() for char in dangerous_chars):
            raise ValueError(f'Invalid characters in name')
        
        return v.strip()
    
    @field_validator('primaryKeys')
    def validate_primary_keys(cls, v):
        """Validate primary keys list"""
        if not v or len(v) == 0:
            raise ValueError('At least one primary key is required')
        
        # Remove empty strings and duplicates
        v = list(set(k.strip() for k in v if k.strip()))
        
        if len(v) == 0:
            raise ValueError('At least one valid primary key is required')
        
        return v


class IngestTargetConfig(BaseModel):
    """Type-safe configuration for ingest target"""
    layer: str = Field(..., pattern="^(bronze|silver|gold)$", description="Data lake layer")
    path: str = Field(..., min_length=1, description="S3 path for data storage")
    format: str = Field(default="delta", pattern="^(delta|parquet|avro|orc)$", description="Storage format")
    
    @field_validator('path')
    def validate_path(cls, v):
        """Validate S3 path format"""
        if not v or not v.strip():
            raise ValueError('Path cannot be empty')
        
        v = v.strip()
        
        # Basic S3 path validation
        if not (v.startswith('s3://') or v.startswith('s3a://')):
            raise ValueError('Path must start with s3:// or s3a://')
        
        return v


class IngestJobCreateRequest(BaseModel):
    """Type-safe request for creating ingest jobs"""
    jobName: str = Field(..., min_length=1, max_length=200, description="Unique job name")
    connectionName: str = Field(..., min_length=1, description="Database connection name")
    source: IngestSourceConfig = Field(..., description="Source configuration")
    target: IngestTargetConfig = Field(..., description="Target configuration")
    scheduleType: ScheduleType = Field(default=ScheduleType.ON_DEMAND, description="Schedule type")
    scheduleCron: Optional[str] = Field(None, description="Cron expression for scheduled jobs")
    createdBy: str = Field(default="admin", description="User who created the job")
    
    @field_validator('jobName')
    def validate_job_name(cls, v):
        """Validate job name format"""
        if not v or not v.strip():
            raise ValueError('Job name cannot be empty')
        
        v = v.strip()
        
        return v
    
    @field_validator('scheduleCron')
    def validate_cron(cls, v, info):
        """Validate cron expression for scheduled jobs"""
        values = info.data
        schedule_type = values.get('scheduleType')
        
        if schedule_type in [ScheduleType.SCHEDULED, ScheduleType.BOTH]:
            if not v or not v.strip():
                raise ValueError('scheduleCron is required for scheduled jobs')
        
        return v
    
    def to_generic(self) -> JobCreateRequest:
        """
        Convert to generic JobCreateRequest for internal processing
        """
        config = {
            "source": self.source.model_dump(),
            "target": self.target.model_dump(),
        }
        
        return JobCreateRequest(
            jobName=self.jobName,
            jobType=JobType.INGEST,
            connectionName=self.connectionName,
            config=config,
            scheduleType=self.scheduleType,
            scheduleCron=self.scheduleCron,
            createdBy=self.createdBy
        )
    
    def to_dag_conf(self) -> Dict[str, Any]:
        """
        Convert to DAG conf dict
        """
        dag_conf = {
            "jobName": self.jobName, 
            "connectionName": self.connectionName,
            "createdBy": self.createdBy
        } | self.source.model_dump() | self.target.model_dump() 