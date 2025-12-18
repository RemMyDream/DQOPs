"""
IngestJob Creation Requests
"""
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator
from domain.entity.job_client import JobType
from domain.entity.job_schemas import ScheduleType
from domain.request.job_request import JobCreateRequest

class IngestSourceConfig(BaseModel):
    """Type-safe configuration for ingest source"""
    schema_name: str = Field(..., min_length=1, description="Database schema name")
    table_name: str = Field(..., min_length=1, description="Table name to ingest")
    primary_keys: List[str] = Field(..., min_length=1, description="Primary key columns")
    
    @field_validator('schema_name', 'table_name')
    def validate_names(cls, v):
        """Validate schema and table names"""
        if not v or not v.strip():
            raise ValueError('Name cannot be empty')
        
        dangerous_chars = [';', '--', '/*', '*/', 'xp_', 'sp_']
        if any(char in v.lower() for char in dangerous_chars):
            raise ValueError(f'Invalid characters in name')
        
        return v.strip()
    
    @field_validator('primary_keys')
    def validate_primary_keys(cls, v):
        """Validate primary keys list"""
        if not v or len(v) == 0:
            raise ValueError('At least one primary key is required')
        
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
        
        if not (v.startswith('s3://') or v.startswith('s3a://')):
            raise ValueError('Path must start with s3:// or s3a://')
        
        return v


class IngestJobCreateRequest(BaseModel):
    """Type-safe request for creating ingest jobs"""
    job_name: str = Field(None, min_length=1, max_length=200, description="Unique job name")
    connection_name: str = Field(..., min_length=1, description="Database connection name")
    source: IngestSourceConfig = Field(..., description="Source configuration")
    target: IngestTargetConfig = Field(..., description="Target configuration")
    schedule_type: ScheduleType = Field(default=ScheduleType.ON_DEMAND, description="Schedule type")
    schedule_cron: Optional[str] = Field(None, description="Cron expression for scheduled jobs")
    created_by: str = Field(default="admin", description="User who created the job")
    
    @field_validator('job_name')
    def validate_job_name(cls, v):
        """Validate job name format"""
        if not v or not v.strip():
            raise ValueError('Job name cannot be empty')
        
        return v.strip()
    
    @field_validator('schedule_cron')
    def validate_cron(cls, v, info):
        """Validate cron expression for scheduled jobs"""
        values = info.data
        schedule_type = values.get('schedule_type')
        
        if schedule_type in [ScheduleType.SCHEDULED, ScheduleType.BOTH]:
            if not v or not v.strip():
                raise ValueError('schedule_cron is required for scheduled jobs')
        
        return v
    
    def to_generic(self) -> JobCreateRequest:
        """Convert to generic JobCreateRequest (used for database)"""
        config = {
            "source": self.source.model_dump(),
            "target": self.target.model_dump(),
        }
        
        return JobCreateRequest(
            job_name=self.job_name,
            job_type=JobType.INGEST,
            connection_name=self.connection_name,
            config=config,
            schedule_type=self.schedule_type,
            schedule_cron=self.schedule_cron,
            created_by=self.created_by
        )
    
    def to_dag_conf(self) -> Dict[str, Any]:
        """Convert to DAG conf dict"""
        return {
            "job_name": self.job_name, 
            "connection_name": self.connection_name,
            "created_by": self.created_by
        } | self.source.model_dump() | self.target.model_dump()