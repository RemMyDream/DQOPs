from typing import Optional, Dict, Any
from pydantic import BaseModel, field_validator, Field
from domain.entity.job_client import JobType
from domain.entity.job_schemas import ScheduleType


class JobCreateRequest(BaseModel):
    """Request model for creating a new job"""
    job_name: str = Field(..., min_length=1, max_length=200, description="Unique job name")
    job_type: JobType = Field(..., description="Type of job (ingest, transform, quality, export)")
    connection_name: str = Field(..., min_length=1, description="Database connection name")
    config: Dict[str, Any] = Field(default_factory=dict, description="Job-specific configuration")
    schedule_type: ScheduleType = Field(..., description="Schedule type for job execution")
    schedule_cron: Optional[str] = Field(None, description="Cron expression for scheduled jobs")
    created_by: str = Field(default="system", description="User who created the job")

    @field_validator('job_name')
    def validate_job_name(cls, v):
        """Validate job name format"""
        if not v or not v.strip():
            raise ValueError('Job name cannot be empty')
        
        v = v.strip()
        
        invalid_chars = ['/', '\\', '?', '*', ':', '|', '"', '<', '>']
        if any(char in v for char in invalid_chars):
            raise ValueError(f'Job name cannot contain: {", ".join(invalid_chars)}')
        
        return v

    @field_validator('connection_name')
    def validate_connection_name(cls, v):
        """Validate connection name is not empty"""
        if not v or not v.strip():
            raise ValueError('Connection name cannot be empty')
        return v.strip()
    
    @field_validator('schedule_cron')
    def validate_cron(cls, v, info):
        """Validate cron expression is provided for scheduled jobs"""
        values = info.data
        schedule_type = values.get('schedule_type')
        
        if schedule_type in [ScheduleType.SCHEDULED, ScheduleType.BOTH]:
            if not v or not v.strip():
                raise ValueError('schedule_cron is required for scheduled jobs')
        
        return v


class JobTriggerRequest(BaseModel):
    """Request model for triggering a job manually"""
    triggered_by: str = Field(default="admin", description="User who triggered the job")


class JobUpdateRequest(BaseModel):
    """Request model for updating an existing job"""
    connection_name: Optional[str] = Field(None, description="Updated connection name")
    config: Optional[Dict[str, Any]] = Field(None, description="Updated job configuration")
    schedule_type: Optional[ScheduleType] = Field(None, description="Updated schedule type")
    schedule_cron: Optional[str] = Field(None, description="Updated cron expression")
    updated_by: str = Field(default="admin", description="User who updated the job")
    
    @field_validator('schedule_cron')
    def validate_cron(cls, v, info):
        """Validate cron expression if schedule type requires it"""
        values = info.data
        schedule_type = values.get('schedule_type')
        
        if schedule_type in [ScheduleType.SCHEDULED, ScheduleType.BOTH]:
            if not v or not v.strip():
                raise ValueError('schedule_cron is required when schedule_type is scheduled or both')
        
        return v
    
    class Config:
        use_enum_values = True