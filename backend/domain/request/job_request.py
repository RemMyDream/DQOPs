from typing import Optional, Dict, Any
from pydantic import BaseModel, field_validator, Field
from backend.domain.entity.job_client import JobType
from backend.domain.entity.job_schemas import ScheduleType

class JobCreateRequest(BaseModel):
    """Request model for creating a new job"""
    jobName: str = Field(..., min_length=1, max_length=200, description="Unique job name")
    jobType: JobType = Field(..., description="Type of job (ingest, transform, quality, export)")
    connectionName: str = Field(..., min_length=1, description="Database connection name")
    config: Dict[str, Any] = Field(default_factory=dict, description="Job-specific configuration")
    scheduleType: ScheduleType = Field(..., description="Schedule type for job execution")
    scheduleCron: Optional[str] = Field(None, description="Cron expression for scheduled jobs")
    createdBy: str = Field(default="system", description="User who created the job")

    @field_validator('jobName')
    def validate_job_name(cls, v):
        """Validate job name format"""
        if not v or not v.strip():
            raise ValueError('Job name cannot be empty')
        
        # Remove leading/trailing whitespace
        v = v.strip()
        
        # Check for invalid characters (optional)
        invalid_chars = ['/', '\\', '?', '*', ':', '|', '"', '<', '>']
        if any(char in v for char in invalid_chars):
            raise ValueError(f'Job name cannot contain: {", ".join(invalid_chars)}')
        
        return v

    @field_validator('connectionName')
    def validate_connection_name(cls, v):
        """Validate connection name is not empty"""
        if not v or not v.strip():
            raise ValueError('Connection name cannot be empty')
        return v.strip()
    
    @field_validator('scheduleCron')
    def validate_cron(cls, v, info):
        """Validate cron expression is provided for scheduled jobs"""
        values = info.data
        schedule_type = values.get('scheduleType')
        
        if schedule_type in [ScheduleType.SCHEDULED, ScheduleType.BOTH]:
            if not v or not v.strip():
                raise ValueError('scheduleCron is required for scheduled jobs')
        
        return v
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return self.model_dump()
    
class JobTriggerRequest(BaseModel):
    """Request model for triggering a job manually"""
    triggeredBy: str = Field(default="admin", description="User who triggered the job")
    
    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()
    

class JobUpdateRequest(BaseModel):
    """Request model for updating an existing job"""
    connectionName: Optional[str] = Field(None, description="Updated connection name")
    config: Optional[Dict[str, Any]] = Field(None, description="Updated job configuration")
    scheduleType: Optional[ScheduleType] = Field(None, description="Updated schedule type")
    scheduleCron: Optional[str] = Field(None, description="Updated cron expression")
    updatedBy: str = Field(default="admin", description="User who updated the job")
    
    @field_validator('scheduleCron')
    def validate_cron(cls, v, info):
        """Validate cron expression if schedule type requires it"""
        values = info.data
        schedule_type = values.get('scheduleType')
        
        if schedule_type in [ScheduleType.SCHEDULED, ScheduleType.BOTH]:
            if not v or not v.strip():
                raise ValueError('scheduleCron is required when scheduleType is scheduled or both')
        
        return v
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding None values"""
        return self.model_dump(exclude_none=True)
    
    class Config:
        use_enum_values = True
