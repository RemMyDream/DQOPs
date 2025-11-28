from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict, field
from typing import Optional, Dict, List, Any
from datetime import datetime
import json
from enum import Enum

class JobType(str, Enum):
    INGEST = "ingest"
    TRANSFORM = "transform"
    QUALITY = "quality"
    EXPORT = "export"

class JobStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    DELETED = "deleted"

class RunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"

class ScheduleType(str, Enum):
    ON_DEMAND = "on_demand"
    SCHEDULED = "scheduled"
    BOTH = "both"

# ================ Base Class ==================
@dataclass
class AirflowJob(ABC):
    """Base job information"""
    job_name: str
    job_type: JobType
    created_by: str
    status: JobStatus = JobStatus.ACTIVE
    job_id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @abstractmethod
    def to_config(self) -> Dict[str, Any]:
        """Convert job to config dict for storage"""
        pass

    @classmethod
    @abstractmethod
    def from_config(cls, job_id: int, job_name: str, config: Dict[str: Any], **kwargs):
        """Build job instance from config dict"""
        pass

    def to_dict(self):
        data = asdict(self)
        if self.created_at:
            data['created_at'] = self.created_at.isoformat()
        if self.updated_at:
            data['updated_at'] = self.updated_at.isoformat()
        data['status'] = self.status.value
        data['job_type'] = self.job_type.value
        return data

# ================ Ingest Job ==================
@dataclass
class IngestJobSource:
    """Ingest source configuration"""
    connection_name: str
    schema: str
    table: str
    primary_keys: List[str]
    
    def to_dict(self):
        return asdict(self)
    
@dataclass
class IngestJobTarget:
    """Ingest target configuration"""
    layer: str
    path: str
    format: str = "delta"

    def to_dict(self):
        return asdict(self)
    
@dataclass
class IngestJob(AirflowJob):
    """ Ingest Job """
    source: IngestJobSource
    target: IngestJobTarget

    def __post_init__(self):
        if self.job_type != JobType.INGEST:
            self.job_type = JobType.INGEST
    
    def to_config(self):
        response = {
            "source": self.source.to_dict() if self.source else {},
            "target": self.target.to_dict() if self.target else {}
        }
        return response
    
    @classmethod
    def from_config(cls, job_id: int, job_name: str, config: Dict[str: Any], 
                    created_by: str = "system", **kwargs):
        """ Create IngestJob from config """ 
        source_data = config.get('source', {})
        target_data = config.get('target', {})

        return cls(
            job_id = job_id,
            job_name = job_name,
            job_type = JobType.INGEST,
            created_by = created_by,
            source = IngestJobSource(**source_data) if source_data else None,
            target = IngestJobTarget(**target_data) if target_data else None,
            **kwargs
        )         

    def validate(self) -> bool:
        """Validate ingest job configuration"""
        if not self.source.connection_name:
            raise ValueError("connection_name is required")
        if not self.source or not self.source.schema or not self.source.table:
            raise ValueError("source schema and table are required")
        if not self.target or not self.target.path:
            raise ValueError("target path is required")
        return True

# ==================== Job Factory ====================

class JobFactory:
    """Factory to create appropriate job type from config"""
    @staticmethod
    def create_job(job_type: str, job_id: int, job_name: str, 
                   config: Dict[str, Any], created_by: str = "system", 
                   **kwargs) -> AirflowJob:
        """ Create job instance based on type """
        job_type_enum = JobType(job_type)
        if job_type_enum == JobType.INGEST:
            return IngestJob.from_config(job_id, job_name, config, created_by, **kwargs)
        # elif job_type_enum == JobType.TRANSFORM:
        #     return TransformJob.from_config(job_id, job_name, config, created_by, **kwargs)
        # elif job_type_enum == JobType.QUALITY:
        #     return QualityJob.from_config(job_id, job_name, config, created_by, **kwargs)
        else:
            raise ValueError(f"Unknown job type: {job_type}")

# ==================== Version & History  ====================

@dataclass
class JobVersion:
    """Job version with generic config"""
    job_id: int
    version_id: int
    connection_name: str
    job_config: Dict[str, Any]  # Generic JSON config
    schedule_type: ScheduleType
    created_by: str
    is_active: bool = False
    schedule_cron: Optional[str] = None
    created_at: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        data = asdict(self)
        if self.created_at:
            data['created_at'] = self.created_at.isoformat()
        data['schedule_type'] = self.schedule_type.value
        return data

@dataclass
class JobHistory:
    """Job execution history"""
    job_id: int
    version_id: int
    run_at: datetime
    status: RunStatus
    trigger_type: str
    triggered_by: str
    history_id: Optional[int] = None
    dag_run_id: Optional[str] = None
    end_at: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    error_message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
   
    def to_dict(self) -> dict:
        data = asdict(self)
        data['run_at'] = self.run_at.isoformat()
        if self.end_at:
            data['end_at'] = self.end_at.isoformat()
        data['status'] = self.status.value
        return data

@dataclass
class JobSummary:
    """Combined job info for UI display"""
    job_id: int
    job_name: str
    job_type: str
    version_id: int
    connection_name: str
    schedule_type: str
    is_active: bool
    created_by: str
    created_at: datetime
    total_runs: int
    success_rate: float
    schedule_cron: Optional[str] = None
    last_run_at: Optional[datetime] = None
    last_run_status: Optional[str] = None
    
    def to_dict(self) -> dict:
        data = asdict(self)
        data['created_at'] = self.created_at.isoformat()
        if self.last_run_at:
            data['last_run_at'] = self.last_run_at.isoformat()
        return data
