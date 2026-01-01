"""
Domain Model: Base Job Entity
"""
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Dict, Any
from datetime import datetime
from dataclasses import dataclass, asdict, field


class JobType(str, Enum):
    INGEST = "ingest"
    TRANSFORM = "transform"
    QUALITY = "quality"
    EXPORT = "export"


class JobStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    DELETED = "deleted"


@dataclass
class Job(ABC):
    """Base Job Domain Entity"""
    job_name: str
    job_type: JobType
    created_by: str
    status: JobStatus = JobStatus.ACTIVE
    job_id: Optional[int] = None
    created_at: Optional[datetime] = field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None

    @abstractmethod
    def to_config(self) -> Dict[str, Any]:
        """Convert job to config dict for storage"""
        pass

    @classmethod
    @abstractmethod
    def from_config(cls, job_name: str, config: Dict[str, Any], **kwargs):
        """Build job instance from config dict"""
        pass

    @abstractmethod
    def validate(self) -> bool:
        """Validate job configuration"""
        pass

    def to_dict(self) -> Dict[str, Any]:
        """Convert job to dictionary with proper serialization"""
        data = asdict(self)
        data['job_type'] = self.job_type.value
        data['status'] = self.status.value
        
        if self.created_at:
            data['created_at'] = self.created_at.isoformat()
        if self.updated_at:
            data['updated_at'] = self.updated_at.isoformat()
            
        return data