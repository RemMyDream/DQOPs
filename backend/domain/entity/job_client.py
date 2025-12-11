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
    jobName: str
    jobType: JobType
    createdBy: str
    status: JobStatus = JobStatus.ACTIVE
    jobId: Optional[int] = None
    createdAt: Optional[datetime] = field(default_factory=datetime.now)
    updatedAt: Optional[datetime] = None

    @abstractmethod
    def to_config(self) -> Dict[str, Any]:
        """Convert job to config dict for storage"""
        pass

    @classmethod
    @abstractmethod
    def from_config(cls, jobName: str, config: Dict[str, Any], **kwargs):
        """Build job instance from config dict"""
        pass

    @abstractmethod
    def validate(self) -> bool:
        """Validate job configuration"""
        pass

    def to_dict(self) -> Dict[str, Any]:
        """Convert job to dictionary with proper serialization"""
        data = asdict(self)
        data['jobType'] = self.jobType.value
        data['status'] = self.status.value
        
        if self.createdAt:
            data['createdAt'] = self.createdAt.isoformat()
        if self.updatedAt:
            data['updatedAt'] = self.updatedAt.isoformat()
            
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """Create Job instance from dictionary"""
        raise NotImplementedError("Use specific job type from_dict method")