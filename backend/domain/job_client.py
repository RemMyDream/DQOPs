from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Dict, List, Any
from datetime import datetime
from dataclasses import dataclass, asdict

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
    status: JobStatus
    jobId: Optional[int] = None
    createdAt: Optional[datetime] = None
    updatedAt: Optional[datetime] = None

    @abstractmethod
    def to_config(self) -> Dict[str, Any]:
        """Convert job to config dict for storage"""
        pass

    @classmethod
    @abstractmethod
    def from_config(cls, jobId: int, jobName: str, config: Dict[str, Any], **kwargs):
        """Build job instance from config dict"""
        pass

    @abstractmethod
    def validate(self) -> bool:
        """Validate job configuration"""
        pass

    def to_dict(self):
        data = asdict(self)
        if self.createdAt:
            data['createdAt'] = self.createdAt.isoformat()
        if self.updatedAt:
            data['updatedAt'] = self.updatedAt.isoformat()
        data['status'] = self.status.value
        data['jobType'] = self.jobType.value
        return data
    