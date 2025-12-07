"""
Domain Models: Job Version, History, and Supporting Types
"""
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum


class ScheduleType(str, Enum):
    ON_DEMAND = "on_demand"
    SCHEDULED = "scheduled"
    BOTH = "both"

class RunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class JobVersion:
    """Job version with configuration"""
    jobId: int
    versionId: int
    connectionName: str
    config: Dict[str, Any]
    scheduleType: ScheduleType
    createdBy: str
    isActive: bool = False
    scheduleCron: Optional[str] = None
    createdAt: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        if self.createdAt:
            data['createdAt'] = self.createdAt.isoformat()
        data['scheduleType'] = self.scheduleType.value
        return data


@dataclass
class JobHistory:
    """Job execution history"""
    jobID: int
    version_id: int
    runAt: datetime
    status: RunStatus
    triggerType: str
    triggeredBy: str
    historyId: Optional[int] = None
    dag_run_id: Optional[str] = None
    endAt: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    error_message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
   
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['runAt'] = self.runAt.isoformat()
        if self.endAt:
            data['endAt'] = self.endAt.isoformat()
        data['status'] = self.status.value
        return data


@dataclass
class JobSummary:
    """Combined job info for UI display"""
    jobID: int
    jobName: str
    jobType: str
    versionId: int
    connectionName: str
    scheduleType: str
    isActive: bool
    createdBy: str
    createdAt: datetime
    total_runs: int
    success_rate: float
    scheduleCron: Optional[str] = None
    last_run_at: Optional[datetime] = None
    last_run_status: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['createdAt'] = self.createdAt.isoformat()
        if self.last_run_at:
            data['last_run_at'] = self.last_run_at.isoformat()
        return data