"""
Domain Models: Job Version, History, and Supporting Types
"""
from dataclasses import dataclass, asdict, field
import json
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
    createdAt: Optional[datetime] = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with proper serialization"""
        data = asdict(self)
        data['scheduleType'] = self.scheduleType.value
        
        if self.createdAt:
            data['createdAt'] = self.createdAt.isoformat()
            
        return data
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any] | str):
        """
        Create JobVersion from dictionary or JSON string
        Handles both snake_case (DB) and camelCase (API) keys
        """
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON string: {e}")
        
        if not isinstance(data, dict):
            raise ValueError(f"Expected dict or str, got {type(data)}")
        
        # Key mapping from snake_case to camelCase
        key_mapping = {
            'job_id': 'jobId',
            'version_id': 'versionId',
            'is_active': 'isActive',
            'connection_name': 'connectionName',
            'schedule_type': 'scheduleType',
            'schedule_cron': 'scheduleCron',
            'created_by': 'createdBy',
            'created_at': 'createdAt'
        }
        
        # Normalize keys
        normalized_data = {}
        for key, value in data.items():
            normalized_key = key_mapping.get(key, key)
            normalized_data[normalized_key] = value
        
        # Handle config field (might be JSON string from DB)
        if 'config' in normalized_data and isinstance(normalized_data['config'], str):
            try:
                normalized_data['config'] = json.loads(normalized_data['config'])
            except json.JSONDecodeError:
                pass  # Keep as string if not valid JSON
        
        # Convert scheduleType to enum if it's a string
        if 'scheduleType' in normalized_data and isinstance(normalized_data['scheduleType'], str):
            normalized_data['scheduleType'] = ScheduleType(normalized_data['scheduleType'])
        
        # Convert timestamps if they're strings
        for time_field in ['createdAt']:
            if time_field in normalized_data and isinstance(normalized_data[time_field], str):
                try:
                    normalized_data[time_field] = datetime.fromisoformat(normalized_data[time_field])
                except ValueError:
                    pass
        
        # Filter only valid fields for dataclass
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in normalized_data.items() if k in valid_fields}
        
        return cls(**filtered_data)


@dataclass
class JobHistory:
    """Job execution history"""
    jobId: int
    versionId: int
    runAt: datetime
    status: RunStatus
    triggerType: str
    triggeredBy: str
    historyId: Optional[int] = None
    dagRunId: Optional[str] = None
    endAt: Optional[datetime] = None
    durationSeconds: Optional[int] = None
    errorMessage: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
   
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with proper serialization"""
        data = asdict(self)
        data['status'] = self.status.value
        data['runAt'] = self.runAt.isoformat()
        
        if self.endAt:
            data['endAt'] = self.endAt.isoformat()
            
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """Create JobHistory from dictionary"""
        key_mapping = {
            'job_id': 'jobId',
            'version_id': 'versionId',
            'history_id': 'historyId',
            'dag_run_id': 'dagRunId',
            'run_at': 'runAt',
            'end_at': 'endAt',
            'trigger_type': 'triggerType',
            'triggered_by': 'triggeredBy',
            'duration_seconds': 'durationSeconds',
            'error_message': 'errorMessage'
        }
        
        normalized_data = {}
        for key, value in data.items():
            normalized_key = key_mapping.get(key, key)
            normalized_data[normalized_key] = value
        
        # Convert status to enum
        if 'status' in normalized_data and isinstance(normalized_data['status'], str):
            normalized_data['status'] = RunStatus(normalized_data['status'])
        
        # Handle result field (might be JSON string)
        if 'result' in normalized_data and isinstance(normalized_data['result'], str):
            try:
                normalized_data['result'] = json.loads(normalized_data['result'])
            except json.JSONDecodeError:
                pass
        
        # Convert timestamps
        for time_field in ['runAt', 'endAt']:
            if time_field in normalized_data and isinstance(normalized_data[time_field], str):
                try:
                    normalized_data[time_field] = datetime.fromisoformat(normalized_data[time_field])
                except ValueError:
                    pass
        
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in normalized_data.items() if k in valid_fields}
        
        return cls(**filtered_data)


@dataclass
class JobSummary:
    """Combined job info for UI display"""
    jobId: int
    jobName: str
    jobType: str
    versionId: int
    connectionName: str
    scheduleType: str
    isActive: bool
    createdBy: str
    createdAt: datetime
    totalRuns: int
    successRate: float
    scheduleCron: Optional[str] = None
    lastRunAt: Optional[datetime] = None
    lastRunStatus: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with proper serialization"""
        data = asdict(self)
        data['createdAt'] = self.createdAt.isoformat()
        
        if self.lastRunAt:
            data['lastRunAt'] = self.lastRunAt.isoformat()
            
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """Create JobSummary from dictionary"""
        key_mapping = {
            'job_id': 'jobId',
            'job_name': 'jobName',
            'job_type': 'jobType',
            'version_id': 'versionId',
            'connection_name': 'connectionName',
            'schedule_type': 'scheduleType',
            'is_active': 'isActive',
            'created_by': 'createdBy',
            'created_at': 'createdAt',
            'total_runs': 'totalRuns',
            'success_rate': 'successRate',
            'schedule_cron': 'scheduleCron',
            'last_run_at': 'lastRunAt',
            'last_run_status': 'lastRunStatus'
        }
        
        normalized_data = {}
        for key, value in data.items():
            normalized_key = key_mapping.get(key, key)
            normalized_data[normalized_key] = value
        
        # Convert timestamps
        for time_field in ['createdAt', 'lastRunAt']:
            if time_field in normalized_data and isinstance(normalized_data[time_field], str):
                try:
                    normalized_data[time_field] = datetime.fromisoformat(normalized_data[time_field])
                except ValueError:
                    pass
        
        # Ensure numeric fields
        if 'successRate' in normalized_data:
            normalized_data['successRate'] = float(normalized_data['successRate'])
        if 'totalRuns' in normalized_data:
            normalized_data['totalRuns'] = int(normalized_data['totalRuns'])
        
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in normalized_data.items() if k in valid_fields}
        
        return cls(**filtered_data)