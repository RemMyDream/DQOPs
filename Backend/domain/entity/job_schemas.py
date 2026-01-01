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
    job_id: int
    version_id: int
    connection_name: str
    config: Dict[str, Any]
    schedule_type: ScheduleType
    created_by: str
    is_active: bool = False
    schedule_cron: Optional[str] = None
    created_at: Optional[datetime] = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with proper serialization"""
        data = asdict(self)
        data['schedule_type'] = self.schedule_type.value
        
        if self.created_at:
            data['created_at'] = self.created_at.isoformat()
            
        return data
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any] | str):
        """Create JobVersion from dictionary or JSON string"""
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON string: {e}")
        
        if not isinstance(data, dict):
            raise ValueError(f"Expected dict or str, got {type(data)}")
        
        # Handle config field (might be JSON string from DB)
        if 'config' in data and isinstance(data['config'], str):
            try:
                data['config'] = json.loads(data['config'])
            except json.JSONDecodeError:
                pass
        
        # Convert schedule_type to enum if it's a string
        if 'schedule_type' in data and isinstance(data['schedule_type'], str):
            data['schedule_type'] = ScheduleType(data['schedule_type'])
        
        # Convert timestamps if they're strings
        if 'created_at' in data and isinstance(data['created_at'], str):
            try:
                data['created_at'] = datetime.fromisoformat(data['created_at'])
            except ValueError:
                pass
        
        # Filter only valid fields for dataclass
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in data.items() if k in valid_fields}
        
        return cls(**filtered_data)


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
   
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with proper serialization"""
        data = asdict(self)
        data['status'] = self.status.value
        data['run_at'] = self.run_at.isoformat()
        
        if self.end_at:
            data['end_at'] = self.end_at.isoformat()
            
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """Create JobHistory from dictionary"""
        # Convert status to enum
        if 'status' in data and isinstance(data['status'], str):
            data['status'] = RunStatus(data['status'])
        
        # Handle result field (might be JSON string)
        if 'result' in data and isinstance(data['result'], str):
            try:
                data['result'] = json.loads(data['result'])
            except json.JSONDecodeError:
                pass
        
        # Convert timestamps
        for time_field in ['run_at', 'end_at']:
            if time_field in data and isinstance(data[time_field], str):
                try:
                    data[time_field] = datetime.fromisoformat(data[time_field])
                except ValueError:
                    pass
        
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in data.items() if k in valid_fields}
        
        return cls(**filtered_data)


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
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with proper serialization"""
        data = asdict(self)
        data['created_at'] = self.created_at.isoformat()
        
        if self.last_run_at:
            data['last_run_at'] = self.last_run_at.isoformat()
            
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """Create JobSummary from dictionary"""
        # Convert timestamps
        for time_field in ['created_at', 'last_run_at']:
            if time_field in data and isinstance(data[time_field], str):
                try:
                    data[time_field] = datetime.fromisoformat(data[time_field])
                except ValueError:
                    pass
        
        # Ensure numeric fields
        if 'success_rate' in data:
            data['success_rate'] = float(data['success_rate'])
        if 'total_runs' in data:
            data['total_runs'] = int(data['total_runs'])
        
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in data.items() if k in valid_fields}
        
        return cls(**filtered_data)