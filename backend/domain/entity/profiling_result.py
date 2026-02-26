from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum


class DQDimension(str, Enum):
    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    VOLUME = "volume"


@dataclass
class ProfilingResult:
    """Single metric result from a profiling run"""
    profile_run_id: str
    schema_name: str
    table_name: str
    column_name: Optional[str]       # None for table-level metrics (row_count)
    dimension: str
    metric_name: str                  # e.g. "null_record_percent"
    actual_value: Optional[float] = None
    executed_sql: Optional[str] = None
    execution_time_ms: Optional[int] = None
    error_message: Optional[str] = None
    column_type: Optional[str] = None
    result_id: Optional[int] = None
    created_at: Optional[datetime] = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        if self.created_at:
            data['created_at'] = self.created_at.isoformat()
        return data
