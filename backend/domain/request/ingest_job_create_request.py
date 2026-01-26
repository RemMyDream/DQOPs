"""
IngestJob Creation Requests
"""
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator
from domain.entity.job_client import JobType
from domain.entity.job_schemas import ScheduleType
from domain.request.job_request import JobCreateRequest

class TableInfo(BaseModel):
    """Single table configuration"""
    schema_name: str = Field(..., min_length=1)
    table_name: str = Field(..., min_length=1)
    primary_keys: List[str] = Field(default=[])
    
    @field_validator('schema_name', 'table_name')
    def validate_names(cls, v):
        if not v or not v.strip():
            raise ValueError('Name cannot be empty')
        
        dangerous_chars = [';', '--', '/*', '*/', 'xp_', 'sp_']
        if any(char in v.lower() for char in dangerous_chars):
            raise ValueError('Invalid characters in name')
        
        return v.strip()


class IngestJobCreateRequest(BaseModel):
    job_name: Optional[str] = Field(None, max_length=200)
    connection_name: str = Field(..., min_length=1)
    tables: List[TableInfo] = Field(..., min_length=1)
    target_layer: str = Field(default="bronze", pattern="^(bronze|silver|gold)$")
    schedule_type: ScheduleType = Field(default=ScheduleType.ON_DEMAND)
    schedule_cron: Optional[str] = None
    created_by: str = Field(default="admin")
    
    @field_validator('tables')
    def validate_tables(cls, v):
        if not v or len(v) == 0:
            raise ValueError('At least one table is required')
        return v
    
    def to_generic(self) -> JobCreateRequest:
        """Convert to generic JobCreateRequest (for database)"""
        config = {
            "tables": [t.model_dump() for t in self.tables],
            "target_layer": self.target_layer,
        }
        
        job_name = self.job_name or self._generate_job_name()
        
        return JobCreateRequest(
            job_name=job_name,
            job_type=JobType.INGEST,
            connection_name=self.connection_name,
            config=config,
            schedule_type=self.schedule_type,
            schedule_cron=self.schedule_cron,
            created_by=self.created_by
        )
    
    def to_dag_conf(self) -> Dict[str, Any]:
        """Convert to DAG conf dict"""
        return {
            "connection_name": self.connection_name,
            "tables": [t.model_dump() for t in self.tables],
            "layer": self.target_layer,
        }
    
    def _generate_job_name(self) -> str:
        """Auto-generate job name from tables"""
        if len(self.tables) == 1:
            t = self.tables[0]
            return f"ingest_{t.schema_name}_{t.table_name}"
        return f"ingest_batch_{len(self.tables)}_tables"