"""
Profiling Job Creation Requests
"""
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator


class ProfilingTableInfo(BaseModel):
    """Single table to profile"""
    schema_name: str = Field(..., min_length=1)
    table_name: str = Field(..., min_length=1)
    columns: Optional[List[str]] = None  # None = profile all columns

    @field_validator('schema_name', 'table_name')
    def validate_names(cls, v):
        if not v or not v.strip():
            raise ValueError('Name cannot be empty')
        dangerous_chars = [';', '--', '/*', '*/', 'xp_', 'sp_']
        if any(char in v.lower() for char in dangerous_chars):
            raise ValueError('Invalid characters in name')
        return v.strip()


class ProfilingJobCreateRequest(BaseModel):
    connection_name: str = Field(..., min_length=1)
    tables: List[ProfilingTableInfo] = Field(..., min_length=1)
    dimensions: List[str] = Field(
        default=["completeness", "uniqueness", "validity", "volume"]
    )
    created_by: str = Field(default="admin")

    @field_validator('tables')
    def validate_tables(cls, v):
        if not v or len(v) == 0:
            raise ValueError('At least one table is required')
        return v

    @field_validator('dimensions')
    def validate_dimensions(cls, v):
        valid = {"completeness", "uniqueness", "validity", "volume"}
        for d in v:
            if d not in valid:
                raise ValueError(f"Invalid dimension: {d}. Must be one of {valid}")
        return v

    def to_dag_conf(self) -> Dict[str, Any]:
        """Convert to DAG conf dict for Airflow trigger"""
        return {
            "connection_name": self.connection_name,
            "tables": [t.model_dump() for t in self.tables],
            "dimensions": self.dimensions,
        }
