from typing import Optional, Dict, List, Any
from pydantic import BaseModel, Field, field_validator

class DBConfig(BaseModel):
    """Database connection configuration"""
    connection_name: str
    host: str
    port: str
    username: str
    password: str
    database: str
    jdbc_properties: Dict[str, str] = Field(default_factory=dict)
    saved_at: Optional[str] = None 

    @field_validator('connection_name')
    def validate_connection_name(cls, v):
        if not v or not v.strip():
            raise ValueError('Connection name cannot be empty')
        return v.strip()

class DBCredential(BaseModel):
    """Database credentials for queries"""
    connection_name: str
    schema_name: Optional[str] = None
    table_name: Optional[str] = None