from dataclasses import field
from typing import Optional, Dict, List, Any
from pydantic import BaseModel, field_validator


class DBConfig(BaseModel):
    """Database connection configuration"""
    connectionName: str
    host: str
    port: str
    username: str
    password: str
    database: str
    jdbcProperties: Dict[str, str] = field(default_factory=dict)
    savedAt: Optional[str] = None 

    @field_validator('connectionName')
    def validate_connection_name(cls, v):
        if not v or not v.strip():
            raise ValueError('Connection name cannot be empty')
        return v.strip()
    
    def to_dict(self):
        return self.model_dump()

class DBCredential(BaseModel):
    """Database credentials for queries"""
    connectionName: str
    schemaName: Optional[str] = None
    tableName: Optional[str] = None

class TableSubmission(BaseModel):
    """Table submission for profilling, monitoring"""
    connectionName: str
    tables: List[Dict[str, str]]
