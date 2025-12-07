"""
Domain Model: Ingest Job Entity
"""
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, asdict
from .job_client import Job, JobType

@dataclass
class IngestJobSource:
    """Ingest source configuration"""
    connectionName: str
    schemaName: str
    tableName: str
    primaryKeys: List[str]
    
    def to_dict(self):
        return asdict(self)
    
@dataclass
class IngestJobTarget:
    """Ingest target configuration"""
    layer: str
    path: str
    format: str = "delta"

    def to_dict(self):
        return asdict(self)
    
@dataclass
class IngestJob(Job):
    """ Ingest Job """
    source: IngestJobSource = None
    target: IngestJobTarget = None

    def __post_init__(self):
        if self.jobType != JobType.INGEST:
            self.jobType = JobType.INGEST
    
    def to_config(self):
        response = {
            "source": self.source.to_dict() if self.source else {},
            "target": self.target.to_dict() if self.target else {}
        }
        return response
    
    @classmethod
    def from_config(cls, jobId: int, jobName: str, config: Dict[str: Any], 
                    createdBy: str = "system", **kwargs):
        """ Create IngestJob from config """ 
        source_data = config.get('source', {})
        target_data = config.get('target', {})

        return cls(
            jobId = jobId,
            jobName = jobName,
            jobType = JobType.INGEST,
            createdBy = createdBy,
            source = IngestJobSource(**source_data) if source_data else None,
            target = IngestJobTarget(**target_data) if target_data else None,
            **kwargs
        )         

    def validate(self) -> bool:
        """Validate ingest job configuration"""
        if not self.source.connectionName:
            raise ValueError("connection_name is required")
        if not self.source or not self.source.schemaName or not self.source.tableName:
            raise ValueError("source schema and table are required")
        if not self.target or not self.target.path:
            raise ValueError("target path is required")
        return True