"""
Domain Model: Ingest Job Entity
"""
from typing import Dict, List, Any
from dataclasses import dataclass, asdict
from domain.entity.job_client import Job, JobType, JobStatus

@dataclass
class IngestJobSource:
    """Ingest source configuration"""
    schema_name: str
    table_name: str
    primary_keys: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """Create from dictionary with key mapping"""
        return cls(**data)

@dataclass
class IngestJobTarget:
    """Ingest target configuration"""
    layer: str
    path: str
    format: str = "delta"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """Create from dictionary"""
        return cls(**data)


@dataclass

class IngestJob(Job):
    """Ingest Job Domain Entity"""
    source: IngestJobSource = None
    target: IngestJobTarget = None

    def __post_init__(self):
        """Ensure job type is always INGEST"""
        if self.job_type != JobType.INGEST:
            self.job_type = JobType.INGEST
    
    def to_config(self) -> Dict[str, Any]:
        """Convert to config dict for storage"""
        return {
            "source": self.source.to_dict() if self.source else {},
            "target": self.target.to_dict() if self.target else {}
        }
    
    @classmethod
    def from_config(
        cls, 
        job_name: str, 
        config: Dict[str, Any], 
        created_by: str = "system",
        **kwargs
    ):
        """Create new IngestJob from config dictionary"""
        source_data = config.get('source', {})
        target_data = config.get('target', {})

        return cls(
            job_name=job_name,
            job_type=JobType.INGEST,
            created_by=created_by,
            source=IngestJobSource.from_dict(source_data) if source_data else None,
            target=IngestJobTarget.from_dict(target_data) if target_data else None,
            **kwargs
        )

    def validate(self) -> bool:
        """Validate ingest job configuration"""
        errors = []
        
        if not self.source:
            errors.append("Source configuration is required")
        else:
            if not self.source.schema_name:
                errors.append("Source schema name is required")
            if not self.source.table_name:
                errors.append("Source table name is required")
            if not self.source.primary_keys or len(self.source.primary_keys) == 0:
                errors.append("At least one primary key is required")
        
        if not self.target:
            errors.append("Target configuration is required")
        else:
            if not self.target.path:
                errors.append("Target path is required")
            if not self.target.layer:
                errors.append("Target layer is required")
        
        if errors:
            raise ValueError(f"Validation failed: {'; '.join(errors)}")
        
        return True
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """Reconstruct Ingest Job from database"""        
        # Handle config field
        if 'config' in data:
            config = data.pop('config')
            if isinstance(config, str):
                import json
                config = json.loads(config)
            
            source_data = config.get('source', {})
            target_data = config.get('target', {})
            
            data['source'] = IngestJobSource.from_dict(source_data) if source_data else None
            data['target'] = IngestJobTarget.from_dict(target_data) if target_data else None
        
        # Convert enums
        if 'job_type' in data and isinstance(data['job_type'], str):
            data['job_type'] = JobType(data['job_type'])
        if 'status' in data and isinstance(data['status'], str):
            data['status'] = JobStatus(data['status'])
        
        return cls(**data)