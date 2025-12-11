"""
Domain Model: Ingest Job Entity
"""
from typing import Dict, List, Any
from dataclasses import dataclass, asdict
from domain.entity.job_client import Job, JobType, JobStatus


@dataclass
class IngestJobSource:
    """Ingest source configuration"""
    schemaName: str
    tableName: str
    primaryKeys: List[str]
    
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
        if self.jobType != JobType.INGEST:
            self.jobType = JobType.INGEST
    
    def to_config(self) -> Dict[str, Any]:
        """Convert to config dict for storage"""
        return {
            "source": self.source.to_dict() if self.source else {},
            "target": self.target.to_dict() if self.target else {}
        }
    
    @classmethod
    def from_config(
        cls, 
        jobName: str, 
        config: Dict[str, Any], 
        createdBy: str = "system",
        **kwargs
    ):
        """Create new IngestJob from config dictionary"""
        source_data = config.get('source', {})
        target_data = config.get('target', {})

        return cls(
            jobName=jobName,
            jobType=JobType.INGEST,
            createdBy=createdBy,
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
            if not self.source.schemaName:
                errors.append("Source schema name is required")
            if not self.source.tableName:
                errors.append("Source table name is required")
            if not self.source.primaryKeys or len(self.source.primaryKeys) == 0:
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
        key_mapping = {
            'job_id': 'jobId',
            'job_name': 'jobName',
            'job_type': 'jobType',
            'created_by': 'createdBy',
            'created_at': 'createdAt',
            'updated_at': 'updatedAt'
        }
        
        normalized_data = {}
        for key, value in data.items():
            normalized_key = key_mapping.get(key, key)
            normalized_data[normalized_key] = value
        
        # Handle config field
        if 'config' in normalized_data:
            config = normalized_data.pop('config')
            if isinstance(config, str):
                import json
                config = json.loads(config)
            
            source_data = config.get('source', {})
            target_data = config.get('target', {})
            
            normalized_data['source'] = IngestJobSource.from_dict(source_data) if source_data else None
            normalized_data['target'] = IngestJobTarget.from_dict(target_data) if target_data else None
        
        # Convert enums
        if 'jobType' in normalized_data and isinstance(normalized_data['jobType'], str):
            normalized_data['jobType'] = JobType(normalized_data['jobType'])
        if 'status' in normalized_data and isinstance(normalized_data['status'], str):
            normalized_data['status'] = JobStatus(normalized_data['status'])
        
        return cls(**normalized_data)