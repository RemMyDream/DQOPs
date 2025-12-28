"""
Domain Model: Ingest Job Entity
"""
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict, field
from domain.entity.job_client import Job, JobType, JobStatus


@dataclass
class IngestTableInfo:
    """Single table configuration"""
    schema_name: str
    table_name: str
    primary_keys: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        return cls(**data)


@dataclass
class IngestJob(Job):
    """Ingest Job Domain Entity - supports batch tables"""
    tables: List[IngestTableInfo] = field(default_factory=list)
    target_layer: str = "bronze"

    def __post_init__(self):
        """Ensure job type is always INGEST"""
        if self.job_type != JobType.INGEST:
            self.job_type = JobType.INGEST
    
    def to_config(self) -> Dict[str, Any]:
        """Convert to config dict for storage"""
        return {
            "tables": [t.to_dict() for t in self.tables],
            "target_layer": self.target_layer
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
        tables_data = config.get('tables', [])
        target_layer = config.get('target_layer', 'bronze')
        
        tables = [IngestTableInfo.from_dict(t) for t in tables_data]

        return cls(
            job_name=job_name,
            job_type=JobType.INGEST,
            created_by=created_by,
            tables=tables,
            target_layer=target_layer,
            **kwargs
        )

    def validate(self) -> bool:
        """Validate ingest job configuration"""
        errors = []
        
        if not self.tables or len(self.tables) == 0:
            errors.append("At least one table is required")
        else:
            for i, table in enumerate(self.tables):
                if not table.schema_name:
                    errors.append(f"Table {i+1}: schema_name is required")
                if not table.table_name:
                    errors.append(f"Table {i+1}: table_name is required")
        
        if not self.target_layer:
            errors.append("Target layer is required")
        elif self.target_layer not in ['bronze', 'silver', 'gold']:
            errors.append("Target layer must be bronze, silver, or gold")
        
        if errors:
            raise ValueError(f"Validation failed: {'; '.join(errors)}")
        
        return True
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """Reconstruct Ingest Job from database"""        
        if 'config' in data:
            config = data.pop('config')
            if isinstance(config, str):
                import json
                config = json.loads(config)
            
            tables_data = config.get('tables', [])
            data['tables'] = [IngestTableInfo.from_dict(t) for t in tables_data]
            data['target_layer'] = config.get('target_layer', 'bronze')
        
        if 'job_type' in data and isinstance(data['job_type'], str):
            data['job_type'] = JobType(data['job_type'])
        if 'status' in data and isinstance(data['status'], str):
            data['status'] = JobStatus(data['status'])
        
        return cls(**data)