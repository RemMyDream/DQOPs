"""
Domain Models: DQ Check Configuration
"""
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from enum import Enum
import yaml
import json


# ==================== ENUMS ====================

class TimeSeriesMode(str, Enum):
    """Time series mode"""
    CURRENT_TIME = "current_time"
    TIMESTAMP_COLUMN = "timestamp_column"


class TimeGradient(str, Enum):
    """Time gradient for grouping"""
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class DataGroupingSource(str, Enum):
    """Source type for data grouping"""
    COLUMN_VALUE = "column_value"
    TAG = "tag"


class CheckSeverity(str, Enum):
    """Severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class CheckStatus(str, Enum):
    """Execution status"""
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"


# ==================== CONTEXT MODELS ====================

@dataclass
class TargetTable:
    """Target table specification"""
    schema_name: str
    table_name: str
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TargetTable':
        return cls(
            schema_name=data['schema_name'],
            table_name=data['table_name']
        )
    
    def full_name(self) -> str:
        return f"{self.schema_name}.{self.table_name}"


@dataclass
class ColumnTypeSnapshot:
    """Column type information"""
    column_type: str  # INT, VARCHAR, DECIMAL, DATE, TIMESTAMP, etc.
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ColumnDefinition:
    """Column definition in table metadata"""
    type_snapshot: ColumnTypeSnapshot
    sql_expression: Optional[str] = None  # For computed columns
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'type_snapshot': self.type_snapshot.to_dict(),
            'sql_expression': self.sql_expression
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ColumnDefinition':
        type_snapshot = ColumnTypeSnapshot(
            column_type=data.get('type_snapshot', {}).get('column_type', 'VARCHAR')
        )
        return cls(
            type_snapshot=type_snapshot,
            sql_expression=data.get('sql_expression')
        )


@dataclass
class TableMetadata:
    """Table metadata with columns"""
    columns: Dict[str, ColumnDefinition]
    filter: Optional[str] = None  # Default filter for table
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'columns': {k: v.to_dict() for k, v in self.columns.items()},
            'filter': self.filter
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TableMetadata':
        columns = {}
        for col_name, col_data in data.get('columns', {}).items():
            columns[col_name] = ColumnDefinition.from_dict(col_data)
        
        return cls(
            columns=columns,
            filter=data.get('filter')
        )
    
    def get(self, key: str, default=None):
        if key == 'columns':
            return self.columns
        elif key == 'filter':
            return self.filter
        return default


@dataclass
class ErrorSampling:
    """Error sampling configuration (Required)"""
    samples_limit: int = 10  # Max samples per group
    total_samples_limit: int = 1000  # Total max samples
    id_columns: List[str] = field(default_factory=list)  # ID columns for tracing
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ErrorSampling':
        return cls(
            samples_limit=data.get('samples_limit', 10),
            total_samples_limit=data.get('total_samples_limit', 1000),
            id_columns=data.get('id_columns', [])
        )


@dataclass
class DataGroupingLevel:
    """Single data grouping level"""
    source: DataGroupingSource  # 'column_value' or 'tag'
    column: Optional[str] = None
    tag: Optional[str] = None

    def __post_init__(self):
        if self.source == DataGroupingSource.COLUMN_VALUE:
            if not self.column:
                raise ValueError(
                    "DataGroupingLevel: 'column' is required when source='column_value'"
                )
            if self.tag is not None:
                raise ValueError(
                    "DataGroupingLevel: 'tag' must be None when source='column_value'"
                )

        elif self.source == DataGroupingSource.TAG:
            if not self.tag:
                raise ValueError(
                    "DataGroupingLevel: 'tag' is required when source='tag'"
                )
            if self.column is not None:
                raise ValueError(
                    "DataGroupingLevel: 'column' must be None when source='tag'"
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source.value,
            "column": self.column,
            "tag": self.tag
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DataGroupingLevel":
        if "source" not in data:
            raise ValueError("DataGroupingLevel: 'source' is required")

        source = data["source"]
        if isinstance(source, str):
            try:
                source = DataGroupingSource(source)
            except ValueError:
                raise ValueError(
                    f"Invalid DataGroupingSource: {source}"
                )

        return cls(
            source=source,
            column=data.get("column"),
            tag=data.get("tag")
        )


@dataclass
class TimeSeries:
    """Time series configuration (Optional)"""
    mode: TimeSeriesMode  # 'current_time' or 'timestamp_column'
    timestamp_column: Optional[str] = None  # Required when mode='timestamp_column'
    time_gradient: TimeGradient = TimeGradient.DAY
    
    def __post_init__(self):
        if self.mode == TimeSeriesMode.TIMESTAMP_COLUMN:
            if not self.timestamp_column:
                raise ValueError(
                    "TimeSeries: 'timestamp_column' is required when mode='timestamp_column'"
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'mode': self.mode.value if isinstance(self.mode, TimeSeriesMode) else self.mode,
            'timestamp_column': self.timestamp_column,
            'time_gradient': self.time_gradient.value if isinstance(self.time_gradient, TimeGradient) else self.time_gradient
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TimeSeries':
        mode = data.get('mode', 'timestamp_column')
        if isinstance(mode, str):
            mode = TimeSeriesMode(mode)
        
        time_gradient = data.get('time_gradient', 'day')
        if isinstance(time_gradient, str):
            time_gradient = TimeGradient(time_gradient)
        
        return cls(
            mode=mode,
            timestamp_column=data.get('timestamp_column'),
            time_gradient=time_gradient
        )
    
    def get(self, key: str, default=None):
        """Dict-like get for template compatibility"""
        return getattr(self, key, default)


@dataclass
class TimeWindowFilter:
    """Time window filter configuration (Optional)"""
    # Fixed range filters
    from_date: Optional[str] = None
    to_date: Optional[str] = None
    from_date_time: Optional[str] = None
    to_date_time: Optional[str] = None
    from_date_time_offset: Optional[str] = None
    to_date_time_offset: Optional[str] = None
    
    # Dynamic range filters
    daily_partitioning_recent_days: Optional[int] = None
    monthly_partitioning_recent_months: Optional[int] = None
    
    # Flags
    daily_partitioning_include_today: bool = True
    monthly_partitioning_include_current_month: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TimeWindowFilter':
        return cls(**{k: v for k, v in data.items() if hasattr(cls, k)})
    
    def get(self, key: str, default=None):
        """Dict-like get for template compatibility"""
        return getattr(self, key, default)


@dataclass
class SensorParameters:
    """Parameters passed to sensor template"""
    filter: Optional[str] = None
    foreign_table: Optional[str] = None
    foreign_column: Optional[str] = None
    # Sensor-specific parameters
    expected_values: Optional[List[Any]] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    pattern: Optional[str] = None
    date_format: Optional[str] = None
    referenced_table: Optional[str] = None
    referenced_column: Optional[str] = None
    # Custom parameters
    custom: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        result = {}
        for key, value in asdict(self).items():
            if value is not None and key != 'custom':
                result[key] = value
        if self.custom:
            result.update(self.custom)
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SensorParameters':
        known_fields = {f.name for f in cls.__dataclass_fields__.values() if f.name != 'custom'}
        known_params = {k: v for k, v in data.items() if k in known_fields}
        custom_params = {k: v for k, v in data.items() if k not in known_fields}
        
        return cls(**known_params, custom=custom_params)
    
    def get(self, key: str, default=None):
        """Dict-like get for template compatibility"""
        if hasattr(self, key):
            return getattr(self, key, default)
        return self.custom.get(key, default)


# ==================== RULE MODELS ====================

@dataclass
class TimeWindow:
    """Time window for historical comparison in rules"""
    prediction_time_window: int = 7  # Number of periods to look back
    min_periods_with_readouts: int = 1  # Minimum periods with data required
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TimeWindow':
        return cls(**{k: v for k, v in data.items() if hasattr(cls, k)})


@dataclass
class RuleConfig:
    """Rule configuration for threshold evaluation"""
    type: str  # Rule type name
    parameters: Dict[str, Any] = field(default_factory=dict)
    time_window: Optional[TimeWindow] = None
    custom_rule_path: Optional[str] = None  # For custom Python rules
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': self.type,
            'parameters': self.parameters,
            'time_window': self.time_window.to_dict() if self.time_window else None,
            'custom_rule_path': self.custom_rule_path
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RuleConfig':
        time_window = None
        if 'time_window' in data and data['time_window']:
            time_window = TimeWindow.from_dict(data['time_window'])
        
        return cls(
            type=data['type'],
            parameters=data.get('parameters', {}),
            time_window=time_window,
            custom_rule_path=data.get('custom_rule_path')
        )


# ==================== SENSOR CONFIG ====================

@dataclass
class SensorConfig:
    """Sensor (Jinja template) configuration"""
    type: str  # Template name (e.g., "null_count", "value_in_set_percent")
    category: str  # Category folder (e.g., "completeness", "validity")
    parameters: SensorParameters = field(default_factory=SensorParameters)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': self.type,
            'category': self.category,
            'parameters': self.parameters.to_dict()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SensorConfig':
        params = data.get('parameters', {})
        if isinstance(params, dict):
            params = SensorParameters.from_dict(params)
        
        return cls(
            type=data['type'],
            category=data['category'],
            parameters=params
        )
    
    def get_template_path(self) -> str:
        """Get the Jinja template path for this sensor"""
        return f"checks/{self.category}/{self.type}.sql.jinja2"


# ==================== CHECK DEFINITION ====================

@dataclass
class CheckDefinition:
    """Complete check definition - matches template context"""
    # Required fields
    check_name: str
    target_table: TargetTable
    table: TableMetadata
    column_name: str
    error_sampling: ErrorSampling
    sensor: SensorConfig
    rule: RuleConfig
    
    # Optional fields
    description: Optional[str] = None
    data_groupings: Optional[Dict[str, DataGroupingLevel]] = None
    time_series: Optional[TimeSeries] = None
    time_window_filter: Optional[TimeWindowFilter] = None
    additional_filters: List[str] = field(default_factory=list)
    
    # Metadata
    severity: CheckSeverity = CheckSeverity.WARNING
    enabled: bool = True
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'check_name': self.check_name,
            'description': self.description,
            'target_table': self.target_table.to_dict(),
            'table': self.table.to_dict(),
            'column_name': self.column_name,
            'error_sampling': self.error_sampling.to_dict(),
            'sensor': self.sensor.to_dict(),
            'rule': self.rule.to_dict(),
            'data_groupings': {k: v.to_dict() for k, v in self.data_groupings.items()} if self.data_groupings else None,
            'time_series': self.time_series.to_dict() if self.time_series else None,
            'time_window_filter': self.time_window_filter.to_dict() if self.time_window_filter else None,
            'additional_filters': self.additional_filters,
            'severity': self.severity.value,
            'enabled': self.enabled,
            'tags': self.tags,
            'metadata': self.metadata
        }
    
    def to_yaml(self) -> str:
        return yaml.dump(self.to_dict(), default_flow_style=False, sort_keys=False, allow_unicode=True)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CheckDefinition':
        # Parse nested objects
        target_table = TargetTable.from_dict(data['target_table'])
        table = TableMetadata.from_dict(data['table'])
        error_sampling = ErrorSampling.from_dict(data.get('error_sampling', {}))
        sensor = SensorConfig.from_dict(data['sensor'])
        rule = RuleConfig.from_dict(data['rule'])
        
        # Optional nested objects
        data_groupings = None
        if data.get('data_groupings'):
            data_groupings = {
                k: DataGroupingLevel.from_dict(v) 
                for k, v in data['data_groupings'].items()
            }
        
        time_series = None
        if data.get('time_series'):
            time_series = TimeSeries.from_dict(data['time_series'])
        
        time_window_filter = None
        if data.get('time_window_filter'):
            time_window_filter = TimeWindowFilter.from_dict(data['time_window_filter'])
        
        severity = data.get('severity', 'warning')
        if isinstance(severity, str):
            severity = CheckSeverity(severity)
        
        return cls(
            check_name=data['check_name'],
            description=data.get('description'),
            target_table=target_table,
            table=table,
            column_name=data['column_name'],
            error_sampling=error_sampling,
            sensor=sensor,
            rule=rule,
            data_groupings=data_groupings,
            time_series=time_series,
            time_window_filter=time_window_filter,
            additional_filters=data.get('additional_filters', []),
            severity=severity,
            enabled=data.get('enabled', True),
            tags=data.get('tags', []),
            metadata=data.get('metadata', {})
        )
    
    @classmethod
    def from_yaml(cls, yaml_str: str) -> 'CheckDefinition':
        data = yaml.safe_load(yaml_str)
        return cls.from_dict(data)
    
    @classmethod
    def from_yaml_file(cls, file_path: str) -> 'CheckDefinition':
        with open(file_path, 'r', encoding='utf-8') as f:
            return cls.from_yaml(f.read())
    
    def build_template_context(self) -> Dict[str, Any]:
        """Build context dict for Jinja template rendering"""
        context = {
            'target_table': self.target_table.to_dict(),
            'table': self.table,  # Keep as object for .get() compatibility
            'column_name': self.column_name,
            'error_sampling': self.error_sampling.to_dict(),
            'parameters': self.sensor.parameters,  # Keep as object for .get()
            'additional_filters': self.additional_filters,
        }
        
        # Optional context
        if self.data_groupings:
            context['data_groupings'] = {
                k: v.to_dict() for k, v in self.data_groupings.items()
            }
        
        if self.time_series:
            context['time_series'] = self.time_series  # Keep as object
        
        if self.time_window_filter:
            context['time_window_filter'] = self.time_window_filter  # Keep as object
        
        return context


# ==================== CHECK SUITE ====================
@dataclass
class CheckSuiteDefinition:
    """Suite of multiple checks"""
    suite_name: str
    description: Optional[str]
    connection_name: str
    checks: List[CheckDefinition] = field(default_factory=list)
    default_schema: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'suite_name': self.suite_name,
            'description': self.description,
            'connection_name': self.connection_name,
            'default_schema': self.default_schema,
            'checks': [c.to_dict() for c in self.checks],
            'tags': self.tags,
            'metadata': self.metadata
        }
    
    def to_yaml(self) -> str:
        return yaml.dump(self.to_dict(), default_flow_style=False, sort_keys=False, allow_unicode=True)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CheckSuiteDefinition':
        checks = [CheckDefinition.from_dict(c) for c in data.get('checks', [])]
        
        return cls(
            suite_name=data['suite_name'],
            description=data.get('description'),
            connection_name=data['connection_name'],
            default_schema=data.get('default_schema'),
            checks=checks,
            tags=data.get('tags', []),
            metadata=data.get('metadata', {})
        )
    
    @classmethod
    def from_yaml(cls, yaml_str: str) -> 'CheckSuiteDefinition':
        data = yaml.safe_load(yaml_str)
        return cls.from_dict(data)
    
    @classmethod
    def from_yaml_file(cls, file_path: str) -> 'CheckSuiteDefinition':
        with open(file_path, 'r', encoding='utf-8') as f:
            return cls.from_yaml(f.read())


# ==================== RESULT MODELS ====================

@dataclass
class HistoricDataPoint:
    """Historical sensor reading for rule evaluation"""
    timestamp: datetime
    back_periods_index: int
    sensor_readout: float
    expected_value: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp': self.timestamp.isoformat(),
            'back_periods_index': self.back_periods_index,
            'sensor_readout': self.sensor_readout,
            'expected_value': self.expected_value
        }


@dataclass
class RuleExecutionResult:
    """Result of rule evaluation"""
    passed: bool
    expected_value: Optional[float] = None
    lower_bound: Optional[float] = None
    upper_bound: Optional[float] = None
    message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class SensorResult:
    """Result of sensor (SQL) execution"""
    actual_value: Optional[float]
    total_count: Optional[int] = None
    additional_columns: Dict[str, Any] = field(default_factory=dict)
    executed_sql: Optional[str] = None
    execution_time_ms: Optional[int] = None
    error_message: Optional[str] = None
    # Error samples
    error_samples: Optional[List[Dict[str, Any]]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CheckResult:
    """Complete result of a check execution"""
    check_definition: CheckDefinition
    sensor_result: SensorResult
    rule_result: Optional[RuleExecutionResult]
    status: CheckStatus
    executed_at: datetime = field(default_factory=datetime.now)
    result_id: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'check_name': self.check_definition.check_name,
            'check_definition': self.check_definition.to_dict(),
            'sensor_result': self.sensor_result.to_dict(),
            'rule_result': self.rule_result.to_dict() if self.rule_result else None,
            'status': self.status.value,
            'executed_at': self.executed_at.isoformat(),
            'result_id': self.result_id
        }
    
    @property
    def passed(self) -> bool:
        return self.status == CheckStatus.PASSED


@dataclass
class CheckSuiteResult:
    """Result of a check suite execution"""
    suite_name: str
    connection_name: str
    results: List[CheckResult]
    executed_at: datetime = field(default_factory=datetime.now)
    execution_time_ms: Optional[int] = None
    suite_result_id: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'suite_name': self.suite_name,
            'connection_name': self.connection_name,
            'results': [r.to_dict() for r in self.results],
            'executed_at': self.executed_at.isoformat(),
            'execution_time_ms': self.execution_time_ms,
            'suite_result_id': self.suite_result_id,
            'summary': self.get_summary()
        }
    
    def get_summary(self) -> Dict[str, Any]:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.status == CheckStatus.PASSED)
        failed = sum(1 for r in self.results if r.status == CheckStatus.FAILED)
        error = sum(1 for r in self.results if r.status == CheckStatus.ERROR)
        skipped = sum(1 for r in self.results if r.status == CheckStatus.SKIPPED)
        
        return {
            'total_checks': total,
            'passed': passed,
            'failed': failed,
            'error': error,
            'skipped': skipped,
            'pass_rate': (passed / total * 100) if total > 0 else 0,
            'overall_status': 'PASSED' if failed == 0 and error == 0 else 'FAILED'
        }