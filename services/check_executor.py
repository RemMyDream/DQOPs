"""
Check Executor: Orchestrates the DQ check pipeline
1. Render sensor SQL from Jinja template
2. Execute SQL on Spark/DB
3. Evaluate rule against sensor result
4. Return CheckResult
"""
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, List, Optional, Union
from enum import Enum
import logging

from services.template_engine import TemplateEngine, get_template_engine
from services.rule_engine import RuleEngine, get_rule_engine, HistoricDataPoint, RuleExecutionResult

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ==============================================================================
# RESULT CLASSES
# ==============================================================================

class CheckStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"


@dataclass
class SensorResult:
    """Result of sensor SQL execution"""
    actual_value: Optional[float]
    executed_sql: Optional[str] = None
    execution_time_ms: Optional[int] = None
    error_message: Optional[str] = None
    row_count: Optional[int] = None
    additional_results: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'actual_value': self.actual_value,
            'executed_sql': self.executed_sql,
            'execution_time_ms': self.execution_time_ms,
            'error_message': self.error_message,
            'row_count': self.row_count,
            'additional_results': self.additional_results
        }


@dataclass
class CheckResult:
    """Complete result of a check execution"""
    check_name: str
    status: CheckStatus
    sensor_result: SensorResult
    rule_result: Optional[RuleExecutionResult]
    executed_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'check_name': self.check_name,
            'status': self.status.value,
            'sensor_result': self.sensor_result.to_dict(),
            'rule_result': self.rule_result.to_dict() if self.rule_result else None,
            'executed_at': self.executed_at.isoformat(),
            'passed': self.status == CheckStatus.PASSED
        }


# ==============================================================================
# CHECK EXECUTOR
# ==============================================================================

class CheckExecutor:
    """
    Executes DQ checks:
    1. Render sensor SQL (Jinja template)
    2. Execute SQL (Spark/PostgreSQL)
    3. Evaluate rule (Python rule file)
    """
    
    def __init__(
        self,
        template_engine: TemplateEngine = None,
        rule_engine: RuleEngine = None,
        spark_session = None
    ):
        self.template_engine = template_engine or get_template_engine()
        self.rule_engine = rule_engine or get_rule_engine()
        self.spark = spark_session
        
        # In-memory history store (replace with DB in production)
        self._history: Dict[str, List[HistoricDataPoint]] = {}
    
    def render_sql(self, sensor_path: str, context: Dict[str, Any]) -> str:
        """Render sensor SQL from template"""
        return self.template_engine.render(sensor_path, context)
    
    def execute_sql(self, sql: str) -> SensorResult:
        """Execute SQL and return result"""
        start_time = time.time()
        
        try:
            if self.spark:
                return self._execute_spark(sql, start_time)
            else:
                # Dry run mode
                return SensorResult(
                    actual_value=None,
                    executed_sql=sql,
                    execution_time_ms=0,
                    error_message="No Spark session - dry run mode"
                )
        except Exception as e:
            return SensorResult(
                actual_value=None,
                executed_sql=sql,
                execution_time_ms=int((time.time() - start_time) * 1000),
                error_message=str(e)
            )
    
    def _execute_spark(self, sql: str, start_time: float) -> SensorResult:
        """Execute SQL on Spark"""
        df = self.spark.sql(sql)
        rows = df.collect()
        
        if not rows:
            return SensorResult(
                actual_value=None,
                executed_sql=sql,
                execution_time_ms=int((time.time() - start_time) * 1000),
                error_message="No rows returned"
            )
        
        row = rows[0]
        actual_value = None
        additional = {}
        
        for col in df.columns:
            val = row[col]
            if col == 'actual_value':
                actual_value = float(val) if val is not None else None
            else:
                additional[col] = val
        
        # Fallback: first column as actual_value
        if actual_value is None and df.columns:
            actual_value = float(row[0]) if row[0] is not None else None
        
        return SensorResult(
            actual_value=actual_value,
            executed_sql=sql,
            execution_time_ms=int((time.time() - start_time) * 1000),
            row_count=len(rows),
            additional_results=additional
        )
    
    def evaluate_rule(
        self,
        rule_path: str,
        actual_value: float,
        parameters: Dict[str, Any],
        time_window: Dict[str, Any] = None,
        history_key: str = None
    ) -> RuleExecutionResult:
        """Evaluate rule against actual value"""
        previous_readouts = []
        if history_key and history_key in self._history:
            previous_readouts = self._history[history_key]
        
        return self.rule_engine.evaluate(
            rule_path=rule_path,
            actual_value=actual_value,
            parameters=parameters,
            previous_readouts=previous_readouts,
            time_window=time_window
        )
    
    def store_history(self, key: str, value: float):
        """Store sensor readout for historical comparison"""
        if key not in self._history:
            self._history[key] = []
        
        point = HistoricDataPoint(
            historical_time=datetime.now(),
            back_periods_index=0,
            sensor_readout=value
        )
        
        self._history[key].insert(0, point)
        
        # Update indices
        for i, p in enumerate(self._history[key]):
            p.back_periods_index = i
        
        # Keep last 100
        self._history[key] = self._history[key][:100]
    
    def run_check(
        self,
        check_name: str,
        sensor_path: str,
        sensor_context: Dict[str, Any],
        rule_path: str,
        rule_parameters: Dict[str, Any],
        rule_time_window: Dict[str, Any] = None,
        store_history: bool = True
    ) -> CheckResult:
        """
        Run a complete DQ check
        
        Args:
            check_name: Unique check identifier
            sensor_path: Path to sensor template
            sensor_context: Context for rendering sensor SQL
            rule_path: Path to rule file
            rule_parameters: Parameters for rule
            rule_time_window: Time window settings for rule
            store_history: Whether to store result in history
        """
        logger.info(f"Running check: {check_name}")
        
        try:
            # 1. Render SQL
            sql = self.render_sql(sensor_path, sensor_context)
            
            # 2. Execute SQL
            sensor_result = self.execute_sql(sql)
            
            if sensor_result.error_message and sensor_result.actual_value is None:
                return CheckResult(
                    check_name=check_name,
                    status=CheckStatus.ERROR,
                    sensor_result=sensor_result,
                    rule_result=None
                )
            
            # 3. Evaluate rule
            history_key = f"{check_name}"
            rule_result = self.evaluate_rule(
                rule_path=rule_path,
                actual_value=sensor_result.actual_value,
                parameters=rule_parameters,
                time_window=rule_time_window,
                history_key=history_key
            )
            
            # 4. Determine status
            status = CheckStatus.PASSED if rule_result.passed else CheckStatus.FAILED
            
            # 5. Store history
            if store_history and sensor_result.actual_value is not None:
                self.store_history(history_key, sensor_result.actual_value)
            
            return CheckResult(
                check_name=check_name,
                status=status,
                sensor_result=sensor_result,
                rule_result=rule_result
            )
            
        except Exception as e:
            logger.error(f"Check error: {check_name} - {e}")
            return CheckResult(
                check_name=check_name,
                status=CheckStatus.ERROR,
                sensor_result=SensorResult(actual_value=None, error_message=str(e)),
                rule_result=None
            )
    
    def run_check_from_config(self, config: Dict[str, Any]) -> CheckResult:
        """
        Run check from config dict
        
        Config format:
        {
            'check_name': 'my_check',
            'sensor': {
                'path': 'column/null/null_percent/spark.sql.jinja2',
                'context': { ... }
            },
            'rule': {
                'path': 'comparison/max_percent.py',
                'parameters': { 'max_percent': 5 },
                'time_window': { ... }  # optional
            }
        }
        """
        return self.run_check(
            check_name=config['check_name'],
            sensor_path=config['sensor']['path'],
            sensor_context=config['sensor']['context'],
            rule_path=config['rule']['path'],
            rule_parameters=config['rule'].get('parameters', {}),
            rule_time_window=config['rule'].get('time_window')
        )
    
    def preview_sql(self, sensor_path: str, context: Dict[str, Any]) -> str:
        """Preview SQL without executing"""
        return self.render_sql(sensor_path, context)


# Factory
def create_check_executor(spark_session=None) -> CheckExecutor:
    return CheckExecutor(spark_session=spark_session)