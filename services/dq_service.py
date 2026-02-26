"""
DQ Service: High-level API for Data Quality checks
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
import yaml
import logging

from services.template_engine import TemplateEngine, get_template_engine
from services.rule_engine import RuleEngine, get_rule_engine
from services.check_executor import CheckExecutor, CheckResult, CheckStatus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DQService:
    """
    High-level Data Quality Service
    
    Usage:
        dq = DQService(spark_session=spark)
        
        # Run check from config
        result = dq.run_check(config)
        
        # Preview SQL
        sql = dq.preview_sql(sensor_path, context)
    """
    
    def __init__(self, spark_session=None):
        self.executor = CheckExecutor(spark_session=spark_session)
        self.template_engine = get_template_engine()
        self.rule_engine = get_rule_engine()
    
    def run_check(self, config: Dict[str, Any]) -> CheckResult:
        """
        Run a single check from config
        
        Config format:
        {
            'check_name': 'null_check_orders_customer_id',
            'sensor': {
                'path': 'column/null/null_percent/spark.sql.jinja2',
                'context': {
                    'target_table': {'schema_name': 'sales', 'table_name': 'orders'},
                    'table': {'filter': None, 'columns': {...}},
                    'column_name': 'customer_id',
                    ...
                }
            },
            'rule': {
                'path': 'comparison/max_percent.py',
                'parameters': {'max_percent': 5}
            }
        }
        """
        return self.executor.run_check_from_config(config)
    
    def run_checks(self, configs: List[Dict[str, Any]]) -> List[CheckResult]:
        """Run multiple checks"""
        results = []
        for config in configs:
            result = self.run_check(config)
            results.append(result)
        return results
    
    def run_from_yaml(self, yaml_path: str) -> CheckResult:
        """Run check from YAML file"""
        with open(yaml_path, 'r') as f:
            config = yaml.safe_load(f)
        return self.run_check(config)
    
    def preview_sql(self, sensor_path: str, context: Dict[str, Any]) -> str:
        """Preview SQL without executing"""
        return self.executor.preview_sql(sensor_path, context)
    
    def list_sensors(self) -> List[str]:
        """List available sensor templates"""
        return self.template_engine.list_templates()
    
    def list_rules(self) -> List[str]:
        """List available rule files"""
        return self.rule_engine.list_rules()
    
    # ==================== QUICK CHECK BUILDERS ====================
    
    def quick_null_check(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        max_null_percent: float = 0,
        sensor_path: str = "column/null/null_percent/spark.sql.jinja2",
        rule_path: str = "comparison/max_percent.py"
    ) -> CheckResult:
        """Quick null percentage check"""
        config = {
            'check_name': f"null_check_{table_name}_{column_name}",
            'sensor': {
                'path': sensor_path,
                'context': {
                    'target_table': {'schema_name': schema_name, 'table_name': table_name},
                    'table': {
                        'filter': None,
                        'columns': {
                            column_name: {'type_snapshot': {'column_type': 'VARCHAR'}, 'sql_expression': None}
                        }
                    },
                    'column_name': column_name,
                    'error_sampling': {'samples_limit': 10, 'total_samples_limit': 1000, 'id_columns': []},
                    'parameters': {},
                    'additional_filters': []
                }
            },
            'rule': {
                'path': rule_path,
                'parameters': {'max_percent': max_null_percent}
            }
        }
        return self.run_check(config)
    
    def quick_value_in_set_check(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        expected_values: List[Any],
        min_percent: float = 100,
        sensor_path: str = "column/accepted_values/text_found_in_set_percent/spark.sql.jinja2",
        rule_path: str = "comparison/min_percent.py"
    ) -> CheckResult:
        """Quick value-in-set check"""
        config = {
            'check_name': f"value_in_set_{table_name}_{column_name}",
            'sensor': {
                'path': sensor_path,
                'context': {
                    'target_table': {'schema_name': schema_name, 'table_name': table_name},
                    'table': {
                        'filter': None,
                        'columns': {
                            column_name: {'type_snapshot': {'column_type': 'VARCHAR'}, 'sql_expression': None}
                        }
                    },
                    'column_name': column_name,
                    'error_sampling': {'samples_limit': 10, 'total_samples_limit': 1000, 'id_columns': []},
                    'parameters': {'expected_values': expected_values},
                    'additional_filters': []
                }
            },
            'rule': {
                'path': rule_path,
                'parameters': {'min_percent': min_percent}
            }
        }
        return self.run_check(config)


# Factory
def create_dq_service(spark_session=None) -> DQService:
    return DQService(spark_session=spark_session)