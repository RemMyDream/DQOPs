"""
Rule Engine: Load and execute rule files dynamically
Each rule file has: evaluate_rule(rule_parameters: RuleExecutionRunParameters) -> RuleExecutionResult
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any
from domain.entity.rule import HistoricDataPoint, RuleExecutionResult, RuleExecutionRunParameters
from pathlib import Path
import importlib.util
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RuleEngine:
    """
    Load and execute rule files dynamically
    
    Rule files must have:
        evaluate_rule(rule_parameters: RuleExecutionRunParameters) -> RuleExecutionResult
    """
    
    def __init__(self):
        current_dir = Path(__file__).parent.parent
        rules_dir = current_dir / "templates"/ "rules"
        
        self.rules_dir = Path(rules_dir)
        self._cache: Dict[str, Any] = {}
        
        logger.info(f"Rule engine initialized: {self.rules_dir}")
    
    def _load_module(self, rule_path: str):
        """Load rule module from file"""
        if rule_path in self._cache:
            return self._cache[rule_path]
        
        full_path = self.rules_dir / rule_path
        if not full_path.exists():
            raise FileNotFoundError(f"Rule file not found: {full_path}")
        
        spec = importlib.util.spec_from_file_location(
            f"rule_{rule_path.split('/')[1].replace('/', '_')}",
            full_path
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        if not hasattr(module, 'evaluate_rule'):
            raise AttributeError(f"Rule file must have evaluate_rule function: {rule_path}")
        
        self._cache[rule_path] = module
        return module
    
    def evaluate(
        self,
        rule_path: str,
        rule_parameters: RuleExecutionRunParameters
    ) -> RuleExecutionResult:
        """
        Evaluate a rule
        
        Args:
            rule_path: Path to rule file (e.g., 'comparison/max_percent.py')
            actual_value: Sensor result
            rule_parameters: Rule Parameters
        """
        if rule_parameters.actual_value is None:
            return RuleExecutionResult(passed=False)
        
        try:
            module = self._load_module(rule_path)
            return module.evaluate_rule(rule_parameters)
            
        except Exception as e:
            logger.error(f"Rule evaluation error: {rule_path} - {e}")
            return RuleExecutionResult(passed=False)
    
    def list_rules(self) -> List[str]:
        """List all available rule files"""
        if not self.rules_dir.exists():
            return []
        
        rules = []
        for path in self.rules_dir.rglob("*.py"):
            if not path.name.startswith('_'):
                rules.append(str(path.relative_to(self.rules_dir)))
        
        return sorted(rules)


# Singleton
_engine: Optional[RuleEngine] = None

def get_rule_engine() -> RuleEngine:
    global _engine
    if _engine is None:
        _engine = RuleEngine()
    return _engine

