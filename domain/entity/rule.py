from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any, Sequence
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class HistoricDataPoint:
    """Historical sensor reading"""
    historical_time: datetime
    back_periods_index: int
    sensor_readout: float
    expected_value: Optional[float] = None


@dataclass 
class RuleTimeWindowSettingsSpec:
    """Time window settings for rules that need historical data"""
    prediction_time_window: int = 7
    min_periods_with_readouts: int = 1


class RuleParametersSpec:
    """
    Dynamic parameters wrapper
    """
    def __init__(self, params: Dict[str, Any]):
        for key, value in params.items():
            setattr(self, key, value)


class RuleExecutionRunParameters:
    """
    Parameters passed to evaluate_rule()
    """
    actual_value: float
    parameters: RuleParametersSpec
    current_time: datetime
    previous_readouts: Sequence[HistoricDataPoint]
    time_window: RuleTimeWindowSettingsSpec

    def __init__(
        self,
        actual_value: float,
        parameters: Dict[str, Any],
        current_time: datetime = None,
        previous_readouts: List[HistoricDataPoint] = None,
        time_window: Dict[str, Any] = None
    ):
        self.actual_value = actual_value
        self.parameters = RuleParametersSpec(parameters)
        self.current_time = current_time or datetime.now()
        self.previous_readouts = previous_readouts or []
        
        if time_window:
            self.time_window = RuleTimeWindowSettingsSpec(
                prediction_time_window=time_window.get('prediction_time_window', 7),
                min_periods_with_readouts=time_window.get('min_periods_with_readouts', 1)
            )
        else:
            self.time_window = RuleTimeWindowSettingsSpec()

class RuleExecutionResult:
    """Result of rule evaluation"""
    passed: bool
    expected_value: float
    lower_bound: float
    upper_bound: float

    def __init__(self, passed=None, expected_value=None, lower_bound=None, upper_bound=None):
        self.passed = passed
        self.expected_value = expected_value
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'passed': self.passed,
            'expected_value': self.expected_value,
            'lower_bound': self.lower_bound,
            'upper_bound': self.upper_bound
        }