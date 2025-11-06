from typing import Sequence
from enum import IntEnum
from datetime import datetime

class DetectedDatatypeCategory(IntEnum):
    integers = 1
    floats = 2
    dates = 3
    timestamps = 4
    booleans = 5
    texts = 6
    mixed = 7


class DetectedDatatypeEqualsRuleParametersSpec:
    expected_datatype: str


class HistoricDataPoint:
    historical_time: datetime
    back_periods_index: int
    sensor_readout: float
    expected_value: float


class RuleTimeWindowSettingsSpec:
    prediction_time_window: int
    min_periods_with_readouts: int


# rule execution parameters, contains the sensor value (actual_value) and the rule parameters
class RuleExecutionRunParameters:
    actual_value: float
    parameters: DetectedDatatypeEqualsRuleParametersSpec
    current_time: datetime
    previous_readouts: Sequence[HistoricDataPoint]
    time_window: RuleTimeWindowSettingsSpec


class RuleExecutionResult:
    passed: bool
    expected_value: float
    lower_bound: float
    upper_bound: float

    def __init__(self, passed=None, expected_value=None, lower_bound=None, upper_bound=None):
        self.passed = passed
        self.expected_value = expected_value
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound


# rule evaluation method that should be modified for each type of rule
def evaluate_rule(rule_parameters: RuleExecutionRunParameters) -> RuleExecutionResult:
    if not hasattr(rule_parameters, 'actual_value') or not hasattr(rule_parameters.parameters, 'expected_datatype'):
        return RuleExecutionResult()

    expected_value = getattr(DetectedDatatypeCategory, rule_parameters.parameters.expected_datatype).value
    lower_bound = None
    upper_bound = None
    passed = (expected_value == rule_parameters.actual_value)

    return RuleExecutionResult(passed, expected_value, lower_bound, upper_bound)