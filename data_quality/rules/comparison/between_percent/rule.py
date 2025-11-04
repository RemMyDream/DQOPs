from datetime import datetime
from typing import Sequence

class BetweenPercentRuleParametersSpec:
    min_percent: float
    default_min_percent = 100
    max_percent: float


class HistoricDataPoint:
    historical_time: datetime
    back_periods_index: int
    sensor_readout: float
    expected_value: float


class RuleTimeWindowSettingsSpec:
    prediction_time_window: int
    max_periods_with_readouts: int


class RuleExecutionRunParameters:
    actual_value: float
    parameters: BetweenPercentRuleParametersSpec
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


def evaluate_rule(rule_parameters: RuleExecutionRunParameters) -> RuleExecutionResult:
    if not hasattr(rule_parameters, 'actual_value'):
        return RuleExecutionResult()

    expected_value = None
    lower_bound = rule_parameters.parameters.min_percent if hasattr(rule_parameters.parameters, 'min_percent') else None
    upper_bound = rule_parameters.parameters.max_percent if hasattr(rule_parameters.parameters, 'max_percent') else None
    passed = (lower_bound if lower_bound is not None else
                 rule_parameters.parameters.default_min_percent) <= rule_parameters.actual_value <= (
                 upper_bound if upper_bound is not None else rule_parameters.parameters.default_min_percent)

    return RuleExecutionResult(passed, expected_value, lower_bound, upper_bound)