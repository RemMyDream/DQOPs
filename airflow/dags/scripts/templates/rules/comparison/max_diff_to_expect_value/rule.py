from datetime import datetime
from typing import Sequence


class DiffRuleParametersSpec:
    error_margin: float


class HistoricDataPoint:
    historical_time: datetime
    back_periods_index: int
    sensor_readout: float
    expected_value: float


class RuleTimeWindowSettingsSpec:
    prediction_time_window: int
    max_periods_with_readouts: int


class RuleExecutionRunParameters:
    expected_value: float
    actual_value: float
    parameters: DiffRuleParametersSpec
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
    has_expected_value = hasattr(rule_parameters, 'expected_value')
    has_actual_value = hasattr(rule_parameters, 'actual_value')
    if not has_expected_value and not has_actual_value:
        return RuleExecutionResult()

    if not has_expected_value:
        return RuleExecutionResult(False, None, None, None)

    expected_value = rule_parameters.expected_value
    if has_actual_value:
        abs_diff = abs(rule_parameters.actual_value - rule_parameters.expected_value)
        passed = (abs_diff <= rule_parameters.parameters.error_margin)
    else:
        passed = False

    return RuleExecutionResult(passed, expected_value, None, None)