from datetime import datetime
from typing import Sequence


class BetweenChangeRuleParametersSpec:
    from_: float
    to: float

    def __getattr__(self, name):
        if name == "from":
            return self.from_
        return object.__getattribute__(self, name)


class HistoricDataPoint:
    historical_time: datetime
    back_periods_index: int
    sensor_readout: float
    expected_value: float


class RuleTimeWindowSettingsSpec:
    prediction_time_window: int
    min_periods_with_readouts: int


class RuleExecutionRunParameters:
    actual_value: float
    parameters: BetweenChangeRuleParametersSpec
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
    if not hasattr(rule_parameters, 'actual_value'):
        return RuleExecutionResult()

    n = rule_parameters.time_window.prediction_time_window
    previous_readout = (
        rule_parameters.previous_readouts[n - 1].sensor_readout
        if len(rule_parameters.previous_readouts) >= n
        and hasattr(rule_parameters.previous_readouts[n - 1], 'sensor_readout')
        and rule_parameters.previous_readouts[n - 1].sensor_readout is not None
        else None
    )    
    
    if previous_readout is None:
        return RuleExecutionResult()

    lower_bound = getattr(rule_parameters.parameters, 'from')
    upper_bound = rule_parameters.parameters.to

    abs_diff = abs(rule_parameters.actual_value - previous_readout)
    passed = (lower_bound if lower_bound is not None else abs_diff) <= abs_diff <= (upper_bound if upper_bound is not None else abs_diff)
    expected_value = None
    
    return RuleExecutionResult(passed, expected_value, lower_bound, upper_bound)