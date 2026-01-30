from datetime import datetime
from typing import Sequence
import scipy
import numpy as np


# rule specific parameters object, contains values received from the quality check threshold configuration
class BetweenPercentMovingAverageNDaysRuleParametersSpec:
    max_percent_above: float
    max_percent_below: float


class HistoricDataPoint:
    historical_time: datetime
    back_periods_index: int
    sensor_readout: float
    expected_value: float


class RuleTimeWindowSettingsSpec:
    prediction_time_window: int # The number of period (day/week/year)
    min_periods_with_readouts: int


# rule execution parameters, contains the sensor value (actual_value) and the rule parameters
class RuleExecutionRunParameters:
    actual_value: float
    parameters: BetweenPercentMovingAverageNDaysRuleParametersSpec
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
    if (not hasattr(rule_parameters, 'actual_value') or not hasattr(rule_parameters.parameters, 'max_percent_above') or
            not hasattr(rule_parameters.parameters, 'max_percent_below')):
        return RuleExecutionResult()

    filtered = [
        r.sensor_readout
        for r in rule_parameters.previous_readouts[:rule_parameters.time_window.prediction_time_window]
        if r is not None and hasattr(r, 'sensor_readout') and r.sensor_readout is not None
    ]

    if len(filtered) == 0:
        return RuleExecutionResult()

    filtered_mean = float(np.mean(filtered))

    # max_percent_below <= actual - mean_window <= max_percent_above  
    upper_bound = filtered_mean * (1.0 + rule_parameters.parameters.max_percent_above / 100.0)
    lower_bound = filtered_mean * (1.0 - rule_parameters.parameters.max_percent_below / 100.0)

    passed = lower_bound <= rule_parameters.actual_value <= upper_bound
    expected_value = filtered_mean

    return RuleExecutionResult(passed, expected_value, lower_bound, upper_bound)