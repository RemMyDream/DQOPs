from datetime import datetime
from typing import Sequence

class BetweenFloatRuleParametersSpec:
    from_: float
    to: float

    def __getatt__(self, name):
        if name == "from":
            return self.from_ if hasattr(self, 'from_') else None
        return object.__getattribute__(self, name)
    
class HistoricDataPoint:
    historical_time: datetime
    back_periods_index: int
    sensor_readout: float
    expected_value: float

class RuleTimeWindowSettingsSpec:
    prediction_time_window: int         # Số kỳ (ngày, tuần, tháng…) trong quá khứ để lấy dữ liệu lịch sử so sánh.
    min_periods_with_readouts: int      # Số kỳ tối thiểu phải có dữ liệu thì rule mới chạy được.

# rule execution param  eters, contains the sensor value (actual_value) and the rule parameters
class RuleExecutionRunParameters:
    actual_value: float
    parameters: BetweenFloatRuleParametersSpec
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
    if not hasattr(rule_parameters, 'actual_value') or not hasattr(rule_parameters.parameters, 'min_percent'):
        return RuleExecutionResult()

    expected_value = None
    lower_bound = getattr(rule_parameters.parameters, "from") if hasattr(rule_parameters, "from") else None
    upper_bound = rule_parameters.parameters.to if hasattr(rule_parameters,"to") else None
    passed = (lower_bound if lower_bound is not None else rule_parameters.actual_value) <= rule_parameters.actual_value <= (upper_bound if upper_bound is not None else rule_parameters.actual_value) 

    return RuleExecutionResult(passed, expected_value, lower_bound, upper_bound)   