"""
文件作用：导出风险事件专用门禁、approved 规则加载和确定性事件计算公共接口。
编辑记录：
【首次生成：2026-08-11，建立 risk_events 包的稳定公共 API。】
【第二次编辑：2026-08-11，导出不可覆盖的风险事件结果写入接口。】
【第三次编辑：2026-08-11，导出真实宽表证券并集与持久化市场分块装配接口。】
"""

from .engine import calculate_risk_events
from .gate import validate_risk_inputs
from .identifiers import normalize_business_security_code
from .input_assembly import (
    BusinessUniverseSummary,
    build_risk_input_tables,
    build_pit_security_status_rows,
    find_missing_pit_board_requests,
    load_business_risk_universe,
    load_standard_code_tables,
    merge_standard_code_runs,
)
from .models import (
    ContinuousLimitDownSegment,
    RiskEvent,
    RiskEventCalculationResult,
    RiskGateFinding,
    RiskInputGateResult,
    RiskInputTables,
    StBaseline,
)
from .pressure import (
    LimitDownPressureObservation,
    LimitDownPressureResult,
    LimitDownPressureRules,
    calculate_limit_down_pressure_observations,
    load_limit_down_pressure_rules,
    write_limit_down_pressure_result,
)
from .rules import RiskEventRules, load_approved_rules
from .storage import write_risk_event_result

__all__ = [
    "ContinuousLimitDownSegment",
    "BusinessUniverseSummary",
    "RiskEvent",
    "RiskEventCalculationResult",
    "RiskEventRules",
    "RiskGateFinding",
    "RiskInputGateResult",
    "RiskInputTables",
    "LimitDownPressureObservation",
    "LimitDownPressureResult",
    "LimitDownPressureRules",
    "StBaseline",
    "calculate_risk_events",
    "calculate_limit_down_pressure_observations",
    "build_risk_input_tables",
    "build_pit_security_status_rows",
    "find_missing_pit_board_requests",
    "load_approved_rules",
    "load_limit_down_pressure_rules",
    "write_limit_down_pressure_result",
    "load_business_risk_universe",
    "load_standard_code_tables",
    "merge_standard_code_runs",
    "normalize_business_security_code",
    "validate_risk_inputs",
    "write_risk_event_result",
]
