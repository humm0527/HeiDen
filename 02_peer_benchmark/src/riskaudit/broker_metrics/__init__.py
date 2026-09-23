"""
文件作用：公开券商提前识别、预警天数和三类汇总指标的确定性离线接口。
编辑记录：
【首次生成：2026-08-12，汇出 approved v4 规则、历史、PIT、指标和存储接口。】
【二次编辑内容：2026-08-12，汇出参数化真实本地批次运行接口。】
"""

from .calendar import MarketCalendar
from .classification import resolve_final_classification
from .event_input import load_risk_events
from .exposures import build_dangerous_exposures
from .history import (
    HistoryBuildResult,
    QuarterHistoryValidation,
    build_history_records,
    discover_quarter_files,
    normalize_security,
    validate_quarter_history_frames,
)
from .metrics import (
    final_broker_metric_summary,
    single_grade_event_artifacts,
    risk_hit_rates,
    warning_days_distribution,
    warning_rates,
)
from .models import (
    BrokerClassificationRecord,
    BrokerMappingBook,
    BrokerMetricFinding,
    BrokerMetricRules,
    DangerousExposure,
    ExposureBuildResult,
    EventBrokerAssessment,
    FinalClassification,
    RiskEventRecord,
)
from .pit import assess_all_events, assess_event_broker
from .rules import load_broker_mapping, load_broker_metric_rules
from .real_run import RealBrokerMetricRunResult, run_real_broker_metrics
from .pressure_classification import (
    PressureClassificationRunResult,
    run_pressure_classification,
)
from .storage import write_broker_metric_result
from .user_output import write_broker_user_output_bundle
from .warning_days import calculate_warning_trading_days

__all__ = [
    "BrokerClassificationRecord",
    "BrokerMappingBook",
    "BrokerMetricFinding",
    "BrokerMetricRules",
    "DangerousExposure",
    "ExposureBuildResult",
    "EventBrokerAssessment",
    "FinalClassification",
    "HistoryBuildResult",
    "QuarterHistoryValidation",
    "MarketCalendar",
    "RiskEventRecord",
    "RealBrokerMetricRunResult",
    "PressureClassificationRunResult",
    "assess_all_events",
    "assess_event_broker",
    "build_history_records",
    "build_dangerous_exposures",
    "calculate_warning_trading_days",
    "discover_quarter_files",
    "final_broker_metric_summary",
    "single_grade_event_artifacts",
    "load_broker_mapping",
    "load_broker_metric_rules",
    "load_risk_events",
    "normalize_security",
    "resolve_final_classification",
    "risk_hit_rates",
    "run_real_broker_metrics",
    "run_pressure_classification",
    "warning_days_distribution",
    "warning_rates",
    "write_broker_user_output_bundle",
    "write_broker_metric_result",
    "validate_quarter_history_frames",
]
