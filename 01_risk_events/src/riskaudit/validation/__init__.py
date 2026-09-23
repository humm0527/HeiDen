"""
文件作用：公开标准业务数据预检模块的稳定 Python 调用接口。
编辑记录：
【首次生成：2026-08-06，导出校验器、结果对象与报告写入函数。】
【第二次编辑：2026-08-06，导出版本化校验包模型、加载器与路径解析接口。】
"""

from .report import write_validation_reports
from .result import (
    FieldCheckResult,
    TableStatistics,
    ValidationCheckResult,
    ValidationIssue,
    ValidationResult,
)
from .quarterly import analyze_quarter_classification_coverage
from .rules import (
    EffectiveDateRule,
    FieldRule,
    SemanticCheckRule,
    TableRule,
    ValidationPack,
    load_validation_pack,
    resolve_validation_pack_path,
)
from .validator import DataValidator

__all__ = [
    "DataValidator",
    "EffectiveDateRule",
    "FieldCheckResult",
    "FieldRule",
    "SemanticCheckRule",
    "TableRule",
    "TableStatistics",
    "ValidationIssue",
    "ValidationCheckResult",
    "ValidationPack",
    "ValidationResult",
    "analyze_quarter_classification_coverage",
    "load_validation_pack",
    "resolve_validation_pack_path",
    "write_validation_reports",
]
