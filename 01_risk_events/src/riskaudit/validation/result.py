"""
文件作用：定义数据预检异常、字段检查、数据统计和结构化 ValidationResult 返回对象。
编辑记录：
【首次生成：2026-08-06，建立 JSON 与 XLSX 报告共用的预检结果模型。】
【二次编辑内容：2026-08-06，在审计结果中记录验证规则包标识与版本。】
【三次编辑：2026-08-14，增加可审计的通过/失败检查项，区分“未检查”与“检查后无异常”。】
【四次编辑：2026-08-14，记录预检完成状态、异常处理策略和处理后计算输入。】
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class ValidationIssue:
    error_code: str
    severity: str
    table: str
    field: str
    affected_rows: int
    example_records: list[dict[str, Any]]
    suggested_action: str
    message: str
    handling_policy: str = "REQUIRES_SOURCE_FIX"
    handling_status: str = "PENDING"
    calculation_disposition: str = "BLOCKED"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FieldCheckResult:
    table: str
    field: str
    required: bool
    nullable: bool
    data_type: str
    status: str
    note: str = ""


@dataclass(frozen=True)
class TableStatistics:
    table: str
    row_count: int
    column_count: int
    min_date: str = ""
    max_date: str = ""
    market_count: int = 0
    security_count: int = 0


@dataclass(frozen=True)
class ValidationCheckResult:
    check_id: str
    label: str
    status: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    overall_status: str
    allow_run: bool
    errors: list[ValidationIssue] = field(default_factory=list)
    field_checks: list[FieldCheckResult] = field(default_factory=list)
    statistics: list[TableStatistics] = field(default_factory=list)
    observation_start: str = ""
    observation_end: str = ""
    validation_pack_id: str = ""
    validation_pack_version: str = ""
    checks: list[ValidationCheckResult] = field(default_factory=list)
    inspection_status: str = "NOT_RUN"
    inspection_message: str = "数据预检尚未执行"
    checked_at: str = ""
    remediations: list[dict[str, Any]] = field(default_factory=list)
    source_row_count: int = 0
    calculation_row_count: int = 0
    calculation_input_path: str = ""
    calculation_input_sha256: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "overall_status": self.overall_status,
            "allow_run": self.allow_run,
            "errors": [item.to_dict() for item in self.errors],
            "field_checks": [asdict(item) for item in self.field_checks],
            "statistics": [asdict(item) for item in self.statistics],
            "observation_start": self.observation_start,
            "observation_end": self.observation_end,
            "validation_pack_id": self.validation_pack_id,
            "validation_pack_version": self.validation_pack_version,
            "checks": [asdict(item) for item in self.checks],
            "inspection_status": self.inspection_status,
            "inspection_message": self.inspection_message,
            "checked_at": self.checked_at,
            "remediations": self.remediations,
            "source_row_count": self.source_row_count,
            "calculation_row_count": self.calculation_row_count,
            "calculation_input_path": self.calculation_input_path,
            "calculation_input_sha256": self.calculation_input_sha256,
        }
