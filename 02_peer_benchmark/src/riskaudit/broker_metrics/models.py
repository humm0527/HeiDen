"""
文件作用：定义券商预警指标的风险事件、分类历史、PIT 评价、汇总与审计数据对象。
编辑记录：
【首次生成：2026-08-12，建立 approved v4 券商指标纯函数所需的稳定对象与中文输出字段。】
【二次编辑内容：2026-08-12，为正式批次明细补充市场日历快照标识。】
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from decimal import Decimal
from typing import Any


EventKey = tuple[str, str, str, date]
SecurityKey = tuple[str, str]


@dataclass(frozen=True)
class BrokerMetricFinding:
    code: str
    severity: str
    message: str
    broker_id: str = ""
    market_code: str = ""
    security_code: str = ""
    fact_date: str = ""
    source_file: str = ""
    source_row_number: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RiskEventRecord:
    market_code: str
    security_code: str
    event_type: str
    first_fact_date: date
    risk_date: date | None = None
    event_rule_version: str = "v2"
    event_batch_id: str = ""

    @property
    def event_key(self) -> EventKey:
        return (
            self.market_code,
            self.security_code,
            self.event_type,
            self.first_fact_date,
        )


@dataclass(frozen=True)
class BrokerClassificationRecord:
    broker_id: str
    market_code: str
    security_code: str
    classification_date: date
    first_usable_trading_date: date | None
    raw_classification: str
    source_file: str = ""
    source_file_sha256: str = ""
    source_row_number: int | None = None
    source_record_id: str = ""


@dataclass(frozen=True)
class FinalClassification:
    final_bucket: str | None
    resolution: str
    mapping_record_id: str = ""
    mapped_bucket: str | None = None


@dataclass(frozen=True)
class EventBrokerAssessment:
    event: RiskEventRecord
    broker_id: str
    cutoff_trading_date: date
    selected_record: BrokerClassificationRecord | None
    cutoff_st_state: str
    final_bucket: str | None
    classification_resolution: str
    mapping_version: str
    mapping_record_id: str
    identification_status: str
    is_hit: bool | None
    warning_days_status: str
    dangerous_run_start_date: date | None
    warning_trading_days: int | None
    reason_code: str
    metric_rule_version: str
    metric_batch_id: str = ""
    calendar_snapshot_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        selected = self.selected_record
        return {
            "event_key": "|".join(
                [
                    self.event.market_code,
                    self.event.security_code,
                    self.event.event_type,
                    self.event.first_fact_date.isoformat(),
                ]
            ),
            "market_code": self.event.market_code,
            "security_code": self.event.security_code,
            "event_type": self.event.event_type,
            "first_fact_date": self.event.first_fact_date.isoformat(),
            "risk_date": self.event.risk_date.isoformat() if self.event.risk_date else "",
            "event_rule_version": self.event.event_rule_version,
            "event_batch_id": self.event.event_batch_id,
            "broker_id": self.broker_id,
            "cutoff_trading_date": self.cutoff_trading_date.isoformat(),
            "cutoff_at": f"{self.cutoff_trading_date.isoformat()} CLOSE",
            "selected_classification_date": (
                selected.classification_date.isoformat() if selected else ""
            ),
            "classification_first_usable_date": (
                selected.first_usable_trading_date.isoformat()
                if selected and selected.first_usable_trading_date
                else ""
            ),
            "raw_classification": selected.raw_classification if selected else "",
            "mapping_version": self.mapping_version,
            "mapping_record_id": self.mapping_record_id,
            "cutoff_st_state": self.cutoff_st_state,
            "final_bucket": self.final_bucket or "",
            "classification_resolution": self.classification_resolution,
            "identification_status": self.identification_status,
            "is_hit": self.is_hit,
            "warning_days_status": self.warning_days_status,
            "dangerous_run_start_date": (
                self.dangerous_run_start_date.isoformat()
                if self.dangerous_run_start_date
                else ""
            ),
            "warning_trading_days": self.warning_trading_days,
            "reason_code": self.reason_code,
            "source_quarter_file": selected.source_file if selected else "",
            "source_file_sha256": selected.source_file_sha256 if selected else "",
            "source_row_number": selected.source_row_number if selected else None,
            "source_record_id": selected.source_record_id if selected else "",
            "calendar_snapshot_id": self.calendar_snapshot_id,
            "metric_rule_version": self.metric_rule_version,
            "metric_batch_id": self.metric_batch_id,
        }


@dataclass(frozen=True)
class DangerousExposure:
    broker_id: str
    snapshot_date: date
    market_code: str
    security_code: str


@dataclass(frozen=True)
class ExposureBuildResult:
    exposures: tuple[DangerousExposure, ...]
    unknown_counts: dict[tuple[str, date], int]


@dataclass(frozen=True)
class BrokerMetricRules:
    rule_version: str
    rule_sha256: str
    mapping_version: str
    csv_schema: str
    weekend_action: str
    weekend_breaks_continuity: bool
    event_type_views: tuple[str, ...]
    decimal_scale: int


@dataclass(frozen=True)
class BrokerMappingBook:
    mapping_version: str
    mappings: dict[str, dict[str, str]]
    unmapped_broker_fields: frozenset[str]

    @property
    def broker_ids(self) -> tuple[str, ...]:
        return tuple(sorted(set(self.mappings) | set(self.unmapped_broker_fields)))

    def map_value(self, broker_id: str, raw_value: str) -> tuple[str | None, str]:
        if raw_value == "":
            return None, ""
        mapped = self.mappings.get(broker_id, {}).get(raw_value)
        record_id = (
            f"{self.mapping_version}:{broker_id}:{raw_value}"
            if mapped is not None
            else ""
        )
        return mapped, record_id


def decimal_to_text(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None
