"""
文件作用：定义风险事件输入、专用门禁结果、连续跌停段和统一风险事件的确定性数据对象。
编辑记录：
【首次生成：2026-08-11，建立五表风险输入、ST 基线、门禁发现项、连续段和事件结果模型。】
【第二次编辑：2026-08-11，为空结果固定 CSV 字段顺序，确保审计文件仍保留表头。】
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

import pandas as pd


SecurityKey = tuple[str, str]
SEGMENT_FIELDS = [
    "market_code",
    "security_code",
    "segment_number",
    "event_type",
    "event_source",
    "board_code",
    "applicable_threshold",
    "consecutive_length",
    "first_fact_date",
    "threshold_reached_date",
    "last_fact_date",
    "threshold_reached",
    "open_at_observation_end",
    "rule_version",
    "calculation_batch_id",
    "upstream_snapshot_id",
    "source_record_ids",
]
EVENT_FIELDS = [
    "market_code",
    "security_code",
    "event_type",
    "event_source",
    "first_fact_date",
    "risk_date",
    "last_fact_date",
    "threshold_reached_date",
    "segment_number",
    "consecutive_length",
    "previous_st_state",
    "current_st_state",
    "rule_version",
    "calculation_batch_id",
    "upstream_snapshot_id",
    "source_record_ids",
]


@dataclass(frozen=True)
class RiskInputTables:
    trading_calendar: pd.DataFrame
    security_status_daily: pd.DataFrame
    stock_market_daily: pd.DataFrame
    security_risk_status: pd.DataFrame
    broker_risk_classification: pd.DataFrame


@dataclass(frozen=True)
class RiskGateFinding:
    code: str
    severity: str
    message: str
    market_code: str = ""
    security_code: str = ""
    fact_date: str = ""

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class StBaseline:
    market_code: str
    security_code: str
    baseline_date: date
    is_st: bool
    source_record_id: str
    selection_reason: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["baseline_date"] = self.baseline_date.isoformat()
        return payload


@dataclass
class RiskInputGateResult:
    allow_run: bool
    raw_business_universe: tuple[SecurityKey, ...] = ()
    eligible_universe: tuple[SecurityKey, ...] = ()
    excluded_securities: dict[SecurityKey, str] = field(default_factory=dict)
    st_baselines: dict[SecurityKey, StBaseline] = field(default_factory=dict)
    findings: list[RiskGateFinding] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "allow_run": self.allow_run,
            "raw_business_universe": [list(item) for item in self.raw_business_universe],
            "eligible_universe": [list(item) for item in self.eligible_universe],
            "excluded_securities": [
                {
                    "market_code": key[0],
                    "security_code": key[1],
                    "reason": reason,
                }
                for key, reason in sorted(self.excluded_securities.items())
            ],
            "st_baselines": [
                baseline.to_dict()
                for _, baseline in sorted(self.st_baselines.items())
            ],
            "findings": [item.to_dict() for item in self.findings],
        }


@dataclass(frozen=True)
class ContinuousLimitDownSegment:
    market_code: str
    security_code: str
    segment_number: int
    event_type: str
    event_source: str
    board_code: str
    applicable_threshold: int
    consecutive_length: int
    first_fact_date: date
    threshold_reached_date: date | None
    last_fact_date: date
    threshold_reached: bool
    open_at_observation_end: bool
    rule_version: str
    calculation_batch_id: str
    upstream_snapshot_id: str
    source_record_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for name in ("first_fact_date", "threshold_reached_date", "last_fact_date"):
            value = payload[name]
            payload[name] = value.isoformat() if value else None
        payload["source_record_ids"] = list(self.source_record_ids)
        return payload


@dataclass(frozen=True)
class RiskEvent:
    market_code: str
    security_code: str
    event_type: str
    event_source: str
    first_fact_date: date
    risk_date: date
    last_fact_date: date
    threshold_reached_date: date | None
    segment_number: int | None
    consecutive_length: int | None
    previous_st_state: str | None
    current_st_state: str | None
    rule_version: str
    calculation_batch_id: str
    upstream_snapshot_id: str
    source_record_ids: tuple[str, ...]

    @property
    def unique_key(self) -> tuple[str, str, str, date]:
        return (
            self.market_code,
            self.security_code,
            self.event_type,
            self.first_fact_date,
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for name in ("first_fact_date", "risk_date", "last_fact_date", "threshold_reached_date"):
            value = payload[name]
            payload[name] = value.isoformat() if value else None
        payload["source_record_ids"] = list(self.source_record_ids)
        return payload


@dataclass(frozen=True)
class RiskEventCalculationResult:
    segments: tuple[ContinuousLimitDownSegment, ...]
    events: tuple[RiskEvent, ...]
    rule_version: str
    rule_sha256: str
    calculation_batch_id: str
    upstream_snapshot_id: str
    observation_start: date
    observation_end: date

    def segment_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            [item.to_dict() for item in self.segments], columns=SEGMENT_FIELDS
        )

    def event_frame(self) -> pd.DataFrame:
        return pd.DataFrame([item.to_dict() for item in self.events], columns=EVENT_FIELDS)

    def manifest(self) -> dict[str, Any]:
        return {
            "status": "SUCCEEDED",
            "rule_version": self.rule_version,
            "rule_sha256": self.rule_sha256,
            "calculation_batch_id": self.calculation_batch_id,
            "upstream_snapshot_id": self.upstream_snapshot_id,
            "observation_start": self.observation_start.isoformat(),
            "observation_end": self.observation_end.isoformat(),
            "segment_count": len(self.segments),
            "event_count": len(self.events),
            "distinct_risk_security_count": len(
                {(item.market_code, item.security_code) for item in self.events}
            ),
        }
