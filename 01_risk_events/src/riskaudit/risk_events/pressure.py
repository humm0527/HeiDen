"""
文件作用：在不改写正式风险事件的前提下，复用连续跌停状态机生成低门槛压力观察事件。
编辑记录：
【首次生成：2026-08-18，建立独立 pressure_v1 规则、观察事件模型及同段升级关系。】
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import date
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Iterable

import pandas as pd
import yaml

from .engine import calculate_risk_events
from .models import RiskEvent, RiskInputGateResult, RiskInputTables
from .rules import RiskEventRules


PRESSURE_OBSERVATION_FIELDS = [
    "market_code",
    "security_code",
    "event_type",
    "board_code",
    "observation_threshold",
    "formal_threshold",
    "first_fact_date",
    "observation_date",
    "last_fact_date",
    "consecutive_length",
    "upgraded_to_formal_risk",
    "formal_risk_date",
    "open_at_observation_end",
    "pressure_rule_version",
    "formal_rule_version",
    "calculation_batch_id",
    "upstream_snapshot_id",
    "source_record_ids",
]
PRESSURE_OUTPUT_FIELDS = {
    "market_code": "交易市场代码",
    "security_code": "证券代码",
    "event_type": "观察事件类型",
    "board_code": "板块代码",
    "observation_threshold": "低门槛观察阈值",
    "formal_threshold": "正式风险阈值",
    "first_fact_date": "首次跌停日期",
    "observation_date": "达到观察门槛日期",
    "last_fact_date": "最后跌停日期",
    "consecutive_length": "连续跌停交易日数",
    "upgraded_to_formal_risk": "是否同段升级为正式风险",
    "formal_risk_date": "正式风险认定日期",
    "open_at_observation_end": "观察期结束时是否未闭合",
    "pressure_rule_version": "压力观察规则版本",
    "formal_rule_version": "正式风险规则版本",
    "calculation_batch_id": "观察计算批次标识",
    "upstream_snapshot_id": "上游数据快照标识",
    "source_record_ids": "来源记录标识列表",
}


@dataclass(frozen=True)
class LimitDownPressureRules:
    rule_version: str
    rule_sha256: str
    event_type: str
    thresholds: dict[str, int]


@dataclass(frozen=True)
class LimitDownPressureObservation:
    market_code: str
    security_code: str
    event_type: str
    board_code: str
    observation_threshold: int
    formal_threshold: int
    first_fact_date: date
    observation_date: date
    last_fact_date: date
    consecutive_length: int
    upgraded_to_formal_risk: bool
    formal_risk_date: date | None
    open_at_observation_end: bool
    pressure_rule_version: str
    formal_rule_version: str
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
        for field in (
            "first_fact_date",
            "observation_date",
            "last_fact_date",
            "formal_risk_date",
        ):
            value = payload[field]
            payload[field] = value.isoformat() if value else None
        payload["source_record_ids"] = list(self.source_record_ids)
        return payload


@dataclass(frozen=True)
class LimitDownPressureResult:
    observations: tuple[LimitDownPressureObservation, ...]
    rule_version: str
    rule_sha256: str
    formal_rule_version: str
    calculation_batch_id: str
    upstream_snapshot_id: str
    observation_start: date
    observation_end: date

    def observation_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            [item.to_dict() for item in self.observations],
            columns=PRESSURE_OBSERVATION_FIELDS,
        )

    def manifest(self) -> dict[str, Any]:
        return {
            "status": "SUCCEEDED",
            "analysis_layer": "LOW_THRESHOLD_LIMIT_DOWN_PRESSURE",
            "writes_formal_risk_events": False,
            "rule_version": self.rule_version,
            "rule_sha256": self.rule_sha256,
            "formal_rule_version": self.formal_rule_version,
            "calculation_batch_id": self.calculation_batch_id,
            "upstream_snapshot_id": self.upstream_snapshot_id,
            "observation_start": self.observation_start.isoformat(),
            "observation_end": self.observation_end.isoformat(),
            "observation_event_count": len(self.observations),
            "upgraded_event_count": sum(
                item.upgraded_to_formal_risk for item in self.observations
            ),
            "distinct_security_count": len(
                {
                    (item.market_code, item.security_code)
                    for item in self.observations
                }
            ),
        }


def load_limit_down_pressure_rules(path: str | Path) -> LimitDownPressureRules:
    rule_path = Path(path).resolve()
    if "approved" not in {part.lower() for part in rule_path.parts}:
        raise ValueError("Pressure observation calculation requires an approved rule")
    raw = rule_path.read_bytes()
    payload = yaml.safe_load(raw.decode("utf-8"))
    if not isinstance(payload, dict) or payload.get("status") != "approved":
        raise ValueError("Pressure observation rule status must be approved")
    thresholds = {
        str(board): int(value)
        for board, value in payload.get("thresholds", {}).items()
    }
    if thresholds != {"MAIN": 2, "CHINEXT": 1, "STAR": 1}:
        raise ValueError("Pressure observation thresholds do not match the approved scope")
    return LimitDownPressureRules(
        rule_version=str(payload["rule_version"]),
        rule_sha256=sha256(raw).hexdigest(),
        event_type=str(payload["event_type"]),
        thresholds=thresholds,
    )


def calculate_limit_down_pressure_observations(
    tables: RiskInputTables,
    gate: RiskInputGateResult,
    formal_rules: RiskEventRules,
    pressure_rules: LimitDownPressureRules,
    formal_events: Iterable[RiskEvent],
    *,
    calculation_batch_id: str,
    upstream_snapshot_id: str,
    calendar_market_code: str = "cn",
) -> LimitDownPressureResult:
    """Generate low-threshold observations without adding formal risk events."""
    if set(pressure_rules.thresholds) != set(formal_rules.thresholds):
        raise ValueError("Pressure and formal board scopes must match")
    if any(
        pressure_rules.thresholds[board] > formal_rules.thresholds[board]
        for board in pressure_rules.thresholds
    ):
        raise ValueError("Pressure thresholds must not exceed formal thresholds")

    derived_rules = replace(
        formal_rules,
        rule_version=pressure_rules.rule_version,
        rule_sha256=pressure_rules.rule_sha256,
        thresholds=dict(pressure_rules.thresholds),
        minimum_segment_length=1,
        continuous_event_type=pressure_rules.event_type,
    )
    derived = calculate_risk_events(
        tables,
        gate,
        derived_rules,
        calculation_batch_id=calculation_batch_id,
        upstream_snapshot_id=upstream_snapshot_id,
        calendar_market_code=calendar_market_code,
        include_new_st_events=False,
    )
    segment_index = {
        (item.market_code, item.security_code, item.first_fact_date): item
        for item in derived.segments
        if item.event_type == pressure_rules.event_type
    }
    formal_index = {
        (item.market_code, item.security_code, item.first_fact_date): item
        for item in formal_events
        if item.event_type == formal_rules.continuous_event_type
    }
    observations = []
    for event in derived.events:
        if event.event_type != pressure_rules.event_type:
            raise ValueError("Pressure calculation emitted a non-pressure event")
        segment_key = (event.market_code, event.security_code, event.first_fact_date)
        segment = segment_index[segment_key]
        formal_event = formal_index.get(segment_key)
        observations.append(
            LimitDownPressureObservation(
                market_code=event.market_code,
                security_code=event.security_code,
                event_type=pressure_rules.event_type,
                board_code=segment.board_code,
                observation_threshold=segment.applicable_threshold,
                formal_threshold=formal_rules.thresholds[segment.board_code],
                first_fact_date=event.first_fact_date,
                observation_date=event.risk_date,
                last_fact_date=segment.last_fact_date,
                consecutive_length=segment.consecutive_length,
                upgraded_to_formal_risk=formal_event is not None,
                formal_risk_date=formal_event.risk_date if formal_event else None,
                open_at_observation_end=segment.open_at_observation_end,
                pressure_rule_version=pressure_rules.rule_version,
                formal_rule_version=formal_rules.rule_version,
                calculation_batch_id=calculation_batch_id,
                upstream_snapshot_id=upstream_snapshot_id,
                source_record_ids=segment.source_record_ids,
            )
        )
    observations.sort(key=lambda item: item.unique_key)
    keys = [item.unique_key for item in observations]
    if len(keys) != len(set(keys)):
        raise ValueError("Pressure observation output contains duplicate business keys")
    return LimitDownPressureResult(
        observations=tuple(observations),
        rule_version=pressure_rules.rule_version,
        rule_sha256=pressure_rules.rule_sha256,
        formal_rule_version=formal_rules.rule_version,
        calculation_batch_id=calculation_batch_id,
        upstream_snapshot_id=upstream_snapshot_id,
        observation_start=formal_rules.observation_start,
        observation_end=formal_rules.observation_end,
    )


def write_limit_down_pressure_result(
    result: LimitDownPressureResult,
    output_root: str | Path,
) -> Path:
    """Atomically persist pressure observations outside formal risk outputs."""
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    final_dir = root / result.calculation_batch_id
    if final_dir.exists():
        raise FileExistsError(
            f"Pressure observation batch already exists: {final_dir}"
        )
    staging = Path(
        tempfile.mkdtemp(prefix=f".{result.calculation_batch_id}.", dir=root)
    )
    try:
        result.observation_frame().rename(columns=PRESSURE_OUTPUT_FIELDS).to_csv(
            staging / "limit_down_pressure_observations.csv",
            index=False,
            encoding="utf-8-sig",
        )
        (staging / "limit_down_pressure_manifest.json").write_text(
            json.dumps(
                result.manifest()
                | {"output_schema_version": "limit_down_pressure_csv_cn_v1"},
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        os.replace(staging, final_dir)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return final_dir
