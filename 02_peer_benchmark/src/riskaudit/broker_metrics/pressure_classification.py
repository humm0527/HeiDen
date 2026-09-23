"""Compute single-broker PIT classifications for low-threshold observations.

The output is deliberately stored outside both formal risk-event results and
formal broker metrics.  A pressure observation is an analysis-layer record,
not a formal risk event.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Callable, Mapping

import pandas as pd

from riskaudit.cancellation import raise_if_canceled

from .models import RiskEventRecord
from .real_run import (
    _assess_events_from_wide_history,
    _load_calendar,
    _load_st_states,
    _read_history,
    _resolve_history_sources,
)
from .rules import load_broker_mapping, load_broker_metric_rules
from .storage import ASSESSMENT_FIELDS


PRESSURE_EVENT_TYPE = "NON_ST_LIMIT_DOWN_PRESSURE_OBSERVATION"
PRESSURE_COLUMNS = {
    "交易市场代码": "market_code",
    "证券代码": "security_code",
    "观察事件类型": "event_type",
    "首次跌停日期": "first_fact_date",
    "达到观察门槛日期": "observation_date",
    "板块代码": "board_code",
    "低门槛观察阈值": "observation_threshold",
    "正式风险阈值": "formal_threshold",
    "最后跌停日期": "last_fact_date",
    "连续跌停交易日数": "consecutive_length",
    "是否同段升级为正式风险": "upgraded_to_formal_risk",
    "正式风险认定日期": "formal_risk_date",
    "观察期结束时是否未闭合": "open_at_observation_end",
}

CLASSIFICATION_ORDER = ("A", "B", "C", "D", "E", "F", "G", "BLANK", "UNKNOWN")
PRESSURE_SUMMARY_FIELDS = {
    "classification": "券商03档位",
    "observation_frequency": "观察事件频次",
    "frequency_rank": "频次排名",
    "distinct_security_count": "去重股票数",
    "upgraded_event_count": "升级为正式风险数",
    "upgrade_rate": "档位升级率",
    "total_limit_down_days": "累计跌停天数",
    "average_limit_down_days": "平均连续跌停天数",
    "event_share": "占全部观察事件比例",
}
PRESSURE_DETAIL_FIELDS = {
    "event_key": "观察事件唯一键",
    "market_code": "交易市场代码",
    "security_code": "证券代码",
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
    "cutoff_trading_date": "评价截止交易日",
    "selected_classification_date": "选中分类日期",
    "raw_classification": "原始分类值",
    "classification": "券商03档位",
    "classification_resolution": "分类解析来源",
    "mapping_version": "分类映射版本",
    "metric_rule_version": "指标规则版本",
    "source_quarter_file": "来源季度文件",
    "source_file_sha256": "来源文件SHA256",
    "source_row_number": "来源数据行号",
    "source_record_id": "来源记录标识",
}


@dataclass(frozen=True)
class PressureClassificationRunResult:
    output_dir: Path
    observation_count: int
    assessment_count: int
    summary_row_count: int


def _as_bool(value: object) -> bool:
    return str(value).strip().lower() in {"true", "1", "是"}


def _classification_bucket(row: pd.Series) -> str:
    raw = str(row.get("raw_classification", "")).strip().upper()
    if raw in set("ABCDEFG"):
        return raw
    if not raw and str(row.get("selected_classification_date", "")).strip():
        return "BLANK"
    return "UNKNOWN"


def build_pressure_classification_artifacts(
    observations: pd.DataFrame,
    assessments: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build deterministic A-G summary and one-row-per-observation audit detail."""
    required_observation = set(PRESSURE_COLUMNS.values())
    missing_observation = required_observation - set(observations.columns)
    if missing_observation:
        raise ValueError(
            f"Pressure observation detail missing: {sorted(missing_observation)}"
        )
    required_assessment = {
        "event_key",
        "market_code",
        "security_code",
        "event_type",
        "first_fact_date",
        "selected_classification_date",
        "raw_classification",
    }
    missing_assessment = required_assessment - set(assessments.columns)
    if missing_assessment:
        raise ValueError(
            f"Pressure assessment detail missing: {sorted(missing_assessment)}"
        )

    detail = observations.copy()
    detail["event_key"] = detail.apply(
        lambda row: "|".join(
            (
                str(row["market_code"]),
                str(row["security_code"]),
                str(row["event_type"]),
                str(row["first_fact_date"])[:10],
            )
        ),
        axis=1,
    )
    if detail["event_key"].duplicated().any():
        raise ValueError("Pressure observation event keys are not unique")
    if assessments["event_key"].duplicated().any():
        raise ValueError("Pressure assessment event keys are not unique")

    assessment_columns = [
        "event_key",
        "cutoff_trading_date",
        "selected_classification_date",
        "raw_classification",
        "classification_resolution",
        "mapping_version",
        "metric_rule_version",
        "source_quarter_file",
        "source_file_sha256",
        "source_row_number",
        "source_record_id",
    ]
    for column in assessment_columns:
        if column not in assessments:
            assessments[column] = ""
    detail = detail.merge(
        assessments.loc[:, assessment_columns],
        on="event_key",
        how="left",
        validate="one_to_one",
        indicator=True,
    )
    if (detail["_merge"] != "both").any():
        missing_keys = detail.loc[detail["_merge"] != "both", "event_key"].tolist()
        raise ValueError(f"Pressure observations missing PIT assessments: {missing_keys[:5]}")
    detail = detail.drop(columns="_merge")
    detail["classification"] = detail.apply(_classification_bucket, axis=1)
    detail["upgraded_to_formal_risk"] = detail["upgraded_to_formal_risk"].map(
        _as_bool
    )
    detail["open_at_observation_end"] = detail["open_at_observation_end"].map(
        _as_bool
    )
    detail["consecutive_length"] = pd.to_numeric(
        detail["consecutive_length"], errors="raise"
    ).astype(int)

    counts = Counter(detail["classification"])
    dense_rank = {
        value: index + 1
        for index, value in enumerate(sorted(set(counts.values()), reverse=True))
    }
    total = len(detail)
    summary_rows = []
    for classification in CLASSIFICATION_ORDER:
        group = detail.loc[detail["classification"] == classification]
        frequency = len(group)
        upgraded = int(group["upgraded_to_formal_risk"].sum())
        total_days = int(group["consecutive_length"].sum())
        summary_rows.append(
            {
                "classification": classification,
                "observation_frequency": frequency,
                "frequency_rank": dense_rank.get(frequency) if frequency else None,
                "distinct_security_count": len(
                    set(zip(group["market_code"], group["security_code"]))
                ),
                "upgraded_event_count": upgraded,
                "upgrade_rate": round(upgraded / frequency, 6) if frequency else None,
                "total_limit_down_days": total_days,
                "average_limit_down_days": (
                    round(total_days / frequency, 6) if frequency else None
                ),
                "event_share": round(frequency / total, 6) if total else None,
            }
        )
    summary = pd.DataFrame(summary_rows, columns=list(PRESSURE_SUMMARY_FIELDS))
    detail = detail.loc[:, list(PRESSURE_DETAIL_FIELDS)]
    return summary, detail


def run_pressure_classification(
    *,
    observation_csv: str | Path,
    history_dir: str | Path,
    calendar_csv: str | Path,
    st_csv: str | Path,
    output_root: str | Path,
    metric_batch_id: str,
    event_batch_id: str,
    rules_path: str | Path,
    mapping_path: str | Path,
    observation_start: date,
    observation_end: date,
    history_files: Mapping[str, str | Path] | None = None,
    read_chunksize: int = 100_000,
    cancel_check: Callable[[], bool] | None = None,
) -> PressureClassificationRunResult:
    """Classify pressure observations for the single-broker view only."""
    raise_if_canceled(cancel_check)
    final_dir = Path(output_root) / metric_batch_id
    if final_dir.exists():
        raise FileExistsError(f"Pressure classification batch already exists: {final_dir}")

    source = Path(observation_csv)
    frame = pd.read_csv(
        source, dtype=str, keep_default_na=False, encoding="utf-8-sig"
    )
    missing = set(PRESSURE_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"Pressure observation schema missing: {sorted(missing)}")
    frame = frame.rename(columns=PRESSURE_COLUMNS)
    invalid_types = set(frame["event_type"]) - {PRESSURE_EVENT_TYPE}
    if invalid_types:
        raise ValueError(f"Unexpected pressure event types: {sorted(invalid_types)}")
    fact_dates = pd.to_datetime(frame["first_fact_date"], errors="coerce").dt.date
    frame = frame.loc[
        fact_dates.between(observation_start, observation_end, inclusive="both")
    ].copy()

    events = tuple(
        RiskEventRecord(
            market_code=row.market_code,
            security_code=row.security_code,
            event_type=PRESSURE_EVENT_TYPE,
            first_fact_date=date.fromisoformat(row.first_fact_date[:10]),
            risk_date=date.fromisoformat(row.observation_date[:10]),
            event_rule_version="limit_down_pressure_v1",
            event_batch_id=event_batch_id,
        )
        for row in frame.loc[:, list(PRESSURE_COLUMNS.values())].itertuples(index=False)
    )
    if len({item.event_key for item in events}) != len(events):
        raise ValueError("Pressure observation event keys are not unique")

    mapping = load_broker_mapping(mapping_path)
    rules = load_broker_metric_rules(rules_path)
    calendar, calendar_dates, calendar_snapshot_id = _load_calendar(
        calendar_csv, observation_start, observation_end
    )
    st_states, _ = _load_st_states(
        st_csv, calendar_dates, observation_end, cancel_check=cancel_check
    )
    sources = _resolve_history_sources(history_dir, history_files)
    event_securities = {(item.market_code, item.security_code) for item in events}
    history = _read_history(
        sources,
        ("券商03",),
        event_securities,
        calendar,
        observation_start,
        observation_end,
        read_chunksize,
        cancel_check=cancel_check,
    )
    missing_history = event_securities - set(history["securities"])
    assessments = _assess_events_from_wide_history(
        events,
        ("券商03",),
        history["event_rows"],
        st_states,
        calendar,
        mapping,
        rules,
        metric_batch_id,
        calendar_snapshot_id,
        missing_history_securities=missing_history,
        cancel_check=cancel_check,
    )
    raise_if_canceled(cancel_check)
    assessment_frame = pd.DataFrame(
        [item.to_dict() for item in assessments], columns=list(ASSESSMENT_FIELDS)
    )
    summary_frame, detail_frame = build_pressure_classification_artifacts(
        frame,
        assessment_frame.copy(),
    )

    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{metric_batch_id}.", dir=root))
    try:
        assessment_path = staging / "pressure_event_assessments.csv"
        assessment_frame.rename(columns=ASSESSMENT_FIELDS).to_csv(
            assessment_path,
            index=False,
            encoding="utf-8-sig",
        )
        summary_path = staging / "低门槛A-G档位分布.csv"
        detail_path = staging / "低门槛观察事件分类明细.csv"
        summary_frame.rename(columns=PRESSURE_SUMMARY_FIELDS).to_csv(
            summary_path,
            index=False,
            encoding="utf-8-sig",
        )
        detail_frame.rename(columns=PRESSURE_DETAIL_FIELDS).to_csv(
            detail_path,
            index=False,
            encoding="utf-8-sig",
        )
        manifest = {
            "status": "SUCCEEDED",
            "analysis_layer": "LOW_THRESHOLD_LIMIT_DOWN_PRESSURE",
            "event_batch_id": event_batch_id,
            "metric_batch_id": metric_batch_id,
            "event_type": PRESSURE_EVENT_TYPE,
            "broker_id": "券商03",
            "observation_count": len(events),
            "assessment_count": len(assessments),
            "classification_summary_row_count": len(summary_frame),
            "classification_detail_count": len(detail_frame),
            "classification_order": list(CLASSIFICATION_ORDER),
            "blank_observation_count": int(
                (detail_frame["classification"] == "BLANK").sum()
            ),
            "unknown_observation_count": int(
                (detail_frame["classification"] == "UNKNOWN").sum()
            ),
            "upgraded_observation_count": int(
                detail_frame["upgraded_to_formal_risk"].sum()
            ),
            "mapping_version": mapping.mapping_version,
            "metric_rule_version": rules.rule_version,
            "observation_start": observation_start.isoformat(),
            "observation_end": observation_end.isoformat(),
            "source_observations": {
                "path": str(source.resolve()),
                "sha256": sha256(source.read_bytes()).hexdigest(),
            },
            "writes_formal_risk_events": False,
            "output_schema_version": "pressure_classification_cn_v1",
            "files": {
                "assessment_detail": assessment_path.name,
                "classification_summary": summary_path.name,
                "classification_detail": detail_path.name,
            },
        }
        manifest["file_sha256"] = {
            assessment_path.name: sha256(assessment_path.read_bytes()).hexdigest(),
            summary_path.name: sha256(summary_path.read_bytes()).hexdigest(),
            detail_path.name: sha256(detail_path.read_bytes()).hexdigest(),
        }
        (staging / "pressure_classification_manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(staging, final_dir)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return PressureClassificationRunResult(
        output_dir=final_dir,
        observation_count=len(events),
        assessment_count=len(assessments),
        summary_row_count=len(summary_frame),
    )
