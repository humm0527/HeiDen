"""Event-classification and pressure response assembly."""

from __future__ import annotations

from collections import Counter
import csv
import json
from statistics import median
from typing import Any
from urllib.parse import quote

from .results_overview import find_broker_metric_result
from .results_support import (
    BrokerResultsContext,
    SINGLE_CLASSIFICATION_ORDER,
    LIMIT_DOWN_EVENT_TYPE,
    NEW_ST_EVENT_TYPE,
    nullable_number,
)


def limit_down_classification_payload(
    event_batch_id: str, *, context: BrokerResultsContext
) -> dict[str, Any]:
    """Aggregate single-broker raw A-G values for formal non-ST limit-down events."""
    return event_classification_payload(
        event_batch_id,
        context=context,
        event_type=LIMIT_DOWN_EVENT_TYPE,
        unavailable_reason="该批次尚未生成券商03 PIT 评价，无法计算跌停全分类对比",
    )

def new_st_classification_payload(
    event_batch_id: str, *, context: BrokerResultsContext
) -> dict[str, Any]:
    """Aggregate single-broker raw A-G values for formal new-ST events."""
    return event_classification_payload(
        event_batch_id,
        context=context,
        event_type=NEW_ST_EVENT_TYPE,
        unavailable_reason="该批次尚未生成券商03 PIT 评价，无法计算新增 ST 全分类对比",
    )

def event_classification_payload(
    event_batch_id: str,
    *,
    context: BrokerResultsContext,
    event_type: str,
    unavailable_reason: str,
) -> dict[str, Any]:
    context.assert_readable(event_batch_id)
    matched_result = find_broker_metric_result(event_batch_id, context=context)
    if matched_result is None:
        return {
            "available": False,
            "event_batch_id": event_batch_id,
            "reason": unavailable_reason,
        }
    metric_dir, manifest = matched_result
    assessment_path = metric_dir / "event_broker_assessments.csv"
    event_path = (
        context.output_root
        / event_batch_id
        / "risk_results"
        / event_batch_id
        / "risk_events.csv"
    )
    if not assessment_path.is_file() or not event_path.is_file():
        return {
            "available": False,
            "event_batch_id": event_batch_id,
            "reason": "该批次缺少正式风险事件或事件×券商评价明细",
        }

    assessments: dict[str, dict[str, str]] = {}
    with assessment_path.open("r", encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            if (
                str(row.get("券商标识", "")).strip() != "券商03"
                or str(row.get("风险事件类型", "")).strip() != event_type
            ):
                continue
            event_key = str(row.get("事件唯一键", "")).strip()
            if event_key in assessments:
                raise ValueError(f"券商03事件评价存在重复唯一键: {event_key}")
            assessments[event_key] = row

    events: list[dict[str, Any]] = []
    with event_path.open("r", encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            if str(row.get("事件类型", "")).strip() != event_type:
                continue
            event_key = "|".join(
                (
                    str(row.get("交易市场代码", "")).strip(),
                    str(row.get("证券代码", "")).strip(),
                    event_type,
                    str(row.get("首次事实日期", "")).strip(),
                )
            )
            assessment = assessments.get(event_key)
            raw_value = str((assessment or {}).get("原始分类值", "")).strip().upper()
            selected_date = str((assessment or {}).get("选中分类日期", "")).strip()
            if raw_value in set("ABCDEFG"):
                classification = raw_value
            elif not raw_value and selected_date:
                classification = "BLANK"
            else:
                classification = "UNKNOWN"
            consecutive_days = int(float(str(row.get("连续跌停交易日数", "0") or "0")))
            warning_status = str((assessment or {}).get("预警天数状态", "")).strip()
            warning_days = nullable_number(
                str((assessment or {}).get("预警交易日数", ""))
            )
            events.append(
                {
                    "event_key": event_key,
                    "market_code": str(row.get("交易市场代码", "")).strip(),
                    "security_code": str(row.get("证券代码", "")).strip(),
                    "first_fact_date": str(row.get("首次事实日期", "")).strip(),
                    "risk_date": str(row.get("风险认定日期", "")).strip(),
                    "last_fact_date": str(row.get("最后事实日期", "")).strip(),
                    "consecutive_limit_down_days": consecutive_days,
                    "classification": classification,
                    "raw_classification": raw_value,
                    "warning_days": (
                        warning_days if warning_status == "DEFINED" else None
                    ),
                }
            )

    counts = Counter(item["classification"] for item in events)
    dense_rank = {
        value: index + 1
        for index, value in enumerate(sorted(set(counts.values()), reverse=True))
    }
    total_events = len(events)
    summary_rows: list[dict[str, Any]] = []
    for classification in SINGLE_CLASSIFICATION_ORDER:
        group = [item for item in events if item["classification"] == classification]
        frequency = len(group)
        days = [item["consecutive_limit_down_days"] for item in group]
        group_warning_values = [
            float(item["warning_days"])
            for item in group
            if item["warning_days"] is not None
        ]
        summary_rows.append(
            {
                "classification": classification,
                "event_frequency": frequency,
                "frequency_rank": dense_rank.get(frequency),
                "distinct_security_count": len(
                    {(item["market_code"], item["security_code"]) for item in group}
                ),
                "total_limit_down_days": sum(days),
                "average_limit_down_days": (
                    round(sum(days) / frequency, 6) if frequency else None
                ),
                "defined_warning_days_count": len(group_warning_values),
                "average_warning_days": (
                    round(sum(group_warning_values) / len(group_warning_values), 6)
                    if group_warning_values
                    else None
                ),
                "intercept_event_count": (
                    frequency
                    if classification in {"F", "G"}
                    else None
                    if classification == "UNKNOWN"
                    else 0
                ),
                "event_share": round(frequency / total_events, 6)
                if total_events
                else None,
            }
        )

    intercepted = [item for item in events if item["classification"] in {"F", "G"}]
    warning_values = [
        float(item["warning_days"])
        for item in intercepted
        if item["warning_days"] is not None
    ]
    base_url = f"/data/outputs/broker_metrics/{metric_dir.name}"
    return {
        "available": True,
        "event_batch_id": event_batch_id,
        "metric_batch_id": manifest["metric_batch_id"],
        "metric_rule_version": manifest["rule_version"],
        "mapping_version": manifest["mapping_version"],
        "event_type": event_type,
        "broker": "券商03",
        "event_count": total_events,
        "distinct_security_count": len(
            {(item["market_code"], item["security_code"]) for item in events}
        ),
        "intercept_event_count": len(intercepted),
        "intercept_security_count": len(
            {(item["market_code"], item["security_code"]) for item in intercepted}
        ),
        "coverage_rate": round(len(intercepted) / total_events, 6)
        if total_events
        else None,
        "blank_event_count": counts.get("BLANK", 0),
        "unknown_event_count": counts.get("UNKNOWN", 0),
        "warning_days": {
            "defined_count": len(warning_values),
            "min": min(warning_values) if warning_values else None,
            "max": max(warning_values) if warning_values else None,
            "median": median(warning_values) if warning_values else None,
            "mean": round(sum(warning_values) / len(warning_values), 6)
            if warning_values
            else None,
        },
        "rows": summary_rows,
        "events": sorted(
            events,
            key=lambda item: (
                item["first_fact_date"],
                item["market_code"],
                item["security_code"],
            ),
        )[:200],
        "files": {
            "event_assessments": f"{base_url}/event_broker_assessments.csv",
            "risk_events": f"/data/outputs/{event_batch_id}/risk_results/{event_batch_id}/risk_events.csv",
        },
    }

def pressure_classification_payload(
    event_batch_id: str, *, context: BrokerResultsContext
) -> dict[str, Any]:
    """Aggregate A-G distribution and same-segment upgrades for pressure events."""
    context.assert_readable(event_batch_id)
    metric_batch_id = f"pressure_{event_batch_id}_v1"
    metric_dir = context.output_root / "pressure_metrics" / metric_batch_id
    observation_dir = context.output_root / event_batch_id / "pressure_results" / event_batch_id
    assessment_path = metric_dir / "pressure_event_assessments.csv"
    observation_path = observation_dir / "limit_down_pressure_observations.csv"
    manifest_path = metric_dir / "pressure_classification_manifest.json"
    if not (
        assessment_path.is_file()
        and observation_path.is_file()
        and manifest_path.is_file()
    ):
        return {
            "available": False,
            "event_batch_id": event_batch_id,
            "reason": "该批次尚未生成压力观察 PIT 分类，请用当前版本重新计算季度任务",
        }
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    artifact_files = manifest.get("files", {})
    summary_name = str(
        artifact_files.get("classification_summary", "低门槛A-G档位分布.csv")
    )
    detail_name = str(
        artifact_files.get("classification_detail", "低门槛观察事件分类明细.csv")
    )
    assessments: dict[str, dict[str, str]] = {}
    with assessment_path.open("r", encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            if str(row.get("券商标识", "")).strip() != "券商03":
                continue
            event_key = str(row.get("事件唯一键", "")).strip()
            if event_key in assessments:
                raise ValueError(f"压力观察评价存在重复唯一键: {event_key}")
            assessments[event_key] = row

    events: list[dict[str, Any]] = []
    with observation_path.open("r", encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            event_key = "|".join(
                (
                    str(row.get("交易市场代码", "")).strip(),
                    str(row.get("证券代码", "")).strip(),
                    str(row.get("观察事件类型", "")).strip(),
                    str(row.get("首次跌停日期", "")).strip(),
                )
            )
            assessment = assessments.get(event_key)
            raw_value = str((assessment or {}).get("原始分类值", "")).strip().upper()
            selected_date = str((assessment or {}).get("选中分类日期", "")).strip()
            classification = (
                raw_value
                if raw_value in set("ABCDEFG")
                else "BLANK"
                if not raw_value and selected_date
                else "UNKNOWN"
            )
            upgraded = str(row.get("是否同段升级为正式风险", "")).strip().lower() in {
                "true",
                "1",
                "是",
            }
            events.append(
                {
                    "event_key": event_key,
                    "market_code": str(row.get("交易市场代码", "")).strip(),
                    "security_code": str(row.get("证券代码", "")).strip(),
                    "classification": classification,
                    "consecutive_limit_down_days": int(
                        float(str(row.get("连续跌停交易日数", "0") or "0"))
                    ),
                    "upgraded": upgraded,
                    "open_at_end": str(
                        row.get("观察期结束时是否未闭合", "")
                    ).strip().lower()
                    in {"true", "1", "是"},
                }
            )

    counts = Counter(item["classification"] for item in events)
    dense_rank = {
        value: index + 1
        for index, value in enumerate(sorted(set(counts.values()), reverse=True))
    }
    total = len(events)
    rows = []
    for classification in SINGLE_CLASSIFICATION_ORDER:
        group = [item for item in events if item["classification"] == classification]
        frequency = len(group)
        upgrades = sum(item["upgraded"] for item in group)
        total_days = sum(item["consecutive_limit_down_days"] for item in group)
        rows.append(
            {
                "classification": classification,
                "observation_frequency": frequency,
                "frequency_rank": dense_rank.get(frequency),
                "distinct_security_count": len(
                    {(item["market_code"], item["security_code"]) for item in group}
                ),
                "total_limit_down_days": total_days,
                "average_limit_down_days": round(total_days / frequency, 6)
                if frequency
                else None,
                "upgraded_event_count": upgrades,
                "upgrade_rate": round(upgrades / frequency, 6) if frequency else None,
                "event_share": round(frequency / total, 6) if total else None,
            }
        )
    upgraded_events = [item for item in events if item["upgraded"]]
    base_url = f"/data/outputs/pressure_metrics/{metric_batch_id}"
    observation_url = (
        f"/data/outputs/{event_batch_id}/pressure_results/{event_batch_id}"
    )
    return {
        "available": True,
        "event_batch_id": event_batch_id,
        "metric_batch_id": metric_batch_id,
        "metric_rule_version": manifest["metric_rule_version"],
        "mapping_version": manifest["mapping_version"],
        "output_schema_version": manifest.get("output_schema_version"),
        "event_type": manifest["event_type"],
        "broker": "券商03",
        "observation_count": total,
        "distinct_security_count": len(
            {(item["market_code"], item["security_code"]) for item in events}
        ),
        "upgraded_event_count": len(upgraded_events),
        "upgraded_security_count": len(
            {
                (item["market_code"], item["security_code"])
                for item in upgraded_events
            }
        ),
        "upgrade_rate": round(len(upgraded_events) / total, 6) if total else None,
        "open_event_count": sum(item["open_at_end"] for item in events),
        "blank_event_count": counts.get("BLANK", 0),
        "unknown_event_count": counts.get("UNKNOWN", 0),
        "rows": rows,
        "files": {
            "observations": f"{observation_url}/limit_down_pressure_observations.csv",
            "event_assessments": f"{base_url}/pressure_event_assessments.csv",
            "classification_summary": f"{base_url}/{quote(summary_name)}",
            "classification_detail": f"{base_url}/{quote(detail_name)}",
        },
    }
