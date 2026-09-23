"""Read-only filtering and export for frozen model-comparison differences."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any, Callable


@dataclass(frozen=True)
class ModelComparisonResultsContext:
    output_root: Path
    comparison_payload: Callable[[str], dict[str, Any]]


def comparison_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "是"}


def comparison_float(value: Any) -> float | None:
    text_value = str(value or "").strip()
    if not text_value:
        return None
    try:
        return float(text_value)
    except ValueError:
        return None


def event_differences_payload(
    parent_job_id: str,
    *,
    context: ModelComparisonResultsContext,
    security_query: str = "",
    event_type: str = "",
    change_type: str = "",
    transition: str = "",
) -> dict[str, Any]:
    """Read the frozen pairwise CSV and apply display-only deterministic filters."""

    comparison = context.comparison_payload(parent_job_id)
    parent = dict(comparison.get("comparison") or {})
    result = dict(comparison.get("result") or {})
    if parent.get("status") != "SUCCEEDED" or result.get("status") != "SUCCEEDED":
        raise ValueError("比较父任务尚未成功，逐事件差异不可用")
    detail_value = dict(result.get("files") or {}).get("event_differences")
    if not detail_value:
        raise ValueError("比较父任务未记录逐事件差异文件")
    detail_path = Path(str(detail_value)).resolve()
    if not detail_path.is_relative_to(context.output_root.resolve()):
        raise ValueError("逐事件差异文件不在受控输出目录内")
    if not detail_path.is_file():
        raise ValueError("逐事件差异文件缺失")

    supported_change_types = {
        "": "全部变化",
        "gained_hit": "新增命中",
        "lost_hit": "丢失命中",
        "hit_changed": "命中状态变化",
        "warning_changed": "预警天数变化",
        "classification_changed": "档位变化",
    }
    if change_type not in supported_change_types:
        raise ValueError(f"不支持的变化类型: {change_type}")

    rows: list[dict[str, Any]] = []
    with detail_path.open("r", encoding="utf-8-sig", newline="") as stream:
        for source in csv.DictReader(stream):
            new_hit = comparison_bool(source.get("new_hit"))
            legacy_hit = comparison_bool(source.get("legacy_hit"))
            new_warning = comparison_float(source.get("new_warning_days"))
            legacy_warning = comparison_float(source.get("legacy_warning_days"))
            new_classification = str(source.get("new_classification") or "UNKNOWN")
            legacy_classification = str(
                source.get("legacy_classification") or "UNKNOWN"
            )
            change_types: list[str] = []
            if new_hit and not legacy_hit:
                change_types.append("gained_hit")
            if legacy_hit and not new_hit:
                change_types.append("lost_hit")
            if new_hit != legacy_hit:
                change_types.append("hit_changed")
            if new_warning != legacy_warning:
                change_types.append("warning_changed")
            if new_classification != legacy_classification:
                change_types.append("classification_changed")
            event_key = str(source.get("event_key") or "")
            event_key_parts = event_key.split("|")
            rows.append(
                {
                    "event_key": event_key,
                    "security_code": str(source.get("security_code") or ""),
                    "event_type": str(source.get("event_type") or ""),
                    "risk_date": event_key_parts[-1] if len(event_key_parts) >= 4 else "",
                    "new_classification": new_classification,
                    "legacy_classification": legacy_classification,
                    "classification_transition": (
                        f"{legacy_classification}>{new_classification}"
                    ),
                    "new_hit": new_hit,
                    "legacy_hit": legacy_hit,
                    "new_warning_days": new_warning,
                    "legacy_warning_days": legacy_warning,
                    "warning_days_delta": comparison_float(
                        source.get("warning_days_delta")
                    ),
                    "change_types": change_types,
                }
            )

    all_rows = rows
    security_term = security_query.strip().upper()
    selected_event_type = event_type.strip()
    selected_transition = transition.strip()
    if selected_event_type and selected_event_type not in {
        row["event_type"] for row in all_rows
    }:
        raise ValueError(f"当前比较任务不存在风险类型: {selected_event_type}")
    if selected_transition and selected_transition not in {
        row["classification_transition"] for row in all_rows
    }:
        raise ValueError(f"当前比较任务不存在档位迁移: {selected_transition}")
    rows = [
        row
        for row in all_rows
        if (
            not security_term
            or security_term in row["security_code"].upper()
            or security_term in row["event_key"].upper()
        )
        and (not selected_event_type or row["event_type"] == selected_event_type)
        and (not change_type or change_type in row["change_types"])
        and (
            not selected_transition
            or row["classification_transition"] == selected_transition
        )
    ]

    def count_change(code: str) -> int:
        return sum(code in row["change_types"] for row in all_rows)

    return {
        "available": True,
        "comparison_batch_id": parent_job_id,
        "quarter": result.get("quarter"),
        "summary": {
            "total_difference_count": len(all_rows),
            "matched_count": len(rows),
            "gained_hit_count": count_change("gained_hit"),
            "lost_hit_count": count_change("lost_hit"),
            "warning_changed_count": count_change("warning_changed"),
            "classification_changed_count": count_change("classification_changed"),
        },
        "filters": {
            "security": security_query.strip(),
            "event_type": selected_event_type,
            "change_type": change_type,
            "transition": selected_transition,
        },
        "filter_options": {
            "event_types": sorted({row["event_type"] for row in all_rows}),
            "change_types": [
                {"value": code, "label": label}
                for code, label in supported_change_types.items()
            ],
            "transitions": sorted(
                {row["classification_transition"] for row in all_rows}
            ),
        },
        "rows": rows,
    }


def event_differences_csv(payload: dict[str, Any]) -> bytes:
    fieldnames = (
        "event_key",
        "security_code",
        "event_type",
        "risk_date",
        "legacy_classification",
        "new_classification",
        "legacy_hit",
        "new_hit",
        "legacy_warning_days",
        "new_warning_days",
        "warning_days_delta",
        "change_types",
    )
    stream = StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fieldnames)
    writer.writeheader()
    for row in payload.get("rows") or []:
        writer.writerow(
            {
                field: (
                    "|".join(row[field])
                    if field == "change_types"
                    else row.get(field)
                )
                for field in fieldnames
            }
        )
    return ("\ufeff" + stream.getvalue()).encode("utf-8")
