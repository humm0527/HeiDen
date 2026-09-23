"""New/legacy broker model comparison response assembly."""

from __future__ import annotations

import csv
from io import StringIO
from typing import Any, Mapping
from urllib.parse import quote

from .results_classification import (
    limit_down_classification_payload,
    pressure_classification_payload,
)
from .results_overview import find_broker_metric_result
from .results_support import (
    BrokerResultsContext,
    SINGLE_CLASSIFICATION_ORDER,
    nullable_difference,
)


def model_family(job: dict[str, Any]) -> str | None:
    label = str(job.get("broker_data_profile_label") or "")
    if "逐日" in label:
        return "DAILY"
    if "季度" in label:
        return "QUARTERLY"
    return None

def model_threshold_comparison_payload(
    event_batch_id: str, *, context: BrokerResultsContext
) -> dict[str, Any]:
    """Pair new/legacy runs and compare formal/low-threshold A-G distributions."""
    selected = context.get_job(event_batch_id)
    quarter = str(selected.get("quarter") or "")
    family = model_family(selected)
    if selected.get("status") != "SUCCEEDED" or not quarter or family is None:
        return {
            "available": False,
            "event_batch_id": event_batch_id,
            "reason": "当前批次无法识别季度模型或逐日模型口径",
        }

    candidates: dict[str, list[dict[str, Any]]] = {"single_new": [], "single_legacy": []}
    for job in context.list_jobs():
        profile = str(job.get("broker_data_profile") or "")
        job_id = str(job.get("job_id") or "")
        if (
            profile not in candidates
            or job.get("status") != "SUCCEEDED"
            or str(job.get("quarter") or "") != quarter
            or model_family(job) != family
            or find_broker_metric_result(job_id, context=context) is None
            or not (
                context.output_root
                / "pressure_metrics"
                / f"pressure_{job_id}_v1"
                / "pressure_classification_manifest.json"
            ).is_file()
        ):
            continue
        candidates[profile].append(job)

    selected_profile = str(selected.get("broker_data_profile") or "")
    paired: dict[str, dict[str, Any]] = {}
    for profile in ("single_new", "single_legacy"):
        if selected_profile == profile:
            paired[profile] = selected
            continue
        if candidates[profile]:
            paired[profile] = max(
                candidates[profile], key=lambda item: str(item.get("created_at") or "")
            )
    if set(paired) != {"single_new", "single_legacy"}:
        return {
            "available": False,
            "event_batch_id": event_batch_id,
            "quarter": quarter,
            "model_family": family,
            "reason": "同季度同口径的新旧模型成功批次尚未齐全",
        }

    new_batch_id = str(paired["single_new"]["job_id"])
    old_batch_id = str(paired["single_legacy"]["job_id"])
    return paired_model_threshold_comparison_payload(
        new_batch_id, old_batch_id, context=context
    )

def paired_model_threshold_comparison_payload(
    new_batch_id: str,
    old_batch_id: str,
    *,
    context: BrokerResultsContext,
    comparison_id: str | None = None,
    new_job_payload: Mapping[str, Any] | None = None,
    old_job_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build A-G threshold output from an explicit audited pair."""

    new_job = dict(new_job_payload) if new_job_payload is not None else context.get_job(new_batch_id)
    old_job = dict(old_job_payload) if old_job_payload is not None else context.get_job(old_batch_id)
    quarter = str(new_job.get("quarter") or "")
    family = model_family(new_job)
    if (
        new_job.get("status") != "SUCCEEDED"
        or old_job.get("status") != "SUCCEEDED"
        or new_job.get("broker_data_profile") != "single_new"
        or old_job.get("broker_data_profile") != "single_legacy"
        or str(old_job.get("quarter") or "") != quarter
        or model_family(old_job) != family
        or not quarter
        or family is None
    ):
        return {
            "available": False,
            "event_batch_id": new_batch_id,
            "reason": "显式新旧模型批次的季度、模型族或任务状态不一致",
        }
    strict_new = limit_down_classification_payload(new_batch_id, context=context)
    strict_old = limit_down_classification_payload(old_batch_id, context=context)
    loose_new = pressure_classification_payload(new_batch_id, context=context)
    loose_old = pressure_classification_payload(old_batch_id, context=context)
    payloads = (strict_new, strict_old, loose_new, loose_old)
    if not all(payload.get("available") for payload in payloads):
        return {
            "available": False,
            "event_batch_id": new_batch_id,
            "quarter": quarter,
            "model_family": family,
            "reason": "配对批次缺少正式门槛或低门槛 A-G 分类结果",
        }

    strict_new_rows = {row["classification"]: row for row in strict_new["rows"]}
    strict_old_rows = {row["classification"]: row for row in strict_old["rows"]}
    loose_new_rows = {row["classification"]: row for row in loose_new["rows"]}
    loose_old_rows = {row["classification"]: row for row in loose_old["rows"]}
    rows: list[dict[str, Any]] = []
    for classification in SINGLE_CLASSIFICATION_ORDER:
        sn = strict_new_rows[classification]
        so = strict_old_rows[classification]
        ln = loose_new_rows[classification]
        lo = loose_old_rows[classification]
        rows.append(
            {
                "classification": classification,
                "strict_new_frequency": sn["event_frequency"],
                "strict_new_share": sn["event_share"],
                "strict_old_frequency": so["event_frequency"],
                "strict_old_share": so["event_share"],
                "strict_frequency_delta": sn["event_frequency"] - so["event_frequency"],
                "strict_share_delta": nullable_difference(
                    sn["event_share"], so["event_share"]
                ),
                "loose_new_frequency": ln["observation_frequency"],
                "loose_new_share": ln["event_share"],
                "loose_new_upgrade_count": ln["upgraded_event_count"],
                "loose_new_upgrade_rate": ln["upgrade_rate"],
                "loose_old_frequency": lo["observation_frequency"],
                "loose_old_share": lo["event_share"],
                "loose_old_upgrade_count": lo["upgraded_event_count"],
                "loose_old_upgrade_rate": lo["upgrade_rate"],
                "loose_frequency_delta": ln["observation_frequency"]
                - lo["observation_frequency"],
                "loose_share_delta": nullable_difference(
                    ln["event_share"], lo["event_share"]
                ),
                "loose_upgrade_rate_delta": nullable_difference(
                    ln["upgrade_rate"], lo["upgrade_rate"]
                ),
            }
        )

    return {
        "available": True,
        "event_batch_id": new_batch_id,
        "quarter": quarter,
        "model_family": family,
        "model_family_label": "逐日模型" if family == "DAILY" else "季度模型",
        "new_batch_id": new_batch_id,
        "old_batch_id": old_batch_id,
        "totals": {
            "strict_new": strict_new["event_count"],
            "strict_old": strict_old["event_count"],
            "loose_new": loose_new["observation_count"],
            "loose_old": loose_old["observation_count"],
            "loose_new_upgraded": loose_new["upgraded_event_count"],
            "loose_old_upgraded": loose_old["upgraded_event_count"],
        },
        "rows": rows,
        "files": {
            "comparison_csv": (
                f"/api/model-comparisons/{quote(comparison_id)}/threshold.csv"
                if comparison_id
                else f"/api/model-threshold-comparison/{quote(new_batch_id)}/csv"
            )
        },
    }

def model_threshold_comparison_csv(payload: dict[str, Any]) -> bytes:
    """Serialize the wide new/old and strict/loose comparison table."""
    fieldnames = (
        "季度",
        "模型数据口径",
        "券商03档位",
        "正式门槛新模型事件频次",
        "正式门槛新模型占比",
        "正式门槛旧模型事件频次",
        "正式门槛旧模型占比",
        "正式门槛频次差_新减旧",
        "正式门槛占比差_新减旧",
        "低门槛新模型观察频次",
        "低门槛新模型占比",
        "低门槛新模型升级数",
        "低门槛新模型升级率",
        "低门槛旧模型观察频次",
        "低门槛旧模型占比",
        "低门槛旧模型升级数",
        "低门槛旧模型升级率",
        "低门槛频次差_新减旧",
        "低门槛占比差_新减旧",
        "低门槛升级率差_新减旧",
        "新模型批次",
        "旧模型批次",
    )
    stream = StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fieldnames)
    writer.writeheader()
    for row in payload["rows"]:
        writer.writerow(
            {
                "季度": payload["quarter"],
                "模型数据口径": payload["model_family_label"],
                "券商03档位": row["classification"],
                "正式门槛新模型事件频次": row["strict_new_frequency"],
                "正式门槛新模型占比": row["strict_new_share"],
                "正式门槛旧模型事件频次": row["strict_old_frequency"],
                "正式门槛旧模型占比": row["strict_old_share"],
                "正式门槛频次差_新减旧": row["strict_frequency_delta"],
                "正式门槛占比差_新减旧": row["strict_share_delta"],
                "低门槛新模型观察频次": row["loose_new_frequency"],
                "低门槛新模型占比": row["loose_new_share"],
                "低门槛新模型升级数": row["loose_new_upgrade_count"],
                "低门槛新模型升级率": row["loose_new_upgrade_rate"],
                "低门槛旧模型观察频次": row["loose_old_frequency"],
                "低门槛旧模型占比": row["loose_old_share"],
                "低门槛旧模型升级数": row["loose_old_upgrade_count"],
                "低门槛旧模型升级率": row["loose_old_upgrade_rate"],
                "低门槛频次差_新减旧": row["loose_frequency_delta"],
                "低门槛占比差_新减旧": row["loose_share_delta"],
                "低门槛升级率差_新减旧": row["loose_upgrade_rate_delta"],
                "新模型批次": payload["new_batch_id"],
                "旧模型批次": payload["old_batch_id"],
            }
        )
    return ("\ufeff" + stream.getvalue()).encode("utf-8")

