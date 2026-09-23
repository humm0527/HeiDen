"""Create the concise, Chinese-named user-facing output bundle for task 02."""

from __future__ import annotations

from datetime import date, datetime
import json
from pathlib import Path
import shutil
from typing import Any, Mapping


_USER_FILE_NAMES = {
    "event_broker_assessments.csv": "风险事件券商评价_{quarter}.csv",
    "broker_warning_days_summary.csv": "券商预警天数汇总_{quarter}.csv",
    "broker_hit_rate_summary.csv": "券商风险命中率汇总_{quarter}.csv",
    "broker_warning_rate_summary.csv": "券商预警率汇总_{quarter}.csv",
    "broker_metric_findings.csv": "券商指标异常清单_{quarter}.csv",
    "all_broker_risk_event_hits.csv": "全券商风险事件命中明细_{quarter}.csv",
    "券商预警指标最终汇总.csv": "券商预警指标最终汇总_{quarter}.csv",
    "券商档位收益率表现汇总.csv": "券商档位收益率表现汇总_{quarter}.csv",
    "券商档位逐日收益率明细.csv": "券商档位逐日收益率明细_{quarter}.csv",
    "券商03A-G档位风险事件汇总.csv": (
        "券商03A-G档位风险事件汇总_{quarter}.csv"
    ),
    "券商03A-G档位风险事件明细.csv": (
        "券商03A-G档位风险事件明细_{quarter}.csv"
    ),
}


def write_broker_user_output_bundle(
    *,
    output_root: str | Path,
    technical_output_dir: str | Path,
    metric_batch_id: str,
    run_scope: str,
    observation_start: date,
    observation_end: date,
    completed_at: datetime | None = None,
    observation_range_context: Mapping[str, Any] | None = None,
    preserve_manifests: bool = False,
) -> dict[str, Any]:
    """Copy task-02 results into one timestamped folder for daily use."""
    finished = completed_at or datetime.now().astimezone()
    timestamp = finished.strftime("%Y%m%d_%H%M%S")
    quarter = f"{observation_end.year}Q{(observation_end.month - 1) // 3 + 1}"
    technical_dir = Path(technical_output_dir)
    target = Path(output_root) / "结果输出" / timestamp
    target.mkdir(parents=True, exist_ok=False)

    copied: list[str] = []
    for source_name, target_template in _USER_FILE_NAMES.items():
        source = technical_dir / source_name
        if not source.is_file():
            continue
        target_name = target_template.format(quarter=quarter)
        shutil.copy2(source, target / target_name)
        copied.append(target_name)

    manifest = json.loads(
        (technical_dir / "broker_metric_manifest.json").read_text(encoding="utf-8")
    )
    if preserve_manifests:
        audit = target / "核对材料"
        audit.mkdir()
        manifest["file_name_map"] = {
            name: template.format(quarter=quarter)
            for name, template in _USER_FILE_NAMES.items()
            if (technical_dir / name).is_file()
        }
        (audit / "broker_metric_manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        shutil.copy2(technical_dir / "broker_history_input_manifest.json", audit)
    summary_name = f"运行摘要_{quarter}.json"
    mode = "全行业券商评价" if run_scope == "peer-benchmark" else "券商03单模型"
    range_context = dict(observation_range_context or {})
    source_start = str(
        range_context.get("source_event_start") or observation_start.isoformat()
    )
    source_end = str(
        range_context.get("source_event_end") or observation_end.isoformat()
    )
    uploaded_start = str(
        range_context.get("uploaded_start") or observation_start.isoformat()
    )
    uploaded_end = str(
        range_context.get("uploaded_end") or observation_end.isoformat()
    )
    history_start = str(
        range_context.get("history_start") or uploaded_start
    )
    history_end = str(range_context.get("history_end") or uploaded_end)
    uncovered = list(range_context.get("history_ranges_outside_calculation") or [])
    concise = {
        "状态": "成功",
        "批次": metric_batch_id,
        "模式": mode,
        "季度": quarter,
        "完成时间": finished.isoformat(timespec="seconds"),
        "01风险事件日期": f"{source_start} 至 {source_end}",
        "历史目录日期": f"{history_start} 至 {history_end}",
        "本季度上传文件日期": f"{uploaded_start} 至 {uploaded_end}",
        "计算日期": f"{observation_start.isoformat()} 至 {observation_end.isoformat()}",
        "01是否覆盖全部历史目录日期": not uncovered,
        "未进入计算的历史目录区间": uncovered,
        "01源事件数": int(manifest.get("source_event_count", 0)),
        "02入选事件数": int(manifest.get("event_count", 0)),
        "范围外排除事件数": int(
            manifest.get("excluded_out_of_range_event_count", 0)
        ),
        "券商数": int(manifest.get("broker_count", 0)),
        "事件券商评价数": int(manifest.get("assessment_count", 0)),
        "最终汇总数": int(manifest.get("final_summary_count", 0)),
        "输出文件": [*copied, summary_name],
    }
    (target / summary_name).write_text(
        json.dumps(concise, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return {
        "status": "SUCCEEDED",
        "batch_id": metric_batch_id,
        "mode": mode,
        "quarter": quarter,
        "completed_at": concise["完成时间"],
        "uploaded_date_range": concise["本季度上传文件日期"],
        "history_date_range": concise["历史目录日期"],
        "source_event_date_range": concise["01风险事件日期"],
        "observation_date_range": concise["计算日期"],
        "source_event_covers_entire_history_range": concise[
            "01是否覆盖全部历史目录日期"
        ],
        "history_date_ranges_outside_calculation": uncovered,
        "counts": {
            "source_events": concise["01源事件数"],
            "selected_events": concise["02入选事件数"],
            "excluded_events": concise["范围外排除事件数"],
            "brokers": concise["券商数"],
            "assessments": concise["事件券商评价数"],
            "final_summary_rows": concise["最终汇总数"],
        },
        "output_dir": str(target),
        "files": [*copied, summary_name],
    }
