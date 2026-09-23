"""Broker metric overview and drill-down response assembly."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any
from riskaudit.broker_ranking import (
    broker_drilldown,
    enrich_broker_rankings,
    verified_assessment_rows,
)

from .results_support import BrokerResultsContext, nullable_number


def find_broker_metric_result(
    event_batch_id: str,
    *,
    context: BrokerResultsContext,
) -> tuple[Path, dict[str, Any]] | None:
    root = context.output_root / "broker_metrics"
    preferred = root / context.final_broker_metric_batch_id if context.final_broker_metric_batch_id else None
    candidates = ([preferred] if preferred is not None and preferred.is_dir() else []) + [
        path
        for path in sorted(root.glob("*"), reverse=True)
        if path.is_dir() and path != preferred
    ]
    for metric_dir in candidates:
        manifest_path = metric_dir / "broker_metric_manifest.json"
        summary_path = metric_dir / "券商预警指标最终汇总.csv"
        if not manifest_path.is_file() or not summary_path.is_file():
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            manifest.get("status") == "SUCCEEDED"
            and manifest.get("event_batch_id") == event_batch_id
        ):
            return metric_dir, manifest
    return None

def broker_metrics_payload(
    event_batch_id: str, *, context: BrokerResultsContext
) -> dict[str, Any]:
    """Return v6 broker warning metrics only for their exact event batch."""
    context.assert_readable(event_batch_id)
    matched_result = find_broker_metric_result(event_batch_id, context=context)
    if matched_result is None:
        return {
            "available": False,
            "event_batch_id": event_batch_id,
            "reason": "该风险事件批次尚未生成券商预警指标；不会套用其他季度结果",
        }
    metric_dir, manifest = matched_result
    summary_path = metric_dir / "券商预警指标最终汇总.csv"
    try:
        job = context.get_job(event_batch_id)
    except (FileNotFoundError, ValueError, json.JSONDecodeError):
        job = {}

    view_codes = {
        "两类风险股票合计": "ALL_RISK_EVENTS",
        "新增ST风险股票": "NEW_ST",
        "非ST连续跌停风险股票": "NON_ST_CONTINUOUS_LIMIT_DOWN",
    }
    rows: list[dict[str, Any]] = []
    with summary_path.open("r", encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            label = str(row["风险股票类型"])
            rows.append(
                {
                    "broker": row["券商"],
                    "risk_view": view_codes[label],
                    "risk_view_label": label,
                    "defined_warning_count": int(row["可计算预警天数的命中事件数"]),
                    "unknown_warning_count": int(row["命中但预警天数未知事件数"]),
                    "warning_days_min": nullable_number(row["预警天数最小值"]),
                    "warning_days_max": nullable_number(row["预警天数最大值"]),
                    "warning_days_median": nullable_number(row["预警天数中位数"]),
                    "warning_days_mean": nullable_number(row["预警天数平均数"]),
                    "event_count": int(row["风险事件数"]),
                    "hit_event_count": int(row["命中风险事件数"]),
                    "event_hit_rate": nullable_number(row["风险事件命中率"]),
                    "security_count": int(row["风险股票数"]),
                    "hit_security_count": int(row["命中风险股票数"]),
                    "security_hit_rate": nullable_number(row["风险股票命中率"]),
                    "dangerous_risk_security_count": int(row["危险档中的风险股票数"]),
                    "dangerous_security_count": int(row["危险档股票数"]),
                    "unknown_classification_count": int(row["分类未知股票数"]),
                    "warning_rate": nullable_number(row["预警率"]),
                }
            )

    rows, ranking_views = enrich_broker_rankings(rows)
    combined = [item for item in rows if item["risk_view"] == "ALL_RISK_EVENTS"]
    base_url = f"/data/outputs/broker_metrics/{metric_dir.name}"
    return_summary_path = metric_dir / "券商档位收益率表现汇总.csv"
    grade_return_rows: list[dict[str, Any]] = []
    if return_summary_path.is_file():
        with return_summary_path.open(
            "r", encoding="utf-8-sig", newline=""
        ) as stream:
            for row in csv.DictReader(stream):
                grade_return_rows.append(
                    {
                        "broker": row["券商"],
                        "raw_classification": row["原始分类档位"],
                        "return_start_date": row["收益观察开始日"],
                        "return_end_date": row["收益观察结束日"],
                        "classified_trading_day_count": int(
                            row["有档位交易日数"]
                        ),
                        "valid_return_trading_day_count": int(
                            row["有效收益交易日数"]
                        ),
                        "classified_stock_day_count": int(
                            row["档位累计股票日数"]
                        ),
                        "valid_return_stock_day_count": int(
                            row["有效收益股票日数"]
                        ),
                        "return_coverage_rate": nullable_number(
                            row["收益覆盖率"]
                        ),
                        "mean_equal_weight_daily_return": nullable_number(
                            row["日均等权收益率"]
                        ),
                        "compounded_equal_weight_return": nullable_number(
                            row["区间复合等权收益率"]
                        ),
                        "stock_day_weighted_mean_return": nullable_number(
                            row["股票日加权平均收益率"]
                        ),
                        "positive_return_rate": nullable_number(
                            row["上涨股票日占比"]
                        ),
                        "classification_timing": row["档位时点口径"],
                        "return_formula": row["个股收益率公式"],
                        "price_adjustment": row["价格口径"],
                    }
                )
    return {
        "available": True,
        "metric_batch_id": manifest["metric_batch_id"],
        "event_batch_id": manifest["event_batch_id"],
        "analysis_mode": job.get("analysis_mode", context.default_analysis_mode),
        "analysis_mode_label": job.get(
            "analysis_mode_label", "券商03模型评价"
        ),
        "rule_version": manifest["rule_version"],
        "mapping_version": manifest["mapping_version"],
        "schema_version": manifest["output_schema_version"],
        "broker_count": manifest["broker_count"],
        "assessment_count": manifest["assessment_count"],
        "finding_count": manifest["finding_count"],
        "ranking_views": ranking_views,
        "sample_status": ranking_views["ALL_RISK_EVENTS"]["sample_status"],
        "expected_peer_broker_count": ranking_views["ALL_RISK_EVENTS"][
            "expected_broker_count"
        ],
        "observed_broker_count": ranking_views["ALL_RISK_EVENTS"][
            "observed_broker_count"
        ],
        "overview": {
            "defined_warning_count": sum(
                item["defined_warning_count"] for item in combined
            ),
            "unknown_warning_count": sum(
                item["unknown_warning_count"] for item in combined
            ),
            "hit_event_count": sum(item["hit_event_count"] for item in combined),
            "broker_count": len(combined),
        },
        "rows": rows,
        "grade_return_available": bool(grade_return_rows),
        "grade_return_rows": grade_return_rows,
        "files": {
            "final_summary": f"{base_url}/券商预警指标最终汇总.csv",
            "event_hits": f"{base_url}/all_broker_risk_event_hits.csv",
            "event_assessments": f"{base_url}/event_broker_assessments.csv",
            "warning_days": f"{base_url}/broker_warning_days_summary.csv",
            "hit_rates": f"{base_url}/broker_hit_rate_summary.csv",
            "warning_rates": f"{base_url}/broker_warning_rate_summary.csv",
            "grade_return_summary": (
                f"{base_url}/券商档位收益率表现汇总.csv"
                if return_summary_path.is_file()
                else None
            ),
            "grade_return_daily": (
                f"{base_url}/券商档位逐日收益率明细.csv"
                if (metric_dir / "券商档位逐日收益率明细.csv").is_file()
                else None
            ),
            "findings": f"{base_url}/broker_metric_findings.csv",
            "manifest": f"{base_url}/broker_metric_manifest.json",
        },
    }

def broker_drilldown_payload(
    event_batch_id: str,
    *,
    context: BrokerResultsContext,
    broker: str,
    risk_view: str,
    level: str,
    security_query: str,
    hit_status: str,
    page: int,
    page_size: int,
) -> dict[str, Any]:
    """Read and verify the frozen event×broker evidence before any aggregation."""
    context.assert_readable(event_batch_id)
    matched_result = find_broker_metric_result(event_batch_id, context=context)
    if matched_result is None:
        raise FileNotFoundError("该风险事件批次尚未生成券商预警指标")
    metric_dir, manifest = matched_result
    file_meta = dict(manifest.get("output_files") or {}).get(
        "event_broker_assessments.csv", {}
    )
    rows = verified_assessment_rows(
        metric_dir / "event_broker_assessments.csv",
        expected_sha256=str(file_meta.get("sha256") or "") or None,
        expected_row_count=(
            int(file_meta["row_count"]) if file_meta.get("row_count") is not None else None
        ),
        event_batch_id=event_batch_id,
    )
    payload = broker_drilldown(
        rows,
        event_batch_id=event_batch_id,
        broker=broker,
        risk_view=risk_view,
        level=level,
        security_query=security_query,
        hit_status=hit_status,
        page=page,
        page_size=page_size,
    )
    payload.update(
        {
            "metric_batch_id": manifest.get("metric_batch_id"),
            "source_verified": True,
            "source_sha256": file_meta.get("sha256"),
            "source_row_count": file_meta.get("row_count"),
        }
    )
    return payload
