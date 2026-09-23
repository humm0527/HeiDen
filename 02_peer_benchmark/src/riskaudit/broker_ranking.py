"""Deterministic broker rankings and evidence drilldown over frozen CSV outputs."""

from __future__ import annotations

import csv
from hashlib import sha256
from io import StringIO
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable


EXPECTED_PEER_BROKER_COUNT = 21
RANKING_METRICS = (
    "event_hit_rate",
    "security_hit_rate",
    "warning_rate",
    "warning_days_median",
    "warning_days_mean",
)
COMPOSITE_METRICS = (
    "event_hit_rate",
    "security_hit_rate",
    "warning_rate",
    "warning_days_median",
)
RISK_VIEWS = {
    "ALL_RISK_EVENTS": None,
    "NEW_ST": "NEW_ST",
    "NON_ST_CONTINUOUS_LIMIT_DOWN": "NON_ST_CONTINUOUS_LIMIT_DOWN",
}


def _competition_ranks(
    rows: Iterable[dict[str, Any]], metric: str
) -> dict[str, int | None]:
    candidates = [
        (str(row["broker"]), float(row[metric]))
        for row in rows
        if row.get(metric) is not None
    ]
    candidates.sort(key=lambda item: (-item[1], item[0]))
    result: dict[str, int | None] = {str(row["broker"]): None for row in rows}
    previous: float | None = None
    previous_rank = 0
    for position, (broker, value) in enumerate(candidates, start=1):
        if previous is None or value != previous:
            previous_rank = position
            previous = value
        result[broker] = previous_rank
    return result


def enrich_broker_rankings(
    rows: list[dict[str, Any]], *, expected_broker_count: int = EXPECTED_PEER_BROKER_COUNT
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """Add stable single-metric ranks and guarded composite ranks per risk view."""
    enriched = [dict(row) for row in rows]
    views: dict[str, dict[str, Any]] = {}
    for risk_view in RISK_VIEWS:
        view_rows = [row for row in enriched if row.get("risk_view") == risk_view]
        broker_count = len({str(row["broker"]) for row in view_rows})
        metric_ranks = {
            metric: _competition_ranks(view_rows, metric) for metric in RANKING_METRICS
        }
        for row in view_rows:
            broker = str(row["broker"])
            row["ranks"] = {
                metric: metric_ranks[metric][broker] for metric in RANKING_METRICS
            }
        complete_sample = broker_count == expected_broker_count
        complete_values = complete_sample and all(
            row.get(metric) is not None
            for row in view_rows
            for metric in COMPOSITE_METRICS
        )
        if complete_values:
            denominator = max(expected_broker_count - 1, 1)
            for row in view_rows:
                row["composite_score"] = round(
                    mean(
                        (
                            expected_broker_count
                            - int(row["ranks"][metric])
                        )
                        / denominator
                        * 100
                        for metric in COMPOSITE_METRICS
                    ),
                    6,
                )
            composite_ranks = _competition_ranks(view_rows, "composite_score")
            for row in view_rows:
                row["composite_rank"] = composite_ranks[str(row["broker"])]
        else:
            for row in view_rows:
                row["composite_score"] = None
                row["composite_rank"] = None
        views[risk_view] = {
            "risk_view": risk_view,
            "expected_broker_count": expected_broker_count,
            "observed_broker_count": broker_count,
            "sample_status": "COMPLETE" if complete_sample else "INCOMPLETE_SAMPLE",
            "composite_available": complete_values,
            "composite_method": (
                "四项竞赛排名百分位等权"
                if complete_values
                else "样本不足 21 家或存在缺失值，不生成综合排名"
            ),
            "metrics": list(RANKING_METRICS),
        }
    return enriched, views


def _hit_status(row: dict[str, str]) -> str:
    value = str(row.get("是否命中") or "").strip().lower()
    if value in {"true", "1", "yes"}:
        return "HIT"
    if (
        str(row.get("提前识别状态") or "").strip().upper() == "UNKNOWN"
        or str(row.get("最终分类档位") or "").strip().upper() == "UNKNOWN"
        or not value
    ):
        return "UNKNOWN"
    return "MISSED"


def _nullable_float(value: Any) -> float | None:
    text = str(value or "").strip()
    return float(text) if text else None


def _assessment_record(row: dict[str, str]) -> dict[str, Any]:
    return {
        "event_key": str(row.get("事件唯一键") or "").strip(),
        "market_code": str(row.get("交易市场代码") or "").strip(),
        "security_code": str(row.get("证券代码") or "").strip(),
        "event_type": str(row.get("风险事件类型") or "").strip(),
        "first_fact_date": str(row.get("第一次事实日期") or "").strip(),
        "risk_date": str(row.get("风险认定日期") or "").strip(),
        "broker": str(row.get("券商标识") or "").strip(),
        "cutoff_trading_date": str(row.get("评价截止交易日") or "").strip(),
        "cutoff_timestamp": str(row.get("评价截止时点") or "").strip(),
        "classification_date": str(row.get("选中分类日期") or "").strip(),
        "classification": str(row.get("原始分类值") or "").strip(),
        "mapped_classification": str(row.get("最终分类档位") or "").strip(),
        "identification_status": str(row.get("提前识别状态") or "").strip(),
        "hit_status": _hit_status(row),
        "warning_days_status": str(row.get("预警天数状态") or "").strip(),
        "warning_days": _nullable_float(row.get("预警交易日数")),
        "reason_code": str(row.get("结果原因代码") or "").strip(),
        "source_file": str(row.get("来源季度文件") or "").strip(),
        "source_sha256": str(row.get("来源文件SHA256") or "").strip(),
        "source_row_number": str(row.get("来源数据行号") or "").strip(),
        "source_record_id": str(row.get("来源记录标识") or "").strip(),
        "calendar_snapshot_id": str(row.get("市场日历快照标识") or "").strip(),
        "metric_batch_id": str(row.get("指标计算批次标识") or "").strip(),
    }


def verified_assessment_rows(
    assessment_path: Path,
    *,
    expected_sha256: str | None,
    expected_row_count: int | None,
    event_batch_id: str,
) -> list[dict[str, Any]]:
    if not assessment_path.is_file():
        raise FileNotFoundError("券商事件评价明细不存在")
    body = assessment_path.read_bytes()
    actual_hash = sha256(body).hexdigest()
    if expected_sha256 and actual_hash != expected_sha256:
        raise ValueError("券商事件评价明细 SHA-256 与 manifest 不一致")
    text = body.decode("utf-8-sig")
    rows = [_assessment_record(row) for row in csv.DictReader(StringIO(text))]
    if expected_row_count is not None and len(rows) != int(expected_row_count):
        raise ValueError("券商事件评价明细行数与 manifest 不一致")
    batch_ids = {row["metric_batch_id"] for row in rows if row["metric_batch_id"]}
    if len(batch_ids) > 1:
        raise ValueError("券商事件评价明细混入多个指标批次")
    # The risk event batch is verified by the enclosing broker manifest. Keep it in
    # the return contract so exports remain explicitly scoped.
    for row in rows:
        row["event_batch_id"] = event_batch_id
    return rows


def broker_drilldown(
    rows: list[dict[str, Any]],
    *,
    event_batch_id: str,
    broker: str,
    risk_view: str = "ALL_RISK_EVENTS",
    level: str = "securities",
    security_query: str = "",
    hit_status: str = "ALL",
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    if risk_view not in RISK_VIEWS:
        raise ValueError("不支持的风险类型")
    if level not in {"securities", "events"}:
        raise ValueError("下钻层级只能是 securities 或 events")
    normalized_hit = hit_status.strip().upper() or "ALL"
    if normalized_hit not in {"ALL", "HIT", "MISSED", "UNKNOWN"}:
        raise ValueError("不支持的命中状态筛选")
    if page < 1 or page_size < 1 or page_size > 10000:
        raise ValueError("分页参数超出允许范围")
    expected_event_type = RISK_VIEWS[risk_view]
    query = security_query.strip().upper()
    filtered = [
        row
        for row in rows
        if row["broker"] == broker
        and (expected_event_type is None or row["event_type"] == expected_event_type)
        and (not query or query in row["security_code"].upper())
        and (normalized_hit == "ALL" or row["hit_status"] == normalized_hit)
    ]
    if not any(row["broker"] == broker for row in rows):
        raise ValueError(f"券商不存在于当前指标批次: {broker}")
    if level == "events":
        result_rows = sorted(
            filtered,
            key=lambda row: (
                str(row["risk_date"]),
                str(row["security_code"]),
                str(row["event_key"]),
            ),
            reverse=True,
        )
    else:
        groups: dict[str, list[dict[str, Any]]] = {}
        for row in filtered:
            groups.setdefault(str(row["security_code"]), []).append(row)
        result_rows = []
        for security_code, group in groups.items():
            warnings = [
                float(row["warning_days"])
                for row in group
                if row.get("warning_days") is not None
            ]
            hit_count = sum(row["hit_status"] == "HIT" for row in group)
            result_rows.append(
                {
                    "security_code": security_code,
                    "market_code": str(group[0]["market_code"]),
                    "event_count": len(group),
                    "hit_event_count": hit_count,
                    "unknown_event_count": sum(
                        row["hit_status"] == "UNKNOWN" for row in group
                    ),
                    "event_hit_rate": hit_count / len(group),
                    "defined_warning_count": len(warnings),
                    "warning_days_earliest": max(warnings) if warnings else None,
                    "warning_days_median": median(warnings) if warnings else None,
                    "latest_risk_date": max(str(row["risk_date"]) for row in group),
                }
            )
        result_rows.sort(
            key=lambda row: (
                -float(row["event_hit_rate"]),
                -int(row["event_count"]),
                str(row["security_code"]),
            )
        )
    total = len(result_rows)
    start = (page - 1) * page_size
    page_rows = result_rows[start : start + page_size]
    return {
        "event_batch_id": event_batch_id,
        "broker": broker,
        "risk_view": risk_view,
        "level": level,
        "security_query": security_query,
        "hit_status": normalized_hit,
        "page": page,
        "page_size": page_size,
        "total_count": total,
        "rows": page_rows,
    }


def drilldown_csv(payload: dict[str, Any]) -> bytes:
    rows = list(payload.get("rows") or [])
    if payload.get("level") == "events":
        fields = (
            "event_batch_id",
            "broker",
            "event_key",
            "market_code",
            "security_code",
            "event_type",
            "first_fact_date",
            "risk_date",
            "cutoff_trading_date",
            "cutoff_timestamp",
            "classification_date",
            "classification",
            "mapped_classification",
            "identification_status",
            "hit_status",
            "warning_days_status",
            "warning_days",
            "reason_code",
            "source_file",
            "source_sha256",
            "source_row_number",
            "source_record_id",
            "calendar_snapshot_id",
            "metric_batch_id",
        )
    else:
        fields = (
            "event_batch_id",
            "broker",
            "market_code",
            "security_code",
            "event_count",
            "hit_event_count",
            "unknown_event_count",
            "event_hit_rate",
            "defined_warning_count",
            "warning_days_earliest",
            "warning_days_median",
            "latest_risk_date",
        )
    stream = StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {
                field: (
                    payload.get("event_batch_id")
                    if field == "event_batch_id"
                    else payload.get("broker")
                    if field == "broker"
                    else row.get(field)
                )
                for field in fields
            }
        )
    return ("\ufeff" + stream.getvalue()).encode("utf-8")
