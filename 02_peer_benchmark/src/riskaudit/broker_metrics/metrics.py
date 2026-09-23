"""
文件作用：从事件×券商明细确定性汇总预警天数分布、风险命中率和预警率。
编辑记录：
【首次生成：2026-08-12，实现 D-052/D-053/D-064 的三类视图、去重分母与 Decimal 统计。】
【二次编辑内容：2026-08-12，补齐零危险档快照，并按证券去重观察期未知分类数。】
【三次编辑内容：2026-08-12，按 D-069 将天数、命中率和观察期预警率合并为单一中文最终汇总。】
【四次改进：2026-09-09，新增券商03原始 A-G 档位风险事件汇总及审计明细口径。】
"""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from statistics import median
from typing import Iterable

from .models import DangerousExposure, EventBrokerAssessment, RiskEventRecord


EVENT_VIEWS = (
    "NON_ST_CONTINUOUS_LIMIT_DOWN",
    "NEW_ST",
    "ALL_RISK_EVENTS",
)

SINGLE_BROKER_ID = "券商03"
SINGLE_GRADE_ORDER = (*tuple("ABCDEFG"), "BLANK", "UNKNOWN")


def single_grade_event_artifacts(
    assessments: Iterable[EventBrokerAssessment],
    *,
    decimal_scale: int = 6,
    metric_rule_version: str = "v6",
    metric_batch_id: str = "",
) -> tuple[list[dict], list[dict]]:
    """按券商03 PIT 原始 A-G 档位生成稳定汇总与可追溯明细。"""
    items = tuple(item for item in assessments if item.broker_id == SINGLE_BROKER_ID)
    details = []
    for item in items:
        raw_value = (
            item.selected_record.raw_classification.strip().upper()
            if item.selected_record is not None
            else ""
        )
        grade = (
            raw_value
            if raw_value in set("ABCDEFG")
            else "BLANK"
            if item.selected_record is not None and not raw_value
            else "UNKNOWN"
        )
        details.append(
            {
                "event_key": "|".join(
                    (
                        item.event.market_code,
                        item.event.security_code,
                        item.event.event_type,
                        item.event.first_fact_date.isoformat(),
                    )
                ),
                "market_code": item.event.market_code,
                "security_code": item.event.security_code,
                "event_type": item.event.event_type,
                "first_fact_date": item.event.first_fact_date.isoformat(),
                "risk_date": (
                    item.event.risk_date.isoformat() if item.event.risk_date else ""
                ),
                "selected_classification_date": (
                    item.selected_record.classification_date.isoformat()
                    if item.selected_record is not None
                    else ""
                ),
                "raw_classification": raw_value,
                "grade": grade,
                "final_bucket": item.final_bucket or "",
                "classification_resolution": item.classification_resolution,
                "identification_status": item.identification_status,
                "is_hit": item.is_hit,
                "warning_days_status": item.warning_days_status,
                "warning_trading_days": item.warning_trading_days,
                "mapping_version": item.mapping_version,
                "metric_rule_version": item.metric_rule_version,
                "metric_batch_id": item.metric_batch_id,
            }
        )

    summaries = []
    for view in EVENT_VIEWS:
        view_rows = [
            row
            for row in details
            if view == "ALL_RISK_EVENTS" or row["event_type"] == view
        ]
        for grade in SINGLE_GRADE_ORDER:
            group = [row for row in view_rows if row["grade"] == grade]
            securities = {(row["market_code"], row["security_code"]) for row in group}
            hit_securities = {
                (row["market_code"], row["security_code"])
                for row in group
                if row["is_hit"] is True
            }
            warning_values = [
                row["warning_trading_days"]
                for row in group
                if row["warning_days_status"] == "DEFINED"
                and row["warning_trading_days"] is not None
            ]
            summaries.append(
                {
                    "broker_id": SINGLE_BROKER_ID,
                    "event_type_view": view,
                    "grade": grade,
                    "risk_event_count": len(group),
                    "hit_event_count": sum(row["is_hit"] is True for row in group),
                    "unknown_event_count": sum(row["is_hit"] is None for row in group),
                    "event_hit_rate": _rate(
                        sum(row["is_hit"] is True for row in group), len(group)
                    ),
                    "risk_security_count": len(securities),
                    "hit_risk_security_count": len(hit_securities),
                    "security_hit_rate": _rate(len(hit_securities), len(securities)),
                    "defined_warning_days_count": len(warning_values),
                    "warning_days_min": min(warning_values) if warning_values else None,
                    "warning_days_max": max(warning_values) if warning_values else None,
                    "warning_days_median": (
                        _quantize(Decimal(str(median(warning_values))), decimal_scale)
                        if warning_values
                        else None
                    ),
                    "warning_days_mean": (
                        _quantize(
                            sum(Decimal(value) for value in warning_values)
                            / Decimal(len(warning_values)),
                            decimal_scale,
                        )
                        if warning_values
                        else None
                    ),
                    "result_status": "DEFINED" if group else "NOT_APPLICABLE",
                    "metric_rule_version": metric_rule_version,
                    "metric_batch_id": metric_batch_id,
                }
            )
    return summaries, details


def warning_days_distribution(
    assessments: Iterable[EventBrokerAssessment],
    *,
    decimal_scale: int = 6,
    metric_rule_version: str = "v4",
    metric_batch_id: str = "",
) -> list[dict]:
    items = tuple(assessments)
    output = []
    for broker_id in sorted({item.broker_id for item in items}):
        broker_items = [item for item in items if item.broker_id == broker_id]
        for view in EVENT_VIEWS:
            group = _assessment_view(broker_items, view)
            values = [
                item.warning_trading_days
                for item in group
                if item.warning_days_status == "DEFINED"
                and item.warning_trading_days is not None
            ]
            output.append(
                {
                    "broker_id": broker_id,
                    "event_type_view": view,
                    "defined_sample_count": len(values),
                    "identified_days_unknown_count": sum(
                        item.identification_status == "IDENTIFIED"
                        and item.warning_days_status == "UNKNOWN"
                        for item in group
                    ),
                    "not_identified_count": sum(
                        item.identification_status == "NOT_IDENTIFIED" for item in group
                    ),
                    "unknown_assessment_count": sum(
                        item.identification_status == "UNKNOWN" for item in group
                    ),
                    "warning_days_min": min(values) if values else None,
                    "warning_days_max": max(values) if values else None,
                    "warning_days_median": (
                        _quantize(Decimal(str(median(values))), decimal_scale)
                        if values
                        else None
                    ),
                    "warning_days_mean": (
                        _quantize(
                            sum(Decimal(value) for value in values) / Decimal(len(values)),
                            decimal_scale,
                        )
                        if values
                        else None
                    ),
                    "result_status": "DEFINED" if values else "NOT_APPLICABLE",
                    "metric_rule_version": metric_rule_version,
                    "metric_batch_id": metric_batch_id,
                }
            )
    return output


def risk_hit_rates(
    assessments: Iterable[EventBrokerAssessment],
    *,
    metric_rule_version: str = "v4",
    metric_batch_id: str = "",
) -> list[dict]:
    items = tuple(assessments)
    output = []
    for broker_id in sorted({item.broker_id for item in items}):
        broker_items = [item for item in items if item.broker_id == broker_id]
        for view in EVENT_VIEWS:
            group = _assessment_view(broker_items, view)
            security_groups: dict[tuple[str, str], list[EventBrokerAssessment]] = defaultdict(list)
            for item in group:
                security_groups[(item.event.market_code, item.event.security_code)].append(item)
            event_denominator = len(group)
            event_numerator = sum(item.is_hit is True for item in group)
            security_denominator = len(security_groups)
            security_numerator = sum(
                any(item.is_hit is True for item in values)
                for values in security_groups.values()
            )
            output.append(
                {
                    "broker_id": broker_id,
                    "event_type_view": view,
                    "risk_event_count": event_denominator,
                    "hit_event_count": event_numerator,
                    "unknown_event_count": sum(item.is_hit is None for item in group),
                    "event_hit_rate": _rate(event_numerator, event_denominator),
                    "risk_security_count": security_denominator,
                    "hit_risk_security_count": security_numerator,
                    "unknown_security_count": sum(
                        not any(item.is_hit is True for item in values)
                        and any(item.is_hit is None for item in values)
                        for values in security_groups.values()
                    ),
                    "security_hit_rate": _rate(
                        security_numerator, security_denominator
                    ),
                    "result_status": (
                        "DEFINED" if event_denominator else "NOT_APPLICABLE"
                    ),
                    "metric_rule_version": metric_rule_version,
                    "metric_batch_id": metric_batch_id,
                }
            )
    return output


def warning_rates(
    exposures: Iterable[DangerousExposure],
    events: Iterable[RiskEventRecord],
    broker_ids: Iterable[str] | None = None,
    *,
    metric_rule_version: str = "v4",
    metric_batch_id: str = "",
    unknown_counts: dict[tuple[str, object], int] | None = None,
    snapshot_dates: Iterable[object] | None = None,
    period_unknown_counts: dict[str, int] | None = None,
) -> list[dict]:
    exposure_items = tuple(exposures)
    event_items = tuple(events)
    brokers = sorted(
        set(broker_ids or ()) | {item.broker_id for item in exposure_items}
    )
    output = []
    event_index: dict[tuple[str, str], list[RiskEventRecord]] = defaultdict(list)
    for event in event_items:
        event_index[(event.market_code, event.security_code)].append(event)
    for broker_id in brokers:
        broker_exposures = [item for item in exposure_items if item.broker_id == broker_id]
        broker_snapshot_dates = sorted(
            set(snapshot_dates or ())
            | {item.snapshot_date for item in broker_exposures}
        )
        for snapshot_date in broker_snapshot_dates:
            snapshot = {
                (item.market_code, item.security_code)
                for item in broker_exposures
                if item.snapshot_date == snapshot_date
            }
            for view in EVENT_VIEWS:
                numerator = sum(
                    _has_later_matching_event(
                        event_index.get(security, ()), snapshot_date, view
                    )
                    for security in snapshot
                )
                output.append(
                    _warning_rate_row(
                        broker_id,
                        view,
                        "SNAPSHOT",
                        snapshot_date.isoformat(),
                        numerator,
                        len(snapshot),
                        metric_rule_version,
                        metric_batch_id,
                        (unknown_counts or {}).get((broker_id, snapshot_date), 0),
                    )
                )
        period_first_exposure: dict[tuple[str, str], object] = {}
        for item in broker_exposures:
            security = (item.market_code, item.security_code)
            current = period_first_exposure.get(security)
            if current is None or item.snapshot_date < current:
                period_first_exposure[security] = item.snapshot_date
        for view in EVENT_VIEWS:
            numerator = sum(
                _has_later_matching_event(
                    event_index.get(security, ()), first_date, view
                )
                for security, first_date in period_first_exposure.items()
            )
            output.append(
                _warning_rate_row(
                    broker_id,
                    view,
                    "OBSERVATION_PERIOD",
                    "",
                    numerator,
                    len(period_first_exposure),
                    metric_rule_version,
                    metric_batch_id,
                    (period_unknown_counts or {}).get(broker_id, 0),
                )
            )
    return output


def final_broker_metric_summary(
    warning_days_summary: Iterable[dict],
    hit_rate_summary: Iterable[dict],
    warning_rate_summary: Iterable[dict],
) -> list[dict]:
    """合并用户确认的三类最终指标，预警率只取观察期去重口径。"""
    days = {(row["broker_id"], row["event_type_view"]): row for row in warning_days_summary}
    hits = {(row["broker_id"], row["event_type_view"]): row for row in hit_rate_summary}
    rates = {
        (row["broker_id"], row["event_type_view"]): row
        for row in warning_rate_summary
        if row["aggregation_scope"] == "OBSERVATION_PERIOD"
    }
    if set(days) != set(hits) or set(days) != set(rates):
        raise ValueError("BM402_SUMMARY_RECONCILIATION_FAILED: final summary keys")
    view_names = {
        "NON_ST_CONTINUOUS_LIMIT_DOWN": "非ST连续跌停风险股票",
        "NEW_ST": "新增ST风险股票",
        "ALL_RISK_EVENTS": "两类风险股票合计",
    }
    rows = []
    for broker_id, view in sorted(days):
        day = days[(broker_id, view)]
        hit = hits[(broker_id, view)]
        rate = rates[(broker_id, view)]
        rows.append({
            "broker_id": broker_id,
            "risk_type_zh": view_names[view],
            "warning_days_defined_count": day["defined_sample_count"],
            "hit_warning_days_unknown_count": day["identified_days_unknown_count"],
            "warning_days_min": day["warning_days_min"],
            "warning_days_max": day["warning_days_max"],
            "warning_days_median": day["warning_days_median"],
            "warning_days_mean": day["warning_days_mean"],
            "risk_event_count": hit["risk_event_count"],
            "hit_event_count": hit["hit_event_count"],
            "event_hit_rate": hit["event_hit_rate"],
            "risk_security_count": hit["risk_security_count"],
            "hit_risk_security_count": hit["hit_risk_security_count"],
            "security_hit_rate": hit["security_hit_rate"],
            "dangerous_risk_security_count": rate["dangerous_risk_security_count"],
            "dangerous_security_count": rate["dangerous_security_count"],
            "unknown_classification_security_count": rate["unknown_classification_security_count"],
            "warning_rate": rate["warning_rate"],
            "metric_rule_version": day["metric_rule_version"],
            "metric_batch_id": day["metric_batch_id"],
        })
    return rows


def _assessment_view(
    items: Iterable[EventBrokerAssessment], view: str
) -> list[EventBrokerAssessment]:
    if view == "ALL_RISK_EVENTS":
        return list(items)
    return [item for item in items if item.event.event_type == view]


def _quantize(value: Decimal, scale: int) -> Decimal:
    quantum = Decimal(1).scaleb(-scale)
    return value.quantize(quantum, rounding=ROUND_HALF_UP)


def _rate(numerator: int, denominator: int) -> Decimal | None:
    if denominator == 0:
        return None
    return _quantize(Decimal(numerator) / Decimal(denominator), 6)


def _has_later_matching_event(
    events: Iterable[RiskEventRecord], snapshot_date, view: str
) -> bool:
    return any(
        event.first_fact_date > snapshot_date
        and (view == "ALL_RISK_EVENTS" or event.event_type == view)
        for event in events
    )


def _warning_rate_row(
    broker_id: str,
    view: str,
    aggregation_scope: str,
    snapshot_date: str,
    numerator: int,
    denominator: int,
    metric_rule_version: str,
    metric_batch_id: str,
    unknown_count: int,
) -> dict:
    return {
        "broker_id": broker_id,
        "event_type_view": view,
        "aggregation_scope": aggregation_scope,
        "snapshot_trading_date": snapshot_date,
        "dangerous_risk_security_count": numerator,
        "dangerous_security_count": denominator,
        "unknown_classification_security_count": unknown_count,
        "warning_rate": _rate(numerator, denominator),
        "result_status": "DEFINED" if denominator else "NOT_APPLICABLE",
        "metric_rule_version": metric_rule_version,
        "metric_batch_id": metric_batch_id,
    }
