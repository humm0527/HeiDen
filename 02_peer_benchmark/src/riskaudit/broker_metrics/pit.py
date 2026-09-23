"""
文件作用：执行事件×券商 D-050/D-051 PIT 选择与逐交易日最终分类状态链。
编辑记录：
【首次生成：2026-08-12，实现截止日前分类携带、ST 覆盖、未知中断和危险轮次证据。】
【二次编辑内容：2026-08-12，将正式市场日历快照标识贯穿事件×券商明细。】
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Iterable, Mapping

from .calendar import MarketCalendar
from .classification import resolve_final_classification
from .models import (
    BrokerClassificationRecord,
    BrokerMappingBook,
    BrokerMetricRules,
    EventBrokerAssessment,
    RiskEventRecord,
)
from .warning_days import calculate_warning_trading_days


StStateKey = tuple[str, str, date]


def assess_event_broker(
    event: RiskEventRecord,
    broker_id: str,
    records: Iterable[BrokerClassificationRecord],
    st_states: Mapping[StStateKey, bool | None],
    calendar: MarketCalendar,
    market_aliases: Mapping[str, str],
    mapping: BrokerMappingBook,
    rules: BrokerMetricRules,
    *,
    metric_batch_id: str = "",
    calendar_snapshot_id: str = "",
) -> EventBrokerAssessment:
    calendar_market = market_aliases.get(event.market_code)
    if calendar_market is None:
        raise ValueError(f"BM202_CALENDAR_ALIAS_MISSING: {event.market_code}")
    cutoff = calendar.previous_trading_day(calendar_market, event.first_fact_date)
    relevant = sorted(
        (
            record
            for record in records
            if record.broker_id == broker_id
            and record.market_code == event.market_code
            and record.security_code == event.security_code
            and record.classification_date <= cutoff
        ),
        key=lambda item: (
            item.classification_date,
            item.source_row_number or 0,
            item.source_record_id,
        ),
    )
    by_date: dict[date, BrokerClassificationRecord] = {}
    for record in relevant:
        if record.classification_date in by_date:
            raise ValueError(
                "BM304_PIT_VERSION_CONFLICT: "
                f"{broker_id} {event.security_code} {record.classification_date}"
            )
        by_date[record.classification_date] = record

    active: BrokerClassificationRecord | None = None
    final_bucket: str | None = None
    final_resolution = "NO_USABLE_CLASSIFICATION"
    mapping_record_id = ""
    current_run_start: date | None = None
    current_run_left_censored = False
    run_has_broker_anchor = False
    previous_final: str | None = None
    known_state_seen = False

    for trading_date in calendar.through(calendar_market, cutoff):
        if trading_date in by_date:
            active = by_date[trading_date]
        st_state = st_states.get(
            (event.market_code, event.security_code, trading_date), None
        )
        resolved = resolve_final_classification(active, st_state, mapping)
        final_bucket = resolved.final_bucket
        final_resolution = resolved.resolution
        mapping_record_id = resolved.mapping_record_id

        if final_bucket == "UNKNOWN":
            current_run_start = None
            current_run_left_censored = False
            run_has_broker_anchor = False
            previous_final = "UNKNOWN"
            continue
        if final_bucket == "NON_DANGEROUS" or final_bucket is None:
            current_run_start = None
            current_run_left_censored = False
            run_has_broker_anchor = False
            if final_bucket == "NON_DANGEROUS":
                known_state_seen = True
            previous_final = final_bucket
            continue

        exact_broker_danger = _is_exact_broker_danger(active, mapping)
        new_record_today = active is not None and active.classification_date == trading_date
        if previous_final != "DANGEROUS":
            if exact_broker_danger and new_record_today:
                current_run_start = active.classification_date
                run_has_broker_anchor = True
                current_run_left_censored = (
                    not known_state_seen
                    and previous_final is None
                    and trading_date == calendar.dates(calendar_market)[0]
                )
            else:
                current_run_start = None
                run_has_broker_anchor = False
                current_run_left_censored = False
        elif not run_has_broker_anchor and exact_broker_danger and new_record_today:
            current_run_start = active.classification_date
            run_has_broker_anchor = True
            current_run_left_censored = False
        known_state_seen = True
        previous_final = "DANGEROUS"

    selected = active
    cutoff_st = st_states.get(
        (event.market_code, event.security_code, cutoff), None
    )
    cutoff_st_state = "UNKNOWN" if cutoff_st is None else ("ST" if cutoff_st else "NON_ST")

    if final_bucket == "UNKNOWN":
        return _assessment(
            event,
            broker_id,
            cutoff,
            selected,
            cutoff_st_state,
            final_bucket,
            final_resolution,
            mapping,
            mapping_record_id,
            "UNKNOWN",
            None,
            "UNKNOWN",
            None,
            None,
            _unknown_reason(final_resolution),
            rules,
            metric_batch_id,
            calendar_snapshot_id,
        )
    if final_bucket != "DANGEROUS":
        return _assessment(
            event,
            broker_id,
            cutoff,
            selected,
            cutoff_st_state,
            final_bucket,
            final_resolution,
            mapping,
            mapping_record_id,
            "NOT_IDENTIFIED",
            False,
            "NOT_APPLICABLE",
            None,
            None,
            (
                "BM302_NO_USABLE_CLASSIFICATION"
                if selected is None
                else "BM306_FINAL_NON_DANGEROUS"
            ),
            rules,
            metric_batch_id,
            calendar_snapshot_id,
        )
    if not run_has_broker_anchor or current_run_start is None:
        return _assessment(
            event,
            broker_id,
            cutoff,
            selected,
            cutoff_st_state,
            final_bucket,
            final_resolution,
            mapping,
            mapping_record_id,
            "IDENTIFIED",
            True,
            "UNKNOWN",
            None,
            None,
            "BM305_NO_BROKER_START_FOR_ST_OVERRIDE",
            rules,
            metric_batch_id,
            calendar_snapshot_id,
        )
    if current_run_left_censored:
        return _assessment(
            event,
            broker_id,
            cutoff,
            selected,
            cutoff_st_state,
            final_bucket,
            final_resolution,
            mapping,
            mapping_record_id,
            "IDENTIFIED",
            True,
            "UNKNOWN",
            current_run_start,
            None,
            "BM303_DANGEROUS_RUN_LEFT_CENSORED",
            rules,
            metric_batch_id,
            calendar_snapshot_id,
        )
    warning_days = calculate_warning_trading_days(
        calendar, calendar_market, current_run_start, event.first_fact_date
    )
    return _assessment(
        event,
        broker_id,
        cutoff,
        selected,
        cutoff_st_state,
        final_bucket,
        final_resolution,
        mapping,
        mapping_record_id,
        "IDENTIFIED",
        True,
        "DEFINED",
        current_run_start,
        warning_days,
        "BM300_IDENTIFIED",
        rules,
        metric_batch_id,
        calendar_snapshot_id,
    )


def assess_all_events(
    events: Iterable[RiskEventRecord],
    broker_ids: Iterable[str],
    records: Iterable[BrokerClassificationRecord],
    st_states: Mapping[StStateKey, bool | None],
    calendar: MarketCalendar,
    market_aliases: Mapping[str, str],
    mapping: BrokerMappingBook,
    rules: BrokerMetricRules,
    *,
    metric_batch_id: str = "",
    calendar_snapshot_id: str = "",
) -> tuple[EventBrokerAssessment, ...]:
    indexed: dict[tuple[str, str, str], list[BrokerClassificationRecord]] = defaultdict(list)
    for record in records:
        indexed[(record.broker_id, record.market_code, record.security_code)].append(record)
    output = []
    for event in sorted(events, key=lambda item: item.event_key):
        for broker_id in sorted(set(broker_ids)):
            output.append(
                assess_event_broker(
                    event,
                    broker_id,
                    indexed.get((broker_id, event.market_code, event.security_code), ()),
                    st_states,
                    calendar,
                    market_aliases,
                    mapping,
                    rules,
                    metric_batch_id=metric_batch_id,
                    calendar_snapshot_id=calendar_snapshot_id,
                )
            )
    keys = {(item.event.event_key, item.broker_id) for item in output}
    if len(keys) != len(output):
        raise ValueError("BM401_ASSESSMENT_KEY_DUPLICATE")
    return tuple(output)


def _is_exact_broker_danger(
    record: BrokerClassificationRecord | None,
    mapping: BrokerMappingBook,
) -> bool:
    if record is None or record.raw_classification == "":
        return False
    mapped, _ = mapping.map_value(record.broker_id, record.raw_classification)
    return mapped == "DANGEROUS"


def _unknown_reason(resolution: str) -> str:
    return {
        "ST_STATUS_MISSING": "BM203_ST_STATUS_MISSING",
        "MAPPING_UNKNOWN": "BM301_MAPPING_UNKNOWN",
    }.get(resolution, "BM307_PIT_EVIDENCE_UNKNOWN")


def _assessment(
    event: RiskEventRecord,
    broker_id: str,
    cutoff: date,
    selected: BrokerClassificationRecord | None,
    cutoff_st_state: str,
    final_bucket: str | None,
    resolution: str,
    mapping: BrokerMappingBook,
    mapping_record_id: str,
    identification_status: str,
    is_hit: bool | None,
    warning_days_status: str,
    run_start: date | None,
    warning_days: int | None,
    reason_code: str,
    rules: BrokerMetricRules,
    metric_batch_id: str,
    calendar_snapshot_id: str,
) -> EventBrokerAssessment:
    return EventBrokerAssessment(
        event=event,
        broker_id=broker_id,
        cutoff_trading_date=cutoff,
        selected_record=selected,
        cutoff_st_state=cutoff_st_state,
        final_bucket=final_bucket,
        classification_resolution=resolution,
        mapping_version=mapping.mapping_version,
        mapping_record_id=mapping_record_id,
        identification_status=identification_status,
        is_hit=is_hit,
        warning_days_status=warning_days_status,
        dangerous_run_start_date=run_start,
        warning_trading_days=warning_days,
        reason_code=reason_code,
        metric_rule_version=rules.rule_version,
        metric_batch_id=metric_batch_id,
        calendar_snapshot_id=calendar_snapshot_id,
    )
