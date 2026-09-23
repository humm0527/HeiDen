"""Event assessment, exposure construction, and result reconciliation."""

from __future__ import annotations

from collections.abc import Callable
from typing import Iterable, Mapping

from riskaudit.cancellation import raise_if_canceled

from .models import (
    BrokerClassificationRecord,
    BrokerMetricFinding,
    DangerousExposure,
    EventBrokerAssessment,
)
from .pit import assess_all_events


MARKET_ALIASES = {"XSHG": "cn", "XSHE": "cn"}


def _all_broker_risk_event_hits(
    assessments: Iterable[EventBrokerAssessment],
    security_names: Mapping[tuple[str, str], str],
) -> list[dict]:
    rows = []
    for item in sorted(
        assessments,
        key=lambda value: (
            value.broker_id,
            value.event.first_fact_date,
            value.event.security_code,
            value.event.event_type,
        ),
    ):
        event = item.event
        rows.append(
            {
                "broker_id": item.broker_id,
                "security_code": event.security_code,
                "security_name": security_names.get(
                    (event.market_code, event.security_code), ""
                ),
                "event_type": ("ST" if event.event_type == "NEW_ST" else "非ST"),
                "risk_date": event.first_fact_date.isoformat(),
                "first_dangerous_date": (
                    item.dangerous_run_start_date.isoformat()
                    if item.dangerous_run_start_date is not None
                    else ""
                ),
                "is_hit": "是"
                if item.is_hit is True
                else ("否" if item.is_hit is False else "未知"),
                "warning_trading_days": item.warning_trading_days,
            }
        )
    return rows

def _assess_events_from_wide_history(
    events,
    broker_ids,
    event_rows,
    st_states,
    calendar,
    mapping,
    rules,
    metric_batch_id,
    calendar_snapshot_id,
    missing_history_securities=frozenset(),
    cancel_check=None,
):
    output = []
    for event in events:
        raise_if_canceled(cancel_check)
        security = (event.market_code, event.security_code)
        if security in missing_history_securities:
            calendar_market = MARKET_ALIASES[event.market_code]
            cutoff = calendar.previous_trading_day(
                calendar_market, event.first_fact_date
            )
            cutoff_st = st_states.get(
                (event.market_code, event.security_code, cutoff), None
            )
            cutoff_st_state = (
                "UNKNOWN" if cutoff_st is None else ("ST" if cutoff_st else "NON_ST")
            )
            for broker_id in broker_ids:
                output.append(
                    EventBrokerAssessment(
                        event=event,
                        broker_id=broker_id,
                        cutoff_trading_date=cutoff,
                        selected_record=None,
                        cutoff_st_state=cutoff_st_state,
                        final_bucket="UNKNOWN",
                        classification_resolution="SECURITY_HISTORY_MISSING",
                        mapping_version=mapping.mapping_version,
                        mapping_record_id="",
                        identification_status="UNKNOWN",
                        is_hit=None,
                        warning_days_status="UNKNOWN",
                        dangerous_run_start_date=None,
                        warning_trading_days=None,
                        reason_code="BM109_SECURITY_HISTORY_MISSING",
                        metric_rule_version=rules.rule_version,
                        metric_batch_id=metric_batch_id,
                        calendar_snapshot_id=calendar_snapshot_id,
                    )
                )
            continue
        rows = event_rows.get((event.market_code, event.security_code), ())
        records = []
        for biz_date, values, source_file, source_hash, row_number in rows:
            for broker_id, raw_value in zip(broker_ids, values):
                records.append(
                    BrokerClassificationRecord(
                        broker_id,
                        event.market_code,
                        event.security_code,
                        biz_date,
                        calendar.next_trading_day("cn", biz_date),
                        raw_value,
                        source_file=source_file,
                        source_file_sha256=source_hash,
                        source_row_number=row_number,
                        source_record_id=f"{source_file}:{row_number}:{broker_id}:{event.security_code}:{biz_date}",
                    )
                )
        output.extend(
            assess_all_events(
                (event,),
                broker_ids,
                records,
                st_states,
                calendar,
                MARKET_ALIASES,
                mapping,
                rules,
                metric_batch_id=metric_batch_id,
                calendar_snapshot_id=calendar_snapshot_id,
            )
        )
    keys = {(item.event.event_key, item.broker_id) for item in output}
    if len(keys) != len(output):
        raise ValueError("BM401_ASSESSMENT_KEY_DUPLICATE")
    return tuple(
        sorted(output, key=lambda item: (item.event.event_key, item.broker_id))
    )

def _build_streaming_exposures(
    daily_rows,
    broker_ids,
    securities,
    snapshot_dates,
    st_states,
    lifecycle,
    mapping,
    initial_rows=None,
    *,
    missing_history_securities=frozenset(),
    cancel_check: Callable[[], bool] | None = None,
):
    broker_positions = {broker: position for position, broker in enumerate(broker_ids)}
    active = {broker: {} for broker in broker_ids}
    for security, values in (initial_rows or {}).items():
        for broker, position in broker_positions.items():
            active[broker][security] = values[position]
    period_unknown_sets = {broker: set() for broker in broker_ids}
    exposures = []
    unknown_counts = {}
    for snapshot_date in snapshot_dates:
        raise_if_canceled(cancel_check)
        rows = daily_rows.get(snapshot_date, {})
        for security, values in rows.items():
            for broker, position in broker_positions.items():
                active[broker][security] = values[position]
        for broker in broker_ids:
            unknown = 0
            for market, code in securities:
                listed, delisted = lifecycle[(market, code)]
                if snapshot_date < listed or (
                    delisted is not None and snapshot_date >= delisted
                ):
                    continue
                if (market, code) in missing_history_securities:
                    unknown += 1
                    period_unknown_sets[broker].add((market, code))
                    continue
                st_state = st_states.get((market, code, snapshot_date))
                if st_state is None:
                    unknown += 1
                    period_unknown_sets[broker].add((market, code))
                    continue
                raw_value = active[broker].get((market, code))
                if st_state:
                    exposures.append(
                        DangerousExposure(broker, snapshot_date, market, code)
                    )
                elif raw_value is None:
                    continue
                elif raw_value == "":
                    continue
                else:
                    bucket, _ = mapping.map_value(broker, raw_value)
                    if bucket == "DANGEROUS":
                        exposures.append(
                            DangerousExposure(broker, snapshot_date, market, code)
                        )
                    elif bucket is None:
                        unknown += 1
                        period_unknown_sets[broker].add((market, code))
            unknown_counts[(broker, snapshot_date)] = unknown
    return (
        tuple(exposures),
        unknown_counts,
        {broker: len(values) for broker, values in period_unknown_sets.items()},
    )

def _assessment_findings(assessments: Iterable[EventBrokerAssessment]):
    output = []
    for item in assessments:
        if item.reason_code in {
            "BM203_ST_STATUS_MISSING",
            "BM301_MAPPING_UNKNOWN",
            "BM303_DANGEROUS_RUN_LEFT_CENSORED",
            "BM305_NO_BROKER_START_FOR_ST_OVERRIDE",
            "BM109_SECURITY_HISTORY_MISSING",
        }:
            output.append(
                BrokerMetricFinding(
                    item.reason_code,
                    "WARNING",
                    f"{item.identification_status}/{item.warning_days_status}",
                    broker_id=item.broker_id,
                    market_code=item.event.market_code,
                    security_code=item.event.security_code,
                    fact_date=item.event.first_fact_date.isoformat(),
                )
            )
    return output

def _reconcile(
    events,
    brokers,
    assessments,
    days_summary,
    hit_summary,
    warning_summary,
    snapshot_dates,
):
    if len(assessments) != len(events) * len(brokers):
        raise ValueError("BM402_SUMMARY_RECONCILIATION_FAILED: assessment count")
    if len({(item.event.event_key, item.broker_id) for item in assessments}) != len(
        assessments
    ):
        raise ValueError("BM401_ASSESSMENT_KEY_DUPLICATE")
    expected_summary = len(brokers) * 3
    if len(days_summary) != expected_summary or len(hit_summary) != expected_summary:
        raise ValueError("BM402_SUMMARY_RECONCILIATION_FAILED: summary count")
    expected_warning = len(brokers) * (len(snapshot_dates) + 1) * 3
    if len(warning_summary) != expected_warning:
        raise ValueError("BM402_SUMMARY_RECONCILIATION_FAILED: warning rate count")
    denominators = {
        (row["event_type_view"], row["risk_event_count"]) for row in hit_summary
    }
    expected = {
        (
            "NON_ST_CONTINUOUS_LIMIT_DOWN",
            sum(e.event_type == "NON_ST_CONTINUOUS_LIMIT_DOWN" for e in events),
        ),
        ("NEW_ST", sum(e.event_type == "NEW_ST" for e in events)),
        ("ALL_RISK_EVENTS", len(events)),
    }
    if denominators != expected:
        raise ValueError("BM402_SUMMARY_RECONCILIATION_FAILED: event denominators")

