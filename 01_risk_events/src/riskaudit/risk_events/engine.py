"""
文件作用：以五表标准输入和 approved 规则确定性计算非 ST 连续跌停段与新增 ST 风险事件。
编辑记录：
【首次生成：2026-08-11，实现逐交易日状态机、同证券多事件保留、Decimal 跌停比较和双表结果。】
【第二次编辑：2026-08-11，统一解析字符串和布尔形式的交易日标记。】
【第三次编辑：2026-08-11，按上市日和退市日右开边界裁剪逐证券交易日。】
【第四次编辑：2026-08-11，跳过与观察区间无生命周期交集的业务并集证券，避免读取不存在的 ST 基线。】
【第五次编辑：2026-08-11，为全量真实运行预建证券和市场日历索引，消除每只证券对整表的重复过滤。】
【第六次编辑：2026-08-12，在确定性结果中持久化本次显式观察区间。】
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd

from .models import (
    ContinuousLimitDownSegment,
    RiskEvent,
    RiskEventCalculationResult,
    RiskInputGateResult,
    RiskInputTables,
)
from .rules import RiskEventRules


_NORMAL_TRADED = {"正常成交", "TRADED", "NORMAL_TRADED"}


def _date_series(frame: pd.DataFrame, field: str) -> pd.Series:
    return pd.to_datetime(frame[field], errors="raise").dt.date


def _is_st(value: Any) -> bool:
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "是", "生效", "st", "*st"}:
        return True
    if text in {"false", "0", "no", "否", "未生效", "非st", "non_st"}:
        return False
    raise ValueError(f"Unrecognized ST state: {value!r}")


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "是"}:
        return True
    if text in {"false", "0", "no", "n", "否"}:
        return False
    raise ValueError(f"Unrecognized boolean value: {value!r}")


def _prices_equal(left: Any, right: Any, scale: int) -> bool:
    try:
        quantum = Decimal(1).scaleb(-scale)
        return Decimal(str(left)).quantize(quantum) == Decimal(str(right)).quantize(quantum)
    except (InvalidOperation, ValueError, TypeError):
        return False


@dataclass
class _OpenSegment:
    board_code: str
    threshold: int
    dates: list[date]
    source_record_ids: list[str]


def calculate_risk_events(
    tables: RiskInputTables,
    gate: RiskInputGateResult,
    rules: RiskEventRules,
    *,
    calculation_batch_id: str,
    upstream_snapshot_id: str,
    calendar_market_code: str = "cn",
    include_new_st_events: bool = True,
) -> RiskEventCalculationResult:
    if not gate.allow_run:
        raise ValueError("Risk event calculation cannot start when the specialized gate rejects the run")
    if not calculation_batch_id or not upstream_snapshot_id:
        raise ValueError("calculation_batch_id and upstream_snapshot_id are required")

    calendar = tables.trading_calendar.copy()
    calendar["_date"] = _date_series(calendar, "calendar_date")
    calendar = calendar.loc[calendar["is_trading_day"].map(_as_bool)]
    market = tables.stock_market_daily.copy()
    market["_date"] = _date_series(market, "trading_date")
    if "market_data_version" in market:
        market = market.sort_values("market_data_version").drop_duplicates(
            ["market_code", "security_code", "_date"], keep="last"
        )
    master = tables.security_status_daily.copy()
    master["_date"] = _date_series(master, "status_date")
    risk = tables.security_risk_status.copy()
    risk = risk.loc[risk["risk_status_type"].astype(str).str.upper() == "ST"].copy()
    risk["_date"] = _date_series(risk, "status_start_date")
    if "record_version" in risk:
        risk = risk.sort_values("record_version").drop_duplicates(
            ["market_code", "security_code", "_date", "risk_status_type"], keep="last"
        )

    market_groups = {
        (str(key[0]), str(key[1])): rows.set_index("_date", drop=False)
        for key, rows in market.groupby(
            ["market_code", "security_code"], sort=False, dropna=False
        )
    }
    risk_groups = {
        (str(key[0]), str(key[1])): rows.set_index("_date", drop=False)
        for key, rows in risk.groupby(
            ["market_code", "security_code"], sort=False, dropna=False
        )
    }
    master_groups = {
        (str(key[0]), str(key[1])): rows.sort_values("_date")
        for key, rows in master.groupby(
            ["market_code", "security_code"], sort=False, dropna=False
        )
    }
    calendar_dates = {
        str(market_code): sorted(
            day
            for day in rows["_date"].dropna().unique()
            if rules.observation_start <= day <= rules.observation_end
        )
        for market_code, rows in calendar.groupby("market_code", sort=False)
    }
    empty_market = market.iloc[0:0].set_index("_date", drop=False)
    empty_risk = risk.iloc[0:0].set_index("_date", drop=False)
    empty_master = master.iloc[0:0]

    segments: list[ContinuousLimitDownSegment] = []
    events: list[RiskEvent] = []

    for key in gate.eligible_universe:
        trading_dates = calendar_dates.get(
            key[0], calendar_dates.get(calendar_market_code, [])
        )
        market_rows = market_groups.get(key, empty_market)
        st_rows = risk_groups.get(key, empty_risk)
        master_rows = master_groups.get(key, empty_master)
        trading_dates = _within_lifecycle(trading_dates, master_rows)
        if not trading_dates:
            continue

        baseline = gate.st_baselines[key]
        previous_st = baseline.is_st
        previous_st_source = baseline.source_record_id
        open_segment: _OpenSegment | None = None
        segment_number = 0

        def close_segment(is_open_at_end: bool = False) -> None:
            nonlocal open_segment, segment_number
            if open_segment is None:
                return
            if len(open_segment.dates) >= rules.minimum_segment_length:
                segment_number += 1
                reached = len(open_segment.dates) >= open_segment.threshold
                threshold_date = (
                    open_segment.dates[open_segment.threshold - 1] if reached else None
                )
                segment = ContinuousLimitDownSegment(
                    market_code=key[0],
                    security_code=key[1],
                    segment_number=segment_number,
                    event_type=rules.continuous_event_type,
                    event_source="RQDATA_MARKET_DAILY_DERIVED",
                    board_code=open_segment.board_code,
                    applicable_threshold=open_segment.threshold,
                    consecutive_length=len(open_segment.dates),
                    first_fact_date=open_segment.dates[0],
                    threshold_reached_date=threshold_date,
                    last_fact_date=open_segment.dates[-1],
                    threshold_reached=reached,
                    open_at_observation_end=is_open_at_end,
                    rule_version=rules.rule_version,
                    calculation_batch_id=calculation_batch_id,
                    upstream_snapshot_id=upstream_snapshot_id,
                    source_record_ids=tuple(open_segment.source_record_ids),
                )
                segments.append(segment)
                if reached and threshold_date is not None:
                    events.append(
                        RiskEvent(
                            market_code=key[0],
                            security_code=key[1],
                            event_type=rules.continuous_event_type,
                            event_source="RQDATA_MARKET_DAILY_DERIVED",
                            first_fact_date=segment.first_fact_date,
                            risk_date=threshold_date,
                            last_fact_date=segment.last_fact_date,
                            threshold_reached_date=threshold_date,
                            segment_number=segment.segment_number,
                            consecutive_length=segment.consecutive_length,
                            previous_st_state=None,
                            current_st_state=None,
                            rule_version=rules.rule_version,
                            calculation_batch_id=calculation_batch_id,
                            upstream_snapshot_id=upstream_snapshot_id,
                            source_record_ids=segment.source_record_ids,
                        )
                    )
            open_segment = None

        for index, day in enumerate(trading_dates):
            market_row = market_rows.loc[day]
            st_row = st_rows.loc[day]
            if isinstance(market_row, pd.DataFrame) or isinstance(st_row, pd.DataFrame):
                raise ValueError(f"Duplicate daily input remains after version selection for {key} {day}")
            current_st = _is_st(st_row["risk_status_value"])
            if not previous_st and current_st:
                close_segment(False)
                if include_new_st_events:
                    events.append(
                        RiskEvent(
                            market_code=key[0],
                            security_code=key[1],
                            event_type=rules.new_st_event_type,
                            event_source="RQDATA_ST_STATUS_TRANSITION",
                            first_fact_date=day,
                            risk_date=day,
                            last_fact_date=day,
                            threshold_reached_date=None,
                            segment_number=None,
                            consecutive_length=None,
                            previous_st_state="NON_ST",
                            current_st_state="ST",
                            rule_version=rules.rule_version,
                            calculation_batch_id=calculation_batch_id,
                            upstream_snapshot_id=upstream_snapshot_id,
                            source_record_ids=(
                                previous_st_source,
                                str(st_row["source_record_id"]),
                            ),
                        )
                    )

            is_limit_down = (
                not current_st
                and str(market_row["trading_status"]).strip() in _NORMAL_TRADED
                and pd.notna(market_row["close_price"])
                and pd.notna(market_row["limit_down_price"])
                and _prices_equal(
                    market_row["close_price"],
                    market_row["limit_down_price"],
                    rules.decimal_scale,
                )
            )
            if is_limit_down:
                if open_segment is None:
                    board = _board_at(master_rows, day, rules)
                    open_segment = _OpenSegment(
                        board_code=board,
                        threshold=rules.thresholds[board],
                        dates=[],
                        source_record_ids=[],
                    )
                open_segment.dates.append(day)
                open_segment.source_record_ids.append(str(market_row["source_record_id"]))
            else:
                close_segment(False)

            previous_st = current_st
            previous_st_source = str(st_row["source_record_id"])
            if index == len(trading_dates) - 1:
                close_segment(is_limit_down)

    events.sort(key=lambda item: item.unique_key)
    segments.sort(key=lambda item: (item.market_code, item.security_code, item.first_fact_date))
    keys = [item.unique_key for item in events]
    if len(keys) != len(set(keys)):
        raise ValueError("Risk event output contains duplicate business keys")
    return RiskEventCalculationResult(
        segments=tuple(segments),
        events=tuple(events),
        rule_version=rules.rule_version,
        rule_sha256=rules.rule_sha256,
        calculation_batch_id=calculation_batch_id,
        upstream_snapshot_id=upstream_snapshot_id,
        observation_start=rules.observation_start,
        observation_end=rules.observation_end,
    )


def _trading_dates(
    calendar: pd.DataFrame,
    market_code: str,
    calendar_market_code: str,
    rules: RiskEventRules,
) -> list[date]:
    rows = calendar.loc[calendar["market_code"].astype(str) == market_code]
    if rows.empty:
        rows = calendar.loc[calendar["market_code"].astype(str) == calendar_market_code]
    return sorted(
        day
        for day in rows["_date"].dropna().unique()
        if rules.observation_start <= day <= rules.observation_end
    )


def _board_at(master_rows: pd.DataFrame, day: date, rules: RiskEventRules) -> str:
    rows = master_rows.loc[master_rows["_date"] <= day]
    if rows.empty:
        raise ValueError(f"No PIT-valid board record at {day}")
    board = rules.canonical_board(rows.iloc[-1]["board_code"])
    if board in rules.excluded_boards:
        raise ValueError(f"Excluded board reached event engine: {board}")
    if board not in rules.thresholds:
        raise ValueError(f"No approved threshold for board {board}")
    return board


def _within_lifecycle(
    trading_dates: list[date],
    master_rows: pd.DataFrame,
) -> list[date]:
    if master_rows.empty:
        return []
    latest = master_rows.iloc[-1]
    listed = pd.to_datetime(latest.get("listed_date"), errors="coerce")
    delisted = pd.to_datetime(latest.get("delisted_date"), errors="coerce")
    listed_date = listed.date() if not pd.isna(listed) else None
    delisted_date = delisted.date() if not pd.isna(delisted) else None
    return [
        day
        for day in trading_dates
        if (listed_date is None or day >= listed_date)
        and (delisted_date is None or day < delisted_date)
    ]
