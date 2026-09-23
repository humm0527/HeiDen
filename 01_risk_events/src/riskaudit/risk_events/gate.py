"""
文件作用：执行第五表证券并集覆盖和新增 ST 期初状态两类风险事件专用门禁。
编辑记录：
【首次生成：2026-08-11，建立证券集合对账、北交所显式排除、行情/ST 覆盖和 ST 基线检查。】
【第二次编辑：2026-08-11，修正候选跌停日期读取，避免 pandas 元组重命名内部日期列。】
【第三次编辑：2026-08-11，仅对正常成交、非 ST 且六位量化价格相等的候选日检查 PIT 板块。】
【第四次编辑：2026-08-11，按显式市场后缀在主数据查找前排除北交所，避免已批准排除项被误报为主数据缺失。】
【第五次编辑：2026-08-11，为全量真实运行预建证券与日历分组索引，消除逐证券反复扫描整表。】
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re
from typing import Any

import pandas as pd

from .models import (
    RiskGateFinding,
    RiskInputGateResult,
    RiskInputTables,
    SecurityKey,
    StBaseline,
)
from .rules import RiskEventRules


_SECURITY_CODE = re.compile(r"^\d{6}(?:\.(?:XSHG|XSHE|XBSE))?$")


def _date_series(frame: pd.DataFrame, field: str) -> pd.Series:
    return pd.to_datetime(frame[field], errors="coerce").dt.date


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "是", "生效", "st", "*st"}:
        return True
    if text in {"false", "0", "no", "n", "否", "未生效", "非st", "non_st"}:
        return False
    raise ValueError(f"Unrecognized boolean/ST value: {value!r}")


def _st_rows(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result = result.loc[result["risk_status_type"].astype(str).str.upper() == "ST"].copy()
    result["_date"] = _date_series(result, "status_start_date")
    result["_is_st"] = result["risk_status_value"].map(_as_bool)
    return result


def _prices_equal(left: Any, right: Any, scale: int) -> bool:
    try:
        quantum = Decimal(1).scaleb(-scale)
        return Decimal(str(left)).quantize(quantum) == Decimal(str(right)).quantize(quantum)
    except (InvalidOperation, ValueError, TypeError):
        return False


def validate_risk_inputs(
    tables: RiskInputTables,
    rules: RiskEventRules,
    *,
    prevalidation_allow_run: bool,
    calendar_market_code: str = "cn",
) -> RiskInputGateResult:
    findings: list[RiskGateFinding] = []
    if not prevalidation_allow_run:
        findings.append(
            RiskGateFinding(
                "RISK_GATE_PREVALIDATION_REJECTED",
                "ERROR",
                "Selected five-table prevalidation result does not allow the run",
            )
        )

    frames_and_fields = (
        ("broker", tables.broker_risk_classification, {"market_code", "security_code", "classification_date"}),
        ("master", tables.security_status_daily, {"market_code", "security_code", "status_date", "board_code", "listed_date", "delisted_date"}),
        ("calendar", tables.trading_calendar, {"market_code", "calendar_date", "is_trading_day"}),
        ("market", tables.stock_market_daily, {"market_code", "security_code", "trading_date", "trading_status", "close_price", "limit_down_price", "source_record_id"}),
        ("st", tables.security_risk_status, {"market_code", "security_code", "risk_status_type", "risk_status_value", "status_start_date", "source_record_id"}),
    )
    for table_name, frame, required in frames_and_fields:
        missing = sorted(required - set(frame.columns))
        if missing:
            findings.append(
                RiskGateFinding(
                    "RISK_GATE_REQUIRED_FIELD_MISSING",
                    "ERROR",
                    f"{table_name} missing required fields: {', '.join(missing)}",
                )
            )
    if findings:
        return RiskInputGateResult(False, findings=findings)

    for table_name, frame, _ in frames_and_fields:
        if "security_code" not in frame:
            continue
        invalid = ~frame["security_code"].astype(str).str.fullmatch(_SECURITY_CODE)
        for value in sorted(frame.loc[invalid, "security_code"].astype(str).unique()):
            findings.append(
                RiskGateFinding(
                    "RISK_GATE_SECURITY_CODE_INVALID",
                    "ERROR",
                    f"{table_name} contains non-canonical security code {value!r}",
                    security_code=value,
                )
            )

    broker = tables.broker_risk_classification.copy()
    broker["_date"] = _date_series(broker, "classification_date")
    broker = broker.loc[broker["_date"] <= rules.observation_end]
    raw_universe = tuple(
        sorted(
            {
                (str(row.market_code), str(row.security_code))
                for row in broker[["market_code", "security_code"]].itertuples(index=False)
            }
        )
    )
    if not raw_universe:
        findings.append(
            RiskGateFinding(
                "RISK_GATE_UNIVERSE_EMPTY",
                "ERROR",
                "The fifth-table security union is empty",
            )
        )

    master = tables.security_status_daily.copy()
    master["_date"] = _date_series(master, "status_date")
    master["_listed"] = _date_series(master, "listed_date")
    master["_delisted"] = _date_series(master, "delisted_date")
    master = master.loc[master["_date"] <= rules.observation_end]
    master_groups = {
        (str(key[0]), str(key[1])): rows.sort_values("_date")
        for key, rows in master.groupby(
            ["market_code", "security_code"], sort=False, dropna=False
        )
    }

    excluded: dict[SecurityKey, str] = {}
    eligible: list[SecurityKey] = []
    master_by_key: dict[SecurityKey, pd.DataFrame] = {}
    for key in raw_universe:
        if key[0] == "XBSE":
            excluded[key] = "BOARD_EXCLUDED_BSE"
            continue
        rows = master_groups.get(key, master.iloc[0:0])
        master_by_key[key] = rows
        if rows.empty:
            findings.append(
                RiskGateFinding(
                    "RISK_GATE_UNIVERSE_MASTER_MISSING",
                    "ERROR",
                    "Security from fifth-table union is missing from security master",
                    *key,
                )
            )
            continue
        latest_board = rules.canonical_board(rows.iloc[-1]["board_code"])
        if latest_board in rules.excluded_boards:
            excluded[key] = "BOARD_EXCLUDED_BSE"
        else:
            eligible.append(key)

    calendar = tables.trading_calendar.copy()
    calendar["_date"] = _date_series(calendar, "calendar_date")
    calendar["_is_trading"] = calendar["is_trading_day"].map(_as_bool)
    market = tables.stock_market_daily.copy()
    market["_date"] = _date_series(market, "trading_date")
    st = _st_rows(tables.security_risk_status)
    calendar_dates = {
        str(market_code): tuple(sorted(rows.loc[rows["_is_trading"], "_date"].dropna().unique()))
        for market_code, rows in calendar.groupby("market_code", sort=False)
    }
    market_groups = {
        (str(key[0]), str(key[1])): rows
        for key, rows in market.groupby(
            ["market_code", "security_code"], sort=False, dropna=False
        )
    }
    st_groups = {
        (str(key[0]), str(key[1])): rows.sort_values("_date")
        for key, rows in st.groupby(
            ["market_code", "security_code"], sort=False, dropna=False
        )
    }
    baselines: dict[SecurityKey, StBaseline] = {}

    for key in sorted(eligible):
        key_calendar_dates = calendar_dates.get(
            key[0], calendar_dates.get(calendar_market_code, ())
        )
        rows = master_by_key[key]
        latest = rows.iloc[-1]
        listed = latest["_listed"]
        delisted = latest["_delisted"]
        expected_dates = sorted(
            day
            for day in key_calendar_dates
            if rules.observation_start <= day <= rules.observation_end
            and (pd.isna(listed) or day >= listed)
            and (pd.isna(delisted) or day < delisted)
        )

        market_rows = market_groups.get(key, market.iloc[0:0])
        market_dates = set(market_rows["_date"].dropna())
        st_rows = st_groups.get(key, st.iloc[0:0])
        st_dates = set(st_rows["_date"].dropna())
        st_state_by_date = dict(
            zip(st_rows["_date"], st_rows["_is_st"], strict=False)
        )
        for day in expected_dates:
            if day not in market_dates:
                findings.append(
                    RiskGateFinding(
                        "RISK_GATE_UNIVERSE_MARKET_COVERAGE_MISSING",
                        "ERROR",
                        "Eligible security lacks an explainable market row",
                        key[0],
                        key[1],
                        day.isoformat(),
                    )
                )
            if day not in st_dates:
                findings.append(
                    RiskGateFinding(
                        "RISK_GATE_UNIVERSE_ST_COVERAGE_MISSING",
                        "ERROR",
                        "Eligible security lacks a daily ST state",
                        key[0],
                        key[1],
                        day.isoformat(),
                    )
                )

        candidate_market = market_rows.loc[
            market_rows["_date"].isin(expected_dates)
            & market_rows["close_price"].notna()
            & market_rows["limit_down_price"].notna()
        ]
        for _, row in candidate_market.iterrows():
            candidate_date = row["_date"]
            if (
                str(row["trading_status"]).strip()
                not in {"正常成交", "TRADED", "NORMAL_TRADED"}
                or candidate_date not in st_state_by_date
                or bool(st_state_by_date[candidate_date])
                or not _prices_equal(
                    row["close_price"], row["limit_down_price"], rules.decimal_scale
                )
            ):
                continue
            board_rows = rows.loc[rows["_date"] <= candidate_date]
            if board_rows.empty:
                findings.append(
                    RiskGateFinding(
                        "RISK_GATE_BOARD_MISSING",
                        "ERROR",
                        "No PIT-valid board record exists at a candidate limit-down date",
                        key[0],
                        key[1],
                        candidate_date.isoformat(),
                    )
                )

        if not expected_dates:
            continue
        if not pd.isna(listed) and listed >= rules.observation_start:
            baseline_rows = st_rows.loc[st_rows["_date"] == expected_dates[0]]
            reason = "FIRST_IN_LIFECYCLE_TRADING_DAY"
        else:
            baseline_rows = st_rows.loc[st_rows["_date"] < rules.observation_start]
            reason = "LATEST_STATE_BEFORE_OBSERVATION_START"
        if baseline_rows.empty:
            findings.append(
                RiskGateFinding(
                    "RISK_GATE_ST_BASELINE_MISSING",
                    "ERROR",
                    "No valid ST baseline exists before transition scanning",
                    key[0],
                    key[1],
                )
            )
        else:
            row = baseline_rows.iloc[-1]
            baselines[key] = StBaseline(
                key[0],
                key[1],
                row["_date"],
                bool(row["_is_st"]),
                str(row["source_record_id"]),
                reason,
            )

    reconciled = set(raw_universe) == set(eligible) | set(excluded) and not (
        set(eligible) & set(excluded)
    )
    if not reconciled:
        findings.append(
            RiskGateFinding(
                "RISK_GATE_SET_RECONCILIATION_FAILED",
                "ERROR",
                "Raw, eligible, and excluded security sets do not reconcile",
            )
        )
    allow_run = not any(item.severity == "ERROR" for item in findings)
    return RiskInputGateResult(
        allow_run=allow_run,
        raw_business_universe=raw_universe,
        eligible_universe=tuple(sorted(eligible)),
        excluded_securities=excluded,
        st_baselines=baselines,
        findings=findings,
    )
