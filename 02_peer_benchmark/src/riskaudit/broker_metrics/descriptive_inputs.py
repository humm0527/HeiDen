"""
文件作用：读取并规范化描述性表格所需的冻结分类、市值及行情输入。
编辑记录：
【首次生成：2026-09-01，从描述性表格编排器拆出输入边界，不改变日期筛选与证券代码规则。】
【二次编辑：2026-09-08，期初分类按业务日期而非文件行序选择，拒绝重复主键。】
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd


def _as_date(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])

def _history_source_paths(sources: Mapping[str, str | Path]) -> list[Path]:
    if not sources:
        raise ValueError("缺少冻结分类历史文件")
    paths = [Path(value) for _, value in sorted(sources.items())]
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"冻结分类历史文件缺失: {missing[0]}")
    return paths

def _load_history(
    sources: Mapping[str, str | Path], *, broker_id: str, start: date, end: date
) -> dict[str, Any]:
    initial: dict[str, str] = {}
    initial_dates: dict[str, date] = {}
    seen: set[tuple[date, str]] = set()
    daily: dict[date, dict[str, str]] = defaultdict(dict)
    source_rows = 0
    for path in _history_source_paths(sources):
        for frame in pd.read_csv(
            path,
            usecols=lambda column: column in {"biz_date", "stk_code", broker_id},
            dtype=str,
            keep_default_na=False,
            encoding="utf-8-sig",
            chunksize=200_000,
        ):
            required = {"biz_date", "stk_code", broker_id}
            if not required <= set(frame.columns):
                raise ValueError(f"分类历史缺少字段: {sorted(required - set(frame.columns))}")
            for day_text, raw_code, grade_value in frame.loc[
                :, ["biz_date", "stk_code", broker_id]
            ].itertuples(index=False, name=None):
                source_rows += 1
                day = _as_date(day_text)
                if day > end or day.weekday() >= 5:
                    continue
                security = str(raw_code).strip()
                if not security.endswith((".SH", ".SZ")):
                    continue
                grade = str(grade_value).strip().upper()
                key = (day, security)
                if key in seen:
                    raise ValueError(f"分类历史主键重复: {day}/{security}")
                seen.add(key)
                if day < start:
                    if security not in initial_dates or day > initial_dates[security]:
                        initial[security] = grade
                        initial_dates[security] = day
                else:
                    daily[day][security] = grade
    active = dict(initial)
    for day in sorted(daily):
        active.update(daily[day])
    return {
        "initial": initial,
        "initial_dates": initial_dates,
        "daily": dict(daily),
        "snapshot": active,
        "source_row_count": source_rows,
    }

def _load_market_caps(path: Path, *, end: date) -> tuple[dict[str, float], date]:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    aliases = {
        "date": ("交易日期", "日期", "trading_date", "business_date"),
        "security": ("证券代码", "股票代码", "security_code", "stk_code"),
        "market": ("交易市场代码", "market_code"),
        "cap": ("总市值", "total_market_cap", "market_cap"),
    }
    columns = {key: _first_column(frame, values, required=key != "market") for key, values in aliases.items()}
    frame["__date"] = frame[columns["date"]].map(_as_date)
    eligible = frame.loc[frame["__date"] <= end]
    if eligible.empty:
        raise ValueError("总市值输入在观察期末前没有记录")
    cap_date = max(eligible["__date"])
    eligible = eligible.loc[eligible["__date"] == cap_date]
    caps: dict[str, float] = {}
    for _, row in eligible.iterrows():
        security = _normalize_security(row[columns["security"]], row[columns["market"]] if columns["market"] else "")
        try:
            value = float(row[columns["cap"]])
        except ValueError:
            continue
        if value >= 0:
            caps[security] = value
    if not caps:
        raise ValueError("总市值输入没有有效证券市值")
    return caps, cap_date

def _load_adjusted_returns(
    path: Path, *, start: date, end: date
) -> tuple[dict[date, dict[str, float]], dict[str, int]]:
    header = tuple(
        pd.read_csv(
            path,
            nrows=0,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8-sig",
        ).columns
    )
    chinese = {
        "market": "交易市场代码",
        "security": "证券代码",
        "date": "交易日期",
        "status": "成交状态",
        "previous": "前收盘价",
        "close": "收盘价",
    }
    standard = {
        "market": "market_code",
        "security": "security_code",
        "date": "trading_date",
        "status": "trading_status",
        "previous": "previous_close",
        "close": "close_price",
    }
    columns = chinese if set(chinese.values()) <= set(header) else standard
    if not set(columns.values()) <= set(header):
        raise ValueError("表11后复权行情缺少日期、证券、前收、收盘或成交状态字段")
    returns: dict[date, dict[str, float]] = defaultdict(dict)
    source_rows = eligible_rows = valid_rows = 0
    normal_status = {"正常成交", "NORMAL", "TRADED"}
    for frame in pd.read_csv(
        path,
        usecols=list(columns.values()),
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
        chunksize=200_000,
    ):
        for values in frame.loc[:, list(columns.values())].itertuples(
            index=False, name=None
        ):
            source_rows += 1
            row = dict(zip(columns, values))
            day = _as_date(row["date"])
            if not start <= day <= end or str(row["status"]).strip() not in normal_status:
                continue
            eligible_rows += 1
            security = _normalize_security(row["security"], row["market"])
            try:
                previous = float(row["previous"])
                close = float(row["close"])
            except ValueError:
                continue
            if previous <= 0 or close < 0:
                continue
            if security in returns[day]:
                raise ValueError(f"表11后复权行情主键重复: {day}/{security}")
            returns[day][security] = close / previous - 1.0
            valid_rows += 1
    return dict(returns), {
        "source_row_count": source_rows,
        "eligible_row_count": eligible_rows,
        "valid_row_count": valid_rows,
    }

def _load_prices(
    path: Path,
    *,
    start: date,
    end: date,
    adjusted_path: Path | None = None,
) -> tuple[
    dict[date, dict[str, tuple[float, float | None, float]]], dict[str, Any]
]:
    header = tuple(pd.read_csv(path, nrows=0, dtype=str, keep_default_na=False, encoding="utf-8-sig").columns)
    chinese = {
        "market": "交易市场代码", "security": "证券代码", "date": "交易日期",
        "status": "成交状态", "previous": "前收盘价", "high": "最高价",
        "low": "最低价", "close": "收盘价", "amount": "成交额",
    }
    standard = {
        "market": "market_code", "security": "security_code", "date": "trading_date",
        "status": "trading_status", "previous": "previous_close", "high": "high_price",
        "low": "low_price", "close": "close_price", "amount": "amount",
    }
    columns = chinese if set(chinese.values()) <= set(header) else standard
    if not set(columns.values()) <= set(header):
        raise ValueError("表11行情输入缺少日期、证券、前收、最高、最低、收盘或成交额字段")
    adjusted_returns: dict[date, dict[str, float]] = {}
    adjusted_stats: dict[str, int] = {}
    if adjusted_path is not None:
        adjusted_returns, adjusted_stats = _load_adjusted_returns(
            adjusted_path, start=start, end=end
        )
    prices: dict[date, dict[str, tuple[float, float | None, float]]] = defaultdict(dict)
    source_rows = eligible_rows = valid_rows = 0
    normal_status = {"正常成交", "NORMAL", "TRADED"}
    for frame in pd.read_csv(path, usecols=list(columns.values()), dtype=str, keep_default_na=False, encoding="utf-8-sig", chunksize=200_000):
        for values in frame.loc[:, list(columns.values())].itertuples(index=False, name=None):
            source_rows += 1
            row = dict(zip(columns, values))
            day = _as_date(row["date"])
            if not start <= day <= end or str(row["status"]).strip() not in normal_status:
                continue
            eligible_rows += 1
            security = _normalize_security(row["security"], row["market"])
            try:
                previous, high, low, close, amount = (float(row[key]) for key in ("previous", "high", "low", "close", "amount"))
            except ValueError:
                continue
            if previous <= 0 or high < 0 or low < 0 or close < 0 or amount < 0:
                continue
            if security in prices[day]:
                raise ValueError(f"表11行情主键重复: {day}/{security}")
            adjusted_return = adjusted_returns.get(day, {}).get(security)
            prices[day][security] = (
                (high - low) / previous,
                adjusted_return,
                amount,
            )
            valid_rows += 1
    matched_return_rows = sum(
        1
        for rows in prices.values()
        for _, simple_return, _ in rows.values()
        if simple_return is not None
    )
    return dict(prices), {
        "source_row_count": source_rows,
        "eligible_row_count": eligible_rows,
        "valid_row_count": valid_rows,
        "adjusted_return_input": str(adjusted_path) if adjusted_path else None,
        "adjusted_return": adjusted_stats,
        "matched_adjusted_return_row_count": matched_return_rows,
    }

def _normalize_security(code_value: Any, market_value: Any = "") -> str:
    code = str(code_value).strip().upper()
    if code.endswith(".XSHG"):
        return code[:-5] + ".SH"
    if code.endswith(".XSHE"):
        return code[:-5] + ".SZ"
    if code.endswith((".SH", ".SZ")):
        return code
    market = str(market_value).strip().upper()
    if market in {"XSHG", "SH", "SSE"}:
        return code + ".SH"
    if market in {"XSHE", "SZ", "SZSE"}:
        return code + ".SZ"
    return code

def _first_column(frame: pd.DataFrame, names: tuple[str, ...], *, required: bool) -> str | None:
    for name in names:
        if name in frame.columns:
            return name
    if required:
        raise ValueError(f"输入缺少字段，候选为: {', '.join(names)}")
    return None

