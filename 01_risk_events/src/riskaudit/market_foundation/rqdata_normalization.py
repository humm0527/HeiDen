"""
文件作用：将 RQData Raw bundle 标准化为市场底座六类事实，并提供证券/日期转换规则。
编辑记录：
【首次生成：2026-09-01，从市场底座 RQData 模块拆出确定性标准化职责。】
"""

from __future__ import annotations

from datetime import date, timedelta
from math import isnan
from typing import Any, Mapping, Sequence

from riskaudit.data_ingestion.adapters.rqdata import RQDataAdapter

from .constants import FOUNDATION_START


SOURCE_MARKET_TO_INTERNAL = {
    "XSHG": "XSHG",
    "XSHE": "XSHE",
    "BJSE": "XBSE",
    "XBSE": "XBSE",
}


def normalize_foundation_bundle(
    bundle: Mapping[str, Mapping[str, Any]],
    *,
    start_date: date,
    end_date: date,
    markets: Sequence[str],
    datasets: Sequence[str],
) -> dict[str, list[dict[str, Any]]]:
    selected = set(datasets)
    result = {dataset_id: [] for dataset_id in datasets}
    trading_dates = {
        _as_date(item["date"])
        for item in bundle["rqdata_trading_calendar"]["data"]
    }
    if "market_calendar" in selected:
        for market_code in markets:
            for current in _date_range(start_date, end_date):
                result["market_calendar"].append(
                    {
                        "market_code": market_code,
                        "business_date": current,
                        "is_trading_day": current in trading_dates,
                    }
                )

    instruments: dict[str, dict[str, Any]] = {}
    for source in bundle["rqdata_security_master"]["data"]:
        order_book_id = str(source["order_book_id"])
        market_code = _market_from_order_book_id(order_book_id)
        if market_code not in markets:
            continue
        listed = _as_date(source["listed_date"])
        terminated = _optional_date(source.get("de_listed_date"))
        instruments[order_book_id] = dict(source)
        if "instrument_master_history" in selected:
            result["instrument_master_history"].append(
                {
                    "market_code": market_code,
                    "security_code": _security_code(order_book_id),
                    "instrument_key": order_book_id,
                    "security_type": str(source.get("type") or "CS"),
                    "board_code": str(source.get("board_type") or "UNKNOWN"),
                    "listing_date": listed,
                    "termination_date": terminated,
                    "effective_start": max(listed, FOUNDATION_START),
                    "effective_end": (
                        terminated - timedelta(days=1) if terminated else None
                    ),
                }
            )

    allowed_ids = set(instruments)
    price_rows = bundle["rqdata_market_daily"]["data"]
    for source in price_rows:
        order_book_id = str(source["order_book_id"])
        if order_book_id not in allowed_ids:
            continue
        market_code = _market_from_order_book_id(order_book_id)
        current = _as_date(source.get("date") or source.get("trading_date"))
        common = {
            "market_code": market_code,
            "security_code": _security_code(order_book_id),
            "instrument_key": order_book_id,
            "business_date": current,
        }
        if "daily_price_unadjusted" in selected:
            result["daily_price_unadjusted"].append(
                {
                    **common,
                    "open": _optional_number(source.get("open")),
                    "high": _optional_number(source.get("high")),
                    "low": _optional_number(source.get("low")),
                    "close": _optional_number(source.get("close")),
                    "volume": _optional_number(source.get("volume")),
                    "amount": _optional_number(source.get("total_turnover")),
                    "adjustment": "NONE",
                }
            )
        if "daily_price_limits" in selected:
            result["daily_price_limits"].append(
                {
                    **common,
                    "limit_up": _optional_number(source.get("limit_up")),
                    "limit_down": _optional_number(source.get("limit_down")),
                }
            )

    if "daily_st_status" in selected:
        for source in RQDataAdapter._long_boolean_records(
            bundle["rqdata_st_status"]["data"], "is_st"
        ):
            order_book_id = str(source["order_book_id"])
            if order_book_id in allowed_ids and _inside_lifecycle(
                _as_date(source["date"]), instruments[order_book_id]
            ):
                result["daily_st_status"].append(
                    _boolean_fact(order_book_id, source, "is_st")
                )
    if "daily_suspension_status" in selected:
        for source in RQDataAdapter._long_boolean_records(
            bundle["rqdata_suspension_status"]["data"], "is_suspended"
        ):
            order_book_id = str(source["order_book_id"])
            if order_book_id in allowed_ids and _inside_lifecycle(
                _as_date(source["date"]), instruments[order_book_id]
            ):
                result["daily_suspension_status"].append(
                    _boolean_fact(order_book_id, source, "is_suspended")
                )
    return result

def _boolean_fact(
    order_book_id: str, source: Mapping[str, Any], field: str
) -> dict[str, Any]:
    return {
        "market_code": _market_from_order_book_id(order_book_id),
        "security_code": _security_code(order_book_id),
        "instrument_key": order_book_id,
        "business_date": _as_date(source["date"]),
        field: RQDataAdapter._as_bool(source[field]),
    }


def _inside_lifecycle(current: date, instrument: Mapping[str, Any]) -> bool:
    listed = _as_date(instrument["listed_date"])
    terminated = _optional_date(instrument.get("de_listed_date"))
    return current >= listed and (terminated is None or current < terminated)


def _market_from_order_book_id(order_book_id: str) -> str:
    if "." not in order_book_id:
        raise ValueError("RQData 证券代码必须包含显式市场后缀")
    source_market = order_book_id.rsplit(".", 1)[1]
    market = SOURCE_MARKET_TO_INTERNAL.get(source_market)
    if market is None:
        raise ValueError(f"P3 不支持证券市场后缀：{source_market}")
    return market


def market_code_from_order_book_id(order_book_id: str) -> str:
    """Map RQData's source suffix to the frozen internal market code."""

    return _market_from_order_book_id(order_book_id)


def _security_code(order_book_id: str) -> str:
    return order_book_id.rsplit(".", 1)[0]


def _date_range(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def _as_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).split("T", 1)[0].split(" ", 1)[0])


def _optional_date(value: Any) -> date | None:
    text = str(value or "").split("T", 1)[0].split(" ", 1)[0]
    return None if text in {"", "0000-00-00", "NaT", "None"} else date.fromisoformat(text)


def _optional_number(value: Any) -> Any:
    if value is None:
        return None
    try:
        if isnan(value):
            return None
    except TypeError:
        pass
    return value.item() if hasattr(value, "item") else value


