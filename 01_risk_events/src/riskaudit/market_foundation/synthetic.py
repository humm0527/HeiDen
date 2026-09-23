"""
文件作用：为 P2 离线测试和本地页面生成最小、明确标识的合成全范围市场事实。
编辑记录：
【首次生成：2026-08-13，实现三市场六数据集合成提供器，禁止连接 RQData。】
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Iterable

from .constants import DATASETS


SYNTHETIC_SECURITIES = {
    "XSHG": ("600001", "syn_xshg_600001", "MAIN"),
    "XSHE": ("000001", "syn_xshe_000001", "MAIN"),
    "XBSE": ("830001", "syn_xbse_830001", "BSE"),
}


def synthetic_records(
    start_date: date,
    end_date: date,
    markets: Iterable[str],
    datasets: Iterable[str],
) -> dict[str, list[dict[str, Any]]]:
    """Return deterministic P2 facts; every payload is visibly synthetic."""
    selected = set(datasets)
    result = {dataset_id: [] for dataset_id in DATASETS if dataset_id in selected}
    dates = list(_date_range(start_date, end_date))
    for market_code in markets:
        security_code, instrument_key, board_code = SYNTHETIC_SECURITIES[market_code]
        if "market_calendar" in selected:
            for current in dates:
                result["market_calendar"].append(
                    {
                        "market_code": market_code,
                        "business_date": current,
                        "is_trading_day": current.weekday() < 5,
                        "synthetic": True,
                    }
                )
        if "instrument_master_history" in selected:
            result["instrument_master_history"].append(
                {
                    "market_code": market_code,
                    "security_code": security_code,
                    "instrument_key": instrument_key,
                    "security_type": "CS",
                    "board_code": board_code,
                    "listing_date": start_date,
                    "termination_date": None,
                    "effective_start": start_date,
                    "effective_end": None,
                    "synthetic": True,
                }
            )
        for current in (item for item in dates if item.weekday() < 5):
            common = {
                "market_code": market_code,
                "security_code": security_code,
                "instrument_key": instrument_key,
                "business_date": current,
                "synthetic": True,
            }
            ordinal = Decimal((current - date(2023, 1, 1)).days % 1000) / Decimal("100")
            close = Decimal("10") + ordinal
            if "daily_price_unadjusted" in selected:
                result["daily_price_unadjusted"].append(
                    {
                        **common,
                        "open": close,
                        "high": close + Decimal("0.10"),
                        "low": close - Decimal("0.10"),
                        "close": close,
                        "volume": 1000,
                        "amount": close * 1000,
                        "adjustment": "NONE",
                    }
                )
            if "daily_st_status" in selected:
                result["daily_st_status"].append({**common, "is_st": False})
            if "daily_suspension_status" in selected:
                result["daily_suspension_status"].append(
                    {**common, "is_suspended": False}
                )
            if "daily_price_limits" in selected:
                result["daily_price_limits"].append(
                    {
                        **common,
                        "limit_up": close * Decimal("1.10"),
                        "limit_down": close * Decimal("0.90"),
                    }
                )
    return result


def _date_range(start_date: date, end_date: date) -> Iterable[date]:
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)

