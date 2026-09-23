"""
文件作用：封装 D-063 预警交易日数唯一公式，拒绝非交易日起点和负值。
编辑记录：
【首次生成：2026-08-12，将预警天数序号差从 PIT 编排中提取为独立纯函数。】
"""

from __future__ import annotations

from datetime import date

from .calendar import MarketCalendar


def calculate_warning_trading_days(
    calendar: MarketCalendar,
    calendar_market: str,
    dangerous_run_start_date: date,
    first_fact_date: date,
) -> int:
    result = calendar.trading_day_difference(
        calendar_market, dangerous_run_start_date, first_fact_date
    )
    if result < 1:
        raise ValueError("Defined early-warning days must be at least one")
    return result
