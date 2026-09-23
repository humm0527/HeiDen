"""
文件作用：提供显式市场交易日历、前一交易日、下一交易日和序号差的确定性运算。
编辑记录：
【首次生成：2026-08-12，实现 D-050/D-063 所需的市场日历纯函数。】
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import date
from typing import Iterable


class MarketCalendar:
    def __init__(self, dates_by_market: dict[str, Iterable[date]]) -> None:
        self._dates: dict[str, tuple[date, ...]] = {}
        self._ordinal: dict[str, dict[date, int]] = {}
        for market, values in dates_by_market.items():
            dates = tuple(sorted(set(values)))
            if not dates:
                raise ValueError(f"Trading calendar is empty: {market}")
            self._dates[market] = dates
            self._ordinal[market] = {value: index for index, value in enumerate(dates)}

    def dates(self, market: str) -> tuple[date, ...]:
        try:
            return self._dates[market]
        except KeyError as exc:
            raise ValueError(f"Calendar market alias missing: {market}") from exc

    def contains(self, market: str, value: date) -> bool:
        return value in self._ordinal.get(market, {})

    def previous_trading_day(self, market: str, value: date) -> date:
        dates = self.dates(market)
        index = bisect_left(dates, value) - 1
        if index < 0:
            raise ValueError(f"No previous trading day for {market} {value}")
        return dates[index]

    def next_trading_day(self, market: str, value: date) -> date | None:
        dates = self.dates(market)
        index = bisect_right(dates, value)
        return dates[index] if index < len(dates) else None

    def ordinal(self, market: str, value: date) -> int:
        try:
            return self._ordinal[market][value]
        except KeyError as exc:
            raise ValueError(f"Date is not a trading day for {market}: {value}") from exc

    def trading_day_difference(self, market: str, start: date, end: date) -> int:
        difference = self.ordinal(market, end) - self.ordinal(market, start)
        if difference < 0:
            raise ValueError("Trading day difference cannot be negative")
        return difference

    def through(self, market: str, end: date) -> tuple[date, ...]:
        dates = self.dates(market)
        return dates[: bisect_right(dates, end)]
