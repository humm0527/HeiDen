"""
文件作用：提供市场底座 RQData 的稳定公共入口，组合证券发现、抓取提供器和标准化模块。
编辑记录：
【首次生成：2026-08-14，接入沪深显式样本的日历、主数据、未复权行情、ST、停牌和涨跌停事实。】
【二次编辑：2026-09-01，将全市场发现、抓取编排与标准化职责拆分并保持导入兼容。】
"""

from __future__ import annotations

from .rqdata_normalization import (
    SOURCE_MARKET_TO_INTERNAL,
    _as_date,
    _boolean_fact,
    _date_range,
    _inside_lifecycle,
    _market_from_order_book_id,
    _optional_date,
    _optional_number,
    _security_code,
    market_code_from_order_book_id,
    normalize_foundation_bundle,
)
from .rqdata_provider import (
    P3_MAX_CALENDAR_DAYS,
    P3_MAX_SECURITIES,
    RQDataFoundationBatch,
    RQDataFoundationProvider,
)
from .rqdata_universe import (
    P3_5_DEFAULT_SHARD_SIZE,
    RQDataUniversePlan,
    discover_full_market_universe,
    read_universe_plan,
    write_universe_plan,
)


__all__ = [
    "P3_5_DEFAULT_SHARD_SIZE",
    "P3_MAX_CALENDAR_DAYS",
    "P3_MAX_SECURITIES",
    "RQDataFoundationBatch",
    "RQDataFoundationProvider",
    "RQDataUniversePlan",
    "SOURCE_MARKET_TO_INTERNAL",
    "discover_full_market_universe",
    "market_code_from_order_book_id",
    "normalize_foundation_bundle",
    "read_universe_plan",
    "write_universe_plan",
    "_as_date",
    "_boolean_fact",
    "_date_range",
    "_inside_lifecycle",
    "_market_from_order_book_id",
    "_optional_date",
    "_optional_number",
    "_security_code",
]
