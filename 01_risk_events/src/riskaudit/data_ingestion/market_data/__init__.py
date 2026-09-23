"""
文件作用：导出市场事实数据接入层的公开接口。
编辑记录：
- 首次生成：2026-08-05，建立米筐市场数据四表接入边界。
"""

from .service import (
    MARKET_DATASETS,
    MARKET_TABLES,
    MarketDataBatch,
    MarketDataIngestionService,
    build_market_mapping_catalog,
)

__all__ = [
    "MARKET_DATASETS",
    "MARKET_TABLES",
    "MarketDataBatch",
    "MarketDataIngestionService",
    "build_market_mapping_catalog",
]
