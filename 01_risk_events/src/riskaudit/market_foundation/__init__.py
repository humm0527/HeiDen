"""
文件作用：公开长期市场数据底座 P2 的本地服务、错误和冻结常量。
编辑记录：
【首次生成：2026-08-13，建立 market_foundation 包的稳定公开入口。】
【二次编辑内容：2026-08-15，公开 P4 全范围质量对账与终态门禁入口。】
【三次编辑：2026-08-15，公开 P4 候选封装和四表影子对账入口。】
"""

from .constants import DATASETS, FOUNDATION_END, FOUNDATION_START, MARKETS, RISK_MARKETS
from .service import MarketFoundationError, MarketFoundationService
from .adapter import FiveTableShadowAdapter
from .rqdata import RQDataUniversePlan, discover_full_market_universe
from .reconciliation import (
    inspect_reconciliation_readiness,
    run_full_reconciliation,
)
from .promotion import inspect_p4_candidate_readiness, create_p4_candidate
from .shadow_reconciliation import run_shadow_reconciliation
from .risk_shadow import (
    reconcile_risk_results,
    run_snapshot_risk_calculation,
    run_snapshot_risk_shadow,
)

__all__ = [
    "DATASETS",
    "FOUNDATION_END",
    "FOUNDATION_START",
    "MARKETS",
    "RISK_MARKETS",
    "MarketFoundationError",
    "MarketFoundationService",
    "FiveTableShadowAdapter",
    "RQDataUniversePlan",
    "discover_full_market_universe",
    "inspect_reconciliation_readiness",
    "run_full_reconciliation",
    "inspect_p4_candidate_readiness",
    "create_p4_candidate",
    "run_shadow_reconciliation",
    "reconcile_risk_results",
    "run_snapshot_risk_calculation",
    "run_snapshot_risk_shadow",
]
