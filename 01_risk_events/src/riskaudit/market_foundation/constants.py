"""
文件作用：集中定义长期市场数据底座 P2 的冻结范围、数据集、市场和状态枚举。
编辑记录：
【首次生成：2026-08-13，实现 P2 合成底座的稳定常量与范围校验契约。】
【二次编辑：2026-08-15，集中定义 P4 全范围对账规则版本，供候选与发布门禁复验。】
"""

from __future__ import annotations

from datetime import date


FOUNDATION_START = date(2023, 1, 1)
FOUNDATION_END = date(2026, 8, 17)
MARKETS = ("XSHG", "XSHE", "XBSE")
# 基础层继续保留北交所事实；当前获批风险业务只消费沪深市场。
RISK_MARKETS = ("XSHG", "XSHE")
DATASETS = (
    "market_calendar",
    "instrument_master_history",
    "daily_price_unadjusted",
    "daily_st_status",
    "daily_suspension_status",
    "daily_price_limits",
)
DATASET_PRESET = "RISK_COMPLETE_V1"
CUSTOM_PRESET = "CUSTOM_DIAGNOSTIC"
REFRESH_MODES = ("FILL_GAPS", "REVALIDATE")
PLAN_TTL_MINUTES = 30
SCHEMA_VERSION = "market_foundation_v1"
QUALITY_RULE_VERSION = "market_foundation_quality_v1"
RECONCILIATION_RULE_VERSION = "market_foundation_reconciliation_v3"
EXECUTION_MODE = "SYNTHETIC_ONLY"

TASK_STATUSES = (
    "DRAFT",
    "PLANNED",
    "QUEUED",
    "RUNNING",
    "VALIDATING",
    "SUCCEEDED",
    "INCOMPLETE",
    "FAILED",
)
CHUNK_STATUSES = (
    "PENDING",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "SKIPPED_IDEMPOTENT",
)
SNAPSHOT_STATUSES = ("DRAFT", "INCOMPLETE", "READY_TO_PUBLISH", "PUBLISHED")
RISK_ADAPTER_STATUSES = ("NOT_RUN", "COMPARING", "PASSED", "FAILED")
