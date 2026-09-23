"""
文件作用：定义风险计算流水线共享的协作式取消异常与检查函数。
编辑记录：
【首次生成：2026-08-14，支持前端任务在阶段和分块边界安全停止。】
"""

from __future__ import annotations

from collections.abc import Callable


class CalculationCanceled(RuntimeError):
    """Raised when a user-requested calculation cancellation is observed."""


def raise_if_canceled(cancel_check: Callable[[], bool] | None) -> None:
    if cancel_check is not None and cancel_check():
        raise CalculationCanceled("用户手动停止计算")
