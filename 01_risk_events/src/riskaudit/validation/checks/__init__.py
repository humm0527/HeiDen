"""
文件作用：公开验证规则包可调用的确定性语义检查执行接口。
编辑记录：
【首次生成：2026-08-06，导出语义检查上下文与显式注册执行函数。】
"""

from .semantic import SemanticContext, run_semantic_checks

__all__ = ["SemanticContext", "run_semantic_checks"]
