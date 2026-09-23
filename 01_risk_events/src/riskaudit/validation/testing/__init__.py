"""
文件作用：公开数据预检测试所需的异常注入接口，不参与正式业务计算。
编辑记录：
【首次生成：2026-08-06，导出 InjectionSpec 与标准表异常注入函数。】
"""

from .injector import InjectionSpec, SUPPORTED_OPERATIONS, inject_standard_tables

__all__ = ["InjectionSpec", "SUPPORTED_OPERATIONS", "inject_standard_tables"]
