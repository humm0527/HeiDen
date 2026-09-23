"""
文件作用：提供集中度证券代码到五表 canonical 证券键的显式、可审计转换。
编辑记录：
【首次生成：2026-08-11，固定 SH/SZ/BJ 到 XSHG/XSHE/XBSE 的后缀映射并禁止数字前缀推断。】
"""

from __future__ import annotations

import re


_BUSINESS_CODE = re.compile(r"^(?P<digits>\d{6})\.(?P<suffix>SH|SZ|BJ)$")
_SUFFIX_MAP = {"SH": "XSHG", "SZ": "XSHE", "BJ": "XBSE"}


def normalize_business_security_code(raw_code: object) -> tuple[str, str]:
    text = str(raw_code).strip().upper()
    match = _BUSINESS_CODE.fullmatch(text)
    if not match:
        raise ValueError(
            "Business security code must be six digits with an explicit .SH/.SZ/.BJ suffix"
        )
    market_code = _SUFFIX_MAP[match.group("suffix")]
    return market_code, f"{match.group('digits')}.{market_code}"
