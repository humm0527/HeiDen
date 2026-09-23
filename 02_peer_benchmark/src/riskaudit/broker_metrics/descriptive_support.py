"""
文件作用：提供描述性表格 CSV 原子边界所需的写出与哈希辅助函数。
编辑记录：
【首次生成：2026-09-01，从描述性表格编排器拆出文件输出支持职责。】
"""

from __future__ import annotations

import csv
from hashlib import sha256
from pathlib import Path


def _write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        raise ValueError(f"不能写入空表: {path.name}")
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

def _sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


