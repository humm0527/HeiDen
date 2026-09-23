"""
文件作用：从正常标准市场表复制生成可追溯的异常注入数据，供 Validator 能力测试使用。
编辑记录：
【首次生成：2026-08-06，支持删字段、重复主键、非法代码、越界日期和未来 PIT 五类注入。】
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


SUPPORTED_OPERATIONS = frozenset(
    {
        "delete_field",
        "duplicate_key",
        "invalid_security_code",
        "out_of_range_date",
        "future_pit",
    }
)


@dataclass(frozen=True)
class InjectionSpec:
    operation: str
    table: str
    field: str = ""
    value: str = ""


def inject_standard_tables(
    source_dir: str | Path,
    output_dir: str | Path,
    spec: InjectionSpec,
) -> Path:
    """Copy a standard run and apply exactly one declared mutation."""

    source = Path(source_dir)
    target = Path(output_dir)
    if not source.is_dir():
        raise FileNotFoundError(f"正常标准表目录不存在：{source}")
    if target.exists():
        raise FileExistsError(f"异常注入输出目录已存在：{target}")
    if spec.operation not in SUPPORTED_OPERATIONS:
        raise ValueError(f"不支持的异常注入类型：{spec.operation}")
    target.mkdir(parents=True)
    for path in source.iterdir():
        if path.is_file() and path.suffix.lower() in {".csv", ".xlsx", ".json"}:
            shutil.copy2(path, target / path.name)

    table_path = _table_path(target, spec.table)
    frame = _read_table(table_path)
    if frame.empty:
        raise ValueError(f"不能向空表注入异常：{spec.table}")

    if spec.operation == "delete_field":
        _require_field(frame, spec.field)
        frame = frame.drop(columns=[spec.field])
    elif spec.operation == "duplicate_key":
        frame = pd.concat([frame, frame.iloc[[0]].copy()], ignore_index=True)
    elif spec.operation == "invalid_security_code":
        _require_field(frame, "证券代码")
        frame.loc[frame.index[0], "证券代码"] = spec.value or "12345"
    elif spec.operation == "out_of_range_date":
        _require_field(frame, spec.field)
        if not spec.value:
            raise ValueError("日期越界注入必须提供 value")
        frame.loc[frame.index[0], spec.field] = spec.value
    elif spec.operation == "future_pit":
        pit_field = spec.field or "信息可得时间"
        _require_field(frame, pit_field)
        if not spec.value:
            raise ValueError("未来 PIT 注入必须提供 value")
        frame.loc[frame.index[0], pit_field] = spec.value

    _write_table(table_path, frame)
    return target


def _table_path(folder: Path, table: str) -> Path:
    candidates = [folder / f"{table}.csv", folder / f"{table}.xlsx"]
    existing = [path for path in candidates if path.exists()]
    if len(existing) != 1:
        raise FileNotFoundError(f"无法唯一识别待注入业务表：{table}")
    return existing[0]


def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, dtype=object, keep_default_na=False, encoding="utf-8-sig")
    return pd.read_excel(path, dtype=object, keep_default_na=False)


def _write_table(path: Path, frame: pd.DataFrame) -> None:
    if path.suffix.lower() == ".csv":
        frame.to_csv(path, index=False, encoding="utf-8-sig")
    else:
        frame.to_excel(path, index=False)


def _require_field(frame: pd.DataFrame, field: str) -> None:
    if not field or field not in frame.columns:
        raise ValueError(f"待注入字段不存在：{field}")
