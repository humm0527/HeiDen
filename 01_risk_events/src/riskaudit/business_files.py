"""Inspect uploaded broker business files without coupling to the HTTP server."""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
import re
from typing import Any

import pandas as pd

DEFAULT_ANALYSIS_MODE = "SINGLE_MODEL_RUN"
PEER_ANALYSIS_MODE = "PEER_BENCHMARK"
QUARTER_PATTERN = re.compile(r"(20\d{2})[ _-]?[Qq]([1-4])")


def business_file_columns(path: str | Path) -> tuple[str, ...]:
    source = Path(path)
    if source.suffix.lower() == ".csv":
        frame = pd.read_csv(
            source,
            nrows=0,
            dtype=object,
            keep_default_na=False,
            encoding="utf-8-sig",
        )
    elif source.suffix.lower() in {".xlsx", ".xlsm"}:
        frame = pd.read_excel(source, nrows=0, dtype=object)
    else:
        raise ValueError("只支持 CSV、XLSX 或 XLSM 集中度文件")
    return tuple(str(item) for item in frame.columns)


def inspect_business_file(path: str | Path) -> dict[str, Any]:
    from riskaudit.broker_metrics.constants import BUSINESS_BROKER_COLUMNS

    source = Path(path)
    minimum, maximum, row_count = inspect_business_date_range(source)
    quarter = quarter_from_name_or_date(source.name, maximum)
    recommended_start = recommended_observation_start(minimum, maximum)
    recognized_brokers = tuple(
        item for item in BUSINESS_BROKER_COLUMNS if item in business_file_columns(source)
    )
    return {
        "file_name": source.name,
        "file_size": source.stat().st_size,
        "row_count": row_count,
        "source_min_date": minimum.isoformat(),
        "source_max_date": maximum.isoformat(),
        "quarter": quarter,
        "recommended_observation_start": recommended_start.isoformat(),
        "recommended_observation_end": maximum.isoformat(),
        "observation_policy": "CALENDAR_YEAR_START_TO_SOURCE_MAX_DATE",
        "recognized_brokers": list(recognized_brokers),
        "recognized_broker_count": len(recognized_brokers),
        "expected_peer_broker_count": len(BUSINESS_BROKER_COLUMNS),
        "missing_peer_brokers": [
            item for item in BUSINESS_BROKER_COLUMNS if item not in recognized_brokers
        ],
        "suggested_analysis_modes": (
            [PEER_ANALYSIS_MODE]
            if set(recognized_brokers) == set(BUSINESS_BROKER_COLUMNS)
            else [DEFAULT_ANALYSIS_MODE]
            if "券商03" in recognized_brokers
            else []
        ),
    }


def inspect_business_date_range(path: str | Path) -> tuple[date, date, int]:
    """Read only the uploaded file's own business-date extent and row count."""
    source = Path(path)
    suffix = source.suffix.lower()
    if suffix == ".csv":
        return inspect_csv(source)
    if suffix in {".xlsx", ".xlsm"}:
        frame = pd.read_excel(source, dtype=object)
        date_field = find_date_field(tuple(str(item) for item in frame.columns))
        dates = pd.to_datetime(frame[date_field], errors="coerce").dropna()
        if dates.empty:
            raise ValueError("源文件业务日期列没有可解析日期")
        return dates.min().date(), dates.max().date(), len(frame)
    raise ValueError("只支持 CSV、XLSX 或 XLSM 集中度文件")


def recommended_observation_start(_minimum: date, maximum: date) -> date:
    """Use January 1 of the uploaded file's year as the risk-event boundary."""
    return date(maximum.year, 1, 1)


def inspect_csv(path: Path) -> tuple[date, date, int]:
    minimum: date | None = None
    maximum: date | None = None
    row_count = 0
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        date_field = find_date_field(tuple(reader.fieldnames or ()))
        for row in reader:
            raw = str(row.get(date_field, "")).strip()
            if not raw:
                continue
            current = date.fromisoformat(raw[:10].replace("/", "-"))
            minimum = current if minimum is None or current < minimum else minimum
            maximum = current if maximum is None or current > maximum else maximum
            row_count += 1
    if minimum is None or maximum is None:
        raise ValueError("源文件业务日期列没有可解析日期")
    return minimum, maximum, row_count


def find_date_field(fields: tuple[str, ...]) -> str:
    for candidate in ("biz_date", "交易日期", "业务日期", "date"):
        if candidate in fields:
            return candidate
    raise ValueError("源文件缺少业务日期列（biz_date/交易日期/业务日期）")


def quarter_from_name_or_date(name: str, maximum: date) -> str:
    matched = QUARTER_PATTERN.search(name)
    if matched:
        return f"{matched.group(1)}Q{matched.group(2)}"
    return f"{maximum.year}Q{(maximum.month - 1) // 3 + 1}"


def read_business_frame(path: str | Path) -> pd.DataFrame:
    source = Path(path)
    if source.suffix.lower() == ".csv":
        return pd.read_csv(
            source,
            dtype=object,
            keep_default_na=False,
            encoding="utf-8-sig",
        )
    return pd.read_excel(source, dtype=object, keep_default_na=False)
