"""
文件作用：标准化券商宽表历史，执行周末行忽略、交易日/主键门禁并保留来源血缘。
编辑记录：
【首次生成：2026-08-12，实现 D-067 周末源行忽略和小规模/流式调用共用的记录构造。】
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
import re
from typing import Mapping

import pandas as pd

from .calendar import MarketCalendar
from .models import BrokerClassificationRecord, BrokerMetricFinding


SUFFIX_MARKETS = {
    ".SH": "XSHG",
    ".SZ": "XSHE",
    ".BJ": "XBSE",
    ".XSHG": "XSHG",
    ".XSHE": "XSHE",
    ".XBSE": "XBSE",
}


@dataclass(frozen=True)
class HistoryBuildResult:
    records: tuple[BrokerClassificationRecord, ...]
    findings: tuple[BrokerMetricFinding, ...]
    source_row_count: int
    ignored_weekend_row_count: int
    ignored_weekend_nonblank_cell_count: int
    ignored_weekend_dates: tuple[str, ...]


@dataclass(frozen=True)
class QuarterHistoryValidation:
    file_count: int
    row_count: int
    unique_key_count: int
    columns: tuple[str, ...]


def normalize_security(raw: object) -> tuple[str, str]:
    text = str(raw).strip()
    for suffix in sorted(SUFFIX_MARKETS, key=len, reverse=True):
        if text.endswith(suffix):
            code = text[: -len(suffix)]
            if len(code) != 6 or not code.isdigit():
                break
            return SUFFIX_MARKETS[suffix], f"{code}.{SUFFIX_MARKETS[suffix]}"
    raise ValueError(f"Security code requires an explicit supported suffix: {text}")


def build_history_records(
    frame: pd.DataFrame,
    broker_ids: tuple[str, ...],
    calendar: MarketCalendar,
    market_aliases: dict[str, str],
    *,
    source_file: str = "synthetic.csv",
    source_file_sha256: str = "",
) -> HistoryBuildResult:
    required = {"biz_date", "stk_code", *broker_ids}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Broker history fields missing: {sorted(missing)}")
    seen: set[tuple[date, str]] = set()
    records: list[BrokerClassificationRecord] = []
    findings: list[BrokerMetricFinding] = []
    ignored_dates: set[str] = set()
    ignored_rows = 0
    ignored_cells = 0
    source_hash = source_file_sha256 or sha256(
        frame.to_csv(index=False).encode("utf-8")
    ).hexdigest()

    for offset, row_data in enumerate(frame.to_dict(orient="records"), start=2):
        biz_date = date.fromisoformat(str(row_data["biz_date"])[:10])
        if biz_date.weekday() >= 5:
            ignored_rows += 1
            ignored_dates.add(biz_date.isoformat())
            ignored_cells += sum(
                bool(_raw_text(row_data[broker_id])) for broker_id in broker_ids
            )
            continue
        market, security_code = normalize_security(row_data["stk_code"])
        key = (biz_date, security_code)
        if key in seen:
            raise ValueError(f"BM103_HISTORY_KEY_DUPLICATE: {key}")
        seen.add(key)
        calendar_market = market_aliases.get(market)
        if calendar_market is None:
            raise ValueError(f"BM202_CALENDAR_ALIAS_MISSING: {market}")
        if not calendar.contains(calendar_market, biz_date):
            raise ValueError(f"BM104_HISTORY_NON_TRADING_DATE_INVALID: {biz_date}")
        first_usable = calendar.next_trading_day(calendar_market, biz_date)
        for broker_id in broker_ids:
            records.append(
                BrokerClassificationRecord(
                    broker_id=broker_id,
                    market_code=market,
                    security_code=security_code,
                    classification_date=biz_date,
                    first_usable_trading_date=first_usable,
                    raw_classification=_raw_text(row_data[broker_id]),
                    source_file=source_file,
                    source_file_sha256=source_hash,
                    source_row_number=offset,
                    source_record_id=(
                        f"{source_file}:{offset}:{broker_id}:{security_code}:{biz_date}"
                    ),
                )
            )
    if ignored_rows:
        findings.append(
            BrokerMetricFinding(
                code="BM106_WEEKEND_HISTORY_ROW_IGNORED",
                severity="INFO",
                message=(
                    f"Ignored {ignored_rows} weekend source rows and "
                    f"{ignored_cells} nonblank broker cells before the state chain"
                ),
                source_file=source_file,
            )
        )
    records.sort(
        key=lambda item: (
            item.broker_id,
            item.market_code,
            item.security_code,
            item.classification_date,
            item.source_row_number or 0,
        )
    )
    return HistoryBuildResult(
        records=tuple(records),
        findings=tuple(findings),
        source_row_count=len(frame),
        ignored_weekend_row_count=ignored_rows,
        ignored_weekend_nonblank_cell_count=ignored_cells,
        ignored_weekend_dates=tuple(sorted(ignored_dates)),
    )


def discover_quarter_files(directory: str | Path) -> tuple[Path, ...]:
    expected = [
        f"集中度{year}Q{quarter}.csv"
        for year, quarters in ((2023, (4,)), (2024, (1, 2, 3, 4)), (2025, (1, 2, 3, 4)), (2026, (1, 2, 3)))
        for quarter in quarters
    ]
    root = Path(directory)
    actual = sorted(path.name for path in root.glob("集中度*.csv"))
    if actual != sorted(expected):
        raise ValueError(f"BM101_HISTORY_FILE_SET_MISMATCH: {actual}")
    return tuple(root / name for name in expected)


def validate_quarter_history_frames(
    frames: Mapping[str, pd.DataFrame],
    broker_ids: tuple[str, ...],
) -> QuarterHistoryValidation:
    expected_names = {
        f"集中度{year}Q{quarter}.csv"
        for year, quarters in (
            (2023, (4,)),
            (2024, (1, 2, 3, 4)),
            (2025, (1, 2, 3, 4)),
            (2026, (1, 2, 3)),
        )
        for quarter in quarters
    }
    if set(frames) != expected_names:
        raise ValueError("BM101_HISTORY_FILE_SET_MISMATCH")
    expected_columns = ("biz_date", "stk_code", *broker_ids)
    seen: set[tuple[str, str]] = set()
    row_count = 0
    for name in sorted(frames):
        frame = frames[name]
        if tuple(frame.columns) != expected_columns:
            raise ValueError(f"BM102_HISTORY_SCHEMA_MISMATCH: {name}")
        match = re.fullmatch(r"集中度(\d{4})Q([1-4])\.csv", name)
        if match is None:
            raise ValueError(f"BM101_HISTORY_FILE_SET_MISMATCH: {name}")
        year, quarter = int(match.group(1)), int(match.group(2))
        start_month = (quarter - 1) * 3 + 1
        end_month = start_month + 2
        for row in frame.to_dict(orient="records"):
            biz_date = date.fromisoformat(str(row["biz_date"])[:10])
            if biz_date.year != year or not start_month <= biz_date.month <= end_month:
                raise ValueError(f"BM104_HISTORY_QUARTER_DATE_INVALID: {name}")
            key = (biz_date.isoformat(), str(row["stk_code"]).strip())
            if key in seen:
                raise ValueError(f"BM103_HISTORY_KEY_DUPLICATE: {key}")
            seen.add(key)
            row_count += 1
    return QuarterHistoryValidation(
        file_count=len(frames),
        row_count=row_count,
        unique_key_count=len(seen),
        columns=expected_columns,
    )


def _raw_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()
