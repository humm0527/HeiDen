"""
文件作用：把真实集中度宽表证券并集与持久化中文市场标准表装配为风险事件算法使用的 canonical 五表输入。
编辑记录：
【首次生成：2026-08-11，实现宽表证券并集提取、中文标准字段反向加载和分块运行确定性合并。】
【第二次编辑：2026-08-11，增加历史板块快照标准化和候选跌停日 PIT 板块补取请求识别。】
【第三次编辑：2026-08-18，允许以全券商基准文件补足单券商窄表的风险事件证券全集。】
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable

import pandas as pd
import yaml

from .identifiers import normalize_business_security_code
from .models import RiskInputTables


MARKET_TABLE_NAMES = (
    "交易日历表",
    "证券基础状态日表",
    "股票日行情表",
    "证券风险状态表",
)


@dataclass(frozen=True)
class BusinessUniverseSummary:
    source_row_count: int
    canonical_row_count: int
    security_count: int
    bse_security_count: int
    min_classification_date: str
    max_classification_date: str


def load_business_risk_universe(
    source_path: str | Path,
    *,
    observation_end: str | date,
    supplemental_source_paths: Iterable[str | Path] = (),
) -> tuple[pd.DataFrame, BusinessUniverseSummary]:
    sources = _unique_paths((source_path, *supplemental_source_paths))
    end = date.fromisoformat(str(observation_end))
    selected_frames: list[pd.DataFrame] = []
    primary_row_count = 0
    for index, source in enumerate(sources):
        raw = pd.read_csv(
            source,
            dtype=object,
            keep_default_na=False,
            encoding="utf-8-sig",
        )
        if index == 0:
            primary_row_count = len(raw)
        required = {"biz_date", "stk_code"}
        missing = required - set(raw.columns)
        if missing:
            raise ValueError(
                f"集中度宽表 {source.name} 缺少证券并集字段: "
                + ", ".join(sorted(missing))
            )
        dates = pd.to_datetime(raw["biz_date"], errors="raise").dt.date
        selected_frames.append(
            raw.loc[dates <= end, ["biz_date", "stk_code"]].copy()
        )
    selected = pd.concat(selected_frames, ignore_index=True)
    selected["classification_date"] = pd.to_datetime(
        selected["biz_date"], errors="raise"
    ).dt.date.astype(str)
    normalized = selected["stk_code"].map(normalize_business_security_code)
    selected["market_code"] = normalized.map(lambda item: item[0])
    selected["security_code"] = normalized.map(lambda item: item[1])
    universe = selected.loc[
        :, ["classification_date", "market_code", "security_code"]
    ].drop_duplicates(ignore_index=True)
    securities = universe.loc[:, ["market_code", "security_code"]].drop_duplicates()
    summary = BusinessUniverseSummary(
        source_row_count=primary_row_count,
        canonical_row_count=len(universe),
        security_count=len(securities),
        bse_security_count=int((securities["market_code"] == "XBSE").sum()),
        min_classification_date=str(universe["classification_date"].min()),
        max_classification_date=str(universe["classification_date"].max()),
    )
    return universe, summary


def _unique_paths(paths: Iterable[str | Path]) -> tuple[Path, ...]:
    output: list[Path] = []
    seen: set[Path] = set()
    for value in paths:
        path = Path(value)
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        output.append(path)
    return tuple(output)


def load_standard_code_tables(
    run_dir: str | Path,
    registry_path: str | Path,
) -> dict[str, pd.DataFrame]:
    root = Path(run_dir)
    registry = yaml.safe_load(Path(registry_path).read_text(encoding="utf-8"))
    loaded: dict[str, pd.DataFrame] = {}
    for table_name in MARKET_TABLE_NAMES:
        path = root / f"{table_name}.csv"
        if not path.exists():
            raise FileNotFoundError(f"标准运行缺少 {path.name}: {root}")
        frame = pd.read_csv(
            path,
            dtype=object,
            keep_default_na=False,
            encoding="utf-8-sig",
        )
        fields = registry["tables"][table_name]["fields"]
        rename = {
            str(item["business_field"]): str(item["python_field"])
            for item in fields
        }
        loaded[table_name] = frame.rename(columns=rename)
    return loaded


def merge_standard_code_runs(
    run_dirs: Iterable[str | Path],
    registry_path: str | Path,
) -> dict[str, pd.DataFrame]:
    batches = [load_standard_code_tables(path, registry_path) for path in run_dirs]
    if not batches:
        raise ValueError("至少需要一个成功的市场分块运行")
    merged: dict[str, pd.DataFrame] = {}
    keys = {
        "交易日历表": ["market_code", "calendar_date", "record_version"],
        "证券基础状态日表": [
            "market_code",
            "security_code",
            "status_date",
            "record_version",
        ],
        "股票日行情表": [
            "market_code",
            "security_code",
            "trading_date",
            "market_data_version",
        ],
        "证券风险状态表": ["risk_status_record_id", "record_version"],
    }
    for table_name in MARKET_TABLE_NAMES:
        frame = pd.concat(
            [batch[table_name] for batch in batches],
            ignore_index=True,
            sort=False,
        )
        merged[table_name] = frame.drop_duplicates(keys[table_name], keep="last")
    return merged


def build_risk_input_tables(
    merged_market_tables: dict[str, pd.DataFrame],
    broker_risk_universe: pd.DataFrame,
) -> RiskInputTables:
    return RiskInputTables(
        trading_calendar=merged_market_tables["交易日历表"],
        security_status_daily=merged_market_tables["证券基础状态日表"],
        stock_market_daily=merged_market_tables["股票日行情表"],
        security_risk_status=merged_market_tables["证券风险状态表"],
        broker_risk_classification=broker_risk_universe,
    )


def build_pit_security_status_rows(
    records: Iterable[dict[str, object]],
    *,
    status_date: str,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for raw in records:
        order_book_id = str(raw["order_book_id"])
        market_code = str(raw.get("exchange") or order_book_id.rsplit(".", 1)[-1])
        lifecycle_status = str(raw.get("status") or "")
        rows.append(
            {
                "market_code": market_code,
                "security_code": order_book_id,
                "status_date": status_date,
                "record_version": "1",
                "board_code": str(raw.get("board_type") or ""),
                "security_type": str(raw.get("type") or "CS"),
                "lifecycle_status": lifecycle_status,
                "listed_date": _date_text(raw.get("listed_date")),
                "delisted_date": _date_text(raw.get("de_listed_date")),
                "trading_eligibility_status": (
                    "终止" if lifecycle_status.lower() == "delisted" else "正常"
                ),
                "source_record_id": f"rqdata-instrument-pit:{order_book_id}:{status_date}",
            }
        )
    return pd.DataFrame(rows)


def find_missing_pit_board_requests(
    tables: RiskInputTables,
    *,
    decimal_scale: int,
) -> dict[str, tuple[str, ...]]:
    master = tables.security_status_daily.copy()
    master["_date"] = pd.to_datetime(master["status_date"], errors="raise").dt.date
    empty_master = master.iloc[0:0]
    master_groups = {
        (str(key[0]), str(key[1])): rows
        for key, rows in master.groupby(
            ["market_code", "security_code"], sort=False, dropna=False
        )
    }
    st = tables.security_risk_status.copy()
    st = st.loc[st["risk_status_type"].astype(str).str.upper() == "ST"].copy()
    st["fact_date"] = pd.to_datetime(st["status_start_date"], errors="raise").dt.date
    st_index = {
        (str(row.market_code), str(row.security_code), row.fact_date): _st_bool(
            row.risk_status_value
        )
        for row in st.itertuples(index=False)
    }
    earliest: dict[tuple[str, str], date] = {}
    market = tables.stock_market_daily.copy()
    market["fact_date"] = pd.to_datetime(market["trading_date"], errors="raise").dt.date
    for row in market.itertuples(index=False):
        key = (str(row.market_code), str(row.security_code))
        if key[0] == "XBSE" or st_index.get((*key, row.fact_date), True):
            continue
        if str(row.trading_status).strip() not in {
            "正常成交",
            "TRADED",
            "NORMAL_TRADED",
        }:
            continue
        if not _decimal_equal(row.close_price, row.limit_down_price, decimal_scale):
            continue
        valid = master_groups.get(key, empty_master)
        valid = valid.loc[valid["_date"] <= row.fact_date]
        if valid.empty:
            earliest[key] = min(earliest.get(key, row.fact_date), row.fact_date)
    grouped: dict[str, list[str]] = {}
    for key, snapshot_date in sorted(earliest.items(), key=lambda item: (item[1], item[0])):
        grouped.setdefault(snapshot_date.isoformat(), []).append(key[1])
    return {day: tuple(codes) for day, codes in grouped.items()}


def _date_text(value: object) -> str:
    text = str(value or "").split("T", 1)[0].split(" ", 1)[0]
    return "" if text in {"", "0000-00-00", "NaT", "None"} else text


def _st_bool(value: object) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "是", "生效", "st", "*st"}


def _decimal_equal(left: object, right: object, scale: int) -> bool:
    try:
        quantum = Decimal(1).scaleb(-scale)
        return Decimal(str(left)).quantize(quantum) == Decimal(str(right)).quantize(quantum)
    except (InvalidOperation, ValueError, TypeError):
        return False
