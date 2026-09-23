"""
文件作用：按券商原始分类档位计算下一交易日收盘价简单收益及汇总表现。
编辑记录：
【首次生成：2026-08-31，新增全行业券商各原始档位的 T+1 收益率评价口径。】
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

import pandas as pd

from riskaudit.cancellation import raise_if_canceled


PRICE_RETURN_FORMULA = "T日后复权收盘价/T-1交易日后复权收盘价-1"
CLASSIFICATION_TIMING = "T-1日收盘档位评价T日收益"
PRICE_ADJUSTMENT = "RQData后复权价格收益（权息修复）"
UNADJUSTED_RETURN_FORMULA = "收盘价/原始前收盘价-1"
UNADJUSTED_PRICE_ADJUSTMENT = "未复权价格收益（旧口径，仅审计对照）"
RETURN_QUANTUM = Decimal("0.0000000001")


def calculate_broker_grade_returns(
    *,
    market_price_csv: str | Path,
    daily_rows: Mapping[date, Mapping[tuple[str, str], tuple[str, ...]]],
    initial_rows: Mapping[tuple[str, str], tuple[str, ...]],
    broker_ids: tuple[str, ...],
    securities: Iterable[tuple[str, str]],
    snapshot_dates: tuple[date, ...],
    lifecycle: Mapping[tuple[str, str], tuple[date, date | None]],
    price_adjustment: str = PRICE_ADJUSTMENT,
    return_formula: str = PRICE_RETURN_FORMULA,
    cancel_check: Callable[[], bool] | None = None,
) -> tuple[list[dict], list[dict], dict]:
    """Return summary, daily detail and compact data-quality statistics.

    A classification row dated T is first usable on T+1.  Therefore the loop
    calculates the return for T from the active state carried through T-1,
    then applies the T classification rows for the next trading day.
    """

    allowed_dates = frozenset(snapshot_dates)
    allowed_securities = frozenset(securities)
    returns_by_date, price_stats = _load_simple_returns(
        market_price_csv,
        allowed_dates=allowed_dates,
        allowed_securities=allowed_securities,
        cancel_check=cancel_check,
    )
    broker_positions = {
        broker: position for position, broker in enumerate(broker_ids)
    }
    active = {broker: {} for broker in broker_ids}
    for security, values in initial_rows.items():
        for broker, position in broker_positions.items():
            active[broker][security] = values[position]

    daily_output: list[dict] = []
    aggregate: dict[tuple[str, str], dict] = {}
    ordered_securities = tuple(sorted(allowed_securities))

    for trading_date in snapshot_dates:
        raise_if_canceled(cancel_check)
        day_returns = returns_by_date.get(trading_date, {})
        day_buckets: dict[tuple[str, str], dict[str, object]] = defaultdict(
            lambda: {
                "classified_count": 0,
                "valid_count": 0,
                "return_sum": Decimal("0"),
                "positive_count": 0,
            }
        )
        for security in ordered_securities:
            lifecycle_row = lifecycle.get(security)
            if lifecycle_row is None:
                continue
            listed, delisted = lifecycle_row
            if trading_date < listed or (
                delisted is not None and trading_date >= delisted
            ):
                continue
            security_return = day_returns.get(security)
            for broker in broker_ids:
                raw_grade = str(active[broker].get(security, "") or "").strip()
                if not raw_grade:
                    continue
                bucket = day_buckets[(broker, raw_grade)]
                bucket["classified_count"] += 1
                if security_return is None:
                    continue
                bucket["valid_count"] += 1
                bucket["return_sum"] += security_return
                if security_return > 0:
                    bucket["positive_count"] += 1

        for (broker, raw_grade), bucket in sorted(day_buckets.items()):
            classified_count = int(bucket["classified_count"])
            valid_count = int(bucket["valid_count"])
            return_sum = Decimal(bucket["return_sum"])
            positive_count = int(bucket["positive_count"])
            daily_return = (
                return_sum / Decimal(valid_count) if valid_count else None
            )
            daily_output.append(
                {
                    "broker_id": broker,
                    "raw_classification": raw_grade,
                    "return_date": trading_date.isoformat(),
                    "classified_security_count": classified_count,
                    "valid_return_security_count": valid_count,
                    "return_coverage_rate": (
                        _rate(Decimal(valid_count) / Decimal(classified_count))
                        if classified_count
                        else None
                    ),
                    "equal_weight_daily_return": _rate(daily_return),
                    "positive_return_security_count": positive_count,
                    "positive_return_rate": (
                        _rate(Decimal(positive_count) / Decimal(valid_count))
                        if valid_count
                        else None
                    ),
                    "return_formula": return_formula,
                    "classification_timing": CLASSIFICATION_TIMING,
                    "price_adjustment": price_adjustment,
                }
            )
            total = aggregate.setdefault(
                (broker, raw_grade),
                {
                    "classified_days": 0,
                    "valid_days": 0,
                    "classified_stock_days": 0,
                    "valid_stock_days": 0,
                    "individual_return_sum": Decimal("0"),
                    "positive_stock_days": 0,
                    "daily_return_sum": Decimal("0"),
                    "compounded_growth": Decimal("1"),
                    "first_date": trading_date,
                    "last_date": trading_date,
                },
            )
            total["classified_days"] += 1
            total["classified_stock_days"] += classified_count
            total["valid_stock_days"] += valid_count
            total["individual_return_sum"] += return_sum
            total["positive_stock_days"] += positive_count
            total["first_date"] = min(total["first_date"], trading_date)
            total["last_date"] = max(total["last_date"], trading_date)
            if daily_return is not None:
                total["valid_days"] += 1
                total["daily_return_sum"] += daily_return
                total["compounded_growth"] *= Decimal("1") + daily_return

        # The row dated T only becomes usable on the following trading day.
        for security, values in daily_rows.get(trading_date, {}).items():
            for broker, position in broker_positions.items():
                active[broker][security] = values[position]

    summary_output: list[dict] = []
    for (broker, raw_grade), total in sorted(aggregate.items()):
        classified_stock_days = int(total["classified_stock_days"])
        valid_stock_days = int(total["valid_stock_days"])
        valid_days = int(total["valid_days"])
        summary_output.append(
            {
                "broker_id": broker,
                "raw_classification": raw_grade,
                "return_start_date": total["first_date"].isoformat(),
                "return_end_date": total["last_date"].isoformat(),
                "classified_trading_day_count": int(total["classified_days"]),
                "valid_return_trading_day_count": valid_days,
                "classified_stock_day_count": classified_stock_days,
                "valid_return_stock_day_count": valid_stock_days,
                "return_coverage_rate": (
                    _rate(
                        Decimal(valid_stock_days) / Decimal(classified_stock_days)
                    )
                    if classified_stock_days
                    else None
                ),
                "mean_equal_weight_daily_return": (
                    _rate(total["daily_return_sum"] / Decimal(valid_days))
                    if valid_days
                    else None
                ),
                "compounded_equal_weight_return": (
                    _rate(total["compounded_growth"] - Decimal("1"))
                    if valid_days
                    else None
                ),
                "stock_day_weighted_mean_return": (
                    _rate(
                        total["individual_return_sum"] / Decimal(valid_stock_days)
                    )
                    if valid_stock_days
                    else None
                ),
                "positive_return_stock_day_count": int(
                    total["positive_stock_days"]
                ),
                "positive_return_rate": (
                    _rate(
                        Decimal(total["positive_stock_days"])
                        / Decimal(valid_stock_days)
                    )
                    if valid_stock_days
                    else None
                ),
                "return_formula": return_formula,
                "classification_timing": CLASSIFICATION_TIMING,
                "price_adjustment": price_adjustment,
            }
        )

    quality = {
        **price_stats,
        "summary_row_count": len(summary_output),
        "daily_row_count": len(daily_output),
        "broker_with_return_count": len(
            {row["broker_id"] for row in summary_output}
        ),
        "broker_without_return": sorted(
            set(broker_ids) - {row["broker_id"] for row in summary_output}
        ),
        "classification_timing": CLASSIFICATION_TIMING,
        "return_formula": return_formula,
        "price_adjustment": price_adjustment,
    }
    return summary_output, daily_output, quality


def _rate(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return value.quantize(RETURN_QUANTUM, rounding=ROUND_HALF_UP)


def _load_simple_returns(
    path: str | Path,
    *,
    allowed_dates: frozenset[date],
    allowed_securities: frozenset[tuple[str, str]],
    cancel_check: Callable[[], bool] | None,
) -> tuple[dict[date, dict[tuple[str, str], Decimal]], dict]:
    source = Path(path)
    header = tuple(
        pd.read_csv(
            source,
            nrows=0,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8-sig",
        ).columns
    )
    chinese = {
        "交易市场代码",
        "证券代码",
        "交易日期",
        "成交状态",
        "前收盘价",
        "收盘价",
    }
    standard = {
        "market_code",
        "security_code",
        "trading_date",
        "trading_status",
        "previous_close",
        "close_price",
    }
    if chinese <= set(header):
        columns = (
            "交易市场代码",
            "证券代码",
            "交易日期",
            "成交状态",
            "前收盘价",
            "收盘价",
        )
        normal_status = {"正常成交", "NORMAL", "TRADED"}
    elif standard <= set(header):
        columns = (
            "market_code",
            "security_code",
            "trading_date",
            "trading_status",
            "previous_close",
            "close_price",
        )
        normal_status = {"正常成交", "NORMAL", "TRADED"}
    else:
        raise ValueError("BM501_RETURN_PRICE_SCHEMA_INVALID")

    output: dict[date, dict[tuple[str, str], Decimal]] = defaultdict(dict)
    source_rows = eligible_rows = valid_rows = invalid_price_rows = 0
    duplicate_rows = 0
    extreme_rows = 0
    for frame in pd.read_csv(
        source,
        usecols=list(columns),
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
        chunksize=200_000,
    ):
        raise_if_canceled(cancel_check)
        for market, code, day_text, status, previous_close, close_price in frame.itertuples(
            index=False, name=None
        ):
            source_rows += 1
            trading_date = date.fromisoformat(str(day_text)[:10])
            security = (str(market).strip(), str(code).strip())
            if trading_date not in allowed_dates or security not in allowed_securities:
                continue
            eligible_rows += 1
            if str(status).strip() not in normal_status:
                continue
            try:
                previous = Decimal(str(previous_close).strip())
                current = Decimal(str(close_price).strip())
            except (InvalidOperation, ValueError):
                invalid_price_rows += 1
                continue
            if previous <= 0 or current < 0:
                invalid_price_rows += 1
                continue
            key_rows = output[trading_date]
            if security in key_rows:
                duplicate_rows += 1
                continue
            simple_return = current / previous - Decimal("1")
            key_rows[security] = simple_return
            valid_rows += 1
            if abs(simple_return) > Decimal("0.30"):
                extreme_rows += 1
    if duplicate_rows:
        raise ValueError("BM502_RETURN_PRICE_KEY_DUPLICATE")
    return dict(output), {
        "market_price_source_path": str(source.resolve()),
        "market_price_source_row_count": source_rows,
        "eligible_price_row_count": eligible_rows,
        "valid_return_row_count": valid_rows,
        "invalid_price_row_count": invalid_price_rows,
        "extreme_absolute_return_over_30pct_count": extreme_rows,
    }
