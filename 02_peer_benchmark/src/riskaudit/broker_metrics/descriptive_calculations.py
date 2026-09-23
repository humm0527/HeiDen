"""
文件作用：计算模型对比描述性表格的档位、市值与行情统计行。
编辑记录：
【首次生成：2026-09-01，从描述性表格编排器拆出纯计算职责，不改变计算公式。】
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import date
from math import sqrt
from statistics import stdev
from typing import Any


GRADES = tuple("ABCDEFG")
MODEL_LABELS = {"legacy": "现行分类", "new": "股票打分"}


def _table4_rows(histories: Mapping[str, dict[str, Any] | None]) -> list[dict]:
    counts: dict[str, dict[str, int]] = {}
    for model in ("legacy", "new"):
        snapshot = (histories.get(model) or {}).get("snapshot", {})
        counts[model] = {
            grade: sum(1 for value in snapshot.values() if value == grade)
            for grade in GRADES
        }
    totals = {model: sum(values.values()) for model, values in counts.items()}
    rows = []
    for grade in GRADES:
        legacy_count = counts["legacy"][grade]
        new_count = counts["new"][grade]
        legacy_share = legacy_count / totals["legacy"] if totals["legacy"] else None
        new_share = new_count / totals["new"] if totals["new"] else None
        rows.append(
            {
                "档位": grade,
                "现行分类数量": legacy_count if totals["legacy"] else "",
                "现行分类占比": legacy_share,
                "股票打分数量": new_count if totals["new"] else "",
                "股票打分占比": new_share,
                "数量变动": new_count - legacy_count if all(totals.values()) else "",
                "占比变动": new_share - legacy_share if legacy_share is not None and new_share is not None else "",
            }
        )
    rows.append(
        {
            "档位": "总计",
            "现行分类数量": totals["legacy"] or "",
            "现行分类占比": 1.0 if totals["legacy"] else "",
            "股票打分数量": totals["new"] or "",
            "股票打分占比": 1.0 if totals["new"] else "",
            "数量变动": "",
            "占比变动": "",
        }
    )
    return rows

def _table5_rows(histories: Mapping[str, dict[str, Any] | None], caps: Mapping[str, float]) -> list[dict]:
    values: dict[str, dict[str, float]] = {}
    coverage: dict[str, dict[str, int]] = {}
    for model in ("legacy", "new"):
        snapshot = histories[model]["snapshot"]
        values[model] = {}
        coverage[model] = {}
        for grade in GRADES:
            securities = [security for security, value in snapshot.items() if value == grade]
            valid = [caps[security] for security in securities if security in caps]
            values[model][grade] = sum(valid)
            coverage[model][grade] = len(valid)
    totals = {model: sum(items.values()) for model, items in values.items()}
    rows = []
    for grade in GRADES:
        old = values["legacy"][grade] / 100_000_000
        new = values["new"][grade] / 100_000_000
        old_share = values["legacy"][grade] / totals["legacy"] if totals["legacy"] else None
        new_share = values["new"][grade] / totals["new"] if totals["new"] else None
        rows.append({
            "档位": grade,
            "现行分类总市值（亿元）": old,
            "现行分类占比": old_share,
            "现行分类有效市值证券数": coverage["legacy"][grade],
            "股票打分总市值（亿元）": new,
            "股票打分占比": new_share,
            "股票打分有效市值证券数": coverage["new"][grade],
            "市值变动（亿元）": new - old,
            "占比变动": new_share - old_share if old_share is not None and new_share is not None else "",
        })
    rows.append({
        "档位": "总计",
        "现行分类总市值（亿元）": totals["legacy"] / 100_000_000,
        "现行分类占比": 1.0 if totals["legacy"] else "",
        "现行分类有效市值证券数": sum(coverage["legacy"].values()),
        "股票打分总市值（亿元）": totals["new"] / 100_000_000,
        "股票打分占比": 1.0 if totals["new"] else "",
        "股票打分有效市值证券数": sum(coverage["new"].values()),
        "市值变动（亿元）": "",
        "占比变动": "",
    })
    return rows

def _blank_table5_rows() -> list[dict]:
    return [{
        "档位": grade,
        "现行分类总市值（亿元）": "",
        "现行分类占比": "",
        "现行分类有效市值证券数": "",
        "股票打分总市值（亿元）": "",
        "股票打分占比": "",
        "股票打分有效市值证券数": "",
        "市值变动（亿元）": "",
        "占比变动": "",
    } for grade in (*GRADES, "总计")]

def _table11_model_rows(
    *,
    model: str,
    history: dict[str, Any],
    prices: Mapping[date, Mapping[str, tuple[float, float | None, float]]],
    start: date,
    end: date,
) -> tuple[list[dict], dict]:
    active = dict(history["initial"])
    totals = {grade: {"amplitude_sum": 0.0, "amount_sum": 0.0, "stock_days": 0, "daily_returns": [], "securities": set()} for grade in GRADES}
    timeline = sorted(
        day
        for day in set(prices) | set(history["daily"])
        if start <= day <= end
    )
    for day in timeline:
        day_returns: dict[str, list[float]] = defaultdict(list)
        for security, (amplitude, simple_return, amount) in prices.get(day, {}).items():
            grade = str(active.get(security, "")).upper()
            if grade not in GRADES:
                continue
            bucket = totals[grade]
            bucket["amplitude_sum"] += amplitude
            bucket["amount_sum"] += amount
            bucket["stock_days"] += 1
            bucket["securities"].add(security)
            if simple_return is not None:
                day_returns[grade].append(simple_return)
        for grade, values in day_returns.items():
            totals[grade]["daily_returns"].append(sum(values) / len(values))
        # The dated classification only becomes available after this day's close.
        active.update(history["daily"].get(day, {}))

    rows = []
    for grade in GRADES:
        bucket = totals[grade]
        returns = bucket["daily_returns"]
        volatility = stdev(returns) * sqrt(252) if len(returns) >= 2 else None
        drawdown = _maximum_drawdown(returns) if returns else None
        stock_days = bucket["stock_days"]
        rows.append({
            "分类方法": MODEL_LABELS[model],
            "档位": grade,
            "平均振幅": bucket["amplitude_sum"] / stock_days if stock_days else "",
            "年化波动率": volatility if volatility is not None else "",
            "最大回撤": drawdown if drawdown is not None else "",
            "平仓样本占比": "",
            "穿仓样本占比": "",
            "日均成交额（亿元）": bucket["amount_sum"] / stock_days / 100_000_000 if stock_days else "",
            "有效交易日数": len(returns),
            "有效股票日数": stock_days,
            "去重股票数": len(bucket["securities"]),
            "状态": (
                "READY"
                if stock_days and returns
                else "ADJUSTED_RETURN_PENDING"
                if stock_days
                else "NO_VALID_SAMPLE"
            ),
        })
    return rows, {
        "valid_stock_day_count": sum(item["stock_days"] for item in totals.values()),
        "valid_grade_day_count": sum(len(item["daily_returns"]) for item in totals.values()),
    }

def _blank_table11_rows() -> list[dict]:
    return [{
        "分类方法": MODEL_LABELS[model], "档位": grade, "平均振幅": "", "年化波动率": "",
        "最大回撤": "", "平仓样本占比": "", "穿仓样本占比": "", "日均成交额（亿元）": "",
        "有效交易日数": "", "有效股票日数": "", "去重股票数": "", "状态": "PENDING_MARKET_PRICE_INPUT",
    } for model in ("new", "legacy") for grade in GRADES]

def _maximum_drawdown(returns: list[float]) -> float:
    nav = peak = 1.0
    worst = 0.0
    for value in returns:
        nav *= 1.0 + value
        peak = max(peak, nav)
        if peak:
            worst = min(worst, nav / peak - 1.0)
    return worst


