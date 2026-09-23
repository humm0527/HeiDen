"""Build model-comparison descriptive tables from frozen PIT classifications.

The module deliberately keeps maintenance-ratio mappings and backtest-only
statistics as explicit pending inputs.  It never infers those values from an
old report.
"""

from __future__ import annotations

from collections.abc import Mapping
import json
from datetime import date
from pathlib import Path
from typing import Any

from .descriptive_calculations import (
    MODEL_LABELS,
    _blank_table11_rows,
    _blank_table5_rows,
    _table11_model_rows,
    _table4_rows,
    _table5_rows,
)
from .descriptive_inputs import (
    _as_date,
    _load_history,
    _load_market_caps,
    _load_prices,
)
from .descriptive_support import _sha256, _write_csv


MAINTENANCE_LINES = ("全维保", "180%+", "200%+", "240%+", "400%+", "500%+")
CLASSIFICATION_TIMING = "T-1日收盘后可得档位评价T日行情"
AMPLITUDE_FORMULA = "(最高价-最低价)/前收盘价"
RETURN_FORMULA = "T日后复权收盘价/T-1交易日后复权收盘价-1"
VOLATILITY_FORMULA = "档位逐日等权收益率样本标准差×sqrt(252)"
MAX_DRAWDOWN_FORMULA = "档位逐日等权收益率复合净值相对历史峰值的最小跌幅"
TURNOVER_FORMULA = "有效股票日成交额算术平均值/1亿元"


def build_model_descriptive_tables(
    *,
    output_dir: str | Path,
    observation_start: str | date,
    observation_end: str | date,
    new_history_files: Mapping[str, str | Path] | None,
    legacy_history_files: Mapping[str, str | Path] | None,
    market_price_csv: str | Path | None = None,
    adjusted_market_price_csv: str | Path | None = None,
    market_cap_csv: str | Path | None = None,
    broker_id: str = "券商03",
) -> dict[str, Any]:
    """Write tables 4--7, 10 and 11 plus a compact auditable manifest."""

    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)
    start = _as_date(observation_start)
    end = _as_date(observation_end)
    histories: dict[str, dict[str, Any] | None] = {}
    history_errors: dict[str, str] = {}
    for model, sources in (
        ("legacy", legacy_history_files),
        ("new", new_history_files),
    ):
        try:
            histories[model] = _load_history(
                sources or {}, broker_id=broker_id, start=start, end=end
            )
        except (FileNotFoundError, ValueError, KeyError) as exc:
            histories[model] = None
            history_errors[model] = str(exc)

    table4_rows = _table4_rows(histories)
    table4_status = "READY" if all(histories.values()) else "PENDING_HISTORY_INPUT"

    cap_path = Path(market_cap_csv) if market_cap_csv else None
    if cap_path and cap_path.is_file() and all(histories.values()):
        try:
            caps, cap_date = _load_market_caps(cap_path, end=end)
            table5_rows = _table5_rows(histories, caps)
            table5_status = "READY"
            table5_reason = None
        except (ValueError, KeyError) as exc:
            table5_rows = _blank_table5_rows()
            table5_status = "PENDING_MARKET_CAP_INPUT"
            table5_reason = str(exc)
            cap_date = None
    else:
        table5_rows = _blank_table5_rows()
        table5_status = "PENDING_MARKET_CAP_INPUT"
        table5_reason = (
            "缺少冻结历史总市值输入；需提供快照日证券总市值，不能复用旧报告数值"
        )
        cap_date = None

    table6_rows = [
        {
            "维持担保比例": line,
            "现行分类对应档位": "",
            "股票打分对应档位": "",
            "现行分类股票数量": "",
            "股票打分股票数量": "",
            "数量变动": "",
            "状态": "MAPPING_PENDING",
        }
        for line in MAINTENANCE_LINES
    ]
    table7_rows = [
        {
            "维持担保比例": line,
            "现行分类对应档位": "",
            "股票打分对应档位": "",
            "现行分类总市值（万亿元）": "",
            "股票打分总市值（万亿元）": "",
            "市值变动（万亿元）": "",
            "状态": "MAPPING_PENDING",
        }
        for line in MAINTENANCE_LINES
    ]
    table10_rows = [
        {
            "分类方法": label,
            "模拟数量": "",
            "收盘担保比例低于130%的数量占比": "",
            "次日T+1收盘担保比例低于110%的强平数量占比": "",
            "平仓数量占比": "",
            "平仓触发平均维保比例": "",
            "穿仓数量占比": "",
            "穿仓样本平均净资产差": "",
            "状态": "BACKTEST_PENDING",
        }
        for label in (MODEL_LABELS["new"], MODEL_LABELS["legacy"])
    ]

    price_path = Path(market_price_csv) if market_price_csv else None
    adjusted_price_path = (
        Path(adjusted_market_price_csv) if adjusted_market_price_csv else None
    )
    if price_path and price_path.is_file() and all(histories.values()):
        try:
            prices, price_stats = _load_prices(
                price_path,
                start=start,
                end=end,
                adjusted_path=(
                    adjusted_price_path
                    if adjusted_price_path and adjusted_price_path.is_file()
                    else None
                ),
            )
            table11_rows = []
            table11_quality: dict[str, Any] = {"price": price_stats, "models": {}}
            for model in ("new", "legacy"):
                rows, quality = _table11_model_rows(
                    model=model,
                    history=histories[model],
                    prices=prices,
                    start=start,
                    end=end,
                )
                table11_rows.extend(rows)
                table11_quality["models"][model] = quality
            if adjusted_price_path and adjusted_price_path.is_file():
                table11_status = "READY"
                table11_reason = None
            else:
                table11_status = "PENDING_ADJUSTED_RETURN_INPUT"
                table11_reason = (
                    "振幅和成交额已按未复权实际行情计算；波动率和最大回撤"
                    "缺少与快照匹配的后复权收盘价，保持为空"
                )
        except (ValueError, KeyError) as exc:
            table11_rows = _blank_table11_rows()
            table11_status = "PENDING_MARKET_PRICE_INPUT"
            table11_reason = str(exc)
            table11_quality = {}
    else:
        table11_rows = _blank_table11_rows()
        table11_status = "PENDING_MARKET_PRICE_INPUT"
        table11_reason = "缺少与父任务市场快照一致的股票日行情表"
        table11_quality = {}

    artifacts = {
        "table4": ("表4_档位股票数量对比.csv", table4_rows),
        "table5": ("表5_档位总市值对比.csv", table5_rows),
        "table6": ("表6_维保线股票数量对比_待映射.csv", table6_rows),
        "table7": ("表7_维保线总市值对比_待映射.csv", table7_rows),
        "table10": ("表10_历史平仓穿仓回测_待补.csv", table10_rows),
        "table11": ("表11_档位流动性波动性对比.csv", table11_rows),
    }
    files: dict[str, dict[str, Any]] = {}
    for key, (filename, rows) in artifacts.items():
        path = target / filename
        _write_csv(path, rows)
        files[key] = {
            "path": str(path),
            "filename": filename,
            "sha256": _sha256(path),
            "row_count": len(rows),
        }

    manifest = {
        "status": "READY_WITH_PENDING_INPUTS",
        "observation_start": start.isoformat(),
        "observation_end": end.isoformat(),
        "snapshot_classification_date": end.isoformat(),
        "broker_id": broker_id,
        "tables": {
            "table4": {"status": table4_status},
            "table5": {
                "status": table5_status,
                "reason": table5_reason,
                "market_cap_date": cap_date.isoformat() if cap_date else None,
            },
            "table6": {"status": "MAPPING_PENDING"},
            "table7": {"status": "MAPPING_PENDING"},
            "table10": {"status": "BACKTEST_PENDING"},
            "table11": {
                "status": table11_status,
                "reason": table11_reason,
                "quality": table11_quality,
            },
        },
        "history_errors": history_errors,
        "formulas": {
            "classification_timing": CLASSIFICATION_TIMING,
            "amplitude": AMPLITUDE_FORMULA,
            "daily_return": RETURN_FORMULA,
            "annualized_volatility": VOLATILITY_FORMULA,
            "maximum_drawdown": MAX_DRAWDOWN_FORMULA,
            "average_daily_turnover": TURNOVER_FORMULA,
            "price_adjustment": (
                "振幅和成交额使用未复权实际行情；收益、波动率和最大回撤使用"
                "RQData后复权收盘价（含权息修复）"
                if adjusted_price_path and adjusted_price_path.is_file()
                else "收益、波动率和最大回撤待后复权行情输入"
            ),
        },
        "preview_rows": {
            "table4": table4_rows,
            "table5": table5_rows,
            "table11": table11_rows,
        },
        "files": files,
    }
    manifest_path = target / "描述性表格计算清单.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    files["manifest"] = {
        "path": str(manifest_path),
        "filename": manifest_path.name,
        "sha256": _sha256(manifest_path),
        "row_count": None,
    }
    manifest["files"] = files
    return manifest
