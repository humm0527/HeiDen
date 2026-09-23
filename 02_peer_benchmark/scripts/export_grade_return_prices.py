"""Export an auditable post-adjusted close-price input for grade returns."""

from __future__ import annotations

import argparse
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Iterable

import pandas as pd


MARKETS = frozenset({"XSHG", "XSHE"})
OUTPUT_COLUMNS = (
    "交易市场代码",
    "证券代码",
    "交易日期",
    "成交状态",
    "前收盘价",
    "收盘价",
)


def _hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _chunks(values: list[str], size: int) -> Iterable[list[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def export_post_adjusted_prices(
    source_path: Path,
    output_dir: Path,
    *,
    batch_size: int = 500,
) -> tuple[Path, Path]:
    if not 1 <= batch_size <= 1000:
        raise ValueError("batch-size 必须为1—1000")
    if output_dir.exists():
        raise FileExistsError(f"输出目录已存在，请使用新目录: {output_dir}")
    source = pd.read_csv(
        source_path,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
    )
    source.rename(columns={
        "market_code": "交易市场代码", "security_code": "证券代码",
        "trading_date": "交易日期", "trading_status": "成交状态",
        "previous_close": "前收盘价", "close_price": "收盘价",
    }, inplace=True)
    missing = set(OUTPUT_COLUMNS) - set(source.columns)
    if missing:
        raise ValueError(f"源行情缺少字段: {', '.join(sorted(missing))}")

    if source.duplicated(["交易市场代码", "证券代码", "交易日期"]).any():
        raise ValueError("源行情存在重复证券日期")

    eligible = source[source["交易市场代码"].isin(MARKETS)].copy()
    codes = sorted(eligible["证券代码"].drop_duplicates())
    start_date = eligible["交易日期"].min()
    end_date = eligible["交易日期"].max()
    if not codes or not start_date or not end_date:
        raise ValueError("源行情没有沪深股票或有效日期")

    import rqdatac
    username, password = os.environ.get("RQDATA_USERNAME"), os.environ.get("RQDATA_PASSWORD")
    if bool(username) != bool(password):
        raise ValueError("RQDATA_USERNAME和RQDATA_PASSWORD需同时配置")
    rqdatac.init(username, password) if username and password else rqdatac.init()
    baseline_date = str(rqdatac.get_previous_trading_date(start_date, market="cn"))[:10]
    trading_dates = pd.to_datetime(rqdatac.get_trading_dates(baseline_date, end_date, market="cn"))
    fetched: list[pd.DataFrame] = []
    batches = list(_chunks(codes, batch_size))
    for index, batch in enumerate(batches, start=1):
        prices = rqdatac.get_price(
            batch,
            start_date=baseline_date,
            end_date=end_date,
            frequency="1d",
            fields=["close"],
            adjust_type="post",
            skip_suspended=False,
            expect_df=True,
            market="cn",
        )
        if prices is not None and not prices.empty:
            frame = prices.reset_index().rename(
                columns={
                    "order_book_id": "证券代码",
                    "date": "交易日期",
                    "close": "后复权收盘价",
                }
            )
            frame["交易日期"] = pd.to_datetime(frame["交易日期"]).dt.strftime(
                "%Y-%m-%d"
            )
            fetched.append(frame[["证券代码", "交易日期", "后复权收盘价"]])
        print(
            f"RQData 后复权行情 {index}/{len(batches)}，累计证券 {min(index * batch_size, len(codes))}/{len(codes)}",
            flush=True,
        )

    if not fetched:
        raise RuntimeError("RQData 未返回后复权行情")
    prices = pd.concat(fetched, ignore_index=True)
    duplicate_count = int(prices.duplicated(["证券代码", "交易日期"]).sum())
    if duplicate_count:
        raise ValueError(f"RQData 后复权行情存在 {duplicate_count} 个重复键")
    prices.sort_values(["证券代码", "交易日期"], inplace=True)
    # 按完整交易日历对齐，不能把缺行情前的更早一天误当成T-1。
    prices = align_previous_close(prices, codes, trading_dates)

    output = eligible[list(OUTPUT_COLUMNS[:4])].merge(
        prices,
        on=["证券代码", "交易日期"],
        how="left",
        validate="many_to_one",
    )
    output.rename(
        columns={
            "后复权前收盘价": "前收盘价",
            "后复权收盘价": "收盘价",
        },
        inplace=True,
    )
    output = output[list(OUTPUT_COLUMNS)]

    normal = output["成交状态"].isin({"正常成交", "NORMAL", "TRADED"})
    matched = output["前收盘价"].notna() & output["收盘价"].notna()
    normal_count = int(normal.sum())
    matched_normal_count = int((normal & matched).sum())

    output_dir.mkdir(parents=True, exist_ok=False)
    output_path = output_dir / "股票日行情表_后复权.csv"
    output.to_csv(
        output_path,
        index=False,
        encoding="utf-8-sig",
        float_format="%.10f",
    )
    manifest_path = output_dir / "后复权行情清单.json"
    manifest = {
        "status": "SUCCEEDED",
        "source_path": str(source_path.resolve()),
        "source_sha256": _hash(source_path),
        "output_path": str(output_path.resolve()),
        "output_sha256": _hash(output_path),
        "market_scope": sorted(MARKETS),
        "start_date": start_date,
        "baseline_date": baseline_date,
        "end_date": end_date,
        "security_count": len(codes),
        "source_eligible_row_count": len(eligible),
        "normal_trading_row_count": normal_count,
        "matched_normal_trading_row_count": matched_normal_count,
        "missing_normal_trading_row_count": normal_count - matched_normal_count,
        "rqdata_call": {
            "api": "get_price",
            "frequency": "1d",
            "fields": ["close"],
            "adjust_type": "post",
            "skip_suspended": False,
            "market": "cn",
        },
        "return_formula": "T日后复权收盘价/T-1交易日后复权收盘价-1",
        "classification_timing": "T-1日收盘档位评价T日收益",
        "price_adjustment": "RQData后复权价格收益（权息修复）",
        "note": "RQData prev_close 为原始昨收，不用于本文件；前收盘价由后复权 close 按证券顺序滞后一交易日生成。",
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return output_path, manifest_path


def align_previous_close(prices, codes, trading_dates):
    prices = prices.copy()
    prices["交易日期"] = pd.to_datetime(prices["交易日期"])
    grid = pd.MultiIndex.from_product([codes, trading_dates], names=["证券代码", "交易日期"])
    aligned = prices.set_index(["证券代码", "交易日期"]).reindex(grid)
    aligned["后复权前收盘价"] = aligned.groupby(level="证券代码")["后复权收盘价"].shift(1)
    aligned = aligned.reset_index()
    aligned["交易日期"] = aligned["交易日期"].dt.strftime("%Y-%m-%d")
    return aligned


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, help="直接从米筐在线拉取；源标准行情文件决定证券、日期和成交状态")
    parser.add_argument("--connection", type=Path, help="从市场底座离线导出，和--source二选一")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    if bool(args.source) == bool(args.connection):
        parser.error("必须且只能提供--connection或--source")
    if args.connection:
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "01_risk_events/src"))
        from riskaudit.market_foundation.manual_increment import export_adjusted
        export_adjusted(args.connection,args.output_dir,args.start_date,args.end_date)
        return
    if args.start_date or args.end_date:
        parser.error("--source模式日期由源文件决定")
    output_path, manifest_path = export_post_adjusted_prices(
        args.source,
        args.output_dir,
        batch_size=args.batch_size,
    )
    print(output_path)
    print(manifest_path)


if __name__ == "__main__":
    main()
