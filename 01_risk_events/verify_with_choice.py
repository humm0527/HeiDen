"""
用 Choice 实时数据独立验证「连续跌停 + 新增 ST」风险事件计算口径。

这不是走 market_lake 冻结快照的正式入口，而是直接拉 Choice 的逐日行情/ST 序列，
按 configs/rules/approved/rules_v2.yaml 的口径重算两种风险事件，
产出与 risk_events.csv / continuous_limit_down_segments.csv 同构（中文列名）的结果表，
用于和正式快照运行结果交叉核对。

关键口径（与 src/riskaudit/risk_events/engine.py 保持一致）：
  - 连续跌停：非 ST 且当日收盘跌停（CLOSE == 跌停价，Choice 用 LOWLIMIT「是否跌停」等价），
    连续累计；达到板块门槛（主板 3 天 / 创业板·科创板 2 天）即触发 NON_ST_CONTINUOUS_LIMIT_DOWN。
  - 新增 ST：由「非 ST」转为「ST 或 *ST」的首次戴帽（摘帽后再次戴帽算新事件）。

数据来源（Choice 指标 ID）：
  - 逐日行情：csd  CLOSE(收盘价)、TRADESTATUS(交易状态)、LOWLIMIT(是否跌停)
  - 逐日 ST：csd  ISSTSTOCK(是否ST)、ISXSTSTOCK(是否*ST)
  - 生命周期：css  LISTDATE(首发上市日)、DELISTDATE(摘牌日期)、LISTMKT(上市地点)
  - 交易日历：tradedates(Market=CNSESH / CNSESZ)

注意（口径近似，需人工复核）：
  1. Choice 的 LOWLIMIT 语义是「收盘跌停」，等价于管道 fact_method=CLOSE_EQUALS_LIMIT_DOWN_PRICE；
     若 Choice 该字段实为「盘中触及跌停」，结果会偏多，需按 CLOSE 复核。
  2. 板块（主板/创业板/科创板）按证券代码前缀推断（60→主板、688→科创板、00→主板、300/301→创业板），
     不读取 Choice 板块字段；北交所(BJ)不参与（与规则排除北交所一致）。
  3. 实时拉取的历史 ST 状态是「今日视角」最终值，不保证当时可得；做「新增 ST」判定时
     戴帽日与 Choice 数据可能存在 PIT 口径差异。

用法：
  python verify_with_choice.py \
      --business-file /path/集中度分类_2026Q3.csv \
      --observation-start 2026-07-01 --observation-end 2026-09-30 \
      [--output-dir results] [--limit 20] [--baseline-buffer-days 20]
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

try:
    import pandas as pd
    import yaml
    from EmQuantAPI import c as api
except ImportError as exc:
    raise SystemExit(
        "缺少依赖（pandas / PyYAML / Choice Python SDK）。"
        "请用安装 Choice SDK 的同一个 Python 解释器运行本脚本。"
    ) from exc


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_RULES = SCRIPT_DIR / "configs" / "rules" / "approved" / "rules_v2.yaml"

# Choice 代码后缀 → 管道 canonical market_code
_SUFFIX_MAP = {"SH": "XSHG", "SZ": "XSHE", "BJ": "XBSE"}

# 板块代码前缀推断（A 股 2025-2026 口径）
def infer_board(choice_code: str) -> str:
    code = choice_code.strip().upper()
    if "." not in code:
        return "MAIN"
    digits, suffix = code.split(".", 1)
    if suffix == "SH":
        return "STAR" if digits.startswith("688") else "MAIN"
    if suffix == "SZ":
        return "CHINEXT" if digits.startswith(("300", "301")) else "MAIN"
    if suffix == "BJ":
        return "BSE"
    return "MAIN"


def canonical_key(choice_code: str) -> tuple[str, str]:
    code = choice_code.strip().upper()
    digits, suffix = code.split(".", 1)
    market = _SUFFIX_MAP.get(suffix, suffix)
    return market, f"{digits}.{market}"


_TRUTHY = {"1", "true", "yes", "y", "是", "生效", "st", "*st"}


def _choice_bool(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    return str(value).strip().lower() in _TRUTHY


def log(stage: str, msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] [{stage}] {msg}", flush=True)


def api_error(result: Any) -> str:
    return f"{getattr(result, 'ErrorCode', 'unknown')}: {getattr(result, 'ErrorMsg', '')}"


def login() -> None:
    log("login", "start ...")
    username = os.environ.get("CHOICE_USERNAME", "").strip()
    password = os.environ.get("CHOICE_PASSWORD", "")
    result = api.start(f"UserName={username},Password={password}") if username and password else api.start()
    if result.ErrorCode != 0:
        raise RuntimeError(f"Choice 登录失败（{api_error(result)}）")
    log("login", "OK")


def logout() -> None:
    try:
        api.stop()
        log("logout", "OK")
    except Exception as exc:  # noqa: BLE001
        log("logout", f"warn: {exc}")


# ---------------------------------------------------------------------------
# 规则加载
# ---------------------------------------------------------------------------
def load_rules(rules_path: Path, observation_start: date, observation_end: date) -> dict[str, Any]:
    raw = rules_path.read_bytes()
    payload = yaml.safe_load(raw.decode("utf-8"))
    board = payload["board_policy"]
    continuous = payload["continuous_limit_down"]
    aliases = {str(k): str(v) for k, v in board["aliases"].items()}
    thresholds = {str(k): int(v) for k, v in board["thresholds"].items()}
    return {
        "rule_version": str(payload["rule_version"]),
        "observation_start": observation_start,
        "observation_end": observation_end,
        "board_aliases": aliases,
        "thresholds": thresholds,
        "excluded_boards": frozenset(str(k) for k in board["excluded_boards"]),
        "decimal_scale": int(continuous["decimal_scale"]),
        "minimum_segment_length": int(continuous["minimum_segment_length_for_detail"]),
        "continuous_event_type": str(continuous["event_type"]),
        "new_st_event_type": str(payload["new_st_event"]["event_type"]),
    }


# ---------------------------------------------------------------------------
# 集中度宽表读取（自动跳过文件头部的注释行 / 空行）
# ---------------------------------------------------------------------------
def read_concentration_csv(path: Path) -> pd.DataFrame:
    with path.open(encoding="utf-8-sig") as stream:
        lines = stream.readlines()
    header_idx = next(
        (
            i
            for i, line in enumerate(lines)
            if line.lstrip("\ufeff \t").split(",", 1)[0].strip().strip("\"'") == "biz_date"
        ),
        None,
    )
    if header_idx is None:
        raise ValueError(f"{path.name} 缺少 biz_date 表头")
    return pd.read_csv(
        path, skiprows=header_idx, dtype=object, keep_default_na=False, encoding="utf-8-sig"
    )


# ---------------------------------------------------------------------------
# Choice 取数
# ---------------------------------------------------------------------------
def chunked_css(codes: Iterable[str], indicators: str, *, options: str = "", chunk: int = 500) -> dict[str, dict[str, Any]]:
    """按块调用 css，并在 10003008（无效代码）时二分定位坏代码继续查询。"""
    code_list = list(dict.fromkeys(codes))
    output: dict[str, dict[str, Any]] = {}
    option_text = options.strip(",")
    option_text = f"{option_text},Ispandas=1" if option_text else "RECVtimeout=60,Ispandas=1"

    def query(subset: list[str]) -> None:
        result = api.css(",".join(subset), indicators, option_text)
        if isinstance(result, pd.DataFrame):
            result.columns = [str(col).upper() for col in result.columns]
            for code, row in result.iterrows():
                output[str(code)] = row.to_dict()
            return
        if getattr(result, "ErrorCode", None) == 10003008:
            if len(subset) == 1:
                log("css-invalid", f"无效证券代码：{subset[0]}，已跳过")
                return
            mid = len(subset) // 2
            query(subset[:mid])
            query(subset[mid:])
            return
        raise RuntimeError(f"css 查询失败（{api_error(result)}）")

    for start in range(0, len(code_list), chunk):
        query(code_list[start : start + chunk])
    return output


def chunked_csd(codes: Iterable[str], indicators: str, startdate: str, enddate: str, *, options: str = "", chunk: int = 150) -> dict[str, pd.DataFrame]:
    """按块调用 csd 拉逐日序列，返回 {code: DataFrame(DATES + 指标列)}。"""
    code_list = list(dict.fromkeys(codes))
    output: dict[str, pd.DataFrame] = {}
    option_text = options.strip(",")
    option_text = f"{option_text},Period=1,AdjustFlag=1,Order=1,RowIndex=1,Ispandas=1"

    def query(subset: list[str]) -> None:
        result = api.csd(",".join(subset), indicators, startdate, enddate, option_text)
        if isinstance(result, pd.DataFrame):
            for code, frame in result.groupby(level=0, sort=False):
                output[str(code)] = frame.reset_index(drop=True)
            return
        if getattr(result, "ErrorCode", None) == 10003008:
            if len(subset) == 1:
                log("csd-invalid", f"无效证券代码：{subset[0]}，已跳过")
                return
            mid = len(subset) // 2
            query(subset[:mid])
            query(subset[mid:])
            return
        raise RuntimeError(f"csd 查询失败（{api_error(result)}）")

    for start in range(0, len(code_list), chunk):
        query(code_list[start : start + chunk])
    return output


def trading_dates(market: str, start: date, end: date) -> list[date]:
    mkt = "CNSESH" if market == "XSHG" else "CNSESZ"
    result = api.tradedates(start.isoformat(), end.isoformat(), f"Market={mkt},Period=1,Order=1")
    if result.ErrorCode != 0 or not result.Data:
        raise RuntimeError(f"tradedates 失败（{api_error(result)}）")
    out: list[date] = []
    for item in result.Data:
        try:
            out.append(date.fromisoformat(str(item).replace("/", "-")))
        except ValueError:
            continue
    return sorted(set(out))


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="用 Choice 实时数据验证连续跌停/新增ST 风险事件")
    parser.add_argument("--business-file", required=True, type=Path, help="集中度分类 CSV 路径")
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES, help="rules_v2.yaml 路径")
    parser.add_argument("--observation-start", type=str, help="观察开始 YYYY-MM-DD")
    parser.add_argument("--observation-end", type=str, help="观察结束 YYYY-MM-DD")
    parser.add_argument("--baseline-buffer-days", type=int, default=20, help="观察期前多拉的天数用于 ST 基线")
    parser.add_argument("--output-dir", type=Path, default=SCRIPT_DIR / "results")
    parser.add_argument("--limit", type=int, default=0, help="只处理前 N 只证券（0=全部），用于冒烟")
    args = parser.parse_args()

    rules = load_rules(
        args.rules,
        date.fromisoformat(args.observation_start),
        date.fromisoformat(args.observation_end),
    )
    obs_start = rules["observation_start"]
    obs_end = rules["observation_end"]
    log("rules", f"规则 {rules['rule_version']}，观察区间 {obs_start} ~ {obs_end}")

    # 1) 读证券宇宙
    frame = read_concentration_csv(args.business_file)
    frame["biz_date"] = pd.to_datetime(frame["biz_date"], errors="coerce").dt.date
    in_window = frame.loc[
        (frame["biz_date"] >= obs_start) & (frame["biz_date"] <= obs_end)
    ]
    choice_codes = sorted({c.strip().upper() for c in in_window["stk_code"] if c.strip()})
    if not choice_codes:
        raise SystemExit("观察区间内没有证券代码（请检查 business_file 与观察窗口是否匹配）")
    if args.limit:
        choice_codes = choice_codes[: args.limit]
    log("universe", f"证券宇宙 {len(choice_codes)} 只")

    # 2) 生命周期（css）
    css_info = chunked_css(choice_codes, "LISTDATE,DELISTDATE,LISTMKT", chunk=300)
    log("css", f"生命周期取回 {len(css_info)} 只")

    # 3) 逐日序列（csd），多拉 buffer 天用于 ST 基线
    series_start = obs_start - timedelta(days=args.baseline_buffer_days)
    daily = chunked_csd(
        choice_codes,
        "CLOSE,TRADESTATUS,LOWLIMIT,ISSTSTOCK,ISXSTSTOCK",
        series_start.strftime("%Y%m%d"),
        obs_end.strftime("%Y%m%d"),
        chunk=150,
    )
    log("csd", f"逐日序列取回 {len(daily)} 只")

    # 4) 交易日历
    calendar: dict[str, list[date]] = {}
    for market in ("XSHG", "XSHE"):
        calendar[market] = trading_dates(market, series_start, obs_end)
    log("calendar", f"交易日历 XSHG={len(calendar['XSHG'])}, XSHE={len(calendar['XSHE'])}")

    # 5) 状态机计算
    events: list[dict[str, Any]] = []
    segments: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    batch_id = f"choice_verify_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    for code in choice_codes:
        market, canonical_code = canonical_key(code)
        info = css_info.get(code, {})
        board = infer_board(code)
        if board in rules["excluded_boards"]:
            skipped.append({"security_code": code, "reason": "BOARD_EXCLUDED"})
            continue
        threshold = rules["thresholds"].get(board)
        if threshold is None:
            skipped.append({"security_code": code, "reason": f"NO_THRESHOLD:{board}"})
            continue

        series = daily.get(code)
        if series is None or series.empty:
            skipped.append({"security_code": code, "reason": "NO_DAILY_SERIES"})
            continue
        series = series.copy()
        series["_date"] = pd.to_datetime(series["DATES"], errors="coerce").dt.date
        series = series.dropna(subset=["_date"]).sort_values("_date")

        # 生命周期裁剪
        listed = info.get("LISTDATE")
        listed_date = None
        if listed:
            try:
                listed_date = date.fromisoformat(str(listed)[:10].replace("/", "-"))
            except ValueError:
                listed_date = None
        delisted = info.get("DELISTDATE")
        delisted_date = None
        if delisted:
            try:
                delisted_date = date.fromisoformat(str(delisted)[:10].replace("/", "-"))
            except ValueError:
                delisted_date = None

        trading = calendar.get(market, [])
        trading = [
            d for d in trading
            if obs_start <= d <= obs_end
            and (listed_date is None or d >= listed_date)
            and (delisted_date is None or d < delisted_date)
        ]
        if not trading:
            skipped.append({"security_code": code, "reason": "NO_LIFECYCLE_DAYS"})
            continue

        # ST 基线
        def st_on_day(row: Any) -> bool:
            return _choice_bool(row.get("ISSTSTOCK")) or _choice_bool(row.get("ISXSTSTOCK"))

        if listed_date is not None and listed_date >= obs_start:
            baseline_rows = series.loc[series["_date"] >= listed_date]
            baseline_st = st_on_day(baseline_rows.iloc[0]) if not baseline_rows.empty else False
        else:
            before = series.loc[series["_date"] < obs_start]
            baseline_st = st_on_day(before.iloc[-1]) if not before.empty else False

        previous_st = baseline_st
        open_segment_dates: list[date] = []
        segment_number = 0

        def close_segment(is_open_at_end: bool) -> None:
            nonlocal open_segment_dates, segment_number
            if not open_segment_dates:
                return
            if len(open_segment_dates) >= rules["minimum_segment_length"]:
                segment_number += 1
                reached = len(open_segment_dates) >= threshold
                threshold_date = open_segment_dates[threshold - 1] if reached else None
                segments.append({
                    "market_code": market,
                    "security_code": canonical_code,
                    "segment_number": segment_number,
                    "event_type": rules["continuous_event_type"],
                    "event_source": "CHOICE_MARKET_DAILY_DERIVED",
                    "board_code": board,
                    "applicable_threshold": threshold,
                    "consecutive_length": len(open_segment_dates),
                    "first_fact_date": open_segment_dates[0].isoformat(),
                    "threshold_reached_date": threshold_date.isoformat() if threshold_date else None,
                    "last_fact_date": open_segment_dates[-1].isoformat(),
                    "threshold_reached": reached,
                    "open_at_observation_end": is_open_at_end,
                    "rule_version": rules["rule_version"],
                    "calculation_batch_id": batch_id,
                    "upstream_snapshot_id": "CHOICE_REALTIME_VERIFY",
                    "source_record_ids": [f"choice:{code}:{d.isoformat()}" for d in open_segment_dates],
                })
                if reached and threshold_date is not None:
                    events.append({
                        "market_code": market,
                        "security_code": canonical_code,
                        "event_type": rules["continuous_event_type"],
                        "event_source": "CHOICE_MARKET_DAILY_DERIVED",
                        "first_fact_date": open_segment_dates[0].isoformat(),
                        "risk_date": threshold_date.isoformat(),
                        "last_fact_date": open_segment_dates[-1].isoformat(),
                        "threshold_reached_date": threshold_date.isoformat(),
                        "segment_number": segment_number,
                        "consecutive_length": len(open_segment_dates),
                        "previous_st_state": None,
                        "current_st_state": None,
                        "rule_version": rules["rule_version"],
                        "calculation_batch_id": batch_id,
                        "upstream_snapshot_id": "CHOICE_REALTIME_VERIFY",
                        "source_record_ids": [f"choice:{code}:{d.isoformat()}" for d in open_segment_dates],
                    })
            open_segment_dates = []

        for day in trading:
            row = series.loc[series["_date"] == day]
            if row.empty:
                # 当日无行情（长期停牌），视为非跌停，打断连续段
                current_st = previous_st
                close_segment(False)
                previous_st = current_st
                continue
            current_st = st_on_day(row.iloc[0])
            if not previous_st and current_st:
                close_segment(False)
                events.append({
                    "market_code": market,
                    "security_code": canonical_code,
                    "event_type": rules["new_st_event_type"],
                    "event_source": "CHOICE_ST_STATUS_TRANSITION",
                    "first_fact_date": day.isoformat(),
                    "risk_date": day.isoformat(),
                    "last_fact_date": day.isoformat(),
                    "threshold_reached_date": None,
                    "segment_number": None,
                    "consecutive_length": None,
                    "previous_st_state": "NON_ST",
                    "current_st_state": "ST",
                    "rule_version": rules["rule_version"],
                    "calculation_batch_id": batch_id,
                    "upstream_snapshot_id": "CHOICE_REALTIME_VERIFY",
                    "source_record_ids": [f"choice:{code}:{day.isoformat()}"],
                })

            is_limit_down = (not current_st) and _choice_bool(row.iloc[0].get("LOWLIMIT"))
            if is_limit_down:
                open_segment_dates.append(day)
            else:
                close_segment(False)

            previous_st = current_st

        close_segment(is_open_at_end=bool(open_segment_dates))

    # 6) 写结果（与正式管道同构的中文列名）
    out_dir = args.output_dir / f"choice_verify_{obs_start}_{obs_end}"
    out_dir.mkdir(parents=True, exist_ok=True)

    EVENT_CN = {
        "market_code": "交易市场代码", "security_code": "证券代码", "event_type": "事件类型",
        "event_source": "事件来源", "first_fact_date": "首次事实日期", "risk_date": "风险认定日期",
        "last_fact_date": "最后事实日期", "threshold_reached_date": "达到门槛日期",
        "segment_number": "连续段序号", "consecutive_length": "连续跌停交易日数",
        "previous_st_state": "前一ST状态", "current_st_state": "当前ST状态",
        "rule_version": "风险规则版本号", "calculation_batch_id": "计算批次标识",
        "upstream_snapshot_id": "上游数据快照标识", "source_record_ids": "来源记录标识列表",
    }
    SEGMENT_CN = {
        "market_code": "交易市场代码", "security_code": "证券代码", "segment_number": "连续段序号",
        "event_type": "事件类型", "event_source": "事件来源", "board_code": "板块代码",
        "applicable_threshold": "适用连续跌停门槛", "consecutive_length": "连续跌停交易日数",
        "first_fact_date": "首次事实日期", "threshold_reached_date": "达到门槛日期",
        "last_fact_date": "最后事实日期", "threshold_reached": "是否达到门槛",
        "open_at_observation_end": "观察期结束时是否未闭合", "rule_version": "风险规则版本号",
        "calculation_batch_id": "计算批次标识", "upstream_snapshot_id": "上游数据快照标识",
        "source_record_ids": "来源记录标识列表",
    }

    event_df = pd.DataFrame(events, columns=list(EVENT_CN))
    segment_df = pd.DataFrame(segments, columns=list(SEGMENT_CN))
    event_df.rename(columns=EVENT_CN).to_csv(out_dir / "risk_events.csv", index=False, encoding="utf-8-sig")
    segment_df.rename(columns=SEGMENT_CN).to_csv(out_dir / "continuous_limit_down_segments.csv", index=False, encoding="utf-8-sig")

    manifest = {
        "source": "CHOICE_REALTIME_VERIFY",
        "rule_version": rules["rule_version"],
        "observation_start": obs_start.isoformat(),
        "observation_end": obs_end.isoformat(),
        "business_file": str(args.business_file),
        "universe_count": len(choice_codes),
        "skipped_count": len(skipped),
        "event_count": len(events),
        "segment_count": len(segments),
        "new_st_count": sum(1 for e in events if e["event_type"] == rules["new_st_event_type"]),
        "continuous_limit_down_count": sum(1 for e in events if e["event_type"] == rules["continuous_event_type"]),
    }
    (out_dir / "verification_manifest.json").write_text(
        __import__("json").dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    if skipped:
        pd.DataFrame(skipped).to_csv(out_dir / "skipped_securities.csv", index=False, encoding="utf-8-sig")

    log("done", f"事件 {len(events)} 条（新增ST {manifest['new_st_count']}，连续跌停 {manifest['continuous_limit_down_count']}），"
                f"连续段 {len(segments)} 条，跳过 {len(skipped)} 只")
    log("done", f"结果目录：{out_dir}")
    return 0


if __name__ == "__main__":
    try:
        login()
        raise SystemExit(main())
    finally:
        logout()
