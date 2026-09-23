"""
用 Choice 数据构建五表，并调用现有引擎计算风险事件（引擎不重写）。

数据流：
    Choice(css/csd/tradedates)
      → 五表(交易日历表 / 证券基础状态日表 / 股票日行情表 / 证券风险状态表 / 券商风险分类)
      → validate_risk_inputs(门禁)
      → calculate_risk_events(引擎)
      → write_risk_event_result(结果)

与 verify_with_choice.py 的区别：本脚本不内联重写状态机，而是把 Choice 数据组装成
引擎要的五张标准表，直接调用 src/riskaudit/risk_events 里唯一的权威实现。

口径近似（需人工复核，同 verify_with_choice.py）：
  1. 跌停判断：Choice 无「跌停价」数值字段，用 LOWLIMIT(是否跌停) 合成 limit_down_price
     —— LOWLIMIT=1 时令 limit_down_price == close_price（等价于引擎的 CLOSE==跌停价）；
     否则置 NaN（引擎的 pd.notna 检查不通过，即非跌停）。语义与引擎一致。
  2. 成交状态：有收盘价 → 「正常成交」，否则「停牌」。
  3. 板块：按证券代码前缀推断（60→主板、688→科创板、00→主板、300/301→创业板）。
  4. ST 逐日状态：缺值按「未生效」(非ST) 补齐，保证门禁的逐日覆盖检查通过。
  5. 主数据：每证券一行（listed_date/delisted_date 来自 Choice LISTDATE/DELISTDATE）。

运行环境：需要能同时 import EmQuantAPI(Choice SDK) 和 riskaudit(本包 src)。
用法：
  python build_five_tables_from_choice.py \
      --business-file /path/全行业 \
      --observation-start 2026-07-01 --observation-end 2026-08-31 \
      [--rules configs/rules/approved/rules_v2.yaml] [--output-dir results] [--limit 0] \
      [--checkpoint-dir results/.choice_checkpoints] [--choice-retries 2] [--refresh-choice] \
      [--stage all|data|market|lifecycle]

断点说明：CSS/CSD 每个成功批次都会写入参数指纹检查点；相同证券、指标、日期和
options 的重跑自动复用。只有显式传入 --refresh-choice 时才会忽略已有检查点重新取数。
用 --stage data 可一次保存 CSS 生命周期、CSD 行情/ST/跌停和交易日历而不运行计算；
也可分别用 --stage market 与 --stage lifecycle 下载。最后用默认 --stage all 复用检查点
并正式计算。
"""

from __future__ import annotations

import argparse
from hashlib import sha256
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Iterator

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
BUNDLE_SRC = SCRIPT_DIR / "src"
if str(BUNDLE_SRC) not in sys.path:
    sys.path.insert(0, str(BUNDLE_SRC))

# 引擎相关（不依赖 Choice SDK）
from riskaudit.risk_events import (  # noqa: E402
    calculate_risk_events,
    load_approved_rules,
    validate_risk_inputs,
    write_risk_event_result,
)
from riskaudit.risk_events.models import RiskInputTables  # noqa: E402

# Choice SDK 延迟导入（这样引擎侧逻辑可在无 SDK 环境单测）
api = None  # type: ignore[assignment]


def _choice_api():
    global api
    if api is None:
        try:
            from EmQuantAPI import c as api_mod
        except ImportError as exc:
            raise SystemExit(
                "缺少 Choice Python SDK。请用安装 Choice SDK 的同一个 Python 解释器运行本脚本。"
            ) from exc
        api = api_mod
    return api


DEFAULT_RULES = SCRIPT_DIR / "configs" / "rules" / "approved" / "rules_v2.yaml"
_SUFFIX_MAP = {"SH": "XSHG", "SZ": "XSHE", "BJ": "XBSE"}
_NORMAL_TRADED = "正常成交"
_SUSPENDED = "停牌"
_RETRYABLE_CHOICE_ERRORS = {
    10000015,  # 服务超时
    10000016,  # 请求频次过高
    10002001,  # 网络错误
    10002002,  # 网络连接失败
    10002003,  # 网络连接超时
    10002004,  # 接收时连接断开
    10002005,  # 网络发送失败
    10002006,  # 网络发送超时
    10002007,  # 网络接收错误
    10002008,  # 网络接收超时
    10002010,  # HTTP 访问失败
    10002011,  # 等待网络响应超时
    10002013,  # 资讯服务器重连
    10002014,  # 资讯服务器连续重连失败
}


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


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip().split(" ")[0].split("T")[0]
    if not text or text in {"0", "None", "NaT", "nan"}:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    parts = re.split(r"[-/]", text)
    if len(parts) == 3 and all(p.isdigit() for p in parts):
        return date(int(parts[0]), int(parts[1]), int(parts[2]))
    return None


def log(stage: str, msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] [{stage}] {msg}", flush=True)


def _query_with_retry(call, *, label: str, retries: int):
    """仅重试 Choice 的瞬时网络/限频错误；参数、权限和额度错误立即失败。"""
    for attempt in range(retries + 1):
        try:
            result = call()
        except Exception as exc:  # noqa: BLE001
            if attempt >= retries:
                raise
            delay = min(2 ** attempt, 8)
            log("retry", f"{label} 异常：{exc}；{delay}s 后重试 {attempt + 1}/{retries}")
            time.sleep(delay)
            continue
        error_code = getattr(result, "ErrorCode", 0)
        if isinstance(result, pd.DataFrame) or error_code not in _RETRYABLE_CHOICE_ERRORS:
            return result
        if attempt >= retries:
            return result
        delay = min(2 ** attempt, 8)
        log(
            "retry",
            f"{label} 暂时失败（{error_code}: {getattr(result, 'ErrorMsg', '')}）；"
            f"{delay}s 后重试 {attempt + 1}/{retries}",
        )
        time.sleep(delay)
    raise AssertionError("unreachable")


def _checkpoint_file(
    checkpoint_dir: Path | None,
    *,
    kind: str,
    batch_no: int,
    payload: dict[str, Any],
) -> Path | None:
    if checkpoint_dir is None:
        return None
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    fingerprint = sha256(raw).hexdigest()[:16]
    return checkpoint_dir / f"{kind}_{batch_no:04d}_{fingerprint}.pkl"


def _load_checkpoint(path: Path | None, *, refresh: bool) -> Any | None:
    if path is None or refresh or not path.is_file():
        return None
    try:
        value = pd.read_pickle(path)
    except Exception as exc:  # noqa: BLE001
        log("checkpoint", f"忽略无法读取的检查点 {path.name}：{exc}")
        return None
    log("resume", f"复用 {path.name}")
    return value


def _write_checkpoint(path: Path | None, value: Any) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        pd.to_pickle(value, temporary)
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


# ---------------------------------------------------------------------------
# Choice 取数
# ---------------------------------------------------------------------------
def login() -> None:
    c = _choice_api()
    username = os.environ.get("CHOICE_USERNAME", "").strip()
    password = os.environ.get("CHOICE_PASSWORD", "")
    result = c.start(f"UserName={username},Password={password}") if username and password else c.start()
    if result.ErrorCode != 0:
        raise RuntimeError(f"Choice 登录失败（{getattr(result, 'ErrorCode', '?')}: {getattr(result, 'ErrorMsg', '')}）")
    log("login", "OK")


def logout() -> None:
    if api is None:
        return
    try:
        api.stop()
    except Exception:  # noqa: BLE001
        pass


def chunked_css(
    codes: Iterable[str],
    indicators: str,
    *,
    options: str = "",
    chunk: int = 300,
    checkpoint_dir: Path | None = None,
    retries: int = 2,
    refresh: bool = False,
) -> dict[str, dict[str, Any]]:
    c = _choice_api()
    code_list = list(dict.fromkeys(codes))
    output: dict[str, dict[str, Any]] = {}
    option_text = options.strip(",")
    option_text = f"{option_text},Ispandas=1" if option_text else "RECVtimeout=60,Ispandas=1"

    def query(subset: list[str]) -> dict[str, dict[str, Any]]:
        subset_output: dict[str, dict[str, Any]] = {}
        result = _query_with_retry(
            lambda: c.css(",".join(subset), indicators, option_text),
            label=f"css {subset[0]}..{subset[-1]}",
            retries=retries,
        )
        if isinstance(result, pd.DataFrame):
            result.columns = [str(col).upper() for col in result.columns]
            for code, row in result.iterrows():
                subset_output[str(code)] = row.to_dict()
            return subset_output
        if getattr(result, "ErrorCode", None) == 10003008:
            if len(subset) == 1:
                log("css-invalid", f"无效证券代码：{subset[0]}，已跳过")
                return subset_output
            mid = len(subset) // 2
            subset_output.update(query(subset[:mid]))
            subset_output.update(query(subset[mid:]))
            return subset_output
        raise RuntimeError(f"css 查询失败（{getattr(result, 'ErrorCode', '?')}: {getattr(result, 'ErrorMsg', '')}）")

    for batch_no, start in enumerate(range(0, len(code_list), chunk)):
        subset = code_list[start : start + chunk]
        path = _checkpoint_file(
            checkpoint_dir,
            kind="css",
            batch_no=batch_no,
            payload={"codes": subset, "indicators": indicators, "options": option_text},
        )
        batch_output = _load_checkpoint(path, refresh=refresh)
        if not isinstance(batch_output, dict):
            batch_output = query(subset)
            _write_checkpoint(path, batch_output)
        output.update(batch_output)
    return output


def iter_chunked_csd(
    codes: Iterable[str],
    indicators: str,
    startdate: str,
    enddate: str,
    *,
    chunk: int = 150,
    checkpoint_dir: Path | None = None,
    retries: int = 2,
    refresh: bool = False,
) -> Iterator[tuple[int, list[str], dict[str, pd.DataFrame]]]:
    """逐批返回 CSD 数据；每批原子落盘，重跑时自动从检查点续传。"""
    c = _choice_api()
    code_list = list(dict.fromkeys(codes))
    option_text = f"Period=1,AdjustFlag=1,Order=1,RowIndex=1,Ispandas=1"

    def query(subset: list[str]) -> dict[str, pd.DataFrame]:
        subset_output: dict[str, pd.DataFrame] = {}
        result = _query_with_retry(
            lambda: c.csd(",".join(subset), indicators, startdate, enddate, option_text),
            label=f"csd {subset[0]}..{subset[-1]}",
            retries=retries,
        )
        if isinstance(result, pd.DataFrame):
            for code, frame in result.groupby(level=0, sort=False):
                frame = frame.reset_index(drop=True)
                frame.columns = [str(col).upper() for col in frame.columns]
                subset_output[str(code)] = frame
            return subset_output
        if getattr(result, "ErrorCode", None) == 10003008:
            if len(subset) == 1:
                log("csd-invalid", f"无效证券代码：{subset[0]}，已跳过")
                return subset_output
            mid = len(subset) // 2
            subset_output.update(query(subset[:mid]))
            subset_output.update(query(subset[mid:]))
            return subset_output
        raise RuntimeError(f"csd 查询失败（{getattr(result, 'ErrorCode', '?')}: {getattr(result, 'ErrorMsg', '')}）")

    for batch_no, start in enumerate(range(0, len(code_list), chunk)):
        subset = code_list[start : start + chunk]
        path = _checkpoint_file(
            checkpoint_dir,
            kind="csd",
            batch_no=batch_no,
            payload={
                "codes": subset,
                "indicators": indicators,
                "startdate": startdate,
                "enddate": enddate,
                "options": option_text,
            },
        )
        batch_output = _load_checkpoint(path, refresh=refresh)
        if not isinstance(batch_output, dict):
            batch_output = query(subset)
            _write_checkpoint(path, batch_output)
        yield batch_no, subset, batch_output


def chunked_csd(
    codes: Iterable[str],
    indicators: str,
    startdate: str,
    enddate: str,
    *,
    chunk: int = 150,
    checkpoint_dir: Path | None = None,
    retries: int = 2,
    refresh: bool = False,
) -> dict[str, pd.DataFrame]:
    """兼容入口；需要控制内存时优先使用 iter_chunked_csd。"""
    output: dict[str, pd.DataFrame] = {}
    for _, _, batch_output in iter_chunked_csd(
        codes,
        indicators,
        startdate,
        enddate,
        chunk=chunk,
        checkpoint_dir=checkpoint_dir,
        retries=retries,
        refresh=refresh,
    ):
        output.update(batch_output)
    return output


def trading_dates(market: str, start: date, end: date) -> list[date]:
    c = _choice_api()
    mkt = "CNSESH" if market == "XSHG" else "CNSESZ"
    result = c.tradedates(start.isoformat(), end.isoformat(), f"Market={mkt},Period=1,Order=1")
    if result.ErrorCode != 0:
        raise RuntimeError(f"tradedates 失败（{getattr(result, 'ErrorCode', '?')}: {getattr(result, 'ErrorMsg', '')}）")
    raw_dates = getattr(result, "Dates", None) or getattr(result, "Data", []) or []
    out: list[date] = []
    for item in raw_dates:
        parsed = _parse_date(item)
        if parsed is not None:
            out.append(parsed)
    return sorted(set(out))


# ---------------------------------------------------------------------------
# 五表组装（纯逻辑，不依赖 Choice）
# ---------------------------------------------------------------------------
def build_universe_table(universe_keys: Iterable[tuple[str, str]], classification_date: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"market_code": mkt, "security_code": sec, "classification_date": classification_date}
            for mkt, sec in sorted(set(universe_keys))
        ],
        columns=["market_code", "security_code", "classification_date"],
    )


def build_calendar_table(calendars: dict[str, list[date]]) -> pd.DataFrame:
    rows = [
        {"market_code": mkt, "calendar_date": day.isoformat(), "is_trading_day": True}
        for mkt, days in calendars.items()
        for day in days
    ]
    return pd.DataFrame(rows, columns=["market_code", "calendar_date", "is_trading_day"])


def build_master_table(
    universe_keys: Iterable[tuple[str, str]],
    css_info: dict[str, dict[str, Any]],
    observation_start: date,
) -> pd.DataFrame:
    rows = []
    for mkt, sec in sorted(set(universe_keys)):
        choice_code = f"{sec.split('.')[0]}.{_suffix_of(mkt)}"
        info = css_info.get(choice_code, {})
        listed = _parse_date(info.get("LISTDATE"))
        delisted = _parse_date(info.get("DELISTDATE"))
        status_date = listed if listed is not None else (observation_start - timedelta(days=3650))
        rows.append(
            {
                "market_code": mkt,
                "security_code": sec,
                "status_date": status_date.isoformat(),
                "board_code": infer_board(choice_code),
                "listed_date": listed.isoformat() if listed else "",
                "delisted_date": delisted.isoformat() if delisted else "",
            }
        )
    return pd.DataFrame(
        rows,
        columns=["market_code", "security_code", "status_date", "board_code", "listed_date", "delisted_date"],
    )


def build_market_and_st_tables(
    universe_keys: Iterable[tuple[str, str]],
    daily: dict[str, pd.DataFrame],
    calendars: dict[str, list[date]],
    observation_start: date,
    observation_end: date,
    st_start: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """行情表覆盖 [observation_start, observation_end]，ST 表覆盖 [st_start, observation_end]
    （ST 表多覆盖一段，是为了给门禁算「观察期前 ST 基线」留数据）。"""
    market_rows: list[dict[str, Any]] = []
    st_rows: list[dict[str, Any]] = []
    for mkt, sec in sorted(set(universe_keys)):
        choice_code = f"{sec.split('.')[0]}.{_suffix_of(mkt)}"
        series = daily.get(choice_code)
        day_map: dict[date, Any] = {}
        if series is not None and not series.empty:
            s = series.copy()
            s = s.assign(dt=pd.to_datetime(s["DATES"], errors="coerce").dt.date)
            s = s.dropna(subset=["dt"]).drop_duplicates(subset=["dt"], keep="last")
            day_map = {row.dt: row for row in s.itertuples(index=False)}

        calendar = calendars.get(mkt, [])
        st_days = [d for d in calendar if st_start <= d <= observation_end]
        scan_days = [d for d in calendar if observation_start <= d <= observation_end]

        for day in st_days:
            row = day_map.get(day)
            is_st = (
                (_choice_bool(getattr(row, "ISSTSTOCK", None)) or _choice_bool(getattr(row, "ISXSTSTOCK", None)))
                if row is not None
                else False
            )
            st_rows.append(
                {
                    "market_code": mkt,
                    "security_code": sec,
                    "risk_status_type": "ST",
                    "risk_status_value": "生效" if is_st else "未生效",
                    "status_start_date": day.isoformat(),
                    "source_record_id": f"choice:{choice_code}:{day.isoformat()}",
                }
            )

        for day in scan_days:
            row = day_map.get(day)
            close_num = pd.to_numeric(
                pd.Series([getattr(row, "CLOSE", None) if row is not None else None]),
                errors="coerce",
            ).iloc[0]
            has_close = pd.notna(close_num)
            lowlimit = _choice_bool(getattr(row, "LOWLIMIT", None)) if row is not None else False
            market_rows.append(
                {
                    "market_code": mkt,
                    "security_code": sec,
                    "trading_date": day.isoformat(),
                    "trading_status": _NORMAL_TRADED if has_close else _SUSPENDED,
                    "close_price": close_num if has_close else None,
                    # 合成跌停价：跌停日=收盘价（等价 CLOSE==跌停价），否则 None → 引擎判非跌停
                    "limit_down_price": close_num if (has_close and lowlimit) else None,
                    "source_record_id": f"choice:{choice_code}:{day.isoformat()}",
                }
            )
    market_df = pd.DataFrame(
        market_rows,
        columns=[
            "market_code", "security_code", "trading_date", "trading_status",
            "close_price", "limit_down_price", "source_record_id",
        ],
    )
    st_df = pd.DataFrame(
        st_rows,
        columns=[
            "market_code", "security_code", "risk_status_type",
            "risk_status_value", "status_start_date", "source_record_id",
        ],
    )
    return market_df, st_df


def _suffix_of(market_code: str) -> str:
    return {"XSHG": "SH", "XSHE": "SZ", "XBSE": "BJ"}.get(market_code, market_code)


def assemble_tables(
    universe_keys: Iterable[tuple[str, str]],
    css_info: dict[str, dict[str, Any]],
    daily: dict[str, pd.DataFrame],
    calendars: dict[str, list[date]],
    observation_start: date,
    observation_end: date,
    st_start: date,
    classification_date: str,
) -> RiskInputTables:
    """把 Choice 取回的数据组装成引擎要的五张表。"""
    market_df, st_df = build_market_and_st_tables(
        universe_keys, daily, calendars, observation_start, observation_end, st_start
    )
    return RiskInputTables(
        trading_calendar=build_calendar_table(calendars),
        security_status_daily=build_master_table(universe_keys, css_info, observation_start),
        stock_market_daily=market_df,
        security_risk_status=st_df,
        broker_risk_classification=build_universe_table(universe_keys, classification_date),
    )


def assemble_tables_from_frames(
    universe_keys: Iterable[tuple[str, str]],
    css_info: dict[str, dict[str, Any]],
    market_parts: list[pd.DataFrame],
    st_parts: list[pd.DataFrame],
    calendars: dict[str, list[date]],
    observation_start: date,
    classification_date: str,
) -> RiskInputTables:
    """合并逐批转换后的表，避免同时保留全量 Choice 原始序列和逐行字典。"""
    market_df = pd.concat(market_parts, ignore_index=True) if market_parts else pd.DataFrame(
        columns=[
            "market_code", "security_code", "trading_date", "trading_status",
            "close_price", "limit_down_price", "source_record_id",
        ]
    )
    st_df = pd.concat(st_parts, ignore_index=True) if st_parts else pd.DataFrame(
        columns=[
            "market_code", "security_code", "risk_status_type",
            "risk_status_value", "status_start_date", "source_record_id",
        ]
    )
    return RiskInputTables(
        trading_calendar=build_calendar_table(calendars),
        security_status_daily=build_master_table(universe_keys, css_info, observation_start),
        stock_market_daily=market_df,
        security_risk_status=st_df,
        broker_risk_classification=build_universe_table(universe_keys, classification_date),
    )


def run_engine(
    tables: RiskInputTables,
    rules_path: Path,
    observation_start: date,
    observation_end: date,
    batch_id: str,
    snapshot_id: str,
):
    """调用现有门禁 + 引擎，返回 (result, gate)。"""
    rules = load_approved_rules(
        rules_path, observation_start=observation_start, observation_end=observation_end
    )
    gate = validate_risk_inputs(tables, rules, prevalidation_allow_run=True)
    log("gate", f"allow_run={gate.allow_run}, findings={len(gate.findings)}, "
                f"eligible={len(gate.eligible_universe)}, excluded={len(gate.excluded_securities)}")
    if not gate.allow_run:
        for finding in gate.findings:
            log("gate-finding", f"{finding.severity} {finding.code} {finding.message} {finding.security_code} {finding.fact_date}")
    result = calculate_risk_events(
        tables,
        gate,
        rules,
        calculation_batch_id=batch_id,
        upstream_snapshot_id=snapshot_id,
    )
    log("engine", f"事件 {len(result.events)} 条，连续段 {len(result.segments)} 条")
    return result, gate


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
_QUARTER_FILE = re.compile(r"^集中度(\d{4})Q([1-4])\.csv$")


def _quarter_ordinal(value: date) -> int:
    return value.year * 4 + (value.month - 1) // 3


def _quarter_name(ordinal: int) -> str:
    year, zero_based_quarter = divmod(ordinal, 4)
    return f"集中度{year}Q{zero_based_quarter + 1}.csv"


def select_concentration_files(path: Path, observation_start: date, observation_end: date) -> tuple[Path, ...]:
    """单文件原样使用；目录模式严格选择覆盖观察期的连续季度文件。"""
    if observation_start > observation_end:
        raise ValueError("observation_start must be on or before observation_end")
    if path.is_file():
        return (path,)
    if not path.is_dir():
        raise FileNotFoundError(f"集中度输入不存在：{path}")
    expected_names = tuple(
        _quarter_name(ordinal)
        for ordinal in range(_quarter_ordinal(observation_start), _quarter_ordinal(observation_end) + 1)
    )
    selected = tuple(path / name for name in expected_names)
    missing = [item.name for item in selected if not item.is_file()]
    if missing:
        raise FileNotFoundError(f"观察期季度文件不完整，缺少：{', '.join(missing)}")
    return selected


def _read_concentration_frame(path: Path) -> pd.DataFrame:
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
    frame = pd.read_csv(
        path,
        skiprows=header_idx,
        usecols=["biz_date", "stk_code"],
        dtype=object,
        keep_default_na=False,
        encoding="utf-8-sig",
    )
    frame["biz_date"] = pd.to_datetime(frame["biz_date"], errors="coerce").dt.date
    return frame


def read_concentration_universe(
    path: Path,
    observation_end: date,
    *,
    observation_start: date | None = None,
) -> tuple[set[tuple[str, str]], str]:
    start = observation_start or date.min
    files = select_concentration_files(path, start, observation_end)
    codes: set[str] = set()
    for source in files:
        frame = _read_concentration_frame(source)
        frame = frame.loc[
            frame["biz_date"].notna()
            & (frame["biz_date"] >= start)
            & (frame["biz_date"] <= observation_end)
        ]
        codes.update(
            code
            for raw in frame["stk_code"]
            if (code := str(raw).strip().upper()).endswith((".SH", ".SZ"))
        )
    universe = {canonical_key(c) for c in codes}
    if not universe:
        raise ValueError(f"观察期内未发现沪深证券代码：{path}")
    log("universe-files", f"已选择 {len(files)} 个季度文件：{', '.join(item.name for item in files)}")
    return universe, observation_end.isoformat()


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _write_csv_atomic(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        frame.to_csv(temporary, index=False, encoding="utf-8-sig")
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description="Choice 数据 → 五表 → 现有引擎计算风险事件")
    parser.add_argument(
        "--business-file",
        "--business-path",
        dest="business_path",
        required=True,
        type=Path,
        help="单个集中度 CSV，或包含集中度YYYYQn.csv的全行业目录",
    )
    parser.add_argument("--observation-start", required=True, type=str)
    parser.add_argument("--observation-end", required=True, type=str)
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    parser.add_argument("--output-dir", type=Path, default=SCRIPT_DIR / "results")
    parser.add_argument("--baseline-buffer-days", type=int, default=20)
    parser.add_argument("--limit", type=int, default=0, help="只处理前 N 只证券（0=全部）")
    parser.add_argument(
        "--checkpoint-dir",
        type=Path,
        help="Choice 分批检查点目录（默认 <output-dir>/.choice_checkpoints）",
    )
    parser.add_argument("--choice-retries", type=int, default=2, help="瞬时网络/限频错误重试次数")
    parser.add_argument("--refresh-choice", action="store_true", help="忽略已有检查点，重新拉取 Choice")
    parser.add_argument(
        "--stage",
        choices=("all", "data", "market", "lifecycle"),
        default="all",
        help=(
            "all=完整取数并计算；data=仅拉取全部 Choice 数据；"
            "market=仅行情/ST/日历；lifecycle=仅生命周期"
        ),
    )
    args = parser.parse_args()

    obs_start = date.fromisoformat(args.observation_start)
    obs_end = date.fromisoformat(args.observation_end)
    batch_id = f"choice_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    universe, classification_date = read_concentration_universe(
        args.business_path,
        obs_end,
        observation_start=obs_start,
    )
    if args.limit:
        universe = set(sorted(universe)[: args.limit])
    log("universe", f"证券宇宙 {len(universe)} 只")
    checkpoint_dir = args.checkpoint_dir or (args.output_dir / ".choice_checkpoints")
    choice_codes = [f"{sec.split('.')[0]}.{_suffix_of(mkt)}" for mkt, sec in sorted(universe)]
    series_start = obs_start - timedelta(days=args.baseline_buffer_days)
    css_info: dict[str, dict[str, Any]] = {}
    calendars: dict[str, list[date]] = {}
    market_parts: list[pd.DataFrame] = []
    st_parts: list[pd.DataFrame] = []
    market_manifest_path = checkpoint_dir / "market_manifest.json"
    market_manifest: dict[str, Any] | None = None
    _write_csv_atomic(
        build_universe_table(universe, classification_date),
        checkpoint_dir / "security_universe.csv",
    )

    login()
    try:
        if args.stage in {"all", "data", "lifecycle"}:
            css_info = chunked_css(
                choice_codes,
                "LISTDATE,DELISTDATE",
                chunk=300,
                checkpoint_dir=checkpoint_dir / "css",
                retries=args.choice_retries,
                refresh=args.refresh_choice,
            )
            _write_json_atomic(
                checkpoint_dir / "lifecycle_manifest.json",
                {
                    "status": "COMPLETE",
                    "source": "Choice.css",
                    "indicators": ["LISTDATE", "DELISTDATE"],
                    "security_count": len(css_info),
                    "expected_security_count": len(choice_codes),
                    "observation_end": obs_end.isoformat(),
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                },
            )
            log("css", f"生命周期取回 {len(css_info)} 只")
            if args.stage == "lifecycle":
                log("done", f"生命周期检查点：{checkpoint_dir}")
                return 0

        calendars = {
            "XSHG": trading_dates("XSHG", series_start, obs_end),
            "XSHE": trading_dates("XSHE", series_start, obs_end),
        }
        calendar_frame = build_calendar_table(calendars)
        _write_csv_atomic(calendar_frame, checkpoint_dir / "trading_calendar.csv")
        log("calendar", f"交易日历 XSHG={len(calendars['XSHG'])}, XSHE={len(calendars['XSHE'])}")

        expected_batches = (len(choice_codes) + 149) // 150
        market_manifest = {
            "status": "IN_PROGRESS",
            "source": "Choice.csd+tradedates",
            "indicators": ["CLOSE", "LOWLIMIT", "ISSTSTOCK", "ISXSTSTOCK"],
            "series_start": series_start.isoformat(),
            "observation_start": obs_start.isoformat(),
            "observation_end": obs_end.isoformat(),
            "security_count": len(choice_codes),
            "expected_batch_count": expected_batches,
            "completed_batch_count": 0,
            "completed_security_count": 0,
            "row_count": 0,
            "calendar_row_count": len(calendar_frame),
            "universe_file": "security_universe.csv",
            "calendar_file": "trading_calendar.csv",
            "storage_format": "pandas_pickle_by_batch",
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        _write_json_atomic(market_manifest_path, market_manifest)

        fetched_count = 0
        processed_count = 0
        row_count = 0
        for batch_no, batch_codes, daily_batch in iter_chunked_csd(
            choice_codes,
            "CLOSE,LOWLIMIT,ISSTSTOCK,ISXSTSTOCK",
            series_start.strftime("%Y%m%d"),
            obs_end.strftime("%Y%m%d"),
            chunk=150,
            checkpoint_dir=checkpoint_dir / "csd",
            retries=args.choice_retries,
            refresh=args.refresh_choice,
        ):
            fetched_count += len(daily_batch)
            processed_count += len(batch_codes)
            row_count += sum(len(frame) for frame in daily_batch.values())
            market_manifest.update(
                {
                    "completed_batch_count": batch_no + 1,
                    "completed_security_count": processed_count,
                    "returned_security_count": fetched_count,
                    "row_count": row_count,
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                }
            )
            _write_json_atomic(market_manifest_path, market_manifest)
            if args.stage == "all":
                batch_universe = {canonical_key(code) for code in batch_codes}
                market_part, st_part = build_market_and_st_tables(
                    batch_universe,
                    daily_batch,
                    calendars,
                    obs_start,
                    obs_end,
                    series_start,
                )
                market_parts.append(market_part)
                st_parts.append(st_part)
            log(
                "csd",
                f"批次 {batch_no + 1}/{expected_batches} 完成："
                f"{len(daily_batch)}/{len(batch_codes)} 只，累计 {fetched_count} 只",
            )
        market_manifest.update(
            {
                "status": "COMPLETE",
                "completed_at": datetime.now().isoformat(timespec="seconds"),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
        )
        _write_json_atomic(market_manifest_path, market_manifest)
        if args.stage in {"market", "data"}:
            if args.stage == "data":
                log("done", f"Choice 全部数据已落盘，未运行风险事件计算：{checkpoint_dir}")
            else:
                log("done", f"行情/ST/交易日历已落盘：{checkpoint_dir}")
            return 0
    except Exception as exc:
        if market_manifest is not None:
            market_manifest.update(
                {
                    "status": "FAILED",
                    "error": str(exc),
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                }
            )
            try:
                _write_json_atomic(market_manifest_path, market_manifest)
            except Exception:  # noqa: BLE001
                pass
        raise
    finally:
        logout()

    tables = assemble_tables_from_frames(
        universe,
        css_info,
        market_parts,
        st_parts,
        calendars,
        obs_start,
        classification_date,
    )
    result, gate = run_engine(tables, args.rules, obs_start, obs_end, batch_id, "CHOICE_FIVE_TABLE_VERIFY")
    result_dir = write_risk_event_result(result, gate, args.output_dir)
    log("done", f"结果目录：{result_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
