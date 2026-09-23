"""从完整01批次提取风险股票拦截、风险股票分档所需的两个小文件，不删除源批次。

仅在准备新底表时运行。逐事件提取前一交易日ST事实，放入计算清单；
风险事件CSV保持原样，后续风险股票拦截、风险股票分档计算不再读取全市场ST日表。
"""

import argparse
from collections import Counter
from datetime import date
import hashlib
import json
from pathlib import Path
import shutil
import sys

import pandas as pd

MODULE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE / "src"))
from riskaudit.broker_metrics.event_input import load_risk_events
from riskaudit.broker_metrics.real_run_inputs import _load_calendar


def sha256(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def prepare(source_batch, output_dir, calendar_path):
    source = source_batch / "risk_results" / source_batch.name
    events_path = source / "risk_events.csv"
    manifest_path = source / "calculation_manifest.json"
    st_path = source_batch / "risk_input_code/security_risk_status.csv"
    output_dir.mkdir(parents=True, exist_ok=True)
    for name in ("risk_events.csv", "calculation_manifest.json"):
        if (output_dir / name).exists():
            raise FileExistsError(f"目标文件已存在：{output_dir / name}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    events = load_risk_events(events_path, manifest_path)
    calendar, _, _ = _load_calendar(calendar_path,
        min((e.first_fact_date for e in events), default=date.fromisoformat(manifest["observation_start"])),
        max((e.first_fact_date for e in events), default=date.fromisoformat(manifest["observation_end"])))
    requests = {e.event_key: (e.market_code, e.security_code,
        calendar.previous_trading_day("cn", e.first_fact_date).isoformat()) for e in events}
    needed = set(requests.values())
    codes = {key[1] for key in needed}
    dates = {key[2] for key in needed}
    states = {}
    columns = ["market_code", "security_code", "risk_status_type", "risk_status_value", "status_start_date"]
    for chunk in pd.read_csv(st_path, usecols=columns, dtype=str,
                             keep_default_na=False, chunksize=200_000):
        chunk = chunk.loc[chunk.security_code.isin(codes)
                          & chunk.status_start_date.isin(dates)
                          & (chunk.risk_status_type == "ST")]
        for market, code, day, value in chunk[
            ["market_code", "security_code", "status_start_date", "risk_status_value"]
        ].itertuples(index=False, name=None):
            key = (market, code, day)
            if key not in needed:
                continue
            if key in states or value not in {"生效", "未生效", "True", "False"}:
                raise ValueError(f"ST事实重复或无效：{key}, {value}")
            states[key] = "ST" if value in {"生效", "True"} else "NON_ST"
    if set(states) != needed:
        raise ValueError(f"事件评价截止日缺少ST事实：{sorted(needed - set(states))[:10]}")
    entries = {}
    for event in events:
        lookup = requests[event.event_key]
        if event.event_type == "NEW_ST" and states[lookup] != "NON_ST":
            raise ValueError(f"新增ST事件与前日状态矛盾：{event.event_key}")
        event_key = "|".join(map(str, event.event_key))
        entries[event_key] = {"cutoff_date": lookup[2], "st_state": states[lookup]}
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["cutoff_st_evidence"] = {
        "schema_version": 1,
        "method": "EXACT_DAILY_ST_LOOKUP_AT_PREVIOUS_TRADING_DAY",
        "event_csv_sha256": sha256(events_path),
        "original_manifest_sha256": sha256(manifest_path),
        "source_st_sha256": sha256(st_path),
        "source_calendar_sha256": sha256(calendar_path),
        "entry_count": len(entries),
        "entries": entries,
    }
    shutil.copy2(events_path, output_dir / "risk_events.csv")
    (output_dir / "calculation_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"已提取 {len(entries)} 个事件的前一交易日ST事实："
          f"{dict(Counter(states[lookup] for lookup in requests.values()))}")
    print(f"输出：{output_dir.resolve()}")
    print("风险事件CSV保持原样；计算清单新增截止日ST证据。源批次未删除。")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-batch", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--calendar", type=Path,
                        default=MODULE / "data/月截面数据转化为逐日数据/trading_calendar.csv")
    args = parser.parse_args()
    prepare(args.source_batch, args.output_dir, args.calendar)
