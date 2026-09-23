"""
文件作用：只读加载既有风险事件 CSV 与 manifest，禁止从市场数据重新生成事件。
编辑记录：
【首次生成：2026-08-12，实现英文内部表头/中文 v1 表头精确识别、唯一键和 manifest 对账。】
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from .models import RiskEventRecord


ENGLISH_FIELDS = {
    "market_code",
    "security_code",
    "event_type",
    "first_fact_date",
    "risk_date",
    "rule_version",
    "calculation_batch_id",
}
CHINESE_TO_ENGLISH = {
    "交易市场代码": "market_code",
    "证券代码": "security_code",
    "事件类型": "event_type",
    "首次事实日期": "first_fact_date",
    "风险认定日期": "risk_date",
    "风险规则版本号": "rule_version",
    "计算批次标识": "calculation_batch_id",
}
ALLOWED_EVENT_TYPES = {"NON_ST_CONTINUOUS_LIMIT_DOWN", "NEW_ST"}


def load_risk_events(
    csv_path: str | Path,
    manifest_path: str | Path,
) -> tuple[RiskEventRecord, ...]:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    if manifest.get("status") != "SUCCEEDED":
        raise ValueError("BM001_EVENT_MANIFEST_MISMATCH: status is not SUCCEEDED")
    frame = pd.read_csv(csv_path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    columns = set(frame.columns)
    if ENGLISH_FIELDS <= columns:
        normalized = frame
    elif set(CHINESE_TO_ENGLISH) <= columns:
        normalized = frame.rename(columns=CHINESE_TO_ENGLISH)
    else:
        raise ValueError("BM003_EVENT_SCHEMA_UNKNOWN")
    if len(normalized) != int(manifest.get("event_count", -1)):
        raise ValueError("BM001_EVENT_MANIFEST_MISMATCH: event_count")
    records = []
    for row in normalized.to_dict(orient="records"):
        event_type = row["event_type"].strip()
        if event_type not in ALLOWED_EVENT_TYPES:
            raise ValueError(f"BM003_EVENT_SCHEMA_UNKNOWN: event_type={event_type}")
        event = RiskEventRecord(
            market_code=row["market_code"].strip(),
            security_code=row["security_code"].strip(),
            event_type=event_type,
            first_fact_date=date.fromisoformat(row["first_fact_date"][:10]),
            risk_date=(date.fromisoformat(row["risk_date"][:10]) if row["risk_date"] else None),
            event_rule_version=row["rule_version"].strip(),
            event_batch_id=row["calculation_batch_id"].strip(),
        )
        records.append(event)
    keys = [item.event_key for item in records]
    if len(keys) != len(set(keys)):
        raise ValueError("BM002_EVENT_KEY_DUPLICATE")
    expected_batch = str(manifest.get("calculation_batch_id", ""))
    expected_rule = str(manifest.get("rule_version", ""))
    if any(
        item.event_batch_id != expected_batch
        or item.event_rule_version != expected_rule
        for item in records
    ):
        raise ValueError("BM001_EVENT_MANIFEST_MISMATCH: batch/rule")
    return tuple(sorted(records, key=lambda item: item.event_key))
