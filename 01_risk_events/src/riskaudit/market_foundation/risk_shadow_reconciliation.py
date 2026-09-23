"""Result-level reconciliation for snapshot risk calculations."""

from __future__ import annotations

import csv
from hashlib import sha256
from pathlib import Path
from typing import Any

from .service import MarketFoundationError


EVENT_KEY_FIELDS = ("交易市场代码", "证券代码", "事件类型", "首次事实日期")
EVENT_IGNORED_FIELDS = ("计算批次标识", "上游数据快照标识", "来源记录标识列表")
SEGMENT_KEY_FIELDS = ("交易市场代码", "证券代码", "连续段序号")
SEGMENT_IGNORED_FIELDS = EVENT_IGNORED_FIELDS
MAX_EXAMPLES = 20


def reconcile_risk_results(
    local_result_dir: str | Path, baseline_result_dir: str | Path
) -> dict[str, Any]:
    local = Path(local_result_dir)
    baseline = Path(baseline_result_dir)
    specifications = (
        (
            "risk_events.csv",
            EVENT_KEY_FIELDS,
            EVENT_IGNORED_FIELDS,
        ),
        (
            "continuous_limit_down_segments.csv",
            SEGMENT_KEY_FIELDS,
            SEGMENT_IGNORED_FIELDS,
        ),
    )
    comparisons = []
    for file_name, key_fields, ignored_fields in specifications:
        local_path = local / file_name
        baseline_path = baseline / file_name
        actual, actual_conflicts = _load_keyed_csv(
            local_path, key_fields, ignored_fields
        )
        expected, baseline_conflicts = _load_keyed_csv(
            baseline_path, key_fields, ignored_fields
        )
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        different = sorted(
            key
            for key in set(expected) & set(actual)
            if expected[key] != actual[key]
        )
        failed = bool(
            missing
            or extra
            or different
            or baseline_conflicts
            or actual_conflicts
        )
        comparisons.append(
            {
                "file_name": file_name,
                "status": "FAIL" if failed else "PASS",
                "baseline_row_count": len(expected),
                "local_row_count": len(actual),
                "missing_count": len(missing),
                "extra_count": len(extra),
                "different_count": len(different),
                "baseline_conflict_count": len(baseline_conflicts),
                "local_conflict_count": len(actual_conflicts),
                "baseline_sha256": _file_hash(baseline_path),
                "local_sha256": _file_hash(local_path),
                "examples": [
                    *(f"MISSING|{_display_key(key)}" for key in missing),
                    *(f"EXTRA|{_display_key(key)}" for key in extra),
                    *(f"DIFFERENT|{_display_key(key)}" for key in different),
                    *(f"BASELINE_CONFLICT|{item}" for item in baseline_conflicts),
                    *(f"LOCAL_CONFLICT|{item}" for item in actual_conflicts),
                ][:MAX_EXAMPLES],
            }
        )
    return {
        "status": (
            "PASS" if all(item["status"] == "PASS" for item in comparisons) else "FAIL"
        ),
        "failed_file_count": sum(item["status"] == "FAIL" for item in comparisons),
        "ignored_provenance_fields": sorted(
            set(EVENT_IGNORED_FIELDS) | set(SEGMENT_IGNORED_FIELDS)
        ),
        "files": comparisons,
        "risk_entry_switch_attempted": False,
    }

def _load_keyed_csv(
    path: Path,
    key_fields: tuple[str, ...],
    ignored_fields: tuple[str, ...],
) -> tuple[dict[tuple[str, ...], tuple[tuple[str, str], ...]], list[str]]:
    if not path.is_file():
        raise MarketFoundationError("MDFSC003", f"结果文件不存在：{path}")
    values: dict[tuple[str, ...], tuple[tuple[str, str], ...]] = {}
    conflicts: list[str] = []
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames is None:
            raise MarketFoundationError("MDFSC003", f"结果文件没有表头：{path}")
        missing_fields = set(key_fields) - set(reader.fieldnames)
        if missing_fields:
            raise MarketFoundationError(
                "MDFSC003", f"结果文件缺少对账键：{', '.join(sorted(missing_fields))}"
            )
        compared_fields = tuple(
            field
            for field in reader.fieldnames
            if field not in key_fields and field not in ignored_fields
        )
        for row in reader:
            key = tuple(_text(row.get(field)) for field in key_fields)
            value = tuple((field, _text(row.get(field))) for field in compared_fields)
            previous = values.get(key)
            if previous is not None and previous != value:
                conflicts.append(_display_key(key))
            values[key] = value
    return values, conflicts

def _file_hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()

def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()

def _display_key(key: tuple[str, ...]) -> str:
    return "|".join(key)

