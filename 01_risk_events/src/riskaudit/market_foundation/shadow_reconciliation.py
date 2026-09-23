"""
文件作用：把已发布市场快照的四表影子导出与既有正式标准运行逐键比较并保存不可覆盖报告。
编辑记录：
【首次生成：2026-08-15，实现多分块基线合并、业务键归一、值差异证据和风险适配状态门禁。】
"""

from __future__ import annotations

import csv
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Iterable, Mapping
from uuid import uuid4

from .adapter import FiveTableShadowAdapter, TABLE_FILES
from .facts import canonical_json
from .service import MarketFoundationError, MarketFoundationService


SHADOW_RECONCILIATION_VERSION = "market_shadow_reconciliation_v2"
MAX_EXAMPLES = 20
VALUE_FIELDS = {
    "交易日历表": ("是否交易日", "交易时段状态"),
    "证券基础状态日表": (
        "板块代码",
        "证券类型",
        "生命周期状态",
        "上市日期",
        "退市日期",
        "交易资格状态",
    ),
    "股票日行情表": (
        "成交状态",
        "开盘价",
        "最高价",
        "最低价",
        "收盘价",
        "成交量",
        "成交额",
        "涨停价",
        "跌停价",
    ),
    "证券风险状态表": ("风险状态值", "状态来源类别"),
}
INFORMATIONAL_VALUE_FIELDS = {
    "股票日行情表": ("前收盘价",),
}
NUMERIC_FIELDS = {
    "前收盘价",
    "开盘价",
    "最高价",
    "最低价",
    "收盘价",
    "成交量",
    "成交额",
    "涨停价",
    "跌停价",
}


def run_shadow_reconciliation(
    service: MarketFoundationService,
    snapshot_id: str,
    baseline_run_dirs: Iterable[str | Path],
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    """Compare baseline-covered business keys; shadow-only history is informational."""

    snapshot = service.get_snapshot(snapshot_id)
    if snapshot["status"] != "PUBLISHED":
        raise MarketFoundationError("MDFS001", "只有已发布快照可以执行四表影子对账")
    baseline_roots = tuple(Path(item).resolve() for item in baseline_run_dirs)
    if not baseline_roots:
        raise MarketFoundationError("MDFS002", "至少需要一个既有正式标准运行目录")
    if (start_date is None) != (end_date is None) or (
        start_date is not None and end_date is not None and start_date > end_date
    ):
        raise MarketFoundationError("MDFS002", "影子对账日期范围无效")
    service.catalog.execute(
        "UPDATE market_snapshot SET risk_adapter_status='COMPARING' WHERE snapshot_id=?",
        [snapshot_id],
    )
    try:
        shadow_root, shadow_manifest = _verified_shadow_export(
            service, snapshot_id, start_date=start_date, end_date=end_date
        )
        baseline, baseline_files = _load_baseline(
            baseline_roots, start_date=start_date, end_date=end_date
        )
        if any(not baseline[name] for name in TABLE_FILES):
            raise MarketFoundationError("MDFS002", "对账范围内四张基准表均必须有可比较记录")
        shadow, shadow_conflicts, shadow_informational = _load_table_root(shadow_root)
        lifecycles = _snapshot_lifecycles(service, snapshot_id)
        results = []
        for table_name in TABLE_FILES:
            expected = baseline[table_name]
            actual = shadow[table_name]
            raw_missing = set(expected) - set(actual)
            informational_missing = sorted(
                key for key in raw_missing if _outside_lifecycle(key, lifecycles)
            )
            missing = sorted(raw_missing - set(informational_missing))
            raw_different = set(
                key
                for key in set(expected) & set(actual)
                if expected[key] != actual[key]
            )
            lifecycle_state_different = sorted(
                key for key in raw_different
                if _inactive_eligibility_difference(table_name, key, expected[key], actual[key], lifecycles)
            )
            different = sorted(raw_different - set(lifecycle_state_different))
            baseline_conflicts = baseline["__conflicts__"].get(table_name, [])
            shadow_table_conflicts = shadow_conflicts.get(table_name, [])
            expected_info = baseline["__informational__"].get(table_name, {})
            actual_info = shadow_informational.get(table_name, {})
            informational_different = sorted(
                key
                for key in set(expected_info) & set(actual_info)
                if expected_info[key] != actual_info[key]
            )
            severe = len(missing) + len(different) + len(baseline_conflicts) + len(
                shadow_table_conflicts
            )
            results.append(
                {
                    "table_name": table_name,
                    "status": "PASS" if severe == 0 else "FAIL",
                    "baseline_key_count": len(expected),
                    "shadow_key_count": len(actual),
                    "missing_key_count": len(missing),
                    "different_value_count": len(different),
                    "baseline_conflict_count": len(baseline_conflicts),
                    "shadow_conflict_count": len(shadow_table_conflicts),
                    "shadow_only_key_count": len(set(actual) - set(expected)),
                    "informational_missing_key_count": len(informational_missing),
                    "informational_different_value_count": len(informational_different),
                    "informational_lifecycle_state_count": len(lifecycle_state_different),
                    "examples": [
                        *(f"MISSING|{_display_key(key)}" for key in missing),
                        *(f"DIFFERENT|{_display_key(key)}" for key in different),
                        *(f"BASELINE_CONFLICT|{item}" for item in baseline_conflicts),
                        *(f"SHADOW_CONFLICT|{item}" for item in shadow_table_conflicts),
                    ][:MAX_EXAMPLES],
                    "informational_examples": [
                        *(f"OUTSIDE_LIFECYCLE_ELIGIBILITY_ONLY|{_display_key(key)}" for key in lifecycle_state_different),
                        *(
                            f"OUTSIDE_LIFECYCLE|{_display_key(key)}"
                            for key in informational_missing
                        ),
                        *(
                            f"NON_RISK_FIELD_DIFFERENT|{_display_key(key)}"
                            for key in informational_different
                        ),
                    ][:MAX_EXAMPLES],
                }
            )
        report = _persist_report(
            service,
            snapshot,
            shadow_root,
            shadow_manifest,
            baseline_roots,
            baseline_files,
            results,
        )
        final_status = "PASSED" if report["status"] == "PASS" else "FAILED"
        service.catalog.execute(
            "UPDATE market_snapshot SET risk_adapter_status=? WHERE snapshot_id=?",
            [final_status, snapshot_id],
        )
        report["risk_adapter_status"] = final_status
        return report
    except Exception:
        service.catalog.execute(
            "UPDATE market_snapshot SET risk_adapter_status='FAILED' WHERE snapshot_id=?",
            [snapshot_id],
        )
        raise


def _verified_shadow_export(
    service: MarketFoundationService,
    snapshot_id: str,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> tuple[Path, dict[str, Any]]:
    scope_suffix = (
        f"_{start_date:%Y%m%d}_{end_date:%Y%m%d}"
        if start_date is not None and end_date is not None
        else ""
    )
    root = service.root / "exports" / snapshot_id / f"five_table_shadow{scope_suffix}"
    manifest_path = root / "shadow_manifest.json"
    if not manifest_path.is_file():
        FiveTableShadowAdapter(service).export(
            snapshot_id, start_date=start_date, end_date=end_date
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("market_snapshot_id") != snapshot_id:
        raise MarketFoundationError("MDFS003", "影子导出清单与快照不一致")
    expected_scope = {
        "start_date": start_date.isoformat() if start_date else None,
        "end_date": end_date.isoformat() if end_date else None,
    }
    manifest_scope = manifest.get("export_scope")
    if manifest_scope is not None and manifest_scope != expected_scope:
        raise MarketFoundationError("MDFS003", "影子导出清单与对账日期范围不一致")
    for table_name, file_name in TABLE_FILES.items():
        path = root / file_name
        expected = manifest.get("tables", {}).get(table_name, {}).get("sha256")
        actual = sha256(path.read_bytes()).hexdigest() if path.is_file() else None
        if actual != expected:
            raise MarketFoundationError("MDFS003", f"影子文件哈希复验失败：{table_name}")
    return root, manifest


def _load_baseline(
    roots: tuple[Path, ...],
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    merged = {table_name: {} for table_name in TABLE_FILES}
    conflicts = {table_name: [] for table_name in TABLE_FILES}
    informational = {table_name: {} for table_name in TABLE_FILES}
    files = []
    for root in roots:
        if not root.is_dir():
            raise MarketFoundationError("MDFS002", f"正式标准运行目录不存在：{root}")
        tables, current_conflicts, current_informational = _load_table_root(
            root, start_date=start_date, end_date=end_date
        )
        for table_name, file_name in TABLE_FILES.items():
            path = root / file_name
            files.append(
                {
                    "table_name": table_name,
                    "path": str(path),
                    "sha256": sha256(path.read_bytes()).hexdigest(),
                }
            )
            for key, value in tables[table_name].items():
                previous = merged[table_name].get(key)
                if previous is not None and previous != value:
                    conflicts[table_name].append(_display_key(key))
                merged[table_name][key] = value
            conflicts[table_name].extend(current_conflicts.get(table_name, []))
            for key, value in current_informational.get(table_name, {}).items():
                previous = informational[table_name].get(key)
                if previous is not None and previous != value:
                    conflicts[table_name].append(_display_key(key))
                informational[table_name][key] = value
    merged["__conflicts__"] = conflicts
    merged["__informational__"] = informational
    return merged, files


def _load_table_root(
    root: Path,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> tuple[
    dict[str, dict[tuple[str, ...], tuple[str, ...]]],
    dict[str, list[str]],
    dict[str, dict[tuple[str, ...], tuple[str, ...]]],
]:
    tables = {}
    conflicts = {}
    informational = {}
    for table_name, file_name in TABLE_FILES.items():
        path = root / file_name
        if not path.is_file():
            raise MarketFoundationError("MDFS002", f"标准运行缺少：{path}")
        values: dict[tuple[str, ...], tuple[str, ...]] = {}
        table_conflicts = []
        info_values: dict[tuple[str, ...], tuple[str, ...]] = {}
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            for row in csv.DictReader(stream):
                if start_date is not None and end_date is not None:
                    date_field = {"交易日历表":"日历日期", "证券基础状态日表":"状态日期", "股票日行情表":"交易日期", "证券风险状态表":"状态开始日期"}[table_name]
                    row_start = date.fromisoformat(_text(row.get(date_field)))
                    row_end = date.fromisoformat(_text(row.get("状态结束日期")) or row_start.isoformat()) if table_name == "证券风险状态表" else row_start
                    if row_end < start_date or row_start > end_date:
                        continue
                for key, value in _normalized_records(table_name, row):
                    # Compare both sides in the explicitly requested window.
                    # In particular, do not retain a full historical baseline in
                    # memory while exporting only a bounded snapshot window.
                    if start_date is not None and end_date is not None:
                        day = date.fromisoformat(key[0] if table_name == "交易日历表" else key[2])
                        if not start_date <= day <= end_date:
                            continue
                    previous = values.get(key)
                    if previous is not None and previous != value:
                        table_conflicts.append(_display_key(key))
                    values[key] = value
                    info_fields = INFORMATIONAL_VALUE_FIELDS.get(table_name, ())
                    if info_fields:
                        info_values[key] = tuple(
                            _normalized_value(field, row.get(field))
                            for field in info_fields
                        )
        tables[table_name] = values
        conflicts[table_name] = table_conflicts
        informational[table_name] = info_values
    return tables, conflicts, informational


def _snapshot_lifecycles(
    service: MarketFoundationService, snapshot_id: str
) -> dict[tuple[str, str], tuple[date, date | None]]:
    rows = service.catalog.rows(
        """
        SELECT source.payload_json
        FROM market_snapshot_record member
        JOIN source_record_catalog source
          ON source.dataset_id = member.dataset_id
         AND source.business_key = member.business_key
         AND source.source_record_version = member.source_record_version
         AND source.payload_hash = member.payload_hash
         AND source.partition_id = member.partition_id
        WHERE member.snapshot_id=?
          AND member.dataset_id='instrument_master_history'
        """,
        [snapshot_id],
    )
    result: dict[tuple[str, str], tuple[date, date | None]] = {}
    for row in rows:
        payload = json.loads(row["payload_json"])
        key = (payload["market_code"], payload["security_code"])
        listed = date.fromisoformat(payload["listing_date"])
        termination_text = payload.get("termination_date")
        terminated = date.fromisoformat(termination_text) if termination_text else None
        previous = result.get(key)
        if previous is None:
            result[key] = (listed, terminated)
            continue
        previous_listed, previous_terminated = previous
        result[key] = (
            min(previous_listed, listed),
            terminated or previous_terminated,
        )
    return result


def _outside_lifecycle(
    key: tuple[str, ...],
    lifecycles: Mapping[tuple[str, str], tuple[date, date | None]],
) -> bool:
    if len(key) < 3:
        return False
    lifecycle = lifecycles.get((key[0], key[1]))
    if lifecycle is None:
        return False
    try:
        current = date.fromisoformat(key[2])
    except ValueError:
        return False
    listed, terminated = lifecycle
    return current < listed or (terminated is not None and current >= terminated)


def _inactive_eligibility_difference(table, key, expected, actual, lifecycles) -> bool:
    """Only the unused trading-eligibility label after termination is INFO.

    Listing/termination dates, board, type and lifecycle disagreements still
    fail. No in-lifecycle status disagreement is accepted by this exception.
    """
    if table != "证券基础状态日表" or not _outside_lifecycle(key, lifecycles):
        return False
    return (expected[:5] == actual[:5]
            and expected[2] == actual[2] == "Delisted"
            and expected[5] != actual[5])


def _normalized_records(
    table_name: str, row: Mapping[str, Any]
) -> list[tuple[tuple[str, ...], tuple[str, ...]]]:
    if table_name == "交易日历表":
        key = (_text(row.get("日历日期")),)
        return [(key, _values(table_name, row))]
    market, security = _security_key(row)
    if table_name == "证券基础状态日表":
        key = (market, security, _text(row.get("状态日期")))
        return [(key, _values(table_name, row))]
    if table_name == "股票日行情表":
        key = (market, security, _text(row.get("交易日期")))
        return [(key, _values(table_name, row))]
    start = date.fromisoformat(_text(row.get("状态开始日期")))
    end_text = _text(row.get("状态结束日期"))
    end = date.fromisoformat(end_text) if end_text else start
    risk_type = _text(row.get("风险状态类型")).upper()
    result = []
    current = start
    while current <= end:
        result.append(
            ((market, security, current.isoformat(), risk_type), _values(table_name, row))
        )
        current += timedelta(days=1)
    return result


def _values(table_name: str, row: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(_normalized_value(field, row.get(field)) for field in VALUE_FIELDS[table_name])


def _normalized_value(field: str, value: Any) -> str:
    text = _text(value)
    if field == "是否交易日":
        return "true" if text.lower() in {"true", "1", "是", "yes"} else "false"
    if field in NUMERIC_FIELDS and text:
        try:
            number = Decimal(text)
            return "0" if number == 0 else format(number.normalize(), "f")
        except InvalidOperation:
            return text
    return text


def _security_key(row: Mapping[str, Any]) -> tuple[str, str]:
    market = _text(row.get("交易市场代码")).upper()
    code = _text(row.get("证券代码")).upper()
    aliases = {"BJSE": "XBSE", "BSE": "XBSE"}
    if "." in code:
        code, suffix = code.rsplit(".", 1)
        market = aliases.get(suffix, suffix)
    market = aliases.get(market, market)
    return market, code


def _persist_report(
    service: MarketFoundationService,
    snapshot: Mapping[str, Any],
    shadow_root: Path,
    shadow_manifest: Mapping[str, Any],
    baseline_roots: tuple[Path, ...],
    baseline_files: list[dict[str, Any]],
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    run_id = f"mdshadow_{uuid4().hex}"
    created_at = datetime.now(timezone.utc)
    failed = sum(item["status"] == "FAIL" for item in results)
    report = {
        "shadow_run_id": run_id,
        "rule_version": SHADOW_RECONCILIATION_VERSION,
        "status": "FAIL" if failed else "PASS",
        "failed_table_count": failed,
        "market_snapshot_id": snapshot["snapshot_id"],
        "market_snapshot_member_hash": snapshot["member_hash"],
        "shadow_root": str(shadow_root),
        "shadow_manifest_hash": sha256(canonical_json(shadow_manifest).encode("utf-8")).hexdigest(),
        "baseline_roots": [str(item) for item in baseline_roots],
        "baseline_files": baseline_files,
        "tables": results,
        "created_at": created_at.isoformat(),
        "risk_entry_switch_attempted": False,
    }
    report["report_hash"] = sha256(canonical_json(report).encode("utf-8")).hexdigest()
    parent = service.root / "exports" / snapshot["snapshot_id"] / "shadow_reconciliation"
    target = parent / run_id
    staging = service.root / ".staging" / f"{run_id}_shadow"
    staging.mkdir(parents=True, exist_ok=False)
    try:
        json_name = "shadow_reconciliation_report.json"
        csv_name = "shadow_reconciliation_tables.csv"
        (staging / json_name).write_bytes(
            json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8")
        )
        with (staging / csv_name).open(
            "x", encoding="utf-8-sig", newline=""
        ) as stream:
            writer = csv.DictWriter(
                stream,
                fieldnames=[
                    "table_name",
                    "status",
                    "baseline_key_count",
                    "shadow_key_count",
                    "missing_key_count",
                    "different_value_count",
                    "baseline_conflict_count",
                    "shadow_conflict_count",
                    "shadow_only_key_count",
                    "informational_missing_key_count",
                    "informational_different_value_count",
                    "informational_lifecycle_state_count",
                    "examples",
                    "informational_examples",
                ],
            )
            writer.writeheader()
            for item in results:
                writer.writerow(
                    {
                        **item,
                        "examples": json.dumps(item["examples"], ensure_ascii=False),
                        "informational_examples": json.dumps(
                            item["informational_examples"], ensure_ascii=False
                        ),
                    }
                )
        parent.mkdir(parents=True, exist_ok=True)
        staging.replace(target)
    except Exception:
        if staging.exists():
            for path in staging.iterdir():
                path.unlink(missing_ok=True)
            staging.rmdir()
        raise
    json_path = target / json_name
    csv_path = target / csv_name
    return {
        **report,
        "report_paths": {"json": str(json_path), "csv": str(csv_path)},
    }


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _display_key(key: tuple[str, ...]) -> str:
    return "|".join(key)
