"""Snapshot export and lifecycle adaptation for offline risk runs."""

from __future__ import annotations

import csv
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import shutil
from typing import Any, Callable, Iterable, Mapping
from uuid import uuid4

import pandas as pd

from riskaudit.cancellation import raise_if_canceled

from .adapter import FiveTableShadowAdapter, TABLE_FILES
from .risk_shadow_reconciliation import _file_hash
from .service import MarketFoundationError, MarketFoundationService


def ensure_broker_metric_snapshot_export(
    service: MarketFoundationService,
    snapshot_id: str,
    *,
    end_date: date,
    cancel_check: Callable[[], bool] | None = None,
    output_root: str | Path | None = None,
    lifecycle_reference_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build one reusable calendar/lifecycle/ST export per snapshot cutoff."""
    root = (
        Path(output_root or service.root / "exports")
        / snapshot_id
        / f"broker_metric_market_input_{end_date:%Y%m%d}"
    )
    manifest_path = root / "manifest.json"
    if root.exists():
        if not manifest_path.is_file():
            raise MarketFoundationError("MDFSC013", "券商指标快照适配目录不完整")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            manifest.get("market_snapshot_id") != snapshot_id
            or manifest.get("end_date") != end_date.isoformat()
        ):
            raise MarketFoundationError("MDFSC013", "券商指标快照适配清单范围冲突")
        for item in manifest.get("files", {}).values():
            path = Path(item["path"])
            if not path.is_file() or _file_hash(path) != item.get("sha256"):
                raise MarketFoundationError("MDFSC013", "券商指标快照适配文件哈希失败")
        return manifest

    temporary = root.with_name(f".{root.name}.building_{uuid4().hex}")
    temporary.mkdir(parents=True, exist_ok=False)
    try:
        raise_if_canceled(cancel_check)
        calendar_path = temporary / "trading_calendar.csv"
        calendar_count = _write_catalog_csv(
            calendar_path,
            ("market_code", "calendar_date", "is_trading_day"),
            service.catalog.iter_rows(
                """
                SELECT 'XSHG' AS market_code,
                       json_extract_string(source.payload_json, '$.business_date') AS calendar_date,
                       true AS is_trading_day
                FROM market_snapshot_record member
                JOIN source_record_catalog source
                  ON source.dataset_id = member.dataset_id
                 AND source.business_key = member.business_key
                 AND source.source_record_version = member.source_record_version
                 AND source.payload_hash = member.payload_hash
                 AND source.partition_id = member.partition_id
                WHERE member.snapshot_id=?
                  AND member.dataset_id='market_calendar'
                  AND json_extract_string(source.payload_json, '$.market_code')='XSHG'
                  AND CAST(json_extract_string(source.payload_json, '$.is_trading_day') AS BOOLEAN)
                  AND CAST(json_extract_string(source.payload_json, '$.business_date') AS DATE) <= ?
                ORDER BY calendar_date
                """,
                [snapshot_id, end_date],
            ),
            cancel_check=cancel_check,
        )
        lifecycle_path = temporary / "security_lifecycle.csv"
        lifecycle_count = _write_catalog_csv(
            lifecycle_path,
            ("market_code", "security_code", "listed_date", "delisted_date"),
            service.catalog.iter_rows(
                """
                SELECT json_extract_string(source.payload_json, '$.market_code') AS market_code,
                       json_extract_string(source.payload_json, '$.security_code') || '.' ||
                           json_extract_string(source.payload_json, '$.market_code') AS security_code,
                       json_extract_string(source.payload_json, '$.listing_date') AS listed_date,
                       coalesce(json_extract_string(source.payload_json, '$.termination_date'), '') AS delisted_date
                FROM market_snapshot_record member
                JOIN source_record_catalog source
                  ON source.dataset_id = member.dataset_id
                 AND source.business_key = member.business_key
                 AND source.source_record_version = member.source_record_version
                 AND source.payload_hash = member.payload_hash
                 AND source.partition_id = member.partition_id
                WHERE member.snapshot_id=?
                  AND member.dataset_id='instrument_master_history'
                  AND json_extract_string(source.payload_json, '$.market_code') IN ('XSHG', 'XSHE')
                ORDER BY market_code, security_code
                """,
                [snapshot_id],
            ),
            cancel_check=cancel_check,
        )
        lifecycle_reference = _supplement_lifecycle_csv(
            lifecycle_path,
            lifecycle_reference_path,
        )
        lifecycle_count += lifecycle_reference["supplemented_row_count"]
        st_path = temporary / "security_st_status.csv"
        st_count = _write_catalog_csv(
            st_path,
            (
                "market_code",
                "security_code",
                "risk_status_type",
                "risk_status_value",
                "status_start_date",
            ),
            service.catalog.iter_rows(
                """
                SELECT json_extract_string(source.payload_json, '$.market_code') AS market_code,
                       json_extract_string(source.payload_json, '$.security_code') || '.' ||
                           json_extract_string(source.payload_json, '$.market_code') AS security_code,
                       'ST' AS risk_status_type,
                       CASE WHEN CAST(json_extract_string(source.payload_json, '$.is_st') AS BOOLEAN)
                            THEN '生效' ELSE '未生效' END AS risk_status_value,
                       json_extract_string(source.payload_json, '$.business_date') AS status_start_date
                FROM market_snapshot_record member
                JOIN source_record_catalog source
                  ON source.dataset_id = member.dataset_id
                 AND source.business_key = member.business_key
                 AND source.source_record_version = member.source_record_version
                 AND source.payload_hash = member.payload_hash
                 AND source.partition_id = member.partition_id
                WHERE member.snapshot_id=?
                  AND member.dataset_id='daily_st_status'
                  AND json_extract_string(source.payload_json, '$.market_code') IN ('XSHG', 'XSHE')
                  AND CAST(json_extract_string(source.payload_json, '$.business_date') AS DATE) <= ?
                ORDER BY status_start_date, market_code, security_code
                """,
                [snapshot_id, end_date],
                batch_size=100_000,
            ),
            cancel_check=cancel_check,
        )
        files = {}
        for key, path, count in (
            ("calendar", calendar_path, calendar_count),
            ("lifecycle", lifecycle_path, lifecycle_count),
            ("st_status", st_path, st_count),
        ):
            final_path = root / path.name
            files[key] = {
                "path": str(final_path),
                "row_count": count,
                "sha256": _file_hash(path),
            }
        manifest = {
            "schema_version": "broker_metric_snapshot_market_input_v1",
            "market_snapshot_id": snapshot_id,
            "end_date": end_date.isoformat(),
            "rqdata_accessed": False,
            "lifecycle_reference": lifecycle_reference,
            "files": files,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        _write_json(temporary / "manifest.json", manifest)
        os.replace(temporary, root)
        return manifest
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        raise

def _supplement_lifecycle_csv(
    snapshot_path: Path,
    reference_path: str | Path | None,
) -> dict[str, Any]:
    """Append audited pre-foundation delisted securities to snapshot lifecycle."""
    if reference_path is None:
        return {
            "provided": False,
            "path": None,
            "sha256": None,
            "row_count": 0,
            "supplemented_row_count": 0,
        }
    reference = Path(reference_path).resolve()
    if not reference.is_file():
        raise MarketFoundationError(
            "MDFSC014", f"券商指标生命周期补充文件不存在：{reference}"
        )
    snapshot = pd.read_csv(
        snapshot_path,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
    )
    reference_frame = pd.read_csv(
        reference,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
    )
    legacy = {
        "order_book_id",
        "listed_date_norm",
        "de_listed_date_norm",
    } <= set(reference_frame.columns)
    standard = {
        "market_code",
        "security_code",
        "listed_date",
        "delisted_date",
    } <= set(reference_frame.columns)
    if not legacy and not standard:
        raise MarketFoundationError("MDFSC014", "券商指标生命周期补充文件字段不完整")

    records = {
        str(row["security_code"]): {
            "market_code": str(row["market_code"]),
            "security_code": str(row["security_code"]),
            "listed_date": str(row["listed_date"]),
            "delisted_date": str(row["delisted_date"]),
        }
        for row in snapshot.to_dict(orient="records")
    }
    supplemented = 0
    for row in reference_frame.to_dict(orient="records"):
        code = str(row["order_book_id"] if legacy else row["security_code"]).strip()
        if not code.endswith((".XSHG", ".XSHE")):
            continue
        market = code.rsplit(".", 1)[-1]
        listed = _normalize_lifecycle_reference_date(
            row["listed_date_norm"] or row.get("listed_date", "")
            if legacy
            else row["listed_date"],
            optional=False,
        )
        delisted = _normalize_lifecycle_reference_date(
            row["de_listed_date_norm"] or row.get("de_listed_date", "")
            if legacy
            else row["delisted_date"],
            optional=True,
        )
        value = {
            "market_code": market,
            "security_code": code,
            "listed_date": listed,
            "delisted_date": delisted,
        }
        previous = records.get(code)
        if previous is not None and previous != value:
            raise MarketFoundationError(
                "MDFSC014", f"券商指标生命周期证据冲突：{code}"
            )
        if previous is None:
            records[code] = value
            supplemented += 1

    temporary = snapshot_path.with_name(f".{snapshot_path.name}.{uuid4().hex}.tmp")
    ordered = sorted(records.values(), key=lambda item: (item["market_code"], item["security_code"]))
    _write_catalog_csv(
        temporary,
        ("market_code", "security_code", "listed_date", "delisted_date"),
        ordered,
        cancel_check=None,
    )
    os.replace(temporary, snapshot_path)
    return {
        "provided": True,
        "path": str(reference),
        "sha256": _file_hash(reference),
        "row_count": len(reference_frame),
        "supplemented_row_count": supplemented,
    }

def _normalize_lifecycle_reference_date(value: Any, *, optional: bool) -> str:
    text = str(value or "").strip()
    if not text or text.startswith("0000-"):
        if optional:
            return ""
        raise MarketFoundationError("MDFSC014", "券商指标生命周期上市日期缺失")
    for date_format in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, date_format).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text).date().isoformat()
    except ValueError as exc:
        raise MarketFoundationError(
            "MDFSC014", f"券商指标生命周期日期无效：{text}"
        ) from exc

def _write_catalog_csv(
    path: Path,
    fields: tuple[str, ...],
    rows: Iterable[Mapping[str, Any]],
    *,
    cancel_check: Callable[[], bool] | None,
) -> int:
    count = 0
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            if count % 100_000 == 0:
                raise_if_canceled(cancel_check)
            writer.writerow({field: row.get(field, "") for field in fields})
            count += 1
    return count

def _require_usable_snapshot(
    snapshot: Mapping[str, Any], start_date: date, end_date: date
) -> None:
    if snapshot.get("status") != "PUBLISHED":
        raise MarketFoundationError("MDFSC007", "只能使用已发布市场快照")
    if snapshot.get("risk_adapter_status") != "PASSED":
        raise MarketFoundationError("MDFSC008", "市场快照尚未通过四表影子适配")
    if date.fromisoformat(str(snapshot["start_date"])) > start_date or date.fromisoformat(
        str(snapshot["end_date"])
    ) < end_date:
        raise MarketFoundationError("MDFSC009", "市场快照未覆盖计算观察区间")

def _normalize_bse_security_codes(
    tables: Mapping[str, pd.DataFrame],
) -> None:
    for frame in tables.values():
        if "security_code" not in frame.columns:
            continue
        frame["security_code"] = frame["security_code"].astype(str).str.replace(
            r"\.BJSE$", ".XBSE", regex=True
        )

def _append_missing_snapshot_master_rows(
    service: MarketFoundationService,
    snapshot_id: str,
    tables: dict[str, pd.DataFrame],
    broker_universe: pd.DataFrame,
    observation_end: date,
) -> None:
    master = tables["证券基础状态日表"]
    existing = {
        (str(row.market_code), str(row.security_code))
        for row in master[["market_code", "security_code"]]
        .drop_duplicates()
        .itertuples(index=False)
    }
    requested = {
        (str(row.market_code), str(row.security_code))
        for row in broker_universe[["market_code", "security_code"]]
        .drop_duplicates()
        .itertuples(index=False)
    }
    missing = requested - existing
    if not missing:
        return
    rows = service.catalog.rows(
        """
        SELECT source.source_record_version, source.source_request_id,
               source.payload_json
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
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for item in rows:
        payload = json.loads(item["payload_json"])
        key = (
            payload["market_code"],
            f"{payload['security_code']}.{payload['market_code']}",
        )
        by_key[key] = {**item, "payload": payload}
    supplements = []
    for key in sorted(missing):
        item = by_key.get(key)
        if item is None:
            continue
        payload = item["payload"]
        terminated = payload.get("termination_date")
        is_delisted = bool(terminated and str(terminated) <= observation_end.isoformat())
        supplements.append(
            {
                "market_code": key[0],
                "security_code": key[1],
                "status_date": observation_end.isoformat(),
                "record_version": item["source_record_version"],
                "board_code": payload["board_code"],
                "security_type": payload["security_type"],
                "lifecycle_status": "Delisted" if is_delisted else "Active",
                "listed_date": payload["listing_date"],
                "delisted_date": terminated or "",
                "trading_eligibility_status": "终止" if is_delisted else "正常",
                "source_record_id": item["source_request_id"],
            }
        )
    if supplements:
        tables["证券基础状态日表"] = pd.concat(
            [master, pd.DataFrame(supplements)],
            ignore_index=True,
            sort=False,
        )

def _previous_trading_date(
    service: MarketFoundationService, snapshot_id: str, observation_start: date
) -> date:
    row = service.catalog.row(
        """
        SELECT max(CAST(json_extract_string(source.payload_json, '$.business_date') AS DATE)) AS day
        FROM market_snapshot_record member
        JOIN source_record_catalog source
          ON source.dataset_id = member.dataset_id
         AND source.business_key = member.business_key
         AND source.source_record_version = member.source_record_version
         AND source.payload_hash = member.payload_hash
         AND source.partition_id = member.partition_id
        WHERE member.snapshot_id=?
          AND member.dataset_id='market_calendar'
          AND json_extract_string(source.payload_json, '$.market_code') IN ('XSHG', 'XSHE')
          AND CAST(json_extract_string(source.payload_json, '$.business_date') AS DATE) < ?
          AND CAST(json_extract_string(source.payload_json, '$.is_trading_day') AS BOOLEAN)
        """,
        [snapshot_id, observation_start],
    )
    if row is None or row["day"] is None:
        raise MarketFoundationError("MDFSC010", "快照中不存在观察期前 ST 基线交易日")
    return row["day"]

def _verified_export(
    service: MarketFoundationService,
    snapshot_id: str,
    *,
    start_date: date,
    end_date: date,
    output_root: str | Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    root = (
        Path(output_root or service.root / "exports")
        / snapshot_id
        / f"five_table_shadow_{start_date:%Y%m%d}_{end_date:%Y%m%d}"
    )
    manifest_path = root / "shadow_manifest.json"
    if not manifest_path.is_file():
        FiveTableShadowAdapter(service).export(
            snapshot_id,
            output_root=output_root,
            start_date=start_date,
            end_date=end_date,
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected_scope = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    if (
        manifest.get("market_snapshot_id") != snapshot_id
        or manifest.get("export_scope") != expected_scope
    ):
        raise MarketFoundationError("MDFSC011", "四表影子清单未绑定当前快照或范围")
    for table_name, file_name in TABLE_FILES.items():
        path = root / file_name
        expected = manifest.get("tables", {}).get(table_name, {}).get("sha256")
        if not path.is_file() or _file_hash(path) != expected:
            raise MarketFoundationError(
                "MDFSC011", f"四表影子文件哈希复验失败：{table_name}"
            )
    return root, manifest

def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8"))

