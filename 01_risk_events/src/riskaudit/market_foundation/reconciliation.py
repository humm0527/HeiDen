"""
文件作用：对已结束的 P4 回填执行冻结证券级全范围质量对账并保存不可覆盖报告。
编辑记录：
【首次生成：2026-08-15，实现终态门禁、证券×交易日覆盖、文件哈希和来源版本对账。】
"""

from __future__ import annotations

from datetime import timedelta
import json
from typing import Any, Iterable

from .facts import canonical_json
from .service import MarketFoundationError, MarketFoundationService
from .reconciliation_support import (
    ReconciliationCheck,
    _as_date,
)
from .reconciliation_checks import (
    _blocked_capability_checks,
    _calendar_checks,
    _daily_coverage_checks,
    _daily_value_checks,
    _file_integrity_checks,
    _master_and_lifecycle_checks,
    _source_version_checks,
    _universe_checks,
)
from .reconciliation_report import _persist_report


def inspect_reconciliation_readiness(
    service: MarketFoundationService, backfill_id: str
) -> dict[str, Any]:
    """Refuse reconciliation while any P4 unit or linked task can still write."""

    run = service.catalog.row(
        "SELECT * FROM market_backfill_run WHERE backfill_id=?", [backfill_id]
    )
    if run is None:
        raise MarketFoundationError("MDFR001", "P4 回填运行不存在")
    counts = service.catalog.row(
        """
        SELECT count(*) AS total,
               count(*) FILTER (WHERE status='SUCCEEDED') AS succeeded,
               count(*) FILTER (WHERE status='FAILED') AS failed,
               count(*) FILTER (WHERE status IN ('PENDING','RUNNING')) AS open
        FROM market_backfill_unit WHERE backfill_id=?
        """,
        [backfill_id],
    )
    linked_open = service.catalog.row(
        """
        SELECT count(*) AS count
        FROM refresh_task
        WHERE task_id IN (
            SELECT task_id FROM market_backfill_unit
            WHERE backfill_id=? AND task_id IS NOT NULL
        ) AND status IN ('QUEUED','RUNNING','VALIDATING')
        """,
        [backfill_id],
    )
    ready = bool(
        run["status"] in {"PARTIAL_COMPLETE", "INCOMPLETE"}
        and int(counts["total"] or 0) > 0
        and int(counts["open"] or 0) == 0
        and int(counts["failed"] or 0) == 0
        and int(counts["succeeded"] or 0) == int(counts["total"] or 0)
        and int(linked_open["count"] or 0) == 0
    )
    result = {
        "ready": ready,
        "backfill_id": backfill_id,
        "status": run["status"],
        "total_units": int(counts["total"] or 0),
        "succeeded_units": int(counts["succeeded"] or 0),
        "failed_units": int(counts["failed"] or 0),
        "open_units": int(counts["open"] or 0),
        "open_linked_tasks": int(linked_open["count"] or 0),
    }
    if not ready:
        raise MarketFoundationError(
            "MDFR002",
            "P4 回填尚未完整结束，禁止扫描正在写入的数据湖",
            result,
        )
    return {**result, "run": dict(run)}


def run_full_reconciliation(
    service: MarketFoundationService,
    backfill_id: str,
    *,
    contributing_backfill_ids: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Run the P4 post-backfill audit without creating or publishing a snapshot."""

    readiness, run, backfill_ids = _combined_reconciliation_context(
        service,
        backfill_id,
        contributing_backfill_ids,
    )
    start_date = _as_date(run["start_date"])
    end_date = _as_date(run["end_date"])
    universe_id = str(run["universe_id"])
    checks: list[ReconciliationCheck] = []
    checks.extend(_universe_checks(service, run))
    checks.extend(_calendar_checks(service, start_date, end_date))
    checks.extend(_master_and_lifecycle_checks(service, universe_id, start_date, end_date))
    checks.extend(_daily_coverage_checks(service, universe_id, start_date, end_date))
    checks.extend(_daily_value_checks(service, universe_id, start_date, end_date))
    checks.extend(_file_integrity_checks(service))
    checks.extend(_source_version_checks(service))
    checks.extend(_blocked_capability_checks(service, run, universe_id, start_date, end_date))
    return _persist_report(
        service,
        run=run,
        readiness=readiness,
        contributing_backfill_ids=backfill_ids,
        checks=checks,
    )


def _combined_reconciliation_context(
    service: MarketFoundationService,
    backfill_id: str,
    contributing_backfill_ids: Iterable[str] | None,
) -> tuple[dict[str, Any], dict[str, Any], tuple[str, ...]]:
    requested = tuple(dict.fromkeys(contributing_backfill_ids or (backfill_id,)))
    if backfill_id not in requested:
        requested = (*requested, backfill_id)
    readiness_items = [
        inspect_reconciliation_readiness(service, item) for item in requested
    ]
    runs = [dict(item["run"]) for item in readiness_items]
    latest = max(runs, key=lambda item: (_as_date(item["end_date"]), item["backfill_id"]))
    if latest["backfill_id"] != backfill_id:
        raise MarketFoundationError(
            "MDFR003",
            "组合对账的主回填必须是截止日最新的运行",
            {
                "backfill_id": backfill_id,
                "latest_backfill_id": latest["backfill_id"],
            },
        )
    ordered = sorted(
        runs,
        key=lambda item: (_as_date(item["start_date"]), _as_date(item["end_date"])),
    )
    range_end = _as_date(ordered[0]["end_date"])
    for item in ordered[1:]:
        item_start = _as_date(item["start_date"])
        if item_start > range_end + timedelta(days=1):
            raise MarketFoundationError(
                "MDFR003",
                "组合回填日期范围不连续，禁止形成全范围质量报告",
                {
                    "previous_end_date": range_end.isoformat(),
                    "next_start_date": item_start.isoformat(),
                    "backfill_ids": list(requested),
                },
            )
        range_end = max(range_end, _as_date(item["end_date"]))
    blocked_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for item in runs:
        for blocked in json.loads(item["blocked_capabilities_json"]):
            key = (str(blocked["dataset_id"]), str(blocked["market_code"]))
            blocked_by_key[key] = dict(blocked)
    combined_run = {
        **latest,
        "start_date": min(_as_date(item["start_date"]) for item in runs),
        "end_date": max(_as_date(item["end_date"]) for item in runs),
        "blocked_capabilities_json": canonical_json(
            [blocked_by_key[key] for key in sorted(blocked_by_key)]
        ),
    }
    readiness = {
        "ready": True,
        "backfill_id": backfill_id,
        "backfill_ids": list(requested),
        "status": "COMBINED_TERMINAL" if len(requested) > 1 else readiness_items[0]["status"],
        "total_units": sum(item["total_units"] for item in readiness_items),
        "succeeded_units": sum(item["succeeded_units"] for item in readiness_items),
        "failed_units": sum(item["failed_units"] for item in readiness_items),
        "open_units": sum(item["open_units"] for item in readiness_items),
        "open_linked_tasks": sum(item["open_linked_tasks"] for item in readiness_items),
        "start_date": combined_run["start_date"].isoformat(),
        "end_date": combined_run["end_date"].isoformat(),
    }
    return readiness, combined_run, requested
