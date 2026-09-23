"""
文件作用：为 P4 全市场 RQData 历史回填生成可恢复工作单元并持久化运行状态。
编辑记录：
【首次生成：2026-08-14，按证券分片和季度窗口规划沪深六类、北交所四类可用事实。】
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
from typing import Any, Iterable, Mapping

from .constants import FOUNDATION_END, FOUNDATION_START, MARKETS
from .facts import canonical_json


P4_DAILY_DATASETS = {
    "XSHG": (
        "daily_price_unadjusted",
        "daily_st_status",
        "daily_suspension_status",
        "daily_price_limits",
    ),
    "XSHE": (
        "daily_price_unadjusted",
        "daily_st_status",
        "daily_suspension_status",
        "daily_price_limits",
    ),
    "XBSE": (
        "daily_st_status",
        "daily_suspension_status",
    ),
}
P4_BLOCKED_CAPABILITIES = (
    {
        "market_code": "XBSE",
        "dataset_id": "daily_price_unadjusted",
        "reason": "RQData license has no BJSE day bar permission",
    },
    {
        "market_code": "XBSE",
        "dataset_id": "daily_price_limits",
        "reason": "RQData license has no BJSE day bar permission",
    },
)


def quarter_ranges(
    start_date: date = FOUNDATION_START, end_date: date = FOUNDATION_END
) -> tuple[tuple[date, date], ...]:
    ranges: list[tuple[date, date]] = []
    cursor = date(start_date.year, ((start_date.month - 1) // 3) * 3 + 1, 1)
    while cursor <= end_date:
        next_month = cursor.month + 3
        next_year = cursor.year
        if next_month > 12:
            next_month -= 12
            next_year += 1
        next_quarter = date(next_year, next_month, 1)
        ranges.append(
            (max(start_date, cursor), min(end_date, next_quarter - timedelta(days=1)))
        )
        cursor = next_quarter
    return tuple(ranges)


def build_backfill_units(
    universe_plan: Any,
    *,
    start_date: date = FOUNDATION_START,
    end_date: date = FOUNDATION_END,
) -> tuple[dict[str, Any], ...]:
    """Create deterministic P4 work units; latest daily windows execute first."""

    if start_date > end_date:
        raise ValueError("开始日期不得晚于结束日期")

    shards = tuple(dict(item) for item in universe_plan.shards)
    first_id_by_market = {
        market: next(
            item["order_book_ids"][0]
            for item in shards
            if item["market_code"] == market
        )
        for market in MARKETS
    }
    units: list[dict[str, Any]] = []
    units.append(
        _unit(
            "CALENDAR",
            None,
            "ALL",
            start_date,
            end_date,
            ("market_calendar",),
            tuple(first_id_by_market[market] for market in MARKETS),
            0,
        )
    )
    for shard in shards:
        units.append(
            _unit(
                "MASTER",
                shard["shard_id"],
                shard["market_code"],
                start_date,
                end_date,
                ("instrument_master_history",),
                tuple(shard["order_book_ids"]),
                10 + int(shard["shard_ordinal"]),
            )
        )
    windows = tuple(reversed(quarter_ranges(start_date, end_date)))
    for window_ordinal, (window_start, window_end) in enumerate(windows, 1):
        for market_rank, market in enumerate(MARKETS):
            for shard in shards:
                if shard["market_code"] != market:
                    continue
                units.append(
                    _unit(
                        "DAILY",
                        shard["shard_id"],
                        market,
                        window_start,
                        window_end,
                        P4_DAILY_DATASETS[market],
                        tuple(shard["order_book_ids"]),
                        1000 + window_ordinal * 100 + market_rank * 20 + int(shard["shard_ordinal"]),
                    )
                )
    return tuple(sorted(units, key=lambda item: (item["priority"], item["unit_id"])))


def create_or_get_backfill(
    service: Any,
    universe_plan: Any,
    *,
    start_date: date = FOUNDATION_START,
    end_date: date = FOUNDATION_END,
) -> dict[str, Any]:
    units = build_backfill_units(
        universe_plan,
        start_date=start_date,
        end_date=end_date,
    )
    config = {
        "schema_version": "rqdata_p4_backfill_v1",
        "universe_id": universe_plan.universe_id,
        "universe_manifest_hash": universe_plan.manifest_hash,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "windowing": "CALENDAR_AND_MASTER_ONCE_DAILY_BY_QUARTER_LATEST_FIRST",
        "max_security_shard_size": universe_plan.shard_size,
        "available_daily_datasets": {
            market: list(P4_DAILY_DATASETS[market]) for market in MARKETS
        },
    }
    digest = sha256(canonical_json(config).encode("utf-8")).hexdigest()
    backfill_id = f"mdbf_{digest[:32]}"
    existing = service.catalog.row(
        "SELECT * FROM market_backfill_run WHERE backfill_id=?", [backfill_id]
    )
    if existing is not None:
        return backfill_summary(service, backfill_id)
    now = _now()
    with service.catalog.transaction() as transaction:
        transaction.execute(
            """
            INSERT INTO market_backfill_run VALUES
            (?, ?, ?, ?, ?, 'PLANNED', ?, ?, 0, 0, ?, ?, ?, NULL, NULL, NULL)
            """,
            [
                backfill_id,
                universe_plan.universe_id,
                universe_plan.manifest_hash,
                start_date,
                end_date,
                canonical_json(config),
                len(units),
                canonical_json(P4_BLOCKED_CAPABILITIES),
                now,
                now,
            ],
        )
        for unit in units:
            transaction.execute(
                """
                INSERT INTO market_backfill_unit VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, NULL, 0, 0, 0, NULL, ?, ?, NULL)
                """,
                [
                    backfill_id,
                    unit["unit_id"],
                    unit["unit_kind"],
                    unit["universe_shard_id"],
                    unit["market_code"],
                    unit["start_date"],
                    unit["end_date"],
                    canonical_json(unit["datasets"]),
                    canonical_json(unit["order_book_ids"]),
                    now,
                    now,
                ],
            )
    return backfill_summary(service, backfill_id)


def pending_backfill_units(
    service: Any, backfill_id: str, *, max_attempts: int = 3
) -> list[dict[str, Any]]:
    rows = service.catalog.rows(
        """
        SELECT * FROM market_backfill_unit
        WHERE backfill_id=? AND status IN ('PENDING','FAILED') AND attempt_count < ?
        ORDER BY CASE unit_kind WHEN 'CALENDAR' THEN 0 WHEN 'MASTER' THEN 1 ELSE 2 END,
                 CASE WHEN unit_kind='DAILY' THEN end_date END DESC,
                 CASE market_code WHEN 'XSHG' THEN 0 WHEN 'XSHE' THEN 1 WHEN 'XBSE' THEN 2 ELSE 0 END,
                 universe_shard_id, unit_id
        """,
        [backfill_id, max_attempts],
    )
    return [_decode_unit(row) for row in rows]


def start_backfill(service: Any, backfill_id: str) -> None:
    now = _now()
    service.catalog.execute(
        """
        UPDATE market_backfill_run SET status='RUNNING', updated_at=?,
            started_at=coalesce(started_at, ?), completed_at=NULL, error_message=NULL
        WHERE backfill_id=?
        """,
        [now, now, backfill_id],
    )


def start_unit(service: Any, backfill_id: str, unit_id: str) -> None:
    service.catalog.execute(
        """
        UPDATE market_backfill_unit SET status='RUNNING', attempt_count=attempt_count+1,
            error_message=NULL, updated_at=?, completed_at=NULL
        WHERE backfill_id=? AND unit_id=?
        """,
        [_now(), backfill_id, unit_id],
    )


def bind_unit_task(service: Any, backfill_id: str, unit_id: str, task_id: str) -> None:
    service.catalog.execute(
        "UPDATE market_backfill_unit SET task_id=?, updated_at=? WHERE backfill_id=? AND unit_id=?",
        [task_id, _now(), backfill_id, unit_id],
    )


def finish_unit(
    service: Any,
    backfill_id: str,
    unit_id: str,
    task: Mapping[str, Any],
) -> None:
    chunks = task.get("chunks") or ()
    service.catalog.execute(
        """
        UPDATE market_backfill_unit SET status='SUCCEEDED', input_rows=?, written_rows=?,
            idempotent_rows=?, error_message=NULL, updated_at=?, completed_at=?
        WHERE backfill_id=? AND unit_id=?
        """,
        [
            sum(int(item.get("input_row_count") or 0) for item in chunks),
            sum(int(item.get("written_row_count") or 0) for item in chunks),
            sum(int(item.get("idempotent_row_count") or 0) for item in chunks),
            _now(),
            _now(),
            backfill_id,
            unit_id,
        ],
    )
    refresh_backfill_summary(service, backfill_id)


def fail_unit(service: Any, backfill_id: str, unit_id: str, error: str) -> None:
    service.catalog.execute(
        """
        UPDATE market_backfill_unit SET status='FAILED', error_message=?,
            updated_at=?, completed_at=? WHERE backfill_id=? AND unit_id=?
        """,
        [error[:1000], _now(), _now(), backfill_id, unit_id],
    )
    refresh_backfill_summary(service, backfill_id, error_message=error)


def refresh_backfill_summary(
    service: Any, backfill_id: str, *, error_message: str | None = None
) -> dict[str, Any]:
    counts = service.catalog.row(
        """
        SELECT count(*) AS total,
               count(*) FILTER (WHERE status='SUCCEEDED') AS completed,
               count(*) FILTER (WHERE status='FAILED') AS failed,
               count(*) FILTER (WHERE status IN ('PENDING','RUNNING')) AS open
        FROM market_backfill_unit WHERE backfill_id=?
        """,
        [backfill_id],
    )
    run = service.catalog.row(
        "SELECT started_at FROM market_backfill_run WHERE backfill_id=?",
        [backfill_id],
    )
    if int(counts["open"]) == 0:
        status = "PARTIAL_COMPLETE" if int(counts["failed"]) == 0 else "INCOMPLETE"
        completed_at = _now()
    elif (
        run is not None
        and run["started_at"] is None
        and int(counts["completed"]) == 0
        and int(counts["failed"]) == 0
    ):
        status = "PLANNED"
        completed_at = None
    else:
        status = "RUNNING"
        completed_at = None
    service.catalog.execute(
        """
        UPDATE market_backfill_run SET status=?, total_units=?, completed_units=?,
            failed_units=?, updated_at=?, completed_at=?, error_message=?
        WHERE backfill_id=?
        """,
        [
            status,
            int(counts["total"]),
            int(counts["completed"]),
            int(counts["failed"]),
            _now(),
            completed_at,
            error_message[:1000] if error_message else None,
            backfill_id,
        ],
    )
    return backfill_summary(service, backfill_id)


def backfill_summary(service: Any, backfill_id: str) -> dict[str, Any]:
    row = service.catalog.row(
        "SELECT * FROM market_backfill_run WHERE backfill_id=?", [backfill_id]
    )
    if row is None:
        raise ValueError(f"P4 回填运行不存在：{backfill_id}")
    result = dict(row)
    result["config"] = json.loads(result.pop("config_json"))
    result["blocked_capabilities"] = json.loads(
        result.pop("blocked_capabilities_json")
    )
    for key, value in tuple(result.items()):
        if isinstance(value, (date, datetime)):
            result[key] = value.isoformat()
    return result


def _unit(
    unit_kind: str,
    shard_id: str | None,
    market_code: str,
    start_date: date,
    end_date: date,
    datasets: Iterable[str],
    order_book_ids: tuple[str, ...],
    priority: int,
) -> dict[str, Any]:
    identity = {
        "unit_kind": unit_kind,
        "universe_shard_id": shard_id,
        "market_code": market_code,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "datasets": list(datasets),
        "order_book_ids": list(order_book_ids),
    }
    digest = sha256(canonical_json(identity).encode("utf-8")).hexdigest()
    return {
        **identity,
        "unit_id": f"mdbfu_{digest[:32]}",
        "start_date": start_date,
        "end_date": end_date,
        "priority": priority,
    }


def _decode_unit(row: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result["datasets"] = json.loads(result.pop("datasets_json"))
    result["order_book_ids"] = json.loads(result.pop("order_book_ids_json"))
    return result


def _now() -> datetime:
    return datetime.now(timezone.utc)
