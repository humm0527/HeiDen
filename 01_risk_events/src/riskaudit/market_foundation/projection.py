"""
文件作用：以严格只读方式把 P4 DuckDB、质量报告和状态清单投影为本地工作台 API 数据。
编辑记录：
【首次生成：2026-08-19，实现 P4 本地数据湖只读概览、覆盖、任务、质量和快照查询。】
【第二次编辑：2026-09-14，展示独立离线缓存范围，不把任务证券缓存标成旧P4全市场恢复。】
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import re
from typing import Any, Iterator, Mapping, Sequence

import duckdb

from .constants import DATASETS, MARKETS, RISK_MARKETS
from .service import MarketFoundationError


_SAFE_MARKET_ID = re.compile(r"^[A-Za-z0-9_]+$")

_RISK_WATERMARK_DATASETS = (
    "market_calendar",
    "daily_price_unadjusted",
    "daily_st_status",
    "daily_suspension_status",
    "daily_price_limits",
)


def _risk_common_data_as_of(coverage: Sequence[Mapping[str, Any]]) -> str | None:
    lookup = {
        (str(item.get("dataset_id")), str(item.get("market_code"))): item.get(
            "maximum_business_date"
        )
        for item in coverage
    }
    required = [
        lookup.get((dataset_id, market_code))
        for dataset_id in _RISK_WATERMARK_DATASETS
        for market_code in RISK_MARKETS
    ]
    if not required or any(value in {None, ""} for value in required):
        return None
    return min(str(value) for value in required)


class P4MarketProjection:
    """Read an existing P4 market lake without migrating or mutating it."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.database_path = self.root / "duckdb" / "market_catalog.duckdb"

    @property
    def available(self) -> bool:
        return self.database_path.is_file()

    @contextmanager
    def _connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        if not self.available:
            raise MarketFoundationError("MDF008", "P4 本地市场目录不可用")
        connection = duckdb.connect(str(self.database_path), read_only=True)
        try:
            yield connection
        finally:
            connection.close()

    def _rows(
        self, sql: str, parameters: Sequence[Any] | None = None
    ) -> list[dict[str, Any]]:
        with self._connection() as connection:
            cursor = connection.execute(sql, parameters or [])
            columns = [item[0] for item in cursor.description]
            return [
                dict(zip(columns, values, strict=True))
                for values in cursor.fetchall()
            ]

    def _row(
        self, sql: str, parameters: Sequence[Any] | None = None
    ) -> dict[str, Any] | None:
        rows = self._rows(sql, parameters)
        return rows[0] if rows else None

    def overview(self) -> dict[str, Any]:
        backfills = self.list_backfills()
        universe = self._latest_universe()
        coverage = self.coverage()
        recent_task = self._latest_task()
        recent_quality = self._latest_quality()
        snapshots = self.list_snapshots()
        latest_snapshot = snapshots[0] if snapshots else None
        published = next(
            (item for item in snapshots if item["status"] == "PUBLISHED"
             and item.get("risk_adapter_status") == "PASSED"), None
        ) or next(
            (item for item in snapshots if item["status"] == "PUBLISHED"), None
        )
        full_reconciliation = self._latest_full_reconciliation()
        cache_scope = (full_reconciliation or {}).get('scope', {})
        from_cache = cache_scope.get('scope_type') == 'OFFLINE_CACHE_RECONCILIATION'
        displayed_markets = list(RISK_MARKETS) if from_cache else list(MARKETS)
        data_as_of = _risk_common_data_as_of(coverage)
        safe_end_date = self._latest_completed_trading_date(coverage)
        blocked = self._blocked_capabilities(backfills)
        available_pairs = sum(
            item["coverage_state"] == "AVAILABLE" for item in coverage
        )
        risk_coverage = [item for item in coverage if item["market_code"] in RISK_MARKETS]
        risk_available_pairs = sum(
            item["coverage_state"] == "AVAILABLE" for item in risk_coverage
        )
        completed_units = sum(item["completed_units"] for item in backfills)
        total_units = sum(item["total_units"] for item in backfills)
        failed_units = sum(item["failed_units"] for item in backfills)
        task_quality_summary = self._task_quality_summary()
        risk_ready = bool(
            published and published.get("risk_adapter_status") == "PASSED"
        )
        risk_shadow = self._latest_risk_shadow_calculation()
        return {
            "status": "READY",
            "execution_mode": "RQDATA_LOCAL_P4",
            "source_system": "RQDATA_LOCAL",
            "source_details": {
                "scope": "OFFLINE_CACHE_RISK_UNIVERSE" if from_cache else "FULL_MARKET_P4_LOCAL_LAKE",
                "phase": "P4_READ_ONLY",
                "root_alias": "P4_LOCAL_MARKET_LAKE",
                "available_markets": displayed_markets,
                "bse_status": "NOT_INCLUDED_IN_CACHE_SCOPE" if from_cache else "RETAINED_NON_BLOCKING" if blocked else "AVAILABLE",
                "read_only": True,
            },
            "risk_scope": {
                "markets": list(RISK_MARKETS),
                "excluded_markets": [market for market in MARKETS if market not in RISK_MARKETS],
                "expected_pairs": len(DATASETS) * len(RISK_MARKETS),
                "available_pairs": risk_available_pairs,
                "blocking_pairs": len([
                    item for item in risk_coverage if item["coverage_state"] != "AVAILABLE"
                ]),
            },
            "refresh_policy": {
                "strategy": "LOCAL_FOUNDATION_FIRST_THEN_GAP_FILL",
                "full_refetch_required": False,
                "comparison_basis": "coverage_watermark_and_snapshot_manifest",
                "watermark_basis": "RISK_REQUIRED_DAILY_COMMON_DATE",
                "same_day_daily_refresh_allowed": False,
                "safe_end_date": safe_end_date,
                "has_available_range": bool(
                    data_as_of
                    and safe_end_date
                    and date.fromisoformat(data_as_of) < date.fromisoformat(safe_end_date)
                ),
            },
            "snapshot_candidates_enabled": False,
            "frozen_range": {
                "start_date": min(
                    (item["start_date"] for item in backfills), default=cache_scope.get('start_date') if from_cache else None
                ),
                "end_date": max(
                    (item["end_date"] for item in backfills), default=cache_scope.get('end_date') if from_cache else None
                ),
            },
            "data_as_of": data_as_of,
            "markets": displayed_markets,
            "datasets": list(DATASETS),
            "healthy_snapshot": published,
            "recent_task": recent_task,
            "recent_quality": recent_quality,
            "universe_plan": universe,
            "backfill_run": backfills[-1] if backfills else None,
            "backfill_runs": backfills,
            "p4_backfill": backfills[-1] if backfills else None,
            "coverage": coverage,
            "ingestion": {
                "status": "SUCCEEDED" if from_cache else self._aggregate_ingestion_status(backfills),
                "completed_units": completed_units,
                "total_units": total_units,
                "failed_units": failed_units,
                "run_count": len(backfills),
                "updated_at": max(
                    (item["updated_at"] for item in backfills), default=None
                ),
            },
            "coverage_summary": {
                "expected_pairs": len(DATASETS) * len(displayed_markets),
                "available_pairs": available_pairs,
                "blocked_pairs": len(blocked),
                "reconciled_pairs": (
                    len(DATASETS) * len(displayed_markets)
                    if full_reconciliation
                    and full_reconciliation.get("status") == "PASS"
                    else 0
                ),
            },
            "blocked_capabilities": blocked,
            "non_blocking_capabilities": blocked,
            "task_quality_summary": task_quality_summary,
            "full_reconciliation": full_reconciliation,
            "risk_shadow_calculation": risk_shadow,
            "release": {
                "snapshot_status": (
                    published["status"]
                    if published
                    else latest_snapshot["status"]
                    if latest_snapshot
                    else "NOT_CREATED"
                ),
                "risk_adapter_status": (
                    published["risk_adapter_status"]
                    if published
                    else latest_snapshot["risk_adapter_status"]
                    if latest_snapshot
                    else "NOT_RUN"
                ),
                "risk_ready": risk_ready,
                "risk_shadow_status": (
                    risk_shadow.get("status") if risk_shadow else "NOT_RUN"
                ),
                "risk_shadow_result_match": bool(
                    risk_shadow
                    and risk_shadow.get("status") == "SUCCEEDED_SHADOW_MATCH"
                    and risk_shadow.get("result_reconciliation", {}).get("status")
                    == "PASS"
                ),
                "risk_entry_switch_attempted": bool(
                    risk_shadow and risk_shadow.get("risk_entry_switch_attempted")
                ),
            },
        }

    def _latest_completed_trading_date(
        self, coverage: Sequence[Mapping[str, Any]]
    ) -> str | None:
        """Return the latest shared SH/SZ trading day strictly before today."""
        try:
            row = self._row(
                """
                SELECT max(business_date) AS safe_end_date
                FROM (
                    SELECT business_date
                    FROM current_market_calendar
                    WHERE market_code IN ('XSHG', 'XSHE')
                      AND is_trading_day
                      AND business_date < ?
                    GROUP BY business_date
                    HAVING count(DISTINCT market_code)=2
                )
                """,
                [date.today()],
            )
            if row and row.get("safe_end_date"):
                return str(row["safe_end_date"])
        except duckdb.Error:
            pass
        calendar_dates = [
            date.fromisoformat(str(item["maximum_business_date"]))
            for item in coverage
            if item.get("dataset_id") == "market_calendar"
            and item.get("market_code") in RISK_MARKETS
            and item.get("maximum_business_date")
        ]
        if len(calendar_dates) != len(RISK_MARKETS):
            return None
        return min(min(calendar_dates), date.today() - timedelta(days=1)).isoformat()

    def _latest_risk_shadow_calculation(self) -> dict[str, Any] | None:
        exports = self.root / "exports"
        if not exports.is_dir():
            return None
        candidates = sorted(
            exports.glob("ms_*/risk_calculation_shadow/*/run_summary.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for path in candidates:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            return {
                "status": payload.get("status"),
                "batch_id": payload.get("batch_id"),
                "market_snapshot_id": payload.get("market_snapshot_id"),
                "observation_start": payload.get("observation_start"),
                "observation_end": payload.get("observation_end"),
                "event_count": payload.get("result_manifest", {}).get("event_count"),
                "segment_count": payload.get("result_manifest", {}).get("segment_count"),
                "result_reconciliation": payload.get("result_reconciliation"),
                "risk_entry_switch_attempted": payload.get(
                    "risk_entry_switch_attempted", False
                ),
                "completed_at": payload.get("completed_at"),
                "run_summary_path": str(path),
            }
        return None

    def coverage(self) -> list[dict[str, Any]]:
        rows = self._rows(
            """
            SELECT
                watermark.dataset_id,
                watermark.market_code,
                watermark.minimum_business_date,
                watermark.maximum_business_date,
                watermark.distinct_business_dates,
                watermark.latest_task_id,
                watermark.updated_at,
                coalesce(partitions.partition_file_count, 0) AS partition_file_count,
                coalesce(partitions.catalog_row_count, 0) AS catalog_row_count
            FROM coverage_watermark AS watermark
            LEFT JOIN (
                SELECT
                    dataset_id,
                    market_code,
                    count(*) AS partition_file_count,
                    sum(row_count) AS catalog_row_count
                FROM partition_catalog
                GROUP BY dataset_id, market_code
            ) AS partitions
              ON partitions.dataset_id = watermark.dataset_id
             AND partitions.market_code = watermark.market_code
            ORDER BY watermark.dataset_id, watermark.market_code
            """
        )
        by_key = {
            (item["dataset_id"], item["market_code"]): _json_row(item)
            for item in rows
        }
        blocked = {
            (item["dataset_id"], item["market_code"]): item
            for item in self._blocked_capabilities(self.list_backfills())
        }
        reconciliation = self._latest_full_reconciliation()
        reconciliation_passed = bool(
            reconciliation and reconciliation.get("status") == "PASS"
        )
        result: list[dict[str, Any]] = []
        for dataset_id in DATASETS:
            for market_code in MARKETS:
                item = by_key.get((dataset_id, market_code), {})
                blocker = blocked.get((dataset_id, market_code))
                state = (
                    "AVAILABLE"
                    if item.get("minimum_business_date")
                    else "OPTIONAL_NOT_APPLICABLE"
                    if blocker
                    else "MISSING"
                )
                result.append(
                    {
                        "dataset_id": dataset_id,
                        "market_code": market_code,
                        "minimum_business_date": item.get("minimum_business_date"),
                        "maximum_business_date": item.get("maximum_business_date"),
                        "distinct_business_dates": int(
                            item.get("distinct_business_dates") or 0
                        ),
                        "latest_task_id": item.get("latest_task_id"),
                        "updated_at": item.get("updated_at"),
                        "partition_file_count": int(
                            item.get("partition_file_count") or 0
                        ),
                        "catalog_row_count": int(item.get("catalog_row_count") or 0),
                        "coverage_state": state,
                        "blocked_reason": blocker.get("reason") if blocker else None,
                        "blocking_for_risk": market_code in RISK_MARKETS,
                        "complete_range": reconciliation_passed and state == "AVAILABLE",
                    }
                )
        return result

    def list_backfills(self) -> list[dict[str, Any]]:
        rows = self._rows(
            "SELECT * FROM market_backfill_run ORDER BY created_at, backfill_id"
        )
        result: list[dict[str, Any]] = []
        for row in rows:
            item = _json_row(row)
            item["config"] = _json_load(row.get("config_json"), {})
            item["blocked_capabilities"] = _json_load(
                row.get("blocked_capabilities_json"), []
            )
            item.pop("config_json", None)
            item.pop("blocked_capabilities_json", None)
            result.append(item)
        return result

    def list_tasks(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self._rows(
            """
            SELECT
                task.*,
                coalesce(chunks.chunk_count, 0) AS chunk_count,
                coalesce(chunks.succeeded_count, 0) AS succeeded_count,
                coalesce(chunks.failed_count, 0) AS failed_count,
                coalesce(chunks.pending_count, 0) AS pending_count,
                coalesce(chunks.running_count, 0) AS running_count,
                coalesce(chunks.idempotent_count, 0) AS idempotent_count
            FROM refresh_task AS task
            LEFT JOIN (
                SELECT
                    task_id,
                    count(*) AS chunk_count,
                    count(*) FILTER (WHERE status = 'SUCCEEDED') AS succeeded_count,
                    count(*) FILTER (WHERE status = 'FAILED') AS failed_count,
                    count(*) FILTER (WHERE status = 'PENDING') AS pending_count,
                    count(*) FILTER (WHERE status = 'RUNNING') AS running_count,
                    count(*) FILTER (WHERE status = 'SKIPPED_IDEMPOTENT') AS idempotent_count
                FROM refresh_chunk
                GROUP BY task_id
            ) AS chunks ON chunks.task_id = task.task_id
            ORDER BY task.created_at DESC
            LIMIT ?
            """,
            [limit],
        )
        return [self._task_summary(row) for row in rows]

    def has_task(self, task_id: str) -> bool:
        return bool(
            self._row("SELECT task_id FROM refresh_task WHERE task_id = ?", [task_id])
        )

    def get_task(self, task_id: str) -> dict[str, Any]:
        if not _SAFE_MARKET_ID.fullmatch(task_id):
            raise MarketFoundationError("MDF009", "市场刷新任务不存在")
        rows = self._rows(
            """
            SELECT
                task.*,
                coalesce(chunks.chunk_count, 0) AS chunk_count,
                coalesce(chunks.succeeded_count, 0) AS succeeded_count,
                coalesce(chunks.failed_count, 0) AS failed_count,
                coalesce(chunks.pending_count, 0) AS pending_count,
                coalesce(chunks.running_count, 0) AS running_count,
                coalesce(chunks.idempotent_count, 0) AS idempotent_count
            FROM refresh_task AS task
            LEFT JOIN (
                SELECT
                    task_id,
                    count(*) AS chunk_count,
                    count(*) FILTER (WHERE status = 'SUCCEEDED') AS succeeded_count,
                    count(*) FILTER (WHERE status = 'FAILED') AS failed_count,
                    count(*) FILTER (WHERE status = 'PENDING') AS pending_count,
                    count(*) FILTER (WHERE status = 'RUNNING') AS running_count,
                    count(*) FILTER (WHERE status = 'SKIPPED_IDEMPOTENT') AS idempotent_count
                FROM refresh_chunk
                GROUP BY task_id
            ) AS chunks ON chunks.task_id = task.task_id
            WHERE task.task_id = ?
            """,
            [task_id],
        )
        if not rows:
            raise MarketFoundationError("MDF009", "市场刷新任务不存在")
        summary = self._task_summary(rows[0])
        summary["chunks"] = [
            _json_row(row)
            for row in self._rows(
                """
                SELECT * FROM refresh_chunk
                WHERE task_id = ?
                ORDER BY dataset_id, market_code, start_date, chunk_id
                """,
                [task_id],
            )
        ]
        quality = self._row(
            """
            SELECT quality_run_id FROM data_quality_run
            WHERE task_id = ? ORDER BY created_at DESC LIMIT 1
            """,
            [task_id],
        )
        summary["quality"] = (
            self.get_quality_run(quality["quality_run_id"]) if quality else None
        )
        summary["candidate"] = None
        summary["raw_assets"] = []
        return summary

    def has_quality_run(self, quality_run_id: str) -> bool:
        return bool(
            self._row(
                "SELECT quality_run_id FROM data_quality_run WHERE quality_run_id = ?",
                [quality_run_id],
            )
        )

    def get_quality_run(self, quality_run_id: str) -> dict[str, Any]:
        if not _SAFE_MARKET_ID.fullmatch(quality_run_id):
            raise MarketFoundationError("MDF010", "质量运行不存在")
        run = self._row(
            "SELECT * FROM data_quality_run WHERE quality_run_id = ?",
            [quality_run_id],
        )
        if run is None:
            raise MarketFoundationError("MDF010", "质量运行不存在")
        findings = self._rows(
            """
            SELECT * FROM data_quality_result
            WHERE quality_run_id = ? ORDER BY finding_id
            """,
            [quality_run_id],
        )
        item = _json_row(run)
        item["scope"] = _json_load(run.get("scope_json"), {})
        item["findings"] = [_json_row(row) for row in findings]
        return item

    def quality_report_path(self, quality_run_id: str, format_name: str) -> Path:
        if not _SAFE_MARKET_ID.fullmatch(quality_run_id):
            raise MarketFoundationError("MDF010", "质量运行不存在")
        if format_name not in {"json", "csv"}:
            raise MarketFoundationError("MDF010", "质量报告格式不支持")
        path = self.root / "quality" / quality_run_id / f"quality_report.{format_name}"
        if not path.is_file():
            raise MarketFoundationError("MDF010", "质量报告不存在")
        return path

    def list_snapshots(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self._rows(
            "SELECT * FROM market_snapshot ORDER BY created_at DESC LIMIT ?", [limit]
        )
        return [_json_row(row) for row in rows]

    def published_trading_dates(self) -> list[str]:
        snapshot = self._row(
            """
            SELECT snapshot_id FROM market_snapshot
            WHERE status='PUBLISHED' AND risk_adapter_status='PASSED'
            ORDER BY published_at DESC LIMIT 1
            """
        )
        if snapshot is None:
            return []
        rows = self._rows(
            """
            SELECT DISTINCT json_extract_string(source.payload_json, '$.business_date') AS day
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
              AND CAST(json_extract_string(source.payload_json, '$.is_trading_day') AS BOOLEAN)
            ORDER BY day
            """,
            [snapshot["snapshot_id"]],
        )
        return [str(row["day"]) for row in rows]

    def has_snapshot(self, snapshot_id: str) -> bool:
        return bool(
            self._row(
                "SELECT snapshot_id FROM market_snapshot WHERE snapshot_id = ?",
                [snapshot_id],
            )
        )

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        if not _SAFE_MARKET_ID.fullmatch(snapshot_id):
            raise MarketFoundationError("MDF010", "市场快照或候选不存在")
        snapshot = self._row(
            "SELECT * FROM market_snapshot WHERE snapshot_id = ?", [snapshot_id]
        )
        if snapshot is None:
            raise MarketFoundationError("MDF010", "市场快照或候选不存在")
        members = self._rows(
            """
            SELECT * FROM market_snapshot_member
            WHERE snapshot_id = ? ORDER BY dataset_id, market_code, partition_id
            """,
            [snapshot_id],
        )
        return {**_json_row(snapshot), "members": [_json_row(row) for row in members]}

    def snapshot_manifest_path(self, snapshot_id: str) -> Path:
        if not _SAFE_MARKET_ID.fullmatch(snapshot_id):
            raise MarketFoundationError("MDF010", "市场快照清单不存在")
        path = self.root / "manifests" / "snapshots" / f"{snapshot_id}.json"
        if not path.is_file():
            raise MarketFoundationError("MDF010", "市场快照清单不存在")
        return path

    def _latest_universe(self) -> dict[str, Any] | None:
        row = self._row(
            """
            SELECT * FROM market_universe_manifest
            ORDER BY end_date DESC, discovered_at DESC LIMIT 1
            """
        )
        if row is None:
            return None
        item = _json_row(row)
        item["counts_by_market"] = _json_load(row.get("counts_by_market_json"), {})
        item.pop("counts_by_market_json", None)
        item["status"] = "REGISTERED"
        return item

    def _latest_task(self) -> dict[str, Any] | None:
        tasks = self.list_tasks(limit=1)
        return tasks[0] if tasks else None

    def _latest_quality(self) -> dict[str, Any] | None:
        row = self._row(
            "SELECT * FROM data_quality_run ORDER BY created_at DESC LIMIT 1"
        )
        if row is None:
            return None
        item = _json_row(row)
        item["scope"] = _json_load(row.get("scope_json"), {})
        return item

    def _latest_full_reconciliation(self) -> dict[str, Any] | None:
        rows = self._rows(
            "SELECT * FROM data_quality_run ORDER BY created_at DESC"
        )
        for row in rows:
            scope = _json_load(row.get("scope_json"), {})
            if scope.get("scope_type") in {
                "FOUNDATION",
                "FOUNDATION_RECONCILIATION",
                "P4_FULL_RECONCILIATION",
                "OFFLINE_CACHE_RECONCILIATION",
            }:
                item = _json_row(row)
                item["scope"] = scope
                return item
        return None

    def _task_quality_summary(self) -> dict[str, Any]:
        row = self._row(
            """
            SELECT
                count(*) AS run_count,
                sum(severe_count) AS severe_count,
                sum(warning_count) AS warning_count,
                sum(info_count) AS info_count,
                max(completed_at) AS completed_at
            FROM data_quality_run
            """
        ) or {}
        return {
            "run_count": int(row.get("run_count") or 0),
            "severe_count": int(row.get("severe_count") or 0),
            "warning_count": int(row.get("warning_count") or 0),
            "info_count": int(row.get("info_count") or 0),
            "completed_at": _json_value(row.get("completed_at")),
        }

    @staticmethod
    def _blocked_capabilities(
        backfills: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for backfill in backfills:
            for item in backfill.get("blocked_capabilities", []):
                key = (str(item.get("dataset_id")), str(item.get("market_code")))
                by_key[key] = {
                    "dataset_id": key[0],
                    "market_code": key[1],
                    "reason": str(item.get("reason") or "来源能力不可用"),
                }
        return [by_key[key] for key in sorted(by_key)]

    @staticmethod
    def _aggregate_ingestion_status(
        backfills: Sequence[Mapping[str, Any]],
    ) -> str:
        if not backfills:
            return "NOT_STARTED"
        if any(item["failed_units"] for item in backfills):
            return "FAILED"
        if any(item["completed_units"] < item["total_units"] for item in backfills):
            return "RUNNING"
        if any(item.get("blocked_capabilities") for item in backfills):
            return "PARTIAL_COMPLETE"
        return "COMPLETE"

    @staticmethod
    def _task_summary(row: Mapping[str, Any]) -> dict[str, Any]:
        item = _json_row(row)
        item["request"] = _json_load(row.get("request_json"), {})
        item["chunk_status_counts"] = {
            "SUCCEEDED": int(row.get("succeeded_count") or 0),
            "FAILED": int(row.get("failed_count") or 0),
            "PENDING": int(row.get("pending_count") or 0),
            "RUNNING": int(row.get("running_count") or 0),
            "SKIPPED_IDEMPOTENT": int(row.get("idempotent_count") or 0),
        }
        item["chunk_count"] = int(row.get("chunk_count") or 0)
        for key in (
            "request_json",
            "succeeded_count",
            "failed_count",
            "pending_count",
            "running_count",
            "idempotent_count",
        ):
            item.pop(key, None)
        return item


def _json_load(value: Any, default: Any) -> Any:
    if value in {None, ""}:
        return default
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _json_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {key: _json_value(value) for key, value in row.items()}
