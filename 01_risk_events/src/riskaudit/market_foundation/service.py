"""
文件作用：编排市场底座的刷新计划、任务、断点恢复、质量门禁、快照候选与人工发布。
编辑记录：
【首次生成：2026-08-13，实现不访问 RQData 的本地 DuckDB + Parquet 确定性服务。】
【二次编辑：2026-08-14，区分本次刷新质量与全范围快照门禁，避免局部刷新被误标为数据失败。】
【三次编辑：2026-08-14，参数化来源执行模式与 Raw 证据，支持隔离的 P3 真实 RQData 样本任务。】
【四次编辑：2026-08-15，固定原子文本为精确 UTF-8 字节，避免 Windows 换行转换导致 Raw SHA-256 失真。】
【五次编辑：2026-08-15，发布前复验候选成员哈希、事实文件和 P4 全范围质量运行绑定。】
【六次编辑：2026-08-15，候选成员改为流式哈希和 DuckDB 集合写入，避免全量记录驻留 Python。】
"""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path
from threading import RLock
from typing import Any, Mapping

from .catalog import MarketCatalog
from .constants import (
    DATASETS,
    EXECUTION_MODE,
    FOUNDATION_END,
    FOUNDATION_START,
    MARKETS,
)
from .facts import ParquetFactStore, canonical_json
from .service_support import (
    MarketFoundationError,
    as_date as _as_date,
    as_datetime as _as_datetime,
    atomic_json as _atomic_json,
    json_row as _json_row,
    now as _now,
)
from .service_quality import QualityServiceMixin
from .service_publication import PublicationServiceMixin
from .service_planning import PlanningServiceMixin
from .service_tasks import TaskExecutionServiceMixin

__all__ = [
    "MarketFoundationError",
    "MarketFoundationService",
    "_atomic_json",
    "_now",
]


class MarketFoundationService(
    PlanningServiceMixin,
    TaskExecutionServiceMixin,
    PublicationServiceMixin,
    QualityServiceMixin,
):
    """Source-neutral catalog service; network clients remain outside this module."""

    def __init__(
        self,
        root: str | Path,
        *,
        execution_mode: str = EXECUTION_MODE,
        source_system: str = "SYNTHETIC_P2",
        allow_snapshot_candidates: bool = True,
        source_details: Mapping[str, Any] | None = None,
        read_only_catalog: bool = False,
        refresh_range_end: date = FOUNDATION_END,
    ) -> None:
        self.root = Path(root).resolve()
        self.execution_mode = str(execution_mode)
        self.source_system = str(source_system)
        self.allow_snapshot_candidates = bool(allow_snapshot_candidates)
        self.source_details = dict(source_details or {})
        self.read_only_catalog = bool(read_only_catalog)
        self.refresh_range_end = refresh_range_end
        if self.refresh_range_end < FOUNDATION_START:
            raise ValueError("市场数据刷新截止日不得早于底座开始日")
        if not self.read_only_catalog:
            self.root.mkdir(parents=True, exist_ok=True)
            for relative in (
                "raw",
                "parquet",
                "manifests/ingestion",
                "manifests/snapshots",
                "duckdb",
                "exports",
                "quality",
            ):
                (self.root / relative).mkdir(parents=True, exist_ok=True)
        self.catalog = MarketCatalog(
            self.root / "duckdb" / "market_catalog.duckdb",
            read_only=self.read_only_catalog,
        )
        self.facts = ParquetFactStore(
            self.root,
            self.catalog,
            create_directories=not self.read_only_catalog,
        )
        if not self.read_only_catalog:
            self.facts.refresh_views()
        self._lock = RLock()

    def close(self) -> None:
        self.catalog.close()

    def _foundation_bounds(self) -> tuple[date, date]:
        """Expose patchable service-module bounds to extracted collaborators."""
        return FOUNDATION_START, FOUNDATION_END

    def overview(self) -> dict[str, Any]:
        published = self.catalog.row(
            """
            SELECT * FROM market_snapshot WHERE status = 'PUBLISHED'
            ORDER BY published_at DESC LIMIT 1
            """
        )
        recent_task = self.catalog.row(
            "SELECT * FROM refresh_task ORDER BY created_at DESC LIMIT 1"
        )
        recent_quality = self.catalog.row(
            "SELECT * FROM data_quality_run ORDER BY created_at DESC LIMIT 1"
        )
        recent_universe = self.catalog.row(
            "SELECT * FROM market_universe_manifest ORDER BY discovered_at DESC LIMIT 1"
        )
        recent_backfill = self.catalog.row(
            "SELECT * FROM market_backfill_run ORDER BY created_at DESC LIMIT 1"
        )
        return {
            "status": "READY",
            "execution_mode": self.execution_mode,
            "source_system": self.source_system,
            "source_details": self.source_details,
            "snapshot_candidates_enabled": self.allow_snapshot_candidates,
            "root": str(self.root),
            "frozen_range": {
                "start_date": FOUNDATION_START.isoformat(),
                "end_date": self.refresh_range_end.isoformat(),
            },
            "markets": list(MARKETS),
            "datasets": list(DATASETS),
            "healthy_snapshot": _json_row(published),
            "recent_task": self._task_summary(recent_task) if recent_task else None,
            "recent_quality": _json_row(recent_quality),
            "universe_plan": self._universe_summary(recent_universe),
            "backfill_run": self._backfill_summary(recent_backfill),
            "coverage": self.coverage(),
        }

    @staticmethod
    def _backfill_summary(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        result = _json_row(row)
        result["config"] = json.loads(row["config_json"])
        result["blocked_capabilities"] = json.loads(
            row["blocked_capabilities_json"]
        )
        result.pop("config_json", None)
        result.pop("blocked_capabilities_json", None)
        return result

    def register_universe_plan(
        self,
        plan: Any,
        *,
        manifest_path: str | Path,
        members_path: str | Path,
    ) -> dict[str, Any]:
        """Register an immutable security universe and its unexecuted shards."""

        manifest = plan.as_manifest()
        universe_id = str(manifest["universe_id"])
        existing = self.catalog.row(
            "SELECT * FROM market_universe_manifest WHERE universe_id = ?",
            [universe_id],
        )
        if existing is not None:
            if existing["manifest_hash"] != manifest["manifest_hash"]:
                raise MarketFoundationError("MDF011", "证券清单 ID 与内容哈希冲突")
            return self._universe_summary(existing)
        scope = manifest["scope"]
        with self.catalog.transaction() as transaction:
            transaction.execute(
                """
                INSERT INTO market_universe_manifest VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    universe_id,
                    manifest["manifest_hash"],
                    manifest["source_system"],
                    _as_date(scope["start_date"]),
                    _as_date(scope["end_date"]),
                    manifest["source_row_count"],
                    manifest["eligible_security_count"],
                    canonical_json(manifest["counts_by_market"]),
                    manifest["delisted_count"],
                    manifest["shard_size"],
                    manifest["shard_count"],
                    str(Path(manifest_path).resolve()),
                    str(Path(members_path).resolve()),
                    _as_datetime(manifest["discovered_at"]),
                ],
            )
            for item in manifest["records"]:
                transaction.execute(
                    """
                    INSERT INTO market_universe_member VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        universe_id,
                        item["instrument_key"],
                        item["source_order_book_id"],
                        item["source_exchange"],
                        item["market_code"],
                        item["security_code"],
                        item["symbol"],
                        item["security_type"],
                        item["board_code"],
                        _as_date(item["listing_date"]),
                        _as_date(item["termination_date"])
                        if item["termination_date"]
                        else None,
                        item["source_status"],
                    ],
                )
            for shard in manifest["shards"]:
                transaction.execute(
                    """
                    INSERT INTO market_universe_shard VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        universe_id,
                        shard["shard_id"],
                        shard["market_code"],
                        shard["shard_ordinal"],
                        shard["security_count"],
                        shard["first_order_book_id"],
                        shard["last_order_book_id"],
                        canonical_json(shard["order_book_ids"]),
                        "PLANNED",
                    ],
                )
        registered = self.catalog.row(
            "SELECT * FROM market_universe_manifest WHERE universe_id = ?",
            [universe_id],
        )
        return self._universe_summary(registered)

    @staticmethod
    def _universe_summary(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "universe_id": row["universe_id"],
            "manifest_hash": row["manifest_hash"],
            "source_system": row["source_system"],
            "start_date": str(row["start_date"]),
            "end_date": str(row["end_date"]),
            "source_row_count": int(row["source_row_count"]),
            "eligible_security_count": int(row["eligible_security_count"]),
            "counts_by_market": json.loads(row["counts_by_market_json"]),
            "delisted_count": int(row["delisted_count"]),
            "shard_size": int(row["shard_size"]),
            "shard_count": int(row["shard_count"]),
            "manifest_path": row["manifest_path"],
            "members_path": row["members_path"],
            "discovered_at": _as_datetime(row["discovered_at"]).isoformat(),
            "status": "PLANNED",
        }
