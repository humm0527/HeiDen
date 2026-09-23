"""Snapshot candidate creation, integrity checks, and immutable publication."""

from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

from .constants import DATASETS, MARKETS, RECONCILIATION_RULE_VERSION
from .facts import canonical_json
from .service_support import (
    MarketFoundationError,
    as_date,
    atomic_json,
    json_row,
    now,
    stream_member_hash,
)


class PublicationServiceMixin:
    """Candidate and published-snapshot behavior for the concrete service."""

    def list_snapshots(self, limit: int = 50) -> list[dict[str, Any]]:
        return [
            json_row(row)
            for row in self.catalog.rows(
                "SELECT * FROM market_snapshot ORDER BY created_at DESC LIMIT ?",
                [limit],
            )
        ]

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        snapshot = self.catalog.row(
            "SELECT * FROM market_snapshot WHERE snapshot_id=?", [snapshot_id]
        )
        if snapshot is None:
            raise MarketFoundationError("MDF010", "市场快照或候选不存在")
        members = self.catalog.rows(
            "SELECT * FROM market_snapshot_member WHERE snapshot_id=? "
            "ORDER BY dataset_id, market_code, partition_id",
            [snapshot_id],
        )
        return {**json_row(snapshot), "members": [json_row(row) for row in members]}

    def publish_candidate(
        self,
        candidate_id: str,
        *,
        candidate_hash: str,
        quality_run_id: str,
        request_id: str,
        confirmed_immutable: bool,
        note: str = "",
        published_by: str = "LOCAL_USER",
    ) -> dict[str, Any]:
        candidate = self.catalog.row(
            "SELECT * FROM market_snapshot WHERE snapshot_id=?", [candidate_id]
        )
        if not confirmed_immutable:
            raise MarketFoundationError("MDF010", "必须确认快照发布后不可修改")
        if candidate is None or candidate["status"] != "READY_TO_PUBLISH":
            raise MarketFoundationError("MDF010", "候选不完整或不能发布")
        if candidate["member_hash"] != candidate_hash:
            raise MarketFoundationError("MDF011", "候选成员哈希不一致")
        if candidate["quality_run_id"] != quality_run_id:
            raise MarketFoundationError("MDF011", "质量运行与候选不一致")
        previous = self.catalog.row(
            "SELECT snapshot_id, candidate_id, candidate_hash "
            "FROM snapshot_publish_log WHERE request_id=?",
            [request_id],
        )
        if previous:
            if (
                previous["candidate_id"] != candidate_id
                or previous["candidate_hash"] != candidate_hash
            ):
                raise MarketFoundationError("MDF012", "发布 request_id 幂等冲突")
            return self.get_snapshot(previous["snapshot_id"])
        already_published = self.catalog.row(
            """
            SELECT snapshot_id FROM market_snapshot
            WHERE candidate_id=? AND status='PUBLISHED'
            ORDER BY published_at LIMIT 1
            """,
            [candidate_id],
        )
        if already_published:
            raise MarketFoundationError(
                "MDF012",
                "该候选已经发布",
                {"snapshot_id": already_published["snapshot_id"]},
            )
        quality = self.catalog.row(
            "SELECT * FROM data_quality_run WHERE quality_run_id=?", [quality_run_id]
        )
        if quality is None or int(quality["severe_count"]) != 0:
            raise MarketFoundationError("MDF010", "严重质量错误未清零")
        if quality["candidate_id"] != candidate_id:
            raise MarketFoundationError("MDF011", "质量运行未绑定当前候选")
        if candidate["execution_mode"] == "RQDATA_REAL_P4" and (
            quality["rule_version"] != RECONCILIATION_RULE_VERSION
            or quality["status"] != "PASS"
        ):
            raise MarketFoundationError("MDF010", "P4 候选未通过指定版本的全范围质量对账")
        if candidate["execution_mode"] == "RQDATA_OFFLINE_CACHE_P4":
            from .cache_publication import verify_cached_publication_evidence
            verify_cached_publication_evidence(self, quality)
        if candidate["execution_mode"] == "RQDATA_MANUAL_INCREMENT":
            from .manual_increment import verify_manual_evidence
            verify_manual_evidence(self, quality)
        self._verify_candidate_integrity(candidate_id, candidate_hash)
        snapshot_id = f"ms_{uuid4().hex}"
        published_at = now()
        with self.catalog.transaction() as transaction:
            transaction.execute(
                """
                INSERT INTO market_snapshot VALUES
                (?, ?, 'PUBLISHED', 'NOT_RUN', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    snapshot_id,
                    candidate_id,
                    candidate["execution_mode"],
                    candidate["start_date"],
                    candidate["end_date"],
                    candidate["markets_json"],
                    candidate["datasets_json"],
                    quality_run_id,
                    candidate_hash,
                    note,
                    published_at,
                    published_at,
                    published_by,
                ],
            )
            transaction.execute(
                """
                INSERT INTO market_snapshot_member
                SELECT ?, partition_id, file_path, file_sha256, dataset_id, market_code
                FROM market_snapshot_member WHERE snapshot_id=?
                """,
                [snapshot_id, candidate_id],
            )
            transaction.execute(
                """
                INSERT INTO market_snapshot_record
                SELECT ?, dataset_id, business_key, source_record_version,
                       payload_hash, partition_id
                FROM market_snapshot_record WHERE snapshot_id=?
                """,
                [snapshot_id, candidate_id],
            )
            transaction.execute(
                "INSERT INTO snapshot_publish_log VALUES "
                "(?, ?, ?, 'PUBLISH', ?, ?, ?, ?, ?)",
                [
                    f"mdpub_{uuid4().hex}",
                    candidate_id,
                    snapshot_id,
                    request_id,
                    candidate_hash,
                    quality_run_id,
                    note,
                    published_at,
                ],
            )
        result = self.get_snapshot(snapshot_id)
        self._write_snapshot_manifest(result)
        return result

    def _verify_candidate_integrity(
        self, candidate_id: str, expected_member_hash: str
    ) -> None:
        members = self.catalog.rows(
            """
            SELECT partition_id, file_path, file_sha256
            FROM market_snapshot_member
            WHERE snapshot_id=? ORDER BY dataset_id, market_code, partition_id
            """,
            [candidate_id],
        )
        # Dataset order is the leading canonical sort key. Separate queries keep
        # a full-history ORDER BY from competing with joins for memory.
        datasets = self.catalog.rows(
            "SELECT DISTINCT dataset_id FROM market_snapshot_record "
            "WHERE snapshot_id=? ORDER BY dataset_id", [candidate_id]
        )
        records = (
            record
            for dataset in datasets
            for record in self.catalog.iter_rows(
                "SELECT dataset_id, business_key, source_record_version, payload_hash, partition_id "
                "FROM market_snapshot_record WHERE snapshot_id=? AND dataset_id=? "
                "ORDER BY business_key", [candidate_id, dataset['dataset_id']]
            )
        )
        for member in members:
            path = Path(member["file_path"])
            actual = sha256(path.read_bytes()).hexdigest() if path.is_file() else None
            if actual != member["file_sha256"]:
                raise MarketFoundationError(
                    "MDF011",
                    "候选事实文件缺失或 SHA-256 已变化",
                    {"partition_id": member["partition_id"]},
                )
        actual_member_hash, item_count = stream_member_hash(
            (
                {
                    "partition_id": item["partition_id"],
                    "file_sha256": item["file_sha256"],
                }
                for item in members
            ),
            records,
        )
        if not members or item_count == len(members):
            raise MarketFoundationError("MDF010", "候选没有可发布的不可变成员")
        if actual_member_hash != expected_member_hash:
            raise MarketFoundationError("MDF011", "候选成员清单哈希复验失败")

    def snapshot_manifest_path(self, snapshot_id: str) -> Path:
        path = self.root / "manifests" / "snapshots" / f"{snapshot_id}.json"
        if not path.is_file():
            snapshot = self.get_snapshot(snapshot_id)
            if snapshot["status"] != "PUBLISHED":
                raise MarketFoundationError("MDF010", "候选尚无已发布快照清单")
            self._write_snapshot_manifest(snapshot)
        return path

    def _create_candidate(self, task_id: str, quality_run_id: str) -> dict[str, Any]:
        quality = self.catalog.row(
            "SELECT * FROM data_quality_run WHERE quality_run_id=?", [quality_run_id]
        )
        quality_scope = json.loads(quality["scope_json"])
        foundation_start, foundation_end = self._foundation_bounds()
        candidate_start = (
            as_date(quality_scope["start_date"])
            if self.execution_mode in {"RQDATA_REAL_P4", "RQDATA_OFFLINE_CACHE_P4", "RQDATA_MANUAL_INCREMENT"}
            else foundation_start
        )
        candidate_end = (
            as_date(quality_scope["end_date"])
            if self.execution_mode in {"RQDATA_REAL_P4", "RQDATA_OFFLINE_CACHE_P4", "RQDATA_MANUAL_INCREMENT"}
            else foundation_end
        )
        status = (
            "READY_TO_PUBLISH"
            if int(quality["severe_count"]) == 0
            else "INCOMPLETE"
        )
        candidate_id = f"mdcand_{uuid4().hex}"
        latest_records_sql = """
            SELECT dataset_id, business_key, source_record_version, payload_hash, partition_id
            FROM source_record_catalog
            QUALIFY row_number() OVER (
                PARTITION BY dataset_id, business_key ORDER BY source_record_version DESC
            ) = 1
        """
        latest_queries = [latest_records_sql]
        if self.execution_mode == "RQDATA_MANUAL_INCREMENT":
            from .manual_increment import manual_selection_sql
            latest_queries = [manual_selection_sql(quality_scope, dataset) for dataset in sorted(DATASETS)]
        partition_queries = [f"""
            SELECT partition_id, file_sha256
            FROM partition_catalog
            WHERE partition_id IN (
                SELECT DISTINCT partition_id FROM ({query}) AS latest
                WHERE partition_id IS NOT NULL
            )
            ORDER BY dataset_id, market_code, partition_id
        """ for query in latest_queries]
        record_queries = [f"""
            SELECT dataset_id, business_key, source_record_version, payload_hash, partition_id
            FROM ({query}) AS latest ORDER BY dataset_id, business_key
        """ for query in latest_queries]
        member_hash, _ = stream_member_hash(
            (row for query in partition_queries for row in self.catalog.iter_rows(query)),
            (row for query in record_queries for row in self.catalog.iter_rows(query)),
        )
        created_at = now()
        with self.catalog.transaction() as transaction:
            transaction.execute(
                """
                INSERT INTO market_snapshot VALUES
                (?, ?, ?, 'NOT_RUN', ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
                """,
                [
                    candidate_id,
                    task_id,
                    status,
                    self.execution_mode,
                    candidate_start,
                    candidate_end,
                    canonical_json(quality_scope['markets'] if self.execution_mode in {'RQDATA_OFFLINE_CACHE_P4', 'RQDATA_MANUAL_INCREMENT'} else list(MARKETS)),
                    canonical_json(list(DATASETS)),
                    quality_run_id,
                    member_hash,
                    f"{self.execution_mode} 市场快照候选",
                    created_at,
                ],
            )
            transaction.execute(
                "UPDATE data_quality_run SET candidate_id=? WHERE quality_run_id=?",
                [candidate_id, quality_run_id],
            )
            for query in latest_queries:
                transaction.execute(
                    f"""
                    INSERT INTO market_snapshot_member
                    SELECT ?, partition_id, file_path, file_sha256, dataset_id, market_code
                    FROM partition_catalog
                    WHERE partition_id IN (
                        SELECT DISTINCT partition_id FROM ({query}) AS latest
                        WHERE partition_id IS NOT NULL
                    )
                    """,
                    [candidate_id],
                )
                transaction.execute(
                    f"""
                    INSERT INTO market_snapshot_record
                    SELECT ?, dataset_id, business_key, source_record_version,
                           payload_hash, partition_id
                    FROM ({query}) AS latest
                    """,
                    [candidate_id],
                )
        return self.get_snapshot(candidate_id)

    def _write_snapshot_manifest(self, snapshot: Mapping[str, Any]) -> None:
        path = self.root / "manifests" / "snapshots" / f"{snapshot['snapshot_id']}.json"
        atomic_json(path, snapshot)
