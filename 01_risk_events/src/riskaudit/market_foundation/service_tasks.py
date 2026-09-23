"""Refresh task execution, recovery, checkpoints, and manifests."""

from __future__ import annotations

from hashlib import sha256
import json
from typing import Any, Iterable, Mapping
from uuid import uuid4

from .constants import DATASET_PRESET, DATASETS, MARKETS
from .facts import canonical_json
from .service_support import (
    MarketFoundationError,
    as_date as _as_date,
    atomic_json as _atomic_json,
    atomic_text as _atomic_text,
    json_row as _json_row,
    json_value as _json_value,
    now as _now,
    record_matches_chunk as _record_matches_chunk,
)


class TaskExecutionServiceMixin:
    """Execute and recover refresh tasks without owning service construction."""

    def execute_task(
        self,
        task_id: str,
        records_by_dataset: Mapping[str, Iterable[Mapping[str, Any]]],
        *,
        raw_records_by_dataset: Mapping[str, Iterable[Mapping[str, Any]]] | None = None,
        source_system: str | None = None,
        fail_chunks: Iterable[str] = (),
    ) -> dict[str, Any]:
        """Execute a task using caller-supplied normalized facts and Raw evidence."""
        with self._lock:
            task = self._require_task(task_id)
            if task["status"] not in {"QUEUED", "FAILED", "INCOMPLETE"}:
                raise MarketFoundationError("MDF009", "任务当前状态不可执行或恢复")
            fail_set = set(fail_chunks)
            now = _now()
            self.catalog.execute(
                """
                UPDATE refresh_task SET status='RUNNING', task_version=task_version+1,
                    updated_at=?, error_code=NULL, error_message=NULL WHERE task_id=?
                """,
                [now, task_id],
            )
            all_records = {
                dataset_id: [dict(item) for item in records]
                for dataset_id, records in records_by_dataset.items()
            }
            all_raw_records = {
                dataset_id: [dict(item) for item in records]
                for dataset_id, records in (raw_records_by_dataset or {}).items()
            }
            active_source_system = str(source_system or self.source_system)
            chunks = self.catalog.rows(
                """
                SELECT * FROM refresh_chunk
                WHERE task_id = ? AND status IN ('PENDING', 'FAILED')
                ORDER BY dataset_id, market_code
                """,
                [task_id],
            )
            any_failed = False
            for chunk in chunks:
                chunk_id = chunk["chunk_id"]
                self._set_chunk_running(task_id, chunk_id)
                try:
                    if chunk_id in fail_set:
                        raise RuntimeError("合成失败注入")
                    source_records = [
                        item
                        for item in all_records.get(chunk["dataset_id"], [])
                        if item.get("market_code") == chunk["market_code"]
                        and _record_matches_chunk(
                            chunk["dataset_id"],
                            item,
                            _as_date(chunk["start_date"]),
                            _as_date(chunk["end_date"]),
                        )
                    ]
                    self._write_raw_asset(
                        task_id,
                        chunk_id,
                        chunk["dataset_id"],
                        chunk["market_code"],
                        int(chunk["attempt_count"]) + 1,
                        all_raw_records.get(chunk["dataset_id"], source_records),
                        source_system=active_source_system,
                    )
                    result = self.facts.append(
                        chunk["dataset_id"],
                        source_records,
                        task_id=task_id,
                        source_request_id=f"{task_id}:{chunk_id}",
                        source_received_at=_now(),
                        source_system=active_source_system,
                        refresh_views=False,
                    )
                    self._finish_chunk(task_id, chunk_id, result)
                except Exception as exc:
                    any_failed = True
                    self._fail_chunk(task_id, chunk_id, str(exc))
            self.facts.refresh_views()
            if any_failed:
                self.catalog.execute(
                    """
                    UPDATE refresh_task SET status='FAILED', task_version=task_version+1,
                        updated_at=?, completed_at=?, error_code='MDF_TASK_FAILED',
                        error_message='一个或多个市场数据分块失败' WHERE task_id=?
                    """,
                    [_now(), _now(), task_id],
                )
                self._write_ingestion_manifest(task_id)
                return self.get_task(task_id)
            self.catalog.execute(
                """
                UPDATE refresh_task SET status='VALIDATING', task_version=task_version+1,
                    updated_at=? WHERE task_id=?
                """,
                [_now(), task_id],
            )
            self._refresh_watermarks(task_id)
            quality = self.run_quality(task_id)
            task_request = json.loads(task["request_json"])
            candidate = None
            if (
                task_request["dataset_preset"] == DATASET_PRESET
                and self.allow_snapshot_candidates
                and self._foundation_coverage_complete()
            ):
                quality = self.run_quality(task_id, foundation_scope=True)
                candidate = self._create_candidate(task_id, quality["quality_run_id"])
            final_status = "SUCCEEDED"
            if candidate and candidate["status"] == "INCOMPLETE":
                final_status = "INCOMPLETE"
            self.catalog.execute(
                """
                UPDATE refresh_task SET status=?, task_version=task_version+1,
                    updated_at=?, completed_at=? WHERE task_id=?
                """,
                [final_status, _now(), _now(), task_id],
            )
            self._write_ingestion_manifest(task_id)
            return self.get_task(task_id)

    def resume_task(
        self,
        task_id: str,
        *,
        expected_task_version: int,
        records_by_dataset: Mapping[str, Iterable[Mapping[str, Any]]],
        raw_records_by_dataset: Mapping[str, Iterable[Mapping[str, Any]]] | None = None,
        source_system: str | None = None,
    ) -> dict[str, Any]:
        self.validate_resume(task_id, expected_task_version)
        return self.execute_task(
            task_id,
            records_by_dataset,
            raw_records_by_dataset=raw_records_by_dataset,
            source_system=source_system,
        )

    def finalize_task_with_succeeded_chunks(
        self,
        task_id: str,
        *,
        expected_task_version: int,
    ) -> dict[str, Any]:
        """Finish an interrupted task whose persisted chunks already succeeded."""

        with self._lock:
            task = self._require_task(task_id)
            if int(task["task_version"]) != int(expected_task_version):
                raise MarketFoundationError("MDF009", "任务版本已变化，请刷新后重试")
            chunks = self.catalog.rows(
                "SELECT * FROM refresh_chunk WHERE task_id=? ORDER BY dataset_id, market_code",
                [task_id],
            )
            successful = {"SUCCEEDED", "SKIPPED_IDEMPOTENT"}
            if (
                task["status"] not in {"FAILED", "INCOMPLETE"}
                or not chunks
                or any(item["status"] not in successful for item in chunks)
            ):
                raise MarketFoundationError(
                    "MDF009", "任务仍有未成功分块，不能只执行收尾"
                )
            self.facts.refresh_views()
            self.catalog.execute(
                """
                UPDATE refresh_task SET status='VALIDATING', task_version=task_version+1,
                    updated_at=?, completed_at=NULL, error_code=NULL, error_message=NULL
                WHERE task_id=?
                """,
                [_now(), task_id],
            )
            self._refresh_watermarks(task_id)
            quality = self.run_quality(task_id)
            task_request = json.loads(task["request_json"])
            candidate = None
            if (
                task_request["dataset_preset"] == DATASET_PRESET
                and self.allow_snapshot_candidates
                and self._foundation_coverage_complete()
            ):
                quality = self.run_quality(task_id, foundation_scope=True)
                candidate = self._create_candidate(task_id, quality["quality_run_id"])
            final_status = (
                "INCOMPLETE"
                if candidate and candidate["status"] == "INCOMPLETE"
                else "SUCCEEDED"
            )
            self.catalog.execute(
                """
                UPDATE refresh_task SET status=?, task_version=task_version+1,
                    updated_at=?, completed_at=? WHERE task_id=?
                """,
                [final_status, _now(), _now(), task_id],
            )
            self._write_ingestion_manifest(task_id)
            return self.get_task(task_id)

    def validate_resume(self, task_id: str, expected_task_version: int) -> dict[str, Any]:
        task = self._require_task(task_id)
        if int(task["task_version"]) != int(expected_task_version):
            raise MarketFoundationError("MDF009", "任务版本已变化，请刷新后重试")
        failed = self.catalog.row(
            "SELECT count(*) AS count FROM refresh_chunk WHERE task_id=? AND status='FAILED'",
            [task_id],
        )
        if task["status"] not in {"FAILED", "INCOMPLETE"} or not failed["count"]:
            raise MarketFoundationError("MDF009", "任务没有可恢复的失败分块")
        return task

    def list_tasks(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.catalog.rows(
            "SELECT * FROM refresh_task ORDER BY created_at DESC LIMIT ?", [limit]
        )
        return [self._task_summary(row) for row in rows]

    def get_task(self, task_id: str) -> dict[str, Any]:
        task = self._require_task(task_id)
        chunks = self.catalog.rows(
            "SELECT * FROM refresh_chunk WHERE task_id=? ORDER BY dataset_id, market_code",
            [task_id],
        )
        quality = self.catalog.row(
            "SELECT * FROM data_quality_run WHERE task_id=? ORDER BY created_at DESC LIMIT 1",
            [task_id],
        )
        candidate = self.catalog.row(
            "SELECT * FROM market_snapshot WHERE candidate_id=? ORDER BY created_at DESC LIMIT 1",
            [task_id],
        )
        summary = self._task_summary(task)
        summary.update(
            {
                "chunks": [_json_row(row) for row in chunks],
                "raw_assets": [
                    _json_row(row)
                    for row in self.catalog.rows(
                        "SELECT * FROM raw_asset WHERE task_id=? ORDER BY chunk_id, attempt_count",
                        [task_id],
                    )
                ],
                "quality": (
                    self.get_quality_run(quality["quality_run_id"])
                    if quality
                    else None
                ),
                "candidate": _json_row(candidate),
            }
        )
        return summary

    def _write_raw_asset(
        self,
        task_id: str,
        chunk_id: str,
        dataset_id: str,
        market_code: str,
        attempt_count: int,
        records: list[Mapping[str, Any]],
        *,
        source_system: str,
    ) -> dict[str, Any]:
        safe_source = "".join(
            character if character.isalnum() or character in {"-", "_"} else "_"
            for character in source_system
        )
        target_dir = self.root / "raw" / safe_source / dataset_id / task_id
        target_dir.mkdir(parents=True, exist_ok=True)
        safe_chunk = "".join(
            character if character.isalnum() or character in {"-", "_"} else "_"
            for character in chunk_id
        )
        target = target_dir / f"{safe_chunk}__attempt_{attempt_count:03d}.jsonl"
        metadata = target.with_suffix(".metadata.json")
        body = "".join(canonical_json(_json_value(dict(record))) + "\n" for record in records)
        digest = sha256(body.encode("utf-8")).hexdigest()
        existing = self.catalog.row("SELECT * FROM raw_asset WHERE file_path=?", [str(target)])
        if existing:
            if existing["file_sha256"] != digest:
                raise MarketFoundationError("MDF011", "同一 Raw 资产路径内容哈希冲突")
            return _json_row(existing)
        _atomic_text(target, body)
        received_at = _now()
        asset_id = f"mdraw_{uuid4().hex}"
        metadata_payload = {
            "asset_id": asset_id,
            "task_id": task_id,
            "chunk_id": chunk_id,
            "dataset_id": dataset_id,
            "market_code": market_code,
            "attempt_count": attempt_count,
            "file_path": str(target),
            "file_sha256": digest,
            "row_count": len(records),
            "received_at": received_at.isoformat(),
            "source_system": source_system,
        }
        _atomic_json(metadata, metadata_payload)
        try:
            self.catalog.execute(
                "INSERT INTO raw_asset VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    asset_id,
                    task_id,
                    chunk_id,
                    dataset_id,
                    market_code,
                    attempt_count,
                    str(target),
                    str(metadata),
                    digest,
                    len(records),
                    received_at,
                ],
            )
        except Exception:
            target.unlink(missing_ok=True)
            metadata.unlink(missing_ok=True)
            raise
        return metadata_payload

    def mark_task_failed(
        self, task_id: str, error_message: str, error_code: str = "MDF_TASK_FAILED"
    ) -> None:
        self._require_task(task_id)
        now = _now()
        with self.catalog.transaction() as transaction:
            transaction.execute(
                """
                UPDATE refresh_chunk SET status='FAILED', error_message=?, updated_at=?
                WHERE task_id=? AND status IN ('PENDING','RUNNING')
                """,
                [error_message[:1000], now, task_id],
            )
            transaction.execute(
                """
                UPDATE refresh_task SET status='FAILED', task_version=task_version+1,
                    updated_at=?, completed_at=?, error_code=?, error_message=?
                WHERE task_id=?
                """,
                [now, now, error_code, error_message[:1000], task_id],
            )
        self._write_ingestion_manifest(task_id)

    def _set_chunk_running(self, task_id: str, chunk_id: str) -> None:
        self.catalog.execute(
            """
            UPDATE refresh_chunk SET status='RUNNING', attempt_count=attempt_count+1,
                error_message=NULL, updated_at=? WHERE task_id=? AND chunk_id=?
            """,
            [_now(), task_id, chunk_id],
        )

    def _finish_chunk(
        self, task_id: str, chunk_id: str, result: Mapping[str, Any]
    ) -> None:
        status = "SKIPPED_IDEMPOTENT" if result["written_rows"] == 0 else "SUCCEEDED"
        now = _now()
        self.catalog.execute(
            """
            UPDATE refresh_chunk SET status=?, input_row_count=?, written_row_count=?,
                idempotent_row_count=?, updated_at=? WHERE task_id=? AND chunk_id=?
            """,
            [
                status,
                result["input_rows"],
                result["written_rows"],
                result["idempotent_rows"],
                now,
                task_id,
                chunk_id,
            ],
        )
        self._append_checkpoint(
            task_id,
            chunk_id,
            {"status": status, "partitions": result["partitions"]},
        )

    def _fail_chunk(self, task_id: str, chunk_id: str, message: str) -> None:
        self.catalog.execute(
            """
            UPDATE refresh_chunk SET status='FAILED', error_message=?, updated_at=?
            WHERE task_id=? AND chunk_id=?
            """,
            [message[:1000], _now(), task_id, chunk_id],
        )
        self._append_checkpoint(task_id, chunk_id, {"status": "FAILED", "error": message})

    def _append_checkpoint(
        self, task_id: str, chunk_id: str, payload: Mapping[str, Any]
    ) -> None:
        current = self.catalog.row(
            """
            SELECT coalesce(max(checkpoint_seq), 0) AS seq FROM task_checkpoint
            WHERE task_id=? AND chunk_id=?
            """,
            [task_id, chunk_id],
        )
        self.catalog.execute(
            "INSERT INTO task_checkpoint VALUES (?, ?, ?, ?, ?)",
            [task_id, chunk_id, int(current["seq"]) + 1, canonical_json(payload), _now()],
        )

    def _refresh_watermarks(self, task_id: str) -> None:
        for dataset_id in DATASETS:
            view = f"current_{dataset_id}"
            for market_code in MARKETS:
                row = self.catalog.row(
                    f"""
                    SELECT min(business_date) AS minimum_business_date,
                           max(business_date) AS maximum_business_date,
                           count(DISTINCT business_date) AS distinct_business_dates
                    FROM {view} WHERE market_code=?
                    """,
                    [market_code],
                )
                if row and row["minimum_business_date"] is not None:
                    self.catalog.execute(
                        """
                        INSERT OR REPLACE INTO coverage_watermark VALUES
                        (?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            dataset_id,
                            market_code,
                            row["minimum_business_date"],
                            row["maximum_business_date"],
                            row["distinct_business_dates"],
                            task_id,
                            _now(),
                        ],
                    )

    def _task_summary(self, task: Mapping[str, Any]) -> dict[str, Any]:
        counts = self.catalog.rows(
            """
            SELECT status, count(*) AS count FROM refresh_chunk
            WHERE task_id=? GROUP BY status ORDER BY status
            """,
            [task["task_id"]],
        )
        return {
            **_json_row(task),
            "request": json.loads(task["request_json"]),
            "chunk_status_counts": {
                item["status"]: int(item["count"]) for item in counts
            },
            "chunk_count": sum(int(item["count"]) for item in counts),
        }

    def _require_task(self, task_id: str) -> dict[str, Any]:
        task = self.catalog.row("SELECT * FROM refresh_task WHERE task_id=?", [task_id])
        if task is None:
            raise MarketFoundationError("MDF009", "市场刷新任务不存在")
        return task

    def _write_ingestion_manifest(self, task_id: str) -> None:
        path = self.root / "manifests" / "ingestion" / f"{task_id}.json"
        _atomic_json(path, self.get_task(task_id))
