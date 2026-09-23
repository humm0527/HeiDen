"""
文件作用：创建并访问长期市场数据底座的 DuckDB 技术目录、任务、质量和快照表。
编辑记录：
【首次生成：2026-08-13，实现 P2 DuckDB 迁移、事务、索引和字典查询辅助。】
【二次编辑：2026-08-15，增加受锁保护的批次只读迭代，避免全量候选哈希一次性加载目录记录。】
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from typing import Any, Iterator, Sequence

import duckdb

from .constants import DATASETS, QUALITY_RULE_VERSION, SCHEMA_VERSION


MIGRATION_SQL = """
CREATE TABLE IF NOT EXISTS schema_migration (
    version VARCHAR PRIMARY KEY,
    applied_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS dataset_registry (
    dataset_id VARCHAR PRIMARY KEY,
    schema_version VARCHAR NOT NULL,
    enabled BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS partition_catalog (
    partition_id VARCHAR PRIMARY KEY,
    dataset_id VARCHAR NOT NULL,
    market_code VARCHAR NOT NULL,
    partition_year INTEGER NOT NULL,
    partition_month INTEGER NOT NULL,
    file_path VARCHAR NOT NULL UNIQUE,
    file_sha256 VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    minimum_business_date DATE,
    maximum_business_date DATE,
    ingestion_task_id VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS raw_asset (
    asset_id VARCHAR PRIMARY KEY,
    task_id VARCHAR NOT NULL,
    chunk_id VARCHAR NOT NULL,
    dataset_id VARCHAR NOT NULL,
    market_code VARCHAR NOT NULL,
    attempt_count INTEGER NOT NULL,
    file_path VARCHAR NOT NULL UNIQUE,
    metadata_path VARCHAR NOT NULL UNIQUE,
    file_sha256 VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    received_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS refresh_plan (
    plan_id VARCHAR PRIMARY KEY,
    plan_hash VARCHAR NOT NULL,
    request_json VARCHAR NOT NULL,
    estimate_json VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS refresh_task (
    task_id VARCHAR PRIMARY KEY,
    request_id VARCHAR NOT NULL UNIQUE,
    plan_id VARCHAR NOT NULL,
    plan_hash VARCHAR NOT NULL,
    request_json VARCHAR NOT NULL,
    mode VARCHAR NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    dataset_preset VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    task_version INTEGER NOT NULL,
    execution_mode VARCHAR NOT NULL,
    note VARCHAR,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    error_code VARCHAR,
    error_message VARCHAR
);
CREATE TABLE IF NOT EXISTS refresh_chunk (
    task_id VARCHAR NOT NULL,
    chunk_id VARCHAR NOT NULL,
    market_code VARCHAR NOT NULL,
    dataset_id VARCHAR NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    status VARCHAR NOT NULL,
    attempt_count INTEGER NOT NULL,
    input_row_count BIGINT NOT NULL,
    written_row_count BIGINT NOT NULL,
    idempotent_row_count BIGINT NOT NULL,
    error_message VARCHAR,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (task_id, chunk_id)
);
CREATE TABLE IF NOT EXISTS task_checkpoint (
    task_id VARCHAR NOT NULL,
    chunk_id VARCHAR NOT NULL,
    checkpoint_seq INTEGER NOT NULL,
    checkpoint_json VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (task_id, chunk_id, checkpoint_seq)
);
CREATE TABLE IF NOT EXISTS coverage_watermark (
    dataset_id VARCHAR NOT NULL,
    market_code VARCHAR NOT NULL,
    minimum_business_date DATE,
    maximum_business_date DATE,
    distinct_business_dates BIGINT NOT NULL,
    latest_task_id VARCHAR NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (dataset_id, market_code)
);
CREATE TABLE IF NOT EXISTS market_universe_manifest (
    universe_id VARCHAR PRIMARY KEY,
    manifest_hash VARCHAR NOT NULL UNIQUE,
    source_system VARCHAR NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    source_row_count BIGINT NOT NULL,
    eligible_security_count BIGINT NOT NULL,
    counts_by_market_json VARCHAR NOT NULL,
    delisted_count BIGINT NOT NULL,
    shard_size INTEGER NOT NULL,
    shard_count INTEGER NOT NULL,
    manifest_path VARCHAR NOT NULL,
    members_path VARCHAR NOT NULL,
    discovered_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS market_universe_member (
    universe_id VARCHAR NOT NULL,
    instrument_key VARCHAR NOT NULL,
    source_order_book_id VARCHAR NOT NULL,
    source_exchange VARCHAR NOT NULL,
    market_code VARCHAR NOT NULL,
    security_code VARCHAR NOT NULL,
    symbol VARCHAR,
    security_type VARCHAR NOT NULL,
    board_code VARCHAR,
    listing_date DATE NOT NULL,
    termination_date DATE,
    source_status VARCHAR NOT NULL,
    PRIMARY KEY (universe_id, instrument_key)
);
CREATE TABLE IF NOT EXISTS market_universe_shard (
    universe_id VARCHAR NOT NULL,
    shard_id VARCHAR NOT NULL,
    market_code VARCHAR NOT NULL,
    shard_ordinal INTEGER NOT NULL,
    security_count INTEGER NOT NULL,
    first_order_book_id VARCHAR NOT NULL,
    last_order_book_id VARCHAR NOT NULL,
    order_book_ids_json VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    PRIMARY KEY (universe_id, shard_id)
);
CREATE TABLE IF NOT EXISTS market_backfill_run (
    backfill_id VARCHAR PRIMARY KEY,
    universe_id VARCHAR NOT NULL,
    universe_manifest_hash VARCHAR NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    status VARCHAR NOT NULL,
    config_json VARCHAR NOT NULL,
    total_units INTEGER NOT NULL,
    completed_units INTEGER NOT NULL,
    failed_units INTEGER NOT NULL,
    blocked_capabilities_json VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message VARCHAR
);
CREATE TABLE IF NOT EXISTS market_backfill_unit (
    backfill_id VARCHAR NOT NULL,
    unit_id VARCHAR NOT NULL,
    unit_kind VARCHAR NOT NULL,
    universe_shard_id VARCHAR,
    market_code VARCHAR NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    datasets_json VARCHAR NOT NULL,
    order_book_ids_json VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    attempt_count INTEGER NOT NULL,
    task_id VARCHAR,
    input_rows BIGINT NOT NULL,
    written_rows BIGINT NOT NULL,
    idempotent_rows BIGINT NOT NULL,
    error_message VARCHAR,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    PRIMARY KEY (backfill_id, unit_id)
);
CREATE TABLE IF NOT EXISTS source_record_catalog (
    dataset_id VARCHAR NOT NULL,
    business_key VARCHAR NOT NULL,
    source_record_version INTEGER NOT NULL,
    payload_hash VARCHAR NOT NULL,
    payload_json VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL,
    source_request_id VARCHAR NOT NULL,
    source_received_at TIMESTAMP NOT NULL,
    ingestion_task_id VARCHAR NOT NULL,
    partition_id VARCHAR,
    supersedes_version INTEGER,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (dataset_id, business_key, source_record_version)
);
CREATE TABLE IF NOT EXISTS data_quality_rule (
    rule_id VARCHAR NOT NULL,
    rule_version VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    description VARCHAR NOT NULL,
    PRIMARY KEY (rule_id, rule_version)
);
CREATE TABLE IF NOT EXISTS data_quality_run (
    quality_run_id VARCHAR PRIMARY KEY,
    task_id VARCHAR,
    candidate_id VARCHAR,
    rule_version VARCHAR NOT NULL,
    scope_json VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    severe_count INTEGER NOT NULL,
    warning_count INTEGER NOT NULL,
    info_count INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS data_quality_result (
    quality_run_id VARCHAR NOT NULL,
    finding_id VARCHAR NOT NULL,
    rule_id VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    dataset_id VARCHAR,
    market_code VARCHAR,
    date_start DATE,
    date_end DATE,
    affected_rows BIGINT NOT NULL,
    message VARCHAR NOT NULL,
    suggested_action VARCHAR NOT NULL,
    example_keys_json VARCHAR NOT NULL,
    PRIMARY KEY (quality_run_id, finding_id)
);
CREATE TABLE IF NOT EXISTS market_snapshot (
    snapshot_id VARCHAR PRIMARY KEY,
    candidate_id VARCHAR,
    status VARCHAR NOT NULL,
    risk_adapter_status VARCHAR NOT NULL,
    execution_mode VARCHAR NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    markets_json VARCHAR NOT NULL,
    datasets_json VARCHAR NOT NULL,
    quality_run_id VARCHAR NOT NULL,
    member_hash VARCHAR NOT NULL,
    note VARCHAR,
    created_at TIMESTAMP NOT NULL,
    published_at TIMESTAMP,
    published_by VARCHAR
);
CREATE TABLE IF NOT EXISTS market_snapshot_member (
    snapshot_id VARCHAR NOT NULL,
    partition_id VARCHAR NOT NULL,
    file_path VARCHAR NOT NULL,
    file_sha256 VARCHAR NOT NULL,
    dataset_id VARCHAR NOT NULL,
    market_code VARCHAR NOT NULL,
    PRIMARY KEY (snapshot_id, partition_id)
);
CREATE TABLE IF NOT EXISTS market_snapshot_record (
    snapshot_id VARCHAR NOT NULL,
    dataset_id VARCHAR NOT NULL,
    business_key VARCHAR NOT NULL,
    source_record_version INTEGER NOT NULL,
    payload_hash VARCHAR NOT NULL,
    partition_id VARCHAR NOT NULL,
    PRIMARY KEY (snapshot_id, dataset_id, business_key)
);
CREATE TABLE IF NOT EXISTS snapshot_publish_log (
    log_id VARCHAR PRIMARY KEY,
    candidate_id VARCHAR NOT NULL,
    snapshot_id VARCHAR,
    action VARCHAR NOT NULL,
    request_id VARCHAR NOT NULL UNIQUE,
    candidate_hash VARCHAR NOT NULL,
    quality_run_id VARCHAR NOT NULL,
    note VARCHAR,
    created_at TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_task_status_created ON refresh_task(status, created_at);
CREATE INDEX IF NOT EXISTS idx_raw_task_chunk ON raw_asset(task_id, chunk_id, attempt_count);
CREATE INDEX IF NOT EXISTS idx_chunk_task_status ON refresh_chunk(task_id, status);
CREATE INDEX IF NOT EXISTS idx_partition_dataset_market ON partition_catalog(dataset_id, market_code, partition_year, partition_month);
CREATE INDEX IF NOT EXISTS idx_universe_member_market ON market_universe_member(universe_id, market_code, security_code);
CREATE INDEX IF NOT EXISTS idx_universe_shard_status ON market_universe_shard(universe_id, status, market_code, shard_ordinal);
CREATE INDEX IF NOT EXISTS idx_backfill_run_status ON market_backfill_run(status, created_at);
CREATE INDEX IF NOT EXISTS idx_backfill_unit_status ON market_backfill_unit(backfill_id, status, unit_kind, end_date, market_code);
CREATE INDEX IF NOT EXISTS idx_source_business_key ON source_record_catalog(dataset_id, business_key, source_record_version);
CREATE INDEX IF NOT EXISTS idx_quality_status ON data_quality_run(status, created_at);
CREATE INDEX IF NOT EXISTS idx_snapshot_status ON market_snapshot(status, created_at);
"""


QUALITY_RULES = (
    ("MDFQ001", "SEVERE", "六类必需数据集与三市场覆盖完整"),
    ("MDFQ002", "SEVERE", "事实文件哈希与目录一致"),
    ("MDFQ003", "SEVERE", "同一事实业务键版本唯一且可追溯"),
    ("MDFQ004", "WARNING", "合成 P2 快照不得直接准入风险计算"),
)


class MarketCatalog:
    """Small DuckDB control catalog; large facts remain in Parquet."""

    def __init__(self, database_path: str | Path, *, read_only: bool = False) -> None:
        self.database_path = Path(database_path)
        self.read_only = bool(read_only)
        if self.read_only:
            if not self.database_path.is_file():
                raise FileNotFoundError(f"Market catalog does not exist: {self.database_path}")
        else:
            self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        self._connection = duckdb.connect(
            str(self.database_path), read_only=self.read_only
        )
        if not self.read_only:
            self.migrate()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def migrate(self) -> None:
        with self._lock:
            self._connection.execute(MIGRATION_SQL)
            self._connection.execute(
                "ALTER TABLE market_snapshot_record ADD COLUMN IF NOT EXISTS partition_id VARCHAR"
            )
            self._connection.execute(
                "INSERT OR IGNORE INTO schema_migration VALUES (?, current_timestamp)",
                [SCHEMA_VERSION],
            )
            for dataset_id in DATASETS:
                self._connection.execute(
                    "INSERT OR IGNORE INTO dataset_registry VALUES (?, ?, true, current_timestamp)",
                    [dataset_id, SCHEMA_VERSION],
                )
            for rule_id, severity, description in QUALITY_RULES:
                self._connection.execute(
                    "INSERT OR IGNORE INTO data_quality_rule VALUES (?, ?, ?, ?)",
                    [rule_id, QUALITY_RULE_VERSION, severity, description],
                )

    @contextmanager
    def transaction(self) -> Iterator[duckdb.DuckDBPyConnection]:
        with self._lock:
            self._connection.execute("BEGIN TRANSACTION")
            try:
                yield self._connection
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
            else:
                self._connection.execute("COMMIT")

    def execute(self, sql: str, parameters: Sequence[Any] | None = None) -> None:
        with self._lock:
            self._connection.execute(sql, parameters or [])

    def rows(
        self, sql: str, parameters: Sequence[Any] | None = None
    ) -> list[dict[str, Any]]:
        with self._lock:
            cursor = self._connection.execute(sql, parameters or [])
            columns = [item[0] for item in cursor.description]
            return [dict(zip(columns, values, strict=True)) for values in cursor.fetchall()]

    def iter_rows(
        self,
        sql: str,
        parameters: Sequence[Any] | None = None,
        *,
        batch_size: int = 10_000,
    ) -> Iterator[dict[str, Any]]:
        if batch_size <= 0:
            raise ValueError("batch_size 必须大于 0")
        with self._lock:
            cursor = self._connection.execute(sql, parameters or [])
            columns = [item[0] for item in cursor.description]
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                for values in batch:
                    yield dict(zip(columns, values, strict=True))

    def row(
        self, sql: str, parameters: Sequence[Any] | None = None
    ) -> dict[str, Any] | None:
        rows = self.rows(sql, parameters)
        return rows[0] if rows else None

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._connection
