"""
文件作用：标准化合成市场事实、生成稳定业务键和 payload 哈希，并写入不可覆盖 Parquet 分区。
编辑记录：
【首次生成：2026-08-13，实现 P2 六类市场事实的统一技术 envelope 与 DuckDB Parquet 写入。】
【第二次编辑：2026-09-14，用受事务保护的批量关系写入替代逐行插入，支持离线历史缓存入库。】
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

import pandas as pd

from .catalog import MarketCatalog
from .constants import DATASETS, MARKETS, SCHEMA_VERSION


FACT_COLUMNS = (
    "dataset_id",
    "business_key",
    "business_date",
    "market_code",
    "security_code",
    "instrument_key",
    "security_type",
    "board_code",
    "listing_date",
    "termination_date",
    "effective_start",
    "effective_end",
    "is_trading_day",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "amount",
    "is_st",
    "is_suspended",
    "limit_up_price",
    "limit_down_price",
    "source_record_version",
    "payload_hash",
    "payload_json",
    "source_system",
    "source_request_id",
    "source_received_at",
    "ingestion_task_id",
    "schema_version",
)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def payload_hash(payload: dict[str, Any]) -> str:
    return sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def business_key(dataset_id: str, payload: dict[str, Any]) -> str:
    market = required_text(payload, "market_code")
    if dataset_id == "market_calendar":
        return f"{market}|{required_date(payload, 'business_date').isoformat()}"
    security = required_text(payload, "security_code")
    if dataset_id == "instrument_master_history":
        effective = required_date(payload, "effective_start")
        return f"{market}|{security}|{effective.isoformat()}"
    return (
        f"{market}|{security}|"
        f"{required_date(payload, 'business_date').isoformat()}"
    )


def validate_payload(dataset_id: str, payload: dict[str, Any]) -> None:
    if dataset_id not in DATASETS:
        raise ValueError(f"未知市场数据集：{dataset_id}")
    if required_text(payload, "market_code") not in MARKETS:
        raise ValueError("市场代码必须是 XSHG、XSHE 或 XBSE")
    if dataset_id != "market_calendar":
        required_text(payload, "security_code")
        required_text(payload, "instrument_key")
    if dataset_id == "instrument_master_history":
        required_date(payload, "effective_start")
        required_date(payload, "listing_date")
    else:
        required_date(payload, "business_date")


def required_text(payload: dict[str, Any], field: str) -> str:
    value = str(payload.get(field, "")).strip()
    if not value:
        raise ValueError(f"{field} 不能为空")
    return value


def required_date(payload: dict[str, Any], field: str) -> date:
    value = payload.get(field)
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise ValueError(f"{field} 必须是 YYYY-MM-DD") from exc


def normalize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(value, datetime):
            normalized[key] = value.isoformat()
        elif isinstance(value, date):
            normalized[key] = value.isoformat()
        elif isinstance(value, Decimal):
            normalized[key] = format(value, "f")
        else:
            normalized[key] = value
    return normalized


class ParquetFactStore:
    """Append-only Parquet writer backed by the technical source catalog."""

    def __init__(
        self,
        root: str | Path,
        catalog: MarketCatalog,
        *,
        create_directories: bool = True,
    ) -> None:
        self.root = Path(root)
        self.catalog = catalog
        self.parquet_root = self.root / "parquet"
        self.temporary_root = self.root / ".staging"
        if create_directories:
            self.parquet_root.mkdir(parents=True, exist_ok=True)
            self.temporary_root.mkdir(parents=True, exist_ok=True)

    def append(
        self,
        dataset_id: str,
        records: Iterable[dict[str, Any]],
        *,
        task_id: str,
        source_request_id: str,
        source_received_at: datetime,
        source_system: str = "SYNTHETIC_P2",
        refresh_views: bool = True,
    ) -> dict[str, Any]:
        normalized_batch: list[tuple[dict[str, Any], str, str]] = []
        idempotent = 0
        batch_payloads: dict[str, str] = {}
        for raw in records:
            validate_payload(dataset_id, raw)
            payload = normalize_payload(raw)
            key = business_key(dataset_id, payload)
            digest = payload_hash(payload)
            prior_batch_hash = batch_payloads.get(key)
            if prior_batch_hash is not None:
                if prior_batch_hash == digest:
                    idempotent += 1
                    continue
                raise ValueError(f"同一输入分块包含冲突业务键：{key}")
            batch_payloads[key] = digest
            normalized_batch.append((payload, key, digest))
        current_by_key = self._latest_sources(dataset_id, tuple(batch_payloads))
        prepared: list[dict[str, Any]] = []
        for payload, key, digest in normalized_batch:
            current = current_by_key.get(key)
            if current and current["payload_hash"] == digest:
                idempotent += 1
                continue
            version = 1 if current is None else int(current["source_record_version"]) + 1
            business_date = _fact_date(dataset_id, payload)
            logical = _logical_columns(dataset_id, payload)
            prepared.append(
                {
                    "dataset_id": dataset_id,
                    "business_key": key,
                    "business_date": business_date,
                    "market_code": payload["market_code"],
                    "security_code": payload.get("security_code"),
                    "instrument_key": payload.get("instrument_key"),
                    **logical,
                    "source_record_version": version,
                    "payload_hash": digest,
                    "payload_json": canonical_json(payload),
                    "source_system": source_system,
                    "source_request_id": source_request_id,
                    "source_received_at": source_received_at,
                    "ingestion_task_id": task_id,
                    "schema_version": SCHEMA_VERSION,
                    "supersedes_version": None if current is None else version - 1,
                }
            )
        partitions: list[dict[str, Any]] = []
        groups: dict[tuple[str, int, int], list[dict[str, Any]]] = {}
        for item in prepared:
            current_date = item["business_date"]
            groups.setdefault(
                (item["market_code"], current_date.year, current_date.month), []
            ).append(item)
        for (market_code, year, month), rows in groups.items():
            partitions.append(
                self._write_partition(
                    dataset_id, market_code, year, month, rows, task_id
                )
            )
        if refresh_views:
            self.refresh_views()
        return {
            "input_rows": len(prepared) + idempotent,
            "written_rows": len(prepared),
            "idempotent_rows": idempotent,
            "partitions": partitions,
        }

    def _latest_sources(
        self, dataset_id: str, business_keys: tuple[str, ...]
    ) -> dict[str, dict[str, Any]]:
        if not business_keys:
            return {}
        with self.catalog.transaction() as connection:
            connection.execute(
                "CREATE OR REPLACE TEMP TABLE market_source_key_lookup (business_key VARCHAR PRIMARY KEY)"
            )
            connection.register("_fact_lookup_input", pd.DataFrame({"business_key": business_keys}))
            try:
                connection.execute("INSERT INTO market_source_key_lookup SELECT business_key FROM _fact_lookup_input")
            finally:
                connection.unregister("_fact_lookup_input")
            cursor = connection.execute(
                """
                SELECT business_key, source_record_version, payload_hash
                FROM (
                    SELECT source.business_key, source.source_record_version,
                           source.payload_hash,
                           row_number() OVER (
                               PARTITION BY source.business_key
                               ORDER BY source.source_record_version DESC
                           ) AS version_rank
                    FROM source_record_catalog AS source
                    INNER JOIN market_source_key_lookup AS lookup
                        ON lookup.business_key = source.business_key
                    WHERE source.dataset_id = ?
                )
                WHERE version_rank = 1
                """,
                [dataset_id],
            )
            columns = [item[0] for item in cursor.description]
            return {
                row[0]: dict(zip(columns, row, strict=True))
                for row in cursor.fetchall()
            }

    def _write_partition(
        self,
        dataset_id: str,
        market_code: str,
        year: int,
        month: int,
        rows: list[dict[str, Any]],
        task_id: str,
    ) -> dict[str, Any]:
        partition_id = f"part_{uuid4().hex}"
        target_dir = (
            self.parquet_root
            / dataset_id
            / f"market={market_code}"
            / f"year={year:04d}"
            / f"month={month:02d}"
        )
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / f"{partition_id}.parquet"
        temporary = self.temporary_root / f"{partition_id}.parquet.tmp"
        try:
            with self.catalog.transaction() as connection:
                connection.execute(
                    """
                    CREATE OR REPLACE TEMP TABLE market_fact_stage (
                dataset_id VARCHAR,
                business_key VARCHAR,
                business_date DATE,
                market_code VARCHAR,
                security_code VARCHAR,
                instrument_key VARCHAR,
                security_type VARCHAR,
                board_code VARCHAR,
                listing_date DATE,
                termination_date DATE,
                effective_start DATE,
                effective_end DATE,
                is_trading_day BOOLEAN,
                open_price DECIMAL(20, 6),
                high_price DECIMAL(20, 6),
                low_price DECIMAL(20, 6),
                close_price DECIMAL(20, 6),
                volume DECIMAL(24, 6),
                amount DECIMAL(24, 6),
                is_st BOOLEAN,
                is_suspended BOOLEAN,
                limit_up_price DECIMAL(20, 6),
                limit_down_price DECIMAL(20, 6),
                source_record_version INTEGER,
                payload_hash VARCHAR,
                payload_json VARCHAR,
                source_system VARCHAR,
                source_request_id VARCHAR,
                source_received_at TIMESTAMP,
                ingestion_task_id VARCHAR,
                schema_version VARCHAR
                    )
                    """
                )
                frame = pd.DataFrame(rows, columns=FACT_COLUMNS)
                # DuckDB infers object-Decimal precision from a sample. Mixed magnitudes
                # can exceed that inferred type; cast exact decimal text into our declared schema.
                for column in ("open_price", "high_price", "low_price", "close_price", "volume", "amount", "limit_up_price", "limit_down_price"):
                    frame[column] = frame[column].map(lambda value: None if value is None else str(value))
                connection.register("_fact_partition_input", frame)
                try:
                    connection.execute("INSERT INTO market_fact_stage SELECT * FROM _fact_partition_input")
                finally:
                    connection.unregister("_fact_partition_input")
                sql_path = str(temporary).replace("'", "''")
                connection.execute(
                    f"COPY market_fact_stage TO '{sql_path}' "
                    "(FORMAT parquet, COMPRESSION zstd)"
                )
                temporary.replace(target)
                digest = sha256(target.read_bytes()).hexdigest()
                minimum = min(row["business_date"] for row in rows)
                maximum = max(row["business_date"] for row in rows)
                now = datetime.now().astimezone()
                connection.execute(
                    """
                    INSERT INTO partition_catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        partition_id,
                        dataset_id,
                        market_code,
                        year,
                        month,
                        str(target),
                        digest,
                        len(rows),
                        minimum,
                        maximum,
                        task_id,
                        now,
                    ],
                )
                connection.execute(
                    """
                    INSERT INTO source_record_catalog
                    SELECT dataset_id, business_key, source_record_version,
                           payload_hash, payload_json, source_system,
                           source_request_id, source_received_at, ingestion_task_id,
                           ?, CASE WHEN source_record_version = 1 THEN NULL
                                   ELSE source_record_version - 1 END, ?
                    FROM market_fact_stage
                    """,
                    [partition_id, now],
                )
        except Exception:
            temporary.unlink(missing_ok=True)
            target.unlink(missing_ok=True)
            raise
        return {
            "partition_id": partition_id,
            "file_path": str(target),
            "file_sha256": digest,
            "row_count": len(rows),
        }

    def refresh_views(self) -> None:
        for dataset_id in DATASETS:
            files = self.catalog.rows(
                "SELECT file_path FROM partition_catalog WHERE dataset_id = ? ORDER BY file_path",
                [dataset_id],
            )
            view_name = f"current_{dataset_id}"
            if not files:
                self.catalog.execute(
                    f"""
                    CREATE OR REPLACE VIEW {view_name} AS
                    SELECT * FROM (
                        SELECT
                            CAST(NULL AS VARCHAR) AS dataset_id,
                            CAST(NULL AS VARCHAR) AS business_key,
                            CAST(NULL AS DATE) AS business_date,
                            CAST(NULL AS VARCHAR) AS market_code,
                            CAST(NULL AS VARCHAR) AS security_code,
                            CAST(NULL AS VARCHAR) AS instrument_key,
                            CAST(NULL AS VARCHAR) AS security_type,
                            CAST(NULL AS VARCHAR) AS board_code,
                            CAST(NULL AS DATE) AS listing_date,
                            CAST(NULL AS DATE) AS termination_date,
                            CAST(NULL AS DATE) AS effective_start,
                            CAST(NULL AS DATE) AS effective_end,
                            CAST(NULL AS BOOLEAN) AS is_trading_day,
                            CAST(NULL AS DECIMAL(20, 6)) AS open_price,
                            CAST(NULL AS DECIMAL(20, 6)) AS high_price,
                            CAST(NULL AS DECIMAL(20, 6)) AS low_price,
                            CAST(NULL AS DECIMAL(20, 6)) AS close_price,
                            CAST(NULL AS DECIMAL(24, 6)) AS volume,
                            CAST(NULL AS DECIMAL(24, 6)) AS amount,
                            CAST(NULL AS BOOLEAN) AS is_st,
                            CAST(NULL AS BOOLEAN) AS is_suspended,
                            CAST(NULL AS DECIMAL(20, 6)) AS limit_up_price,
                            CAST(NULL AS DECIMAL(20, 6)) AS limit_down_price,
                            CAST(NULL AS INTEGER) AS source_record_version,
                            CAST(NULL AS VARCHAR) AS payload_hash,
                            CAST(NULL AS VARCHAR) AS payload_json,
                            CAST(NULL AS VARCHAR) AS source_system,
                            CAST(NULL AS VARCHAR) AS source_request_id,
                            CAST(NULL AS TIMESTAMP) AS source_received_at,
                            CAST(NULL AS VARCHAR) AS ingestion_task_id,
                            CAST(NULL AS VARCHAR) AS schema_version
                    ) WHERE false
                    """
                )
                continue
            paths = ",".join(
                "'" + row["file_path"].replace("'", "''") + "'" for row in files
            )
            self.catalog.execute(
                f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT * EXCLUDE (version_rank) FROM (
                    SELECT *, row_number() OVER (
                        PARTITION BY business_key
                        ORDER BY source_record_version DESC
                    ) AS version_rank
                    FROM read_parquet([{paths}])
                ) WHERE version_rank = 1
                """
            )


def _fact_date(dataset_id: str, payload: dict[str, Any]) -> date:
    field = "effective_start" if dataset_id == "instrument_master_history" else "business_date"
    return required_date(payload, field)


def _logical_columns(dataset_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    columns = {
        "security_type": None,
        "board_code": None,
        "listing_date": None,
        "termination_date": None,
        "effective_start": None,
        "effective_end": None,
        "is_trading_day": None,
        "open_price": None,
        "high_price": None,
        "low_price": None,
        "close_price": None,
        "volume": None,
        "amount": None,
        "is_st": None,
        "is_suspended": None,
        "limit_up_price": None,
        "limit_down_price": None,
    }
    if dataset_id == "market_calendar":
        columns["is_trading_day"] = bool(payload.get("is_trading_day"))
    elif dataset_id == "instrument_master_history":
        columns.update(
            {
                "security_type": payload.get("security_type"),
                "board_code": payload.get("board_code"),
                "listing_date": _optional_date(payload.get("listing_date")),
                "termination_date": _optional_date(payload.get("termination_date")),
                "effective_start": _optional_date(payload.get("effective_start")),
                "effective_end": _optional_date(payload.get("effective_end")),
            }
        )
    elif dataset_id == "daily_price_unadjusted":
        columns.update(
            {
                "open_price": _optional_decimal(payload.get("open")),
                "high_price": _optional_decimal(payload.get("high")),
                "low_price": _optional_decimal(payload.get("low")),
                "close_price": _optional_decimal(payload.get("close")),
                "volume": _optional_decimal(payload.get("volume")),
                "amount": _optional_decimal(payload.get("amount")),
            }
        )
    elif dataset_id == "daily_st_status":
        columns["is_st"] = bool(payload.get("is_st"))
    elif dataset_id == "daily_suspension_status":
        columns["is_suspended"] = bool(payload.get("is_suspended"))
    elif dataset_id == "daily_price_limits":
        columns["limit_up_price"] = _optional_decimal(payload.get("limit_up"))
        columns["limit_down_price"] = _optional_decimal(payload.get("limit_down"))
    return columns


def _optional_date(value: Any) -> date | None:
    return None if value in {None, ""} else required_date({"value": value}, "value")


def _optional_decimal(value: Any) -> Decimal | None:
    return None if value in {None, ""} else Decimal(str(value))


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    raise TypeError(f"不可序列化类型：{type(value).__name__}")
