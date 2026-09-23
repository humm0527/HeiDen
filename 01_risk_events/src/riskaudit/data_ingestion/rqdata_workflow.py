"""
文件作用：执行受限的米筐真实市场数据接入烟测，不包含风险或指标计算。
编辑记录：
【首次生成：2026-08-05，实现小范围真实米筐接入与 Raw 追溯。】
【二次编辑内容：2026-08-05，将米筐输出严格限定为四张市场标准表。】
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Sequence

from .adapters import RQDataClientProtocol
from .mapping import FieldMapper
from .market_data import MarketDataIngestionService, build_market_mapping_catalog
from .pipeline import DataIngestionService
from .storage import RawStore, StandardStore


@dataclass(frozen=True)
class RQDataSmokeRunResult:
    run_id: str
    output_dir: Path
    raw_dataset_names: tuple[str, ...]
    standard_table_names: tuple[str, ...]
    row_counts: dict[str, int]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "output_dir": str(self.output_dir),
            "raw_dataset_names": list(self.raw_dataset_names),
            "standard_table_names": list(self.standard_table_names),
            "row_counts": self.row_counts,
        }


def validate_smoke_scope(
    order_book_ids: Sequence[str], start_date: str, end_date: str
) -> None:
    """Keep a real smoke run deliberately small and reproducible."""

    if not 1 <= len(order_book_ids) <= 5:
        raise ValueError("A real smoke run must request between 1 and 5 securities")
    if len(set(order_book_ids)) != len(order_book_ids):
        raise ValueError("Duplicate securities are not allowed in a smoke run")
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("end_date must not precede start_date")
    if (end - start).days > 10:
        raise ValueError("A real smoke run may span at most 10 calendar days")


def run_rqdata_smoke(
    *,
    client: RQDataClientProtocol,
    repository_root: str | Path,
    output_root: str | Path,
    run_id: str,
    order_book_ids: Sequence[str],
    start_date: str,
    end_date: str,
    retrieved_at: datetime | None = None,
) -> RQDataSmokeRunResult:
    """将真实或模拟米筐数据送入Raw、字段映射和四张市场标准表。"""

    validate_smoke_scope(order_book_ids, start_date, end_date)
    repository_root = Path(repository_root)
    output_root = Path(output_root)
    retrieved = (retrieved_at or datetime.now(timezone.utc)).astimezone(timezone.utc)

    catalog = build_market_mapping_catalog(repository_root)
    service = DataIngestionService(
        FieldMapper(catalog),
        RawStore(output_root / "raw"),
        StandardStore(output_root / "standard"),
    )
    market_service = MarketDataIngestionService(
        client=client,
        ingestion_service=service,
        source_name="rqdata_real",
    )
    batch = market_service.ingest(
        order_book_ids=order_book_ids,
        start_date=start_date,
        end_date=end_date,
        run_id=run_id,
        retrieved_at=retrieved,
    )

    output_dir = service.save_standard_run(
        run_id,
        batch.outcomes,
        additional_raw_artifacts=batch.trace_raw_artifacts,
    )
    tables = {
        outcome.mapping_result.target_table: outcome.mapping_result.business_data
        for outcome in batch.outcomes
    }
    result = RQDataSmokeRunResult(
        run_id=run_id,
        output_dir=output_dir,
        raw_dataset_names=tuple(
            outcome.raw_artifact.dataset_name for outcome in batch.outcomes
        )
        + tuple(
            artifact.dataset_name for artifact in batch.trace_raw_artifacts
        ),
        standard_table_names=tuple(tables),
        row_counts={name: len(frame) for name, frame in tables.items()},
    )
    (output_dir / "run_summary.json").write_text(
        json.dumps(result.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return result
