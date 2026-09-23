"""
文件作用：将米筐等市场事实数据保存至Raw层并标准化为四张市场业务表。
编辑记录：
- 首次生成：2026-08-05，拆分市场数据来源边界并保留停牌Raw追溯。
【第二次编辑：2026-08-11，支持风险运行传入观察期前 ST 基线日并保留生命周期不重叠证券主数据。】
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from ..adapters.rqdata import RQDataAdapter, RQDataClientProtocol
from ..mapping import MappingCatalog
from ..models import RawArtifact
from ..pipeline import DataIngestionService, IngestionOutcome


MARKET_DATASETS = (
    "rqdata_trading_calendar",
    "rqdata_security_master",
    "rqdata_market_daily",
    "rqdata_st_status",
)

MARKET_TABLES = frozenset(
    {
        "交易日历表",
        "证券基础状态日表",
        "股票日行情表",
        "证券风险状态表",
    }
)


def build_market_mapping_catalog(repository_root: str | Path) -> MappingCatalog:
    """只加载市场数据映射，避免市场入口看到券商业务输入配置。"""

    root = Path(repository_root)
    return MappingCatalog.from_files(
        root / "configs/mapping/business_field_registry_v1.yaml",
        root / "configs/mapping/rqdata_v1.yaml",
    )


@dataclass(frozen=True)
class MarketDataBatch:
    """一次市场数据接入产生的四表结果及无独立目标表的Raw资源。"""

    outcomes: tuple[IngestionOutcome, ...]
    trace_raw_artifacts: tuple[RawArtifact, ...]

    @property
    def ready(self) -> bool:
        return all(outcome.mapping_result.ready for outcome in self.outcomes)


class MarketDataIngestionService:
    """米筐市场事实入口；不接受、构造或输出券商风险分类。"""

    def __init__(
        self,
        *,
        client: RQDataClientProtocol,
        ingestion_service: DataIngestionService,
        source_name: str = "rqdata",
    ) -> None:
        self.adapter = RQDataAdapter(client)
        self.ingestion_service = ingestion_service
        self.source_name = source_name

    def ingest(
        self,
        *,
        order_book_ids: Sequence[str],
        start_date: str,
        end_date: str,
        run_id: str,
        st_baseline_date: str | None = None,
        include_non_overlapping: bool = False,
        market: str = "cn",
        retrieved_at: datetime | None = None,
    ) -> MarketDataBatch:
        retrieved = (retrieved_at or datetime.now(timezone.utc)).astimezone(
            timezone.utc
        )
        retrieved_iso = retrieved.isoformat()
        bundle = self.adapter.fetch_bundle(
            order_book_ids=order_book_ids,
            start_date=start_date,
            end_date=end_date,
            st_baseline_date=st_baseline_date,
            include_non_overlapping=include_non_overlapping,
            market=market,
            retrieved_at=retrieved,
        )
        mapping_payloads = self.adapter.build_mapping_payloads(
            bundle,
            status_date=end_date,
        )
        contexts = {
            "rqdata_trading_calendar": {
                "market_code": market,
                "information_available_at": retrieved_iso,
                "ingested_at": retrieved_iso,
            },
            "rqdata_security_master": {
                "status_effective_at": f"{end_date}T00:00:00+08:00",
                "information_available_at": retrieved_iso,
                "ingested_at": retrieved_iso,
            },
            "rqdata_market_daily": {
                "information_available_at": retrieved_iso,
                "ingested_at": retrieved_iso,
            },
            "rqdata_st_status": {
                "evidence_cutoff_at": retrieved_iso,
                "information_available_at": retrieved_iso,
                "upstream_snapshot_id": f"{self.source_name}:{run_id}",
                "ingested_at": retrieved_iso,
            },
        }

        outcomes = tuple(
            self.ingestion_service.ingest_json(
                source_name=self.source_name,
                dataset_name=dataset_name,
                payload=bundle[dataset_name],
                mapping_payload=mapping_payloads[dataset_name],
                context=contexts[dataset_name],
                received_at=retrieved,
            )
            for dataset_name in MARKET_DATASETS
        )
        target_tables = {
            outcome.mapping_result.target_table for outcome in outcomes
        }
        if target_tables != MARKET_TABLES:
            raise ValueError(
                "Market data source must map to exactly the four market tables"
            )

        suspension_artifact = self.ingestion_service.capture_json(
            source_name=self.source_name,
            dataset_name="rqdata_suspension_status",
            payload=bundle["rqdata_suspension_status"],
            received_at=retrieved,
        )
        return MarketDataBatch(
            outcomes=outcomes,
            trace_raw_artifacts=(suspension_artifact,),
        )
