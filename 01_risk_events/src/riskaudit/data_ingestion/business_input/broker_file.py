"""
文件作用：读取券商分类CSV/Excel，保存原文件并生成券商风险分类标准表。
编辑记录：
- 首次生成：2026-08-05，实现业务文件读取、显式档位映射和标准化输出。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ..adapters.broker import BrokerRiskValueMapper
from ..mapping import MappingCatalog
from ..pipeline import DataIngestionService, IngestionOutcome
from ..readers import DataReader


BROKER_INPUT_DATASET = "broker_classification_file"
BROKER_TARGET_TABLE = "券商风险分类表"
REQUIRED_SOURCE_FIELDS = frozenset(
    {"券商名称", "证券代码", "交易日期", "风险等级"}
)


def build_business_input_mapping_catalog(
    repository_root: str | Path,
) -> MappingCatalog:
    """只加载券商业务输入映射，避免业务入口看到市场数据配置。"""

    root = Path(repository_root)
    return MappingCatalog.from_files(
        root / "configs/mapping/business_field_registry_v1.yaml",
        root / "configs/mapping/broker_json_v1.yaml",
        root / "configs/mapping/broker_file_v1.yaml",
    )


@dataclass(frozen=True)
class BrokerImportContext:
    """简单四字段文件之外必须由调用方明确提供的PIT与追溯信息。"""

    market_code: str
    source_snapshot_version: int
    classification_effective_at: str | datetime
    source_information_available_at: str | datetime
    mapping_effective_at: str | datetime
    mapping_generated_at: str | datetime
    ingested_at: str | datetime
    source_type: str
    source_snapshot_id: str

    def to_mapping_context(self, mapping_version: str) -> dict[str, Any]:
        return {
            "market_code": self.market_code,
            "source_snapshot_version": self.source_snapshot_version,
            "classification_mapping_version": mapping_version,
            "classification_effective_at": self.classification_effective_at,
            "source_information_available_at": (
                self.source_information_available_at
            ),
            "mapping_effective_at": self.mapping_effective_at,
            "mapping_generated_at": self.mapping_generated_at,
            "ingested_at": self.ingested_at,
            "source_type": self.source_type,
            "source_snapshot_id": self.source_snapshot_id,
        }


class BrokerBusinessInputService:
    """业务输入入口；只生成券商风险分类表，不接受市场数据集。"""

    def __init__(
        self,
        *,
        ingestion_service: DataIngestionService,
        value_mapper: BrokerRiskValueMapper,
    ) -> None:
        self.ingestion_service = ingestion_service
        self.value_mapper = value_mapper

    def import_file(
        self,
        path: str | Path,
        *,
        context: BrokerImportContext,
        source_name: str = "broker_business_file",
        sheet_name: str | int = 0,
        received_at: datetime | None = None,
    ) -> IngestionOutcome:
        """先原样保存CSV/Excel，再执行确定性字段及分类值映射。"""

        artifact = self.ingestion_service.raw_store.save_file(
            source_name,
            BROKER_INPUT_DATASET,
            path,
            received_at=received_at,
        )
        raw_data = DataReader.read_file(path, sheet_name=sheet_name)
        mapping_context = context.to_mapping_context(
            self.value_mapper.mapping_version
        )

        missing_source_fields = REQUIRED_SOURCE_FIELDS - set(raw_data.columns)
        if missing_source_fields:
            result = self.ingestion_service.mapper.map(
                BROKER_INPUT_DATASET,
                raw_data,
                context=mapping_context,
            )
            return IngestionOutcome(artifact, result)

        normalization = self.value_mapper.normalize(
            raw_data.to_dict(orient="records"),
            broker_field="券商名称",
            raw_value_field="风险等级",
        )
        if not normalization.ready:
            result = self.ingestion_service.mapper.map(
                BROKER_INPUT_DATASET,
                raw_data,
                context=mapping_context,
            )
            result.pending_fields.extend(normalization.pending_values)
            return IngestionOutcome(artifact, result)

        mapping_data = pd.DataFrame.from_records(normalization.records)
        result = self.ingestion_service.mapper.map(
            BROKER_INPUT_DATASET,
            mapping_data,
            context=mapping_context,
        )
        if result.target_table != BROKER_TARGET_TABLE:
            raise ValueError(
                "Business input source must map only to 券商风险分类表"
            )
        return IngestionOutcome(artifact, result)
