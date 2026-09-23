"""
文件作用：编排 Raw 保存、显式字段映射和标准业务表持久化，并生成可追溯运行清单。
编辑记录：
【首次生成：2026-08-04，建立文件/JSON 接入、字段映射和标准运行保存流程。】
【二次编辑内容：2026-08-07，在 manifest 的每个 Raw 资产中直接记录 received_at，避免 PIT 下载时间识别依赖路径解析。】
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from .mapping import FieldMapper
from .models import MappingResult, RawArtifact
from .readers import DataReader
from .storage import RawStore, StandardStore


@dataclass(frozen=True)
class IngestionOutcome:
    raw_artifact: RawArtifact
    mapping_result: MappingResult


class DataIngestionService:
    """Persist raw input first, then apply an explicit source mapping."""

    def __init__(
        self,
        mapper: FieldMapper,
        raw_store: RawStore,
        standard_store: StandardStore,
    ) -> None:
        self.mapper = mapper
        self.raw_store = raw_store
        self.standard_store = standard_store

    def ingest_file(
        self,
        *,
        source_name: str,
        dataset_name: str,
        path: str | Path,
        context: dict[str, Any] | None = None,
        sheet_name: str | int = 0,
        received_at: datetime | None = None,
    ) -> IngestionOutcome:
        artifact = self.raw_store.save_file(
            source_name,
            dataset_name,
            path,
            received_at=received_at,
        )
        raw_data = DataReader.read_file(path, sheet_name=sheet_name)
        result = self.mapper.map(dataset_name, raw_data, context=context)
        return IngestionOutcome(artifact, result)

    def ingest_json(
        self,
        *,
        source_name: str,
        dataset_name: str,
        payload: Any,
        mapping_payload: Any | None = None,
        context: dict[str, Any] | None = None,
        received_at: datetime | None = None,
    ) -> IngestionOutcome:
        artifact = self.raw_store.save_json(
            source_name,
            dataset_name,
            payload,
            received_at=received_at,
        )
        raw_data = DataReader.read_json(
            payload if mapping_payload is None else mapping_payload
        )
        result = self.mapper.map(dataset_name, raw_data, context=context)
        return IngestionOutcome(artifact, result)

    def capture_json(
        self,
        *,
        source_name: str,
        dataset_name: str,
        payload: Any,
        received_at: datetime | None = None,
    ) -> RawArtifact:
        """Persist a trace-only Raw resource that has no standalone target table."""

        return self.raw_store.save_json(
            source_name,
            dataset_name,
            payload,
            received_at=received_at,
        )

    def save_standard_run(
        self,
        run_id: str,
        outcomes: Iterable[IngestionOutcome],
        *,
        additional_raw_artifacts: Iterable[RawArtifact] = (),
    ) -> Path:
        outcome_list = list(outcomes)
        additional_artifacts = list(additional_raw_artifacts)
        blocked = [
            outcome.mapping_result.target_table
            for outcome in outcome_list
            if not outcome.mapping_result.ready
        ]
        if blocked:
            raise ValueError(
                "Standard output is blocked by missing or pending mappings: "
                + ", ".join(blocked)
            )

        tables = {
            outcome.mapping_result.target_table: outcome.mapping_result.business_data
            for outcome in outcome_list
        }
        if len(tables) != len(outcome_list):
            raise ValueError("Each standard run must contain at most one input per table")

        manifest = {
            "raw_artifacts": [
                {
                    "source_name": outcome.raw_artifact.source_name,
                    "dataset_name": outcome.raw_artifact.dataset_name,
                    "sha256": outcome.raw_artifact.sha256,
                    "raw_data_path": str(outcome.raw_artifact.data_path),
                    "raw_metadata_path": str(outcome.raw_artifact.metadata_path),
                    "received_at": outcome.raw_artifact.received_at,
                }
                for outcome in outcome_list
            ]
            + [
                {
                    "source_name": artifact.source_name,
                    "dataset_name": artifact.dataset_name,
                    "sha256": artifact.sha256,
                    "raw_data_path": str(artifact.data_path),
                    "raw_metadata_path": str(artifact.metadata_path),
                    "received_at": artifact.received_at,
                }
                for artifact in additional_artifacts
            ],
            "field_lineage": {
                outcome.mapping_result.target_table: [
                    {
                        "raw_field": item.raw_field,
                        "business_field": item.business_field,
                        "python_field": item.python_field,
                        "match_method": item.match_method,
                    }
                    for item in outcome.mapping_result.lineage
                ]
                for outcome in outcome_list
            },
            "mapping_results": {
                outcome.mapping_result.target_table: {
                    "ready": outcome.mapping_result.ready,
                    "missing_required_fields": outcome.mapping_result.missing_required_fields,
                    "unknown_fields": outcome.mapping_result.unknown_fields,
                    "pending_fields": outcome.mapping_result.confirmation_payload(),
                }
                for outcome in outcome_list
            },
        }
        return self.standard_store.save_tables(run_id, tables, manifest=manifest)
