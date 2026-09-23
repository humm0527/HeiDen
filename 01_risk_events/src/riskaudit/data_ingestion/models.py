"""Value objects for ingestion, mapping lineage, and human confirmation."""
# 中间状态处理器：定义“数据从外部进入系统后，如何被描述、追踪、等待人工确认”的数据结构，连接外部数据和内部标准数据模型。

# 这个文件定义了“金融数据接入与字段映射过程中的中间数据结构”，用于记录字段如何映射、哪些字段需要人工确认、原始数据来源以及最终映射结果，为后续标准化转换、数据预检和Agent辅助确认提供基础。
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class FieldCandidate:
    business_field: str
    python_field: str

    def to_dict(self) -> dict[str, str]:
        return {"业务字段": self.business_field, "Python字段": self.python_field}


@dataclass(frozen=True)
class PendingField:
    raw_field: str
    candidates: tuple[FieldCandidate, ...]
    reason: str
    status: str = "需要人工确认"

    def to_dict(self) -> dict[str, Any]:
        return {
            "字段": self.raw_field,
            "候选": [candidate.to_dict() for candidate in self.candidates],
            "原因": self.reason,
            "状态": self.status,
        }


@dataclass(frozen=True)
class FieldLineage:
    raw_field: str | None
    business_field: str
    python_field: str
    match_method: str


@dataclass(frozen=True)
class RawArtifact:
    source_name: str
    dataset_name: str
    data_path: Path
    metadata_path: Path
    sha256: str
    received_at: str


@dataclass
class MappingResult:
    target_table: str
    business_data: pd.DataFrame
    code_data: pd.DataFrame
    lineage: list[FieldLineage] = field(default_factory=list)
    missing_required_fields: list[str] = field(default_factory=list)
    unknown_fields: list[str] = field(default_factory=list)
    pending_fields: list[PendingField] = field(default_factory=list)

    @property
    def ready(self) -> bool:
        return not self.missing_required_fields and not self.pending_fields

    def confirmation_payload(self) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self.pending_fields]

