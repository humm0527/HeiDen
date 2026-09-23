"""
文件作用：按 approved v2 精确映射和 D-051 优先级合成券商最终日分类。
编辑记录：
【首次生成：2026-08-12，实现 ST 优先、空白一般股票、未映射未知和无分类状态。】
"""

from __future__ import annotations

from .models import (
    BrokerClassificationRecord,
    BrokerMappingBook,
    FinalClassification,
)


def resolve_final_classification(
    record: BrokerClassificationRecord | None,
    st_state: bool | None,
    mapping: BrokerMappingBook,
) -> FinalClassification:
    if st_state is None:
        return FinalClassification("UNKNOWN", "ST_STATUS_MISSING")
    if st_state:
        mapped, record_id = _mapped(record, mapping)
        return FinalClassification(
            "DANGEROUS",
            "ST_OVERRIDE",
            mapping_record_id=record_id,
            mapped_bucket=mapped,
        )
    if record is None:
        return FinalClassification(None, "NO_USABLE_CLASSIFICATION")
    if record.raw_classification == "":
        return FinalClassification("NON_DANGEROUS", "BLANK_GENERAL")
    mapped, record_id = mapping.map_value(record.broker_id, record.raw_classification)
    if mapped is None:
        return FinalClassification("UNKNOWN", "MAPPING_UNKNOWN")
    return FinalClassification(
        mapped,
        "EXACT_MAPPING",
        mapping_record_id=record_id,
        mapped_bucket=mapped,
    )


def _mapped(
    record: BrokerClassificationRecord | None,
    mapping: BrokerMappingBook,
) -> tuple[str | None, str]:
    if record is None or not record.raw_classification:
        return None, ""
    return mapping.map_value(record.broker_id, record.raw_classification)
