"""
文件作用：逐券商、逐交易日生成 D-053 预警率所需的自身危险档证券集合。
编辑记录：
【首次生成：2026-08-12，实现分类携带、ST 覆盖、生命周期过滤与未知证券计数。】
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Iterable, Mapping

from .classification import resolve_final_classification
from .models import (
    BrokerClassificationRecord,
    BrokerMappingBook,
    DangerousExposure,
    ExposureBuildResult,
)


Lifecycle = Mapping[tuple[str, str], tuple[date, date | None]]
StStates = Mapping[tuple[str, str, date], bool | None]


def build_dangerous_exposures(
    records: Iterable[BrokerClassificationRecord],
    broker_ids: Iterable[str],
    snapshot_dates: Iterable[date],
    securities: Iterable[tuple[str, str]],
    st_states: StStates,
    lifecycle: Lifecycle,
    mapping: BrokerMappingBook,
) -> ExposureBuildResult:
    record_index: dict[
        tuple[str, str, str, date], BrokerClassificationRecord
    ] = {}
    for record in records:
        key = (
            record.broker_id,
            record.market_code,
            record.security_code,
            record.classification_date,
        )
        if key in record_index:
            raise ValueError(f"BM304_PIT_VERSION_CONFLICT: {key}")
        record_index[key] = record
    exposures: list[DangerousExposure] = []
    unknown_counts: dict[tuple[str, date], int] = defaultdict(int)
    for broker_id in sorted(set(broker_ids)):
        for market_code, security_code in sorted(set(securities)):
            life = lifecycle.get((market_code, security_code))
            if life is None:
                raise ValueError(
                    f"BM204_LIFECYCLE_EVIDENCE_MISSING: {market_code} {security_code}"
                )
            listed_date, delisted_date = life
            active: BrokerClassificationRecord | None = None
            for snapshot_date in sorted(set(snapshot_dates)):
                if snapshot_date < listed_date or (
                    delisted_date is not None and snapshot_date >= delisted_date
                ):
                    continue
                new_record = record_index.get(
                    (broker_id, market_code, security_code, snapshot_date)
                )
                if new_record is not None:
                    active = new_record
                final = resolve_final_classification(
                    active,
                    st_states.get((market_code, security_code, snapshot_date), None),
                    mapping,
                )
                if final.final_bucket == "DANGEROUS":
                    exposures.append(
                        DangerousExposure(
                            broker_id,
                            snapshot_date,
                            market_code,
                            security_code,
                        )
                    )
                elif final.final_bucket == "UNKNOWN":
                    unknown_counts[(broker_id, snapshot_date)] += 1
    exposures.sort(
        key=lambda item: (
            item.broker_id,
            item.snapshot_date,
            item.market_code,
            item.security_code,
        )
    )
    return ExposureBuildResult(tuple(exposures), dict(unknown_counts))
