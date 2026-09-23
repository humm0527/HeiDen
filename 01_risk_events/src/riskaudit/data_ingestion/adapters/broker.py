"""Exact broker risk-value mapping with no semantic inference."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import yaml

from ..models import PendingField


@dataclass(frozen=True)
class BrokerNormalizationResult:
    records: list[dict[str, Any]]
    pending_values: list[PendingField]

    @property
    def ready(self) -> bool:
        return not self.pending_values


class BrokerRiskValueMapper:
    """Map only an exact (broker, raw value) pair from a versioned file."""

    def __init__(
        self,
        *,
        mapping_version: str,
        broker_mappings: dict[str, dict[str, str]],
    ) -> None:
        self.mapping_version = mapping_version
        self.broker_mappings = broker_mappings

    @classmethod
    def from_file(cls, path: str | Path) -> "BrokerRiskValueMapper":
        with Path(path).open(encoding="utf-8") as stream:
            config = yaml.safe_load(stream)
        broker_mappings = {
            broker_id: dict(details["mappings"])
            for broker_id, details in config["brokers"].items()
        }
        return cls(
            mapping_version=config["mapping_version"],
            broker_mappings=broker_mappings,
        )

    def normalize(
        self,
        records: Iterable[dict[str, Any]],
        *,
        broker_field: str = "broker_id",
        raw_value_field: str = "raw_classification",
    ) -> BrokerNormalizationResult:
        normalized: list[dict[str, Any]] = []
        pending: list[PendingField] = []
        for record in records:
            item = dict(record)
            broker_id = str(item.get(broker_field, ""))
            raw_value = str(item.get(raw_value_field, ""))
            mapped_value = self.broker_mappings.get(broker_id, {}).get(raw_value)
            if mapped_value is None:
                pending.append(
                    PendingField(
                        raw_field=f"{broker_id}:{raw_value}",
                        candidates=(),
                        reason="券商和原始分类值未精确命中已配置映射",
                    )
                )
                continue
            item["standardized_risk_bucket"] = mapped_value
            item["mapping_record_id"] = (
                f"{self.mapping_version}:{broker_id}:{raw_value}"
            )
            normalized.append(item)
        return BrokerNormalizationResult(normalized, pending)

