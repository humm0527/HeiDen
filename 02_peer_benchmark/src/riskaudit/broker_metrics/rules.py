"""
文件作用：只加载不可覆盖的 approved 券商指标规则和精确券商映射。
编辑记录：
【首次生成：2026-08-12，建立规则版本、周末忽略策略、SHA-256 与映射精确性门禁。】
【二次编辑内容：2026-08-12，支持 D-068 approved v5/v3 纠正映射，同时保留 v4/v2 历史批次可复现性。】
【三次编辑内容：2026-08-12，支持 D-069 approved v6 单一中文最终汇总输出。】
"""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path

import yaml

from .models import BrokerMappingBook, BrokerMetricRules


def _approved_payload(
    path: str | Path, *, require_approved_directory: bool
) -> tuple[Path, bytes, dict]:
    resolved = Path(path).resolve()
    if require_approved_directory and "approved" not in {
        part.lower() for part in resolved.parts
    }:
        raise ValueError("Formal broker metrics can only load approved configuration")
    raw = resolved.read_bytes()
    payload = yaml.safe_load(raw.decode("utf-8"))
    if not isinstance(payload, dict) or payload.get("status") != "approved":
        raise ValueError("Broker metric configuration status must be approved")
    return resolved, raw, payload


def load_broker_metric_rules(path: str | Path) -> BrokerMetricRules:
    _, raw, payload = _approved_payload(path, require_approved_directory=True)
    version = str(payload.get("rule_version", ""))
    expected_previous = {"v4": "v3", "v5": "v4", "v6": "v5"}
    if version not in expected_previous or payload.get("supersedes") != expected_previous[version]:
        raise ValueError("Broker metric implementation requires approved rules v4, v5, or v6")
    weekend = payload["broker_classification"]["weekend_source_rows"]
    if weekend.get("action") != "IGNORE_ROW_BEFORE_CLASSIFICATION_STATE_CHAIN":
        raise ValueError("Approved weekend history action is not frozen")
    forbidden_true = (
        "creates_classification_state",
        "creates_unknown_state",
        "breaks_dangerous_run_continuity",
        "resets_dangerous_run_start",
        "participates_in_pit_selection",
        "participates_in_warning_rate_snapshot",
    )
    if any(bool(weekend.get(name)) for name in forbidden_true):
        raise ValueError("Weekend rows must not affect the classification state chain")
    metrics = payload["metrics"]
    views = tuple(metrics["warning_days_distribution"]["event_type_views"])
    return BrokerMetricRules(
        rule_version=version,
        rule_sha256=sha256(raw).hexdigest(),
        mapping_version=str(payload["broker_classification"]["mapping_version"]),
        csv_schema=str(payload["outputs"]["broker_metric_results"]["csv_schema"]),
        weekend_action=str(weekend["action"]),
        weekend_breaks_continuity=bool(weekend["breaks_dangerous_run_continuity"]),
        event_type_views=views,
        decimal_scale=int(metrics["warning_days_distribution"]["median_mean_decimal_scale"]),
    )


def load_broker_mapping(path: str | Path) -> BrokerMappingBook:
    resolved, _, payload = _approved_payload(
        path, require_approved_directory=False
    )
    version = str(payload.get("mapping_version", ""))
    if resolved.name != f"broker_mapping_{version}.yaml" or resolved.parent.name != "broker_mapping":
        raise ValueError("Formal broker metrics require a versioned broker mapping")
    if version not in {"v2", "v3"}:
        raise ValueError("Broker metric implementation requires broker mapping v2 or v3")
    brokers = payload.get("brokers", {})
    mappings = {
        str(broker): {str(raw): str(bucket) for raw, bucket in data["mappings"].items()}
        for broker, data in brokers.items()
    }
    unmapped = frozenset(
        str(value) for value in payload["unmapped_broker_fields"]["fields"]
    )
    return BrokerMappingBook(
        mapping_version=version,
        mappings=mappings,
        unmapped_broker_fields=unmapped,
    )
