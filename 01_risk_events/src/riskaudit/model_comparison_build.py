"""Build deterministic, auditable results for an explicit new/legacy model pair."""

from __future__ import annotations

from collections import Counter
import csv
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import quote

from riskaudit.model_comparison_results import comparison_bool, comparison_float


@dataclass(frozen=True)
class ModelComparisonBuildContext:
    output_root: Path
    threshold_comparison: Callable[..., dict[str, Any]]
    descriptive_tables_builder: Callable[..., dict[str, Any]]
    file_sha256: Callable[[str | Path], str]
    write_json: Callable[[Path, dict[str, Any]], None]
    now: Callable[[], datetime]


def _discover_frozen_artifact(
    *,
    output_root: Path,
    collection: str,
    manifest_name: str,
    source_sha256: str,
    date_field: str,
    expected_date: str,
    file_sha256: Callable[[str | Path], str],
) -> Path | None:
    manifests = sorted(
        (output_root / collection).glob(f"*/{manifest_name}"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for manifest_path in manifests:
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            artifact = Path(str(manifest.get("output_path") or ""))
            if (
                manifest.get("status") == "SUCCEEDED"
                and manifest.get("source_sha256") == source_sha256
                and str(manifest.get(date_field) or "") == expected_date
                and artifact.is_file()
                and manifest.get("output_sha256") == file_sha256(artifact)
            ):
                return artifact
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue
    return None


def comparison_csv_rows(path: Path, key_field: str) -> dict[str, dict[str, str]]:
    if not path.is_file():
        raise FileNotFoundError(f"比较输入不存在: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    if rows and key_field not in rows[0]:
        raise ValueError(f"CMP120_COMPARISON_SCHEMA_INVALID: 缺少比较主键字段 {key_field}")
    keyed = {str(row.get(key_field) or "").strip(): row for row in rows}
    if "" in keyed or len(keyed) != len(rows):
        raise ValueError(f"CMP120_COMPARISON_SCHEMA_INVALID: {key_field} 为空或重复")
    return keyed


def risk_event_keys(path: Path) -> set[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        required = ("交易市场代码", "证券代码", "事件类型", "首次事实日期")
        if not reader.fieldnames or not set(required) <= set(reader.fieldnames):
            raise ValueError("CMP120_COMPARISON_SCHEMA_INVALID: 风险事件文件缺少稳定键字段")
        return {
            "|".join(str(row.get(field) or "").strip() for field in required)
            for row in reader
        }


def migration_summary(
    old_rows: Mapping[str, Mapping[str, Any]],
    new_rows: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    counts = Counter(
        (
            str(old_rows[key].get("原始分类值") or "UNKNOWN"),
            str(new_rows[key].get("原始分类值") or "UNKNOWN"),
        )
        for key in sorted(old_rows)
    )
    return [
        {"from": source, "to": target, "count": count}
        for (source, target), count in sorted(counts.items())
    ]


def build_model_comparison_result(
    parent: dict[str, Any],
    new_job: dict[str, Any],
    legacy_job: dict[str, Any],
    *,
    context: ModelComparisonBuildContext,
) -> dict[str, Any]:
    """Enforce the shared contract and materialize deterministic pairwise deltas."""

    gates: list[dict[str, Any]] = []

    def require_equal(code: str, label: str, left: Any, right: Any) -> None:
        if left != right:
            raise ValueError(f"{code}: {label}不一致（new={left!r}, legacy={right!r}）")
        gates.append({"code": code, "label": label, "status": "PASS", "value": left})

    require_equal(
        "CMP101_SNAPSHOT_ID_MATCH",
        "市场快照标识",
        new_job.get("market_snapshot_id"),
        legacy_job.get("market_snapshot_id"),
    )
    require_equal(
        "CMP102_SNAPSHOT_HASH_MATCH",
        "市场快照成员哈希",
        new_job.get("market_snapshot_member_hash"),
        legacy_job.get("market_snapshot_member_hash"),
    )
    require_equal(
        "CMP103_QUARTER_MATCH",
        "目标季度",
        new_job.get("quarter"),
        legacy_job.get("quarter"),
    )
    require_equal(
        "CMP104_OBSERVATION_START_MATCH",
        "观察开始日",
        new_job.get("observation_start"),
        legacy_job.get("observation_start"),
    )
    require_equal(
        "CMP105_OBSERVATION_END_MATCH",
        "观察结束日",
        new_job.get("observation_end"),
        legacy_job.get("observation_end"),
    )
    new_universe_hashes = list(new_job.get("risk_universe_override_sha256") or [])
    legacy_universe_hashes = list(
        legacy_job.get("risk_universe_override_sha256") or []
    )
    if not new_universe_hashes or not legacy_universe_hashes:
        raise ValueError("CMP106_RISK_UNIVERSE_MATCH: 子任务缺少共享风险证券全集哈希")
    require_equal(
        "CMP106_RISK_UNIVERSE_MATCH",
        "共享风险证券全集哈希",
        new_universe_hashes,
        legacy_universe_hashes,
    )

    new_id = str(new_job["job_id"])
    legacy_id = str(legacy_job["job_id"])
    new_event_dir = context.output_root / new_id / "risk_results" / new_id
    legacy_event_dir = context.output_root / legacy_id / "risk_results" / legacy_id
    new_event_manifest = json.loads(
        (new_event_dir / "calculation_manifest.json").read_text(encoding="utf-8")
    )
    legacy_event_manifest = json.loads(
        (legacy_event_dir / "calculation_manifest.json").read_text(encoding="utf-8")
    )
    for field, label in (
        ("rule_version", "风险规则版本"),
        ("rule_sha256", "风险规则哈希"),
    ):
        require_equal(
            f"CMP107_EVENT_{field.upper()}_MATCH",
            label,
            new_event_manifest.get(field),
            legacy_event_manifest.get(field),
        )
    new_event_keys = risk_event_keys(new_event_dir / "risk_events.csv")
    legacy_event_keys = risk_event_keys(legacy_event_dir / "risk_events.csv")
    require_equal(
        "CMP108_EVENT_KEYS_MATCH",
        "风险事件稳定键集合",
        sorted(new_event_keys),
        sorted(legacy_event_keys),
    )

    new_metric_dir = Path(
        str(new_job.get("result", {}).get("broker_metric_output_dir") or "")
    )
    legacy_metric_dir = Path(
        str(legacy_job.get("result", {}).get("broker_metric_output_dir") or "")
    )
    new_metric_manifest = json.loads(
        (new_metric_dir / "broker_metric_manifest.json").read_text(encoding="utf-8")
    )
    legacy_metric_manifest = json.loads(
        (legacy_metric_dir / "broker_metric_manifest.json").read_text(encoding="utf-8")
    )
    for field, label in (
        ("rule_version", "指标规则版本"),
        ("rule_sha256", "指标规则哈希"),
        ("mapping_version", "分类映射版本"),
    ):
        require_equal(
            f"CMP109_METRIC_{field.upper()}_MATCH",
            label,
            new_metric_manifest.get(field),
            legacy_metric_manifest.get(field),
        )

    new_rows = comparison_csv_rows(
        new_metric_dir / "event_broker_assessments.csv", "事件唯一键"
    )
    legacy_rows = comparison_csv_rows(
        legacy_metric_dir / "event_broker_assessments.csv", "事件唯一键"
    )
    require_equal(
        "CMP110_FORMAL_ASSESSMENT_KEYS_MATCH",
        "正式门槛评价事件集合",
        sorted(new_rows),
        sorted(legacy_rows),
    )
    differences: list[dict[str, Any]] = []
    new_warning_values: list[float] = []
    legacy_warning_values: list[float] = []
    gained = lost = warning_changed = 0
    for key in sorted(new_rows):
        new_row = new_rows[key]
        legacy_row = legacy_rows[key]
        new_hit = comparison_bool(new_row.get("是否命中"))
        legacy_hit = comparison_bool(legacy_row.get("是否命中"))
        new_warning = comparison_float(new_row.get("预警交易日数"))
        legacy_warning = comparison_float(legacy_row.get("预警交易日数"))
        if new_warning is not None:
            new_warning_values.append(new_warning)
        if legacy_warning is not None:
            legacy_warning_values.append(legacy_warning)
        gained += int(new_hit and not legacy_hit)
        lost += int(legacy_hit and not new_hit)
        warning_changed += int(new_warning != legacy_warning)
        if (
            new_hit != legacy_hit
            or new_warning != legacy_warning
            or new_row.get("原始分类值") != legacy_row.get("原始分类值")
        ):
            differences.append(
                {
                    "event_key": key,
                    "security_code": new_row.get("证券代码"),
                    "event_type": new_row.get("风险事件类型"),
                    "new_classification": new_row.get("原始分类值"),
                    "legacy_classification": legacy_row.get("原始分类值"),
                    "new_hit": new_hit,
                    "legacy_hit": legacy_hit,
                    "new_warning_days": new_warning,
                    "legacy_warning_days": legacy_warning,
                    "warning_days_delta": (
                        round(new_warning - legacy_warning, 6)
                        if new_warning is not None and legacy_warning is not None
                        else None
                    ),
                }
            )

    new_pressure_dir = Path(
        str(new_job.get("result", {}).get("pressure_metric_output_dir") or "")
    )
    legacy_pressure_dir = Path(
        str(legacy_job.get("result", {}).get("pressure_metric_output_dir") or "")
    )
    new_pressure = comparison_csv_rows(
        new_pressure_dir / "pressure_event_assessments.csv", "事件唯一键"
    )
    legacy_pressure = comparison_csv_rows(
        legacy_pressure_dir / "pressure_event_assessments.csv", "事件唯一键"
    )
    require_equal(
        "CMP111_PRESSURE_ASSESSMENT_KEYS_MATCH",
        "低门槛观察事件集合",
        sorted(new_pressure),
        sorted(legacy_pressure),
    )
    left_manifest = json.loads(
        (new_pressure_dir / "pressure_classification_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    right_manifest = json.loads(
        (legacy_pressure_dir / "pressure_classification_manifest.json").read_text(
            encoding="utf-8"
        )
    )
    require_equal(
        "CMP112_PRESSURE_RULE_MATCH",
        "低门槛指标规则与映射版本",
        (left_manifest.get("metric_rule_version"), left_manifest.get("mapping_version")),
        (right_manifest.get("metric_rule_version"), right_manifest.get("mapping_version")),
    )
    threshold_comparison = context.threshold_comparison(
        new_id,
        legacy_id,
        comparison_id=str(parent["job_id"]),
        new_job_payload=new_job,
        old_job_payload=legacy_job,
    )
    if not threshold_comparison.get("available"):
        raise ValueError(
            "CMP113_THRESHOLD_COMPARISON_UNAVAILABLE: "
            + str(threshold_comparison.get("reason") or "严格/低门槛结果不可用")
        )

    event_count = len(new_rows)
    new_hit_count = sum(
        comparison_bool(row.get("是否命中")) for row in new_rows.values()
    )
    legacy_hit_count = sum(
        comparison_bool(row.get("是否命中")) for row in legacy_rows.values()
    )

    def average(values: list[float]) -> float | None:
        return round(sum(values) / len(values), 6) if values else None

    result_dir = context.output_root / "model_comparisons" / str(parent["job_id"])
    result_dir.mkdir(parents=True, exist_ok=True)
    detail_path = result_dir / "event_differences.csv"
    detail_fields = (
        "event_key",
        "security_code",
        "event_type",
        "new_classification",
        "legacy_classification",
        "new_hit",
        "legacy_hit",
        "new_warning_days",
        "legacy_warning_days",
        "warning_days_delta",
    )
    with detail_path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=detail_fields)
        writer.writeheader()
        writer.writerows(differences)

    new_child_result = dict(new_job.get("result") or {})
    legacy_child_result = dict(legacy_job.get("result") or {})
    snapshot_export_root = Path(
        str(new_child_result.get("snapshot_export_root") or "")
    )
    market_price_candidate = snapshot_export_root / "股票日行情表.csv"
    market_cap_candidates = (
        Path(str(parent.get("market_cap_csv") or "")),
        Path(str(new_child_result.get("market_cap_csv") or "")),
        snapshot_export_root / "股票总市值表.csv",
        snapshot_export_root / "股票市值表.csv",
    )
    market_cap_candidate = next(
        (path for path in market_cap_candidates if str(path) and path.is_file()),
        None,
    )
    adjusted_price_candidates = (
        Path(str(parent.get("adjusted_market_price_csv") or "")),
        Path(str(new_child_result.get("adjusted_market_price_csv") or "")),
    )
    adjusted_price_candidate = next(
        (path for path in adjusted_price_candidates if str(path) and path.is_file()),
        None,
    )
    observation_end = str(
        parent.get("observation_end") or new_job["observation_end"]
    )[:10]
    if market_price_candidate.is_file() and (
        market_cap_candidate is None or adjusted_price_candidate is None
    ):
        source_sha256 = context.file_sha256(market_price_candidate)
        if market_cap_candidate is None:
            market_cap_candidate = _discover_frozen_artifact(
                output_root=context.output_root,
                collection="market_caps",
                manifest_name="总市值清单.json",
                source_sha256=source_sha256,
                date_field="snapshot_date",
                expected_date=observation_end,
                file_sha256=context.file_sha256,
            )
        if adjusted_price_candidate is None:
            adjusted_price_candidate = _discover_frozen_artifact(
                output_root=context.output_root,
                collection="adjusted_market_prices",
                manifest_name="后复权行情清单.json",
                source_sha256=source_sha256,
                date_field="end_date",
                expected_date=observation_end,
                file_sha256=context.file_sha256,
            )
    descriptive = context.descriptive_tables_builder(
        output_dir=result_dir / "descriptive_tables",
        observation_start=str(
            parent.get("observation_start") or new_job["observation_start"]
        ),
        observation_end=str(
            parent.get("observation_end") or new_job["observation_end"]
        ),
        new_history_files=dict(new_child_result.get("broker_history_files") or {}),
        legacy_history_files=dict(
            legacy_child_result.get("broker_history_files") or {}
        ),
        market_price_csv=(
            market_price_candidate if market_price_candidate.is_file() else None
        ),
        adjusted_market_price_csv=adjusted_price_candidate,
        market_cap_csv=market_cap_candidate,
    )
    descriptive_public = {
        key: value for key, value in descriptive.items() if key != "files"
    }
    descriptive_public["files"] = {
        key: {
            **dict(value),
            "url": (
                f"/data/outputs/model_comparisons/{quote(str(parent['job_id']))}"
                f"/descriptive_tables/{quote(str(value['filename']))}"
            ),
        }
        for key, value in dict(descriptive.get("files") or {}).items()
    }

    result = {
        "status": "SUCCEEDED",
        "comparison_batch_id": parent["job_id"],
        "comparison_contract_id": parent["comparison_contract_id"],
        "quarter": parent["quarter"],
        "new_batch_id": new_id,
        "legacy_batch_id": legacy_id,
        "market_snapshot_id": parent["market_snapshot_id"],
        "market_snapshot_member_hash": parent["market_snapshot_member_hash"],
        "consistency_gates": gates,
        "event_key_digest": sha256(
            "\n".join(sorted(new_event_keys)).encode("utf-8")
        ).hexdigest(),
        "overall": {
            "event_count": event_count,
            "new_hit_count": new_hit_count,
            "legacy_hit_count": legacy_hit_count,
            "hit_count_delta": new_hit_count - legacy_hit_count,
            "new_hit_rate": round(new_hit_count / event_count, 6) if event_count else None,
            "legacy_hit_rate": (
                round(legacy_hit_count / event_count, 6) if event_count else None
            ),
            "new_average_warning_days": average(new_warning_values),
            "legacy_average_warning_days": average(legacy_warning_values),
            "average_warning_days_delta": (
                round(average(new_warning_values) - average(legacy_warning_values), 6)
                if new_warning_values and legacy_warning_values
                else None
            ),
        },
        "changes": {
            "gained_hit_count": gained,
            "lost_hit_count": lost,
            "warning_days_changed_count": warning_changed,
            "event_difference_count": len(differences),
        },
        "strict_classification_migrations": migration_summary(legacy_rows, new_rows),
        "low_threshold_classification_migrations": migration_summary(
            legacy_pressure, new_pressure
        ),
        "threshold_comparison": threshold_comparison,
        "descriptive_tables": descriptive_public,
        "files": {
            "event_differences": str(detail_path),
            "event_differences_sha256": context.file_sha256(detail_path),
            "event_differences_url": (
                f"/data/outputs/model_comparisons/{quote(str(parent['job_id']))}"
                "/event_differences.csv"
            ),
        },
        "generated_at": context.now().isoformat(),
    }
    manifest_path = result_dir / "comparison_manifest.json"
    context.write_json(manifest_path, result)
    result["comparison_manifest_path"] = str(manifest_path)
    result["comparison_result_url"] = (
        f"/api/model-comparisons/{quote(str(parent['job_id']))}"
    )
    return result
