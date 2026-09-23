"""Standalone comparison of two frozen single-broker assessment outputs."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from statistics import mean
from typing import Any

from riskaudit.model_comparison_build import comparison_csv_rows, migration_summary
from riskaudit.model_comparison_results import comparison_bool, comparison_float


def _hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_model_comparison(
    *,
    new_assessments: str | Path,
    legacy_assessments: str | Path,
    output_dir: str | Path,
    new_pressure_assessments: str | Path | None = None,
    legacy_pressure_assessments: str | Path | None = None,
) -> dict[str, Any]:
    """Compare paired results using stable event keys and immutable inputs."""

    new_path = Path(new_assessments).resolve()
    legacy_path = Path(legacy_assessments).resolve()
    target = Path(output_dir).resolve()
    if target.exists():
        raise FileExistsError(f"比较输出目录已存在，禁止覆盖: {target}")
    new_rows = comparison_csv_rows(new_path, "事件唯一键")
    legacy_rows = comparison_csv_rows(legacy_path, "事件唯一键")
    if set(new_rows) != set(legacy_rows):
        raise ValueError("CMP110_FORMAL_ASSESSMENT_KEYS_MATCH: 新旧评价事件集合不一致")

    differences: list[dict[str, Any]] = []
    new_warnings: list[float] = []
    legacy_warnings: list[float] = []
    gained = lost = 0
    for event_key in sorted(new_rows):
        new_row = new_rows[event_key]
        legacy_row = legacy_rows[event_key]
        new_hit = comparison_bool(new_row.get("是否命中"))
        legacy_hit = comparison_bool(legacy_row.get("是否命中"))
        new_warning = comparison_float(new_row.get("预警交易日数"))
        legacy_warning = comparison_float(legacy_row.get("预警交易日数"))
        if new_warning is not None:
            new_warnings.append(new_warning)
        if legacy_warning is not None:
            legacy_warnings.append(legacy_warning)
        gained += int(new_hit and not legacy_hit)
        lost += int(legacy_hit and not new_hit)
        if (
            new_hit != legacy_hit
            or new_warning != legacy_warning
            or new_row.get("原始分类值") != legacy_row.get("原始分类值")
        ):
            differences.append(
                {
                    "事件唯一键": event_key,
                    "证券代码": new_row.get("证券代码"),
                    "风险事件类型": new_row.get("风险事件类型"),
                    "新模型分类": new_row.get("原始分类值"),
                    "旧模型分类": legacy_row.get("原始分类值"),
                    "新模型是否命中": new_hit,
                    "旧模型是否命中": legacy_hit,
                    "新模型预警交易日数": new_warning,
                    "旧模型预警交易日数": legacy_warning,
                    "预警天数变化_新减旧": (
                        round(new_warning - legacy_warning, 6)
                        if new_warning is not None and legacy_warning is not None
                        else None
                    ),
                }
            )

    pressure_migrations: list[dict[str, Any]] | None = None
    pressure_sources: list[Path] = []
    if new_pressure_assessments or legacy_pressure_assessments:
        if not new_pressure_assessments or not legacy_pressure_assessments:
            raise ValueError("低门槛比较必须同时提供新旧两份评价明细")
        new_pressure_path = Path(new_pressure_assessments).resolve()
        legacy_pressure_path = Path(legacy_pressure_assessments).resolve()
        new_pressure = comparison_csv_rows(new_pressure_path, "事件唯一键")
        legacy_pressure = comparison_csv_rows(legacy_pressure_path, "事件唯一键")
        if set(new_pressure) != set(legacy_pressure):
            raise ValueError("CMP111_PRESSURE_ASSESSMENT_KEYS_MATCH: 新旧压力事件集合不一致")
        pressure_migrations = migration_summary(legacy_pressure, new_pressure)
        pressure_sources = [new_pressure_path, legacy_pressure_path]

    summary = {
        "status": "SUCCEEDED",
        "event_count": len(new_rows),
        "changed_event_count": len(differences),
        "new_hit_count": sum(
            comparison_bool(row.get("是否命中")) for row in new_rows.values()
        ),
        "legacy_hit_count": sum(
            comparison_bool(row.get("是否命中")) for row in legacy_rows.values()
        ),
        "gained_hit_count": gained,
        "lost_hit_count": lost,
        "new_warning_days_mean": round(mean(new_warnings), 6) if new_warnings else None,
        "legacy_warning_days_mean": (
            round(mean(legacy_warnings), 6) if legacy_warnings else None
        ),
        "formal_classification_migrations": migration_summary(legacy_rows, new_rows),
        "pressure_classification_migrations": pressure_migrations,
    }

    target.mkdir(parents=True, exist_ok=False)
    difference_path = target / "event_differences.csv"
    fields = list(differences[0]) if differences else [
        "事件唯一键",
        "证券代码",
        "风险事件类型",
        "新模型分类",
        "旧模型分类",
        "新模型是否命中",
        "旧模型是否命中",
        "新模型预警交易日数",
        "旧模型预警交易日数",
        "预警天数变化_新减旧",
    ]
    with difference_path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(differences)
    summary_path = target / "comparison_summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    sources = [new_path, legacy_path, *pressure_sources]
    manifest = {
        "status": "SUCCEEDED",
        "task_type": "model-comparison",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "sources": [
            {"filename": path.name, "sha256": _hash(path)} for path in sources
        ],
        "outputs": [
            {"filename": difference_path.name, "sha256": _hash(difference_path)},
            {"filename": summary_path.name, "sha256": _hash(summary_path)},
        ],
    }
    manifest_path = target / "comparison_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return {**summary, "output_dir": str(target), "manifest_path": str(manifest_path)}
