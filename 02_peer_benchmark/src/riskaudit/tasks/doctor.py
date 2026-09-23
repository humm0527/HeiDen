"""Read-only environment diagnostics for portable RiskAudit execution."""

from __future__ import annotations

from importlib import metadata
import json
from pathlib import Path
import platform
import sys
from typing import Any, Mapping

from riskaudit.runtime_config import RuntimeSettings


REQUIRED_DISTRIBUTIONS = (
    "bcrypt",
    "duckdb",
    "openpyxl",
    "pandas",
    "python-multipart",
    "PyYAML",
)

BASE_REQUIRED_FILES = ("pyproject.toml",)
TASK_REQUIRED_FILES: dict[str, tuple[str, ...]] = {
    "validate-input": ("configs/mapping/business_field_registry_v1.yaml",),
    "risk-events": (
        "configs/rules/approved/rules_v2.yaml",
        "configs/rules/approved/limit_down_pressure_v1.yaml",
        "configs/mapping/business_field_registry_v1.yaml",
        "configs/validation/packs/broker_risk_universe_v1.yaml",
    ),
    "peer-benchmark": (
        "configs/rules/approved/rules_v6.yaml",
        "configs/broker_mapping/broker_mapping_v3.yaml",
    ),
    "single-model": (
        "configs/rules/approved/rules_v6.yaml",
        "configs/broker_mapping/broker_mapping_v3.yaml",
    ),
    "pressure-observation": (
        "configs/rules/approved/rules_v6.yaml",
        "configs/broker_mapping/broker_mapping_v3.yaml",
    ),
    "model-comparison": (),
    "descriptive-comparison": (),
    "period-report": (),
}


def _bundle_task_type(root: Path) -> str | None:
    metadata_path = root / "module.json"
    if not metadata_path.is_file():
        return None
    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    task_type = str(payload.get("task") or "").strip()
    return task_type or None


def required_project_files(
    project_root: str | Path,
    task_type: str | None = None,
) -> tuple[Path, ...]:
    """Return only the project files required by the selected task bundle."""

    root = Path(project_root).resolve()
    selected_task = task_type or _bundle_task_type(root)
    if selected_task is not None and selected_task not in TASK_REQUIRED_FILES:
        raise ValueError(f"未知任务类型：{selected_task}")
    task_files = (
        TASK_REQUIRED_FILES[selected_task]
        if selected_task is not None
        else tuple(
            dict.fromkeys(
                path
                for paths in TASK_REQUIRED_FILES.values()
                for path in paths
            )
        )
    )
    return tuple(root / path for path in (*BASE_REQUIRED_FILES, *task_files))


def environment_report(
    project_root: str | Path,
    environment: Mapping[str, str] | None = None,
    task_type: str | None = None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    selected_task = task_type or _bundle_task_type(root)
    settings = RuntimeSettings.from_environment(root, environment)
    dependencies = []
    for distribution in REQUIRED_DISTRIBUTIONS:
        try:
            version = metadata.version(distribution)
            status = "INSTALLED"
        except metadata.PackageNotFoundError:
            version = None
            status = "MISSING"
        dependencies.append(
            {"distribution": distribution, "version": version, "status": status}
        )
    python_ready = sys.version_info >= (3, 11)
    required_files = required_project_files(root, selected_task)
    missing_files = [str(path.relative_to(root)) for path in required_files if not path.is_file()]
    ready = (
        python_ready
        and not missing_files
        and all(item["status"] == "INSTALLED" for item in dependencies)
    )
    return {
        "status": "READY" if ready else "NOT_READY",
        "task": selected_task or "all",
        "python": {
            "version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "supported": python_ready,
            "minimum": "3.11",
        },
        "platform": platform.platform(),
        "dependencies": dependencies,
        "required_project_files": [
            str(path.relative_to(root)) for path in required_files
        ],
        "missing_project_files": missing_files,
        "project_root": str(root),
        "data_root": str(settings.data_root),
        "output_root": str(settings.output_root),
        "market_source": settings.market_source_mode,
        "rqdata_required_for_existing_snapshot_tasks": False,
        "gpu_required": False,
    }
