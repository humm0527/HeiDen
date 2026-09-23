"""Portable task configuration and anchored path resolution."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Any, Mapping

from riskaudit.runtime_config import RuntimeSettings


_WINDOWS_ABSOLUTE = re.compile(r"^[A-Za-z]:[\\/]")
_PATH_FIELDS = frozenset(
    {
        "input_dir",
        "output_dir",
        "output_root",
        "market_lake_root",
        "business_file",
        "uploaded_history_file",
        "risk_input_manifest",
        "rules_path",
        "mapping_path",
        "registry_path",
        "event_csv",
        "event_manifest",
        "history_dir",
        "calendar_csv",
        "st_csv",
        "lifecycle_csv",
        "universe_csv",
        "market_price_csv",
        "observation_csv",
        "new_assessments",
        "legacy_assessments",
        "new_pressure_assessments",
        "legacy_pressure_assessments",
        "market_cap_csv",
        "adjusted_market_price_csv",
        "lifecycle_reference_path",
    }
)
_PATH_LIST_FIELDS = frozenset({"risk_universe_files"})
_PATH_MAPPING_FIELDS = frozenset(
    {"history_files", "new_history_files", "legacy_history_files"}
)


def discover_project_root(start: str | Path | None = None) -> Path:
    candidate = Path(start or Path.cwd()).expanduser().resolve()
    if candidate.is_file():
        candidate = candidate.parent
    for directory in (candidate, *candidate.parents):
        marker = directory / "pyproject.toml"
        if marker.is_file() and (directory / "src" / "riskaudit").is_dir():
            return directory
    raise FileNotFoundError(
        "找不到 RiskAudit 项目根目录；请在仓库内运行或传入 --project-root"
    )


def _looks_absolute(value: str) -> bool:
    return Path(value).expanduser().is_absolute() or bool(
        _WINDOWS_ABSOLUTE.match(value)
    ) or value.startswith("\\\\")


@dataclass(frozen=True)
class PortableTaskConfig:
    task_type: str
    parameters: dict[str, Any]
    config_path: Path
    project_root: Path
    data_root: Path

    def resolve_path(self, value: str | Path | None) -> Path | None:
        if value is None or str(value).strip() == "":
            return None
        raw = str(value).strip()
        anchors = {
            "@project/": self.project_root,
            "@data/": self.data_root,
            "@config/": self.config_path.parent,
        }
        for prefix, root in anchors.items():
            if raw.startswith(prefix):
                return (root / raw[len(prefix) :]).resolve()
        if _looks_absolute(raw):
            raise ValueError(
                f"任务配置禁止机器绝对路径：{raw}；请使用 @project/、@data/ 或 @config/"
            )
        return (self.config_path.parent / raw).resolve()

    def resolved_parameters(self) -> dict[str, Any]:
        resolved = dict(self.parameters)
        for field in _PATH_FIELDS & resolved.keys():
            resolved[field] = self.resolve_path(resolved[field])
        for field in _PATH_LIST_FIELDS & resolved.keys():
            resolved[field] = tuple(
                self.resolve_path(item) for item in resolved[field] or ()
            )
        for field in _PATH_MAPPING_FIELDS & resolved.keys():
            resolved[field] = {
                str(name): self.resolve_path(path)
                for name, path in dict(resolved[field] or {}).items()
            }
        if "models" in resolved:
            models: dict[str, dict[str, Any]] = {}
            for label, raw_spec in dict(resolved["models"] or {}).items():
                spec = dict(raw_spec)
                for field in ("assessment_csv", "metric_manifest"):
                    if field in spec:
                        spec[field] = self.resolve_path(spec[field])
                if "history_files" in spec:
                    spec["history_files"] = {
                        str(name): self.resolve_path(path)
                        for name, path in dict(spec["history_files"] or {}).items()
                    }
                models[str(label)] = spec
            resolved["models"] = models
        return resolved


def load_task_config(
    path: str | Path,
    *,
    project_root: str | Path | None = None,
    environment: Mapping[str, str] | None = None,
) -> PortableTaskConfig:
    config_path = Path(path).expanduser().resolve()
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("任务配置根节点必须是 JSON 对象")
    task_type = str(payload.get("task") or "").strip()
    parameters = payload.get("parameters")
    if not task_type or not isinstance(parameters, dict):
        raise ValueError("任务配置必须包含 task 和 parameters")
    root = discover_project_root(project_root or config_path)
    settings = RuntimeSettings.from_environment(root, environment)
    return PortableTaskConfig(
        task_type=task_type,
        parameters=dict(parameters),
        config_path=config_path,
        project_root=root,
        data_root=settings.data_root,
    )
