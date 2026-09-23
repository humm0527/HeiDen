"""Independent, portable task entry points for RiskAudit.

Each task accepts explicit inputs and writes to an explicit output directory.
The web workbench may orchestrate these tasks, but the task implementations do
not import ``app.server`` or depend on a developer workstation path.
"""

from .catalog import TASK_CATALOG, TaskDefinition, list_task_definitions
from .broker_handoff import (
    BROKER_INPUT_SCHEMA,
    apply_broker_input_manifest,
    load_broker_input_manifest,
    write_broker_input_manifest,
)
from .config import PortableTaskConfig, discover_project_root, load_task_config
from .doctor import environment_report
from .model_comparison import run_model_comparison
from .runner import run_configured_task

__all__ = [
    "PortableTaskConfig",
    "BROKER_INPUT_SCHEMA",
    "TASK_CATALOG",
    "TaskDefinition",
    "discover_project_root",
    "apply_broker_input_manifest",
    "environment_report",
    "list_task_definitions",
    "load_task_config",
    "load_broker_input_manifest",
    "run_configured_task",
    "run_model_comparison",
    "write_broker_input_manifest",
]
