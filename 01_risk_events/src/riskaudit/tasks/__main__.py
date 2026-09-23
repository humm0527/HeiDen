"""Command line interface for portable RiskAudit task modules."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from .catalog import TASK_CATALOG, list_task_definitions
from .config import load_task_config
from .config import discover_project_root
from .doctor import environment_report
from .runner import run_configured_task


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="riskaudit-task")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("list", help="列出独立任务模块")
    doctor = subparsers.add_parser("doctor", help="检查 Python、依赖和运行目录")
    doctor.add_argument("--project-root", type=Path)
    doctor.add_argument("--task", choices=tuple(TASK_CATALOG))
    check = subparsers.add_parser("check", help="检查任务配置和路径可移植性")
    check.add_argument("config", type=Path)
    check.add_argument("--project-root", type=Path)
    run = subparsers.add_parser("run", help="执行一个独立任务")
    run.add_argument("config", type=Path)
    run.add_argument("--project-root", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "list":
        print(json.dumps(list_task_definitions(), ensure_ascii=False, indent=2))
        return 0
    if args.command == "doctor":
        root = discover_project_root(args.project_root)
        report = environment_report(root, task_type=args.task)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["status"] == "READY" else 2
    config = load_task_config(args.config, project_root=args.project_root)
    if args.command == "check":
        resolved = config.resolved_parameters()
        print(
            json.dumps(
                {
                    "status": "VALID",
                    "task": config.task_type,
                    "project_root": str(config.project_root),
                    "data_root": str(config.data_root),
                    "resolved_path_count": sum(
                        isinstance(value, Path) for value in resolved.values()
                    ),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    print(json.dumps(run_configured_task(config), ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
