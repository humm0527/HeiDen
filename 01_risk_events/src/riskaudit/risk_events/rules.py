"""
文件作用：只从 approved 目录加载并校验风险事件规则，向算法提供不可变的规范化规则对象。
编辑记录：
【首次生成：2026-08-11，建立 approved 规则路径门禁、SHA-256 追溯和 canonical security_code 校验。】
【第二次编辑：2026-08-12，允许正式入口显式覆盖观察区间，同时保留 approved 规则版本与哈希并校验日期边界。】
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path

import yaml


@dataclass(frozen=True)
class RiskEventRules:
    rule_version: str
    rule_sha256: str
    observation_start: date
    observation_end: date
    board_aliases: dict[str, str]
    thresholds: dict[str, int]
    excluded_boards: frozenset[str]
    decimal_scale: int
    minimum_segment_length: int
    continuous_event_type: str
    new_st_event_type: str

    def canonical_board(self, value: object) -> str:
        text = str(value).strip()
        return self.board_aliases.get(text, text)


def load_approved_rules(
    path: str | Path,
    *,
    observation_start: str | date | None = None,
    observation_end: str | date | None = None,
) -> RiskEventRules:
    rule_path = Path(path).resolve()
    if "approved" not in {part.lower() for part in rule_path.parts}:
        raise ValueError("Formal risk calculation can only load rules from an approved directory")
    raw = rule_path.read_bytes()
    payload = yaml.safe_load(raw.decode("utf-8"))
    if not isinstance(payload, dict) or payload.get("status") != "approved":
        raise ValueError("Risk rule status must be approved")
    if payload.get("rule_version") != "v2":
        raise ValueError("Risk event implementation requires canonical approved rule version v2")

    identity = payload["event_identity"]["unique_key"]
    if identity != ["market_code", "security_code", "event_type", "first_fact_date"]:
        raise ValueError("Approved event identity must use canonical security_code")

    task = payload["task_scope"]
    board = payload["board_policy"]
    continuous = payload["continuous_limit_down"]
    configured_start = date.fromisoformat(str(task["observation_start"]))
    configured_end = date.fromisoformat(str(task["observation_end"]))
    selected_start = _as_date(observation_start) if observation_start is not None else configured_start
    selected_end = _as_date(observation_end) if observation_end is not None else configured_end
    if selected_start > selected_end:
        raise ValueError("Risk observation start must not be later than observation end")
    return RiskEventRules(
        rule_version=str(payload["rule_version"]),
        rule_sha256=sha256(raw).hexdigest(),
        observation_start=selected_start,
        observation_end=selected_end,
        board_aliases={str(key): str(value) for key, value in board["aliases"].items()},
        thresholds={str(key): int(value) for key, value in board["thresholds"].items()},
        excluded_boards=frozenset(str(key) for key in board["excluded_boards"]),
        decimal_scale=int(continuous["decimal_scale"]),
        minimum_segment_length=int(
            continuous["minimum_segment_length_for_detail"]
        ),
        continuous_event_type=str(continuous["event_type"]),
        new_st_event_type=str(payload["new_st_event"]["event_type"]),
    )


def _as_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))
