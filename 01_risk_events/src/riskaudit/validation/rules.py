"""
文件作用：加载通用数据预检规则包，将版本化 YAML 转换为确定性表、字段和语义检查契约。
编辑记录：
【首次生成：2026-08-06，依据 1.5-approved 数据字典建立预检规则目录。】
【二次编辑内容：2026-08-06，按冻结数据字典纳入记录/行情版本字段组成完整主键。】
【三次改进：2026-08-06，重构为业务无关的版本化验证规则包加载器并保留市场四表兼容入口。】
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml


@dataclass(frozen=True)
class FieldRule:
    name: str
    data_type: str
    required: bool
    nullable: bool
    allowed_values: frozenset[str] | None = None


@dataclass(frozen=True)
class EffectiveDateRule:
    effective_field: str
    usage_date_field: str


@dataclass(frozen=True)
class TableRule:
    name: str
    fields: tuple[FieldRule, ...]
    primary_key: tuple[str, ...]
    business_date_field: str
    required: bool = True
    security_code_field: str | None = None
    pit_fields: tuple[str, ...] = ()
    effective_date_rules: tuple[EffectiveDateRule, ...] = ()


@dataclass(frozen=True)
class SemanticCheckRule:
    name: str
    options: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ValidationPack:
    pack_id: str
    version: str
    description: str
    tables: Mapping[str, TableRule]
    ignored_tables: frozenset[str]
    semantic_checks: tuple[SemanticCheckRule, ...]
    source_path: Path
    repository_root: Path


DEFAULT_MARKET_PACK = "market_data_v1"


def resolve_validation_pack_path(
    repository_root: str | Path,
    pack: str | Path | None,
) -> Path:
    root = Path(repository_root)
    if pack is None:
        return root / "configs/validation/packs" / f"{DEFAULT_MARKET_PACK}.yaml"
    candidate = Path(pack)
    if candidate.exists():
        return candidate
    if candidate.suffix.lower() in {".yaml", ".yml"}:
        rooted = root / candidate
        if rooted.exists():
            return rooted
    named = root / "configs/validation/packs" / f"{candidate.stem}.yaml"
    if named.exists():
        return named
    raise FileNotFoundError(f"验证规则包不存在：{pack}")


def load_validation_pack(
    registry_path: str | Path,
    pack_path: str | Path,
) -> ValidationPack:
    registry_file = Path(registry_path)
    pack_file = Path(pack_path)
    registry: dict[str, Any] = yaml.safe_load(
        registry_file.read_text(encoding="utf-8")
    )
    payload: dict[str, Any] = yaml.safe_load(pack_file.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 1:
        raise ValueError("仅支持 schema_version=1 的验证规则包")

    registry_tables = registry.get("tables", {})
    table_rules: dict[str, TableRule] = {}
    for table_name, table_config in payload.get("tables", {}).items():
        registry_name = table_config.get("registry_table")
        configured_fields = table_config.get("fields")
        if registry_name:
            if registry_name not in registry_tables:
                raise ValueError(f"字段注册表中不存在业务表：{registry_name}")
            configured_fields = registry_tables[registry_name].get("fields", [])
        if not configured_fields:
            raise ValueError(f"验证规则包未定义字段：{table_name}")

        enum_config = table_config.get("enums", {})
        fields = tuple(
            FieldRule(
                name=str(item.get("business_field") or item.get("name")),
                data_type=str(item["data_type"]),
                required=bool(item.get("required", True)),
                nullable=bool(item.get("nullable", False)),
                allowed_values=(
                    frozenset(str(value) for value in enum_config[field_name])
                    if (field_name := str(item.get("business_field") or item.get("name")))
                    in enum_config
                    else None
                ),
            )
            for item in configured_fields
        )
        field_names = {item.name for item in fields}
        primary_key = tuple(str(value) for value in table_config.get("primary_key", []))
        business_date_field = str(table_config.get("business_date_field") or "")
        referenced = set(primary_key)
        if business_date_field:
            referenced.add(business_date_field)
        referenced.update(str(value) for value in table_config.get("pit_fields", []))
        missing_references = referenced - field_names
        if missing_references:
            raise ValueError(
                f"验证规则包 {table_name} 引用了不存在字段："
                + "、".join(sorted(missing_references))
            )
        table_rules[table_name] = TableRule(
            name=table_name,
            fields=fields,
            primary_key=primary_key,
            business_date_field=business_date_field,
            required=bool(table_config.get("required", True)),
            security_code_field=table_config.get("security_code_field"),
            pit_fields=tuple(str(value) for value in table_config.get("pit_fields", [])),
            effective_date_rules=tuple(
                EffectiveDateRule(
                    effective_field=str(item["effective_field"]),
                    usage_date_field=str(item["usage_date_field"]),
                )
                for item in table_config.get("effective_date_rules", [])
            ),
        )

    if not table_rules:
        raise ValueError("验证规则包至少需要定义一张表")
    repository_root = registry_file.resolve().parents[2]
    return ValidationPack(
        pack_id=str(payload["pack_id"]),
        version=str(payload["version"]),
        description=str(payload.get("description") or ""),
        tables=table_rules,
        ignored_tables=frozenset(str(value) for value in payload.get("ignored_tables", [])),
        semantic_checks=tuple(
            SemanticCheckRule(
                name=str(item["name"]),
                options=dict(item.get("options") or {}),
            )
            for item in payload.get("semantic_checks", [])
        ),
        source_path=pack_file.resolve(),
        repository_root=repository_root,
    )


def load_market_rules(registry_path: str | Path) -> dict[str, TableRule]:
    """兼容旧调用：加载默认市场四表规则包。"""

    registry_file = Path(registry_path)
    repository_root = registry_file.resolve().parents[2]
    pack_path = resolve_validation_pack_path(repository_root, DEFAULT_MARKET_PACK)
    return dict(load_validation_pack(registry_file, pack_path).tables)
