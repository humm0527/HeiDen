"""Configuration-driven raw -> Chinese business -> Python field mapping."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from .models import (
    FieldCandidate,
    FieldLineage,
    MappingResult,
    PendingField,
)


@dataclass(frozen=True)
class BusinessField:
    business_name: str
    python_name: str
    data_type: str
    required: bool
    nullable: bool


@dataclass(frozen=True)
class TableSchema:
    business_name: str
    python_name: str
    fields: tuple[BusinessField, ...]

    @property
    def by_business_name(self) -> dict[str, BusinessField]:
        return {field.business_name: field for field in self.fields}


@dataclass(frozen=True)
class MappingRule:
    business_field: str
    python_field: str
    exact: str | None
    aliases: tuple[str, ...]
    candidates: tuple[str, ...]
    required: bool
    context_key: str | None
    constant: Any = None
    has_constant: bool = False
    value_map: dict[str, Any] | None = None


@dataclass(frozen=True)
class DatasetMapping:
    dataset_name: str
    target_table: str
    rules: tuple[MappingRule, ...]
    unresolved_candidates: dict[str, tuple[FieldCandidate, ...]]


class MappingCatalog:
    """Load and validate stable business/Python schemas and source mappings."""

    def __init__(
        self,
        tables: dict[str, TableSchema],
        datasets: dict[str, DatasetMapping],
    ) -> None:
        self.tables = tables
        self.datasets = datasets
        self._validate()

    @classmethod
    def from_files(
        cls,
        registry_path: str | Path,
        *mapping_paths: str | Path,
    ) -> "MappingCatalog":
        with Path(registry_path).open(encoding="utf-8") as stream:
            registry = yaml.safe_load(stream)

        tables: dict[str, TableSchema] = {}
        for table_name, table_config in registry["tables"].items():
            fields = tuple(
                BusinessField(
                    business_name=item["business_field"],
                    python_name=item["python_field"],
                    data_type=item["data_type"],
                    required=bool(item["required"]),
                    nullable=bool(item["nullable"]),
                )
                for item in table_config["fields"]
            )
            tables[table_name] = TableSchema(
                business_name=table_name,
                python_name=table_config["python_table"],
                fields=fields,
            )

        datasets: dict[str, DatasetMapping] = {}
        for mapping_path in mapping_paths:
            with Path(mapping_path).open(encoding="utf-8") as stream:
                mapping_config = yaml.safe_load(stream)
            for dataset_name, dataset_config in mapping_config["datasets"].items():
                rules: list[MappingRule] = []
                for item in dataset_config["fields"]:
                    rules.append(
                        MappingRule(
                            business_field=item["business_field"],
                            python_field=item["python_field"],
                            exact=item.get("exact"),
                            aliases=tuple(item.get("aliases", [])),
                            candidates=tuple(item.get("candidates", [])),
                            required=bool(item.get("required", False)),
                            context_key=item.get("context_key"),
                            constant=item.get("constant"),
                            has_constant="constant" in item,
                            value_map=item.get("value_map"),
                        )
                    )
                unresolved = {
                    raw_field: tuple(
                        FieldCandidate(
                            business_field=candidate["business_field"],
                            python_field=candidate["python_field"],
                        )
                        for candidate in candidates
                    )
                    for raw_field, candidates in dataset_config.get(
                        "unresolved_candidates", {}
                    ).items()
                }
                if dataset_name in datasets:
                    raise ValueError(f"Duplicate dataset mapping: {dataset_name}")
                datasets[dataset_name] = DatasetMapping(
                    dataset_name=dataset_name,
                    target_table=dataset_config["target_table"],
                    rules=tuple(rules),
                    unresolved_candidates=unresolved,
                )
        return cls(tables=tables, datasets=datasets)

    def _validate(self) -> None:
        for dataset in self.datasets.values():
            if dataset.target_table not in self.tables:
                raise ValueError(f"Unknown target table: {dataset.target_table}")
            schema = self.tables[dataset.target_table]
            registry_fields = schema.by_business_name
            seen: set[str] = set()
            for rule in dataset.rules:
                if rule.business_field in seen:
                    raise ValueError(
                        f"Duplicate business mapping in {dataset.dataset_name}: "
                        f"{rule.business_field}"
                    )
                seen.add(rule.business_field)
                field = registry_fields.get(rule.business_field)
                if field is None:
                    raise ValueError(
                        f"Unknown business field {rule.business_field} for "
                        f"{dataset.target_table}"
                    )
                if field.python_name != rule.python_field:
                    raise ValueError(
                        f"Python field mismatch for {rule.business_field}: "
                        f"{rule.python_field} != {field.python_name}"
                    )


class FieldMapper:
    """Apply only configured matches; never infer semantics from field names."""

    TRUE_VALUES = {True, 1, "1", "true", "True", "是", "Y", "YES"}
    FALSE_VALUES = {False, 0, "0", "false", "False", "否", "N", "NO"}

    def __init__(self, catalog: MappingCatalog) -> None:
        self.catalog = catalog

    def map(
        self,
        dataset_name: str,
        raw_data: pd.DataFrame,
        *,
        context: dict[str, Any] | None = None,
    ) -> MappingResult:
        if dataset_name not in self.catalog.datasets:
            raise KeyError(f"No mapping configured for dataset: {dataset_name}")
        dataset = self.catalog.datasets[dataset_name]
        schema = self.catalog.tables[dataset.target_table]
        context = context or {}
        used_raw_fields: set[str] = set()
        mapped: dict[str, pd.Series] = {}
        lineage: list[FieldLineage] = []
        missing: list[str] = []
        pending: list[PendingField] = []

        for rule in dataset.rules:
            resolution = self._resolve_rule(rule, raw_data, context)
            if resolution[0] == "missing":
                if rule.required:
                    missing.append(rule.business_field)
                continue
            if resolution[0] == "pending":
                pending.append(
                    PendingField(
                        raw_field=" | ".join(resolution[1]),
                        candidates=(
                            FieldCandidate(rule.business_field, rule.python_field),
                        ),
                        reason="多个已配置候选字段同时存在，系统不能自动选择",
                    )
                )
                continue

            method, raw_field, series = resolution
            if raw_field is not None:
                used_raw_fields.add(raw_field)
            field_definition = schema.by_business_name[rule.business_field]
            if rule.value_map:
                series = series.map(
                    lambda value: rule.value_map.get(str(value).lower(), value)
                )
            mapped[rule.business_field] = self._convert_series(
                series,
                field_definition.data_type,
                field_definition.nullable,
            )
            lineage.append(
                FieldLineage(
                    raw_field=raw_field,
                    business_field=rule.business_field,
                    python_field=rule.python_field,
                    match_method=method,
                )
            )

        unknown = [
            str(column) for column in raw_data.columns if str(column) not in used_raw_fields
        ]
        for raw_field in unknown:
            candidates = dataset.unresolved_candidates.get(raw_field)
            if candidates:
                pending.append(
                    PendingField(
                        raw_field=raw_field,
                        candidates=candidates,
                        reason="配置声明该原始字段存在多个业务含义候选",
                    )
                )

        mapped_names = set(mapped)
        for field in schema.fields:
            if field.required and field.business_name not in mapped_names:
                if field.business_name not in missing:
                    missing.append(field.business_name)
            if field.business_name not in mapped:
                mapped[field.business_name] = pd.Series(
                    [pd.NA] * len(raw_data), index=raw_data.index
                )

        business_data = pd.DataFrame(
            {field.business_name: mapped[field.business_name] for field in schema.fields},
            index=raw_data.index,
        )
        python_names = {
            field.business_name: field.python_name for field in schema.fields
        }
        code_data = business_data.rename(columns=python_names)
        return MappingResult(
            target_table=dataset.target_table,
            business_data=business_data,
            code_data=code_data,
            lineage=lineage,
            missing_required_fields=missing,
            unknown_fields=unknown,
            pending_fields=pending,
        )

    @staticmethod
    def _resolve_rule(
        rule: MappingRule,
        raw_data: pd.DataFrame,
        context: dict[str, Any],
    ) -> tuple[str, str | None | list[str], pd.Series | None]:
        if rule.exact and rule.exact in raw_data.columns:
            return "exact", rule.exact, raw_data[rule.exact]

        alias_matches = [field for field in rule.aliases if field in raw_data.columns]
        if len(alias_matches) == 1:
            field = alias_matches[0]
            return "alias", field, raw_data[field]
        if len(alias_matches) > 1:
            return "pending", alias_matches, None

        candidate_matches = [
            field for field in rule.candidates if field in raw_data.columns
        ]
        if len(candidate_matches) == 1:
            field = candidate_matches[0]
            return "configured_candidate", field, raw_data[field]
        if len(candidate_matches) > 1:
            return "pending", candidate_matches, None

        if rule.context_key and rule.context_key in context:
            return (
                "context",
                None,
                pd.Series([context[rule.context_key]] * len(raw_data), index=raw_data.index),
            )
        if rule.has_constant:
            return (
                "constant",
                None,
                pd.Series([rule.constant] * len(raw_data), index=raw_data.index),
            )
        return "missing", None, None

    @classmethod
    def _convert_series(
        cls,
        series: pd.Series,
        data_type: str,
        nullable: bool,
    ) -> pd.Series:
        empty_as_na = series.map(lambda value: pd.NA if value == "" else value)
        if data_type == "string":
            converted = empty_as_na.astype("string")
        elif data_type == "integer":
            converted = pd.to_numeric(empty_as_na, errors="raise").astype("Int64")
        elif data_type == "boolean":
            converted = empty_as_na.map(cls._parse_boolean).astype("boolean")
        elif data_type == "date":
            converted = pd.to_datetime(empty_as_na, errors="raise").dt.strftime(
                "%Y-%m-%d"
            ).astype("string")
        elif data_type == "datetime":
            converted = pd.to_datetime(empty_as_na, errors="raise", utc=True)
        elif data_type == "decimal":
            converted = empty_as_na.map(cls._parse_decimal)
        else:
            raise ValueError(f"Unsupported target data type: {data_type}")

        if not nullable and converted.isna().any():
            raise ValueError("Non-nullable mapped field contains empty values")
        return converted

    @classmethod
    def _parse_boolean(cls, value: Any) -> Any:
        if pd.isna(value):
            return pd.NA
        if value in cls.TRUE_VALUES:
            return True
        if value in cls.FALSE_VALUES:
            return False
        raise ValueError(f"Invalid boolean value: {value!r}")

    @staticmethod
    def _parse_decimal(value: Any) -> Any:
        if pd.isna(value):
            return pd.NA
        try:
            return Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError(f"Invalid decimal value: {value!r}") from exc
