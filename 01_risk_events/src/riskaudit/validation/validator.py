"""
文件作用：按版本化校验包只读检查标准业务表的可用性、缺失、跨表关系和基础 PIT 风险。
编辑记录：
【首次生成：2026-08-06，实现目录识别、字段/类型/主键、覆盖与 PIT 数据预检。】
【二次编辑内容：2026-08-06，补充逐证券交易日覆盖检查并统一字段枚举检查状态。】
【三次改进：2026-08-06，修复全无效日期序列的观察范围比较。】
【四次编辑：2026-08-06，固定日期解析格式并消除覆盖检查的隐式类型转换。】
【五次编辑：2026-08-06，按冻结契约校验带时区时间戳。】
【六次编辑：2026-08-06，区分下载时间代理与可信 PIT 时间，缺失 PIT 证据降为警告。】
【七次改进：2026-08-06，统一带时区时间按 UTC 解析，兼容异常注入中的混合时区。】
【八次编辑：2026-08-06，以版本化验证规则包驱动表、字段角色和确定性语义检查器。】
【九次改进：2026-08-07，兼容 manifest 内下载时间及旧产物的多基准相对 metadata 路径。】
"""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from .checks import SemanticContext, run_semantic_checks
from .report import write_validation_reports
from .result import FieldCheckResult, TableStatistics, ValidationIssue, ValidationResult
from .rules import (
    FieldRule,
    TableRule,
    load_validation_pack,
    resolve_validation_pack_path,
)


_SECURITY_CODE = re.compile(r"^\d{6}(?:\.(?:XSHG|XSHE|XBSE))?$")
_KNOWN_SUPPORT_FILES = {
    "manifest.json",
    "run_summary.json",
    "validation_result.json",
    "data_quality_report.xlsx",
}


class DataValidator:
    """只读校验标准业务表，不修改输入值。"""

    def __init__(
        self,
        registry_path: str | Path,
        validation_pack: str | Path | None = None,
    ) -> None:
        registry_file = Path(registry_path).resolve()
        repository_root = registry_file.parents[2]
        self.repository_root = repository_root
        pack_path = resolve_validation_pack_path(repository_root, validation_pack)
        self.pack = load_validation_pack(registry_file, pack_path)
        self.rules = dict(self.pack.tables)

    def validate_directory(
        self,
        input_dir: str | Path,
        *,
        observation_start: str | date,
        observation_end: str | date,
        output_dir: str | Path | None = None,
    ) -> ValidationResult:
        source = Path(input_dir)
        start, end = self._observation_range(observation_start, observation_end)
        issues: list[ValidationIssue] = []
        tables: dict[str, pd.DataFrame] = {}
        if not source.is_dir():
            result = self._result(
                [self._issue("E001", "严重", "", "", 0, [], "提供存在的标准运行目录", "输入目录不存在")],
                [],
                [],
                start,
                end,
            )
            if output_dir is not None:
                write_validation_reports(result, output_dir)
            return result

        recognized_paths: set[Path] = set()
        for table_name, table_rule in self.rules.items():
            candidates = [source / f"{table_name}.csv", source / f"{table_name}.xlsx"]
            existing = [path for path in candidates if path.exists()]
            recognized_paths.update(existing)
            if not existing:
                if table_rule.required:
                    issues.append(self._issue("E002", "严重", table_name, "", 0, [], "补充缺失的标准业务表文件", "必需业务表文件不存在"))
                continue
            if len(existing) > 1:
                issues.append(self._issue("E003", "严重", table_name, "", len(existing), [{"文件": p.name} for p in existing], "每张业务表只保留一个输入文件", "同一业务表存在多个文件"))
                continue
            path = existing[0]
            try:
                tables[table_name] = self._read_table(path)
            except Exception as exc:
                issues.append(self._issue("E004", "严重", table_name, "", 0, [{"文件": path.name, "错误": str(exc)}], "修复文件格式后重新预检", "业务表文件无法读取"))

        for table_name in self.pack.ignored_tables:
            recognized_paths.update(
                path
                for path in (
                    source / f"{table_name}.csv",
                    source / f"{table_name}.xlsx",
                )
                if path.exists()
            )

        for path in source.iterdir():
            if not path.is_file() or path in recognized_paths or path.name in _KNOWN_SUPPORT_FILES:
                continue
            issues.append(self._issue("W001", "一般", "", "", 1, [{"文件": path.name}], "确认文件用途并移出标准运行目录或登记为标准文件", "发现未知文件"))

        result = self.validate_tables(
            tables,
            observation_start=start,
            observation_end=end,
            initial_issues=issues,
            fetch_times=self._load_fetch_times(source),
        )
        if output_dir is not None:
            write_validation_reports(result, output_dir)
        return result

    def validate_tables(
        self,
        tables: Mapping[str, pd.DataFrame],
        *,
        observation_start: str | date,
        observation_end: str | date,
        initial_issues: list[ValidationIssue] | None = None,
        fetch_times: set[pd.Timestamp] | None = None,
    ) -> ValidationResult:
        start, end = self._observation_range(observation_start, observation_end)
        issues = list(initial_issues or [])
        field_checks: list[FieldCheckResult] = []
        statistics: list[TableStatistics] = []
        parsed_dates: dict[tuple[str, str], pd.Series] = {}

        for table_name, rule in self.rules.items():
            if table_name not in tables:
                if rule.required and not any(i.error_code == "E002" and i.table == table_name for i in issues):
                    issues.append(self._issue("E002", "严重", table_name, "", 0, [], "补充缺失的标准业务表", "缺少必需业务表"))
                continue
            frame = tables[table_name]
            if frame.empty:
                issues.append(self._issue("E005", "严重", table_name, "", 0, [], "补充关键表数据", "关键表为空"))
            known_fields = {field.name for field in rule.fields}
            for unknown in sorted(set(frame.columns) - known_fields):
                issues.append(self._issue("W101", "一般", table_name, unknown, len(frame), self._examples(frame, [unknown]), "核对字段来源；原字段继续保留但不参与计算", "发现未知字段"))
            for field_rule in rule.fields:
                self._check_field(table_name, frame, rule, field_rule, issues, field_checks, parsed_dates)
            self._check_primary_key(table_name, frame, rule, issues)
            self._check_security_codes(
                table_name,
                frame,
                rule.security_code_field,
                issues,
            )
            self._check_date_range(table_name, frame, rule, parsed_dates, start, end, issues)
            self._check_pit(
                table_name,
                frame,
                rule,
                parsed_dates,
                end,
                issues,
                fetch_times or set(),
            )
            statistics.append(self._statistics(table_name, frame, rule, parsed_dates))

        issues.extend(
            run_semantic_checks(
                SemanticContext(
                    tables=tables,
                    parsed_dates=parsed_dates,
                    observation_start=start,
                    observation_end=end,
                    pack=self.pack,
                )
            )
        )
        return self._result(issues, field_checks, statistics, start, end)

    @staticmethod
    def _read_table(path: Path) -> pd.DataFrame:
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path, dtype=object, keep_default_na=False, encoding="utf-8-sig")
        return pd.read_excel(path, dtype=object, keep_default_na=False)

    def _check_field(self, table: str, frame: pd.DataFrame, table_rule: TableRule, field_rule: FieldRule, issues: list[ValidationIssue], checks: list[FieldCheckResult], parsed_dates: dict[tuple[str, str], pd.Series]) -> None:
        if field_rule.name not in frame.columns:
            if field_rule.name in table_rule.pit_fields:
                issues.append(self._issue("W701", "一般", table, field_rule.name, len(frame), [], "如任务要求严格 PIT，请补充独立于下载时间的源信息可得时间", "源数据未提供独立 PIT 时间，当前无法严格验证；这不代表存在未来数据"))
                checks.append(FieldCheckResult(table, field_rule.name, field_rule.required, field_rule.nullable, field_rule.data_type, "警告", "PIT 字段不存在"))
                return
            severity = "严重" if field_rule.required else "一般"
            code = "E101" if field_rule.required else "W102"
            issues.append(self._issue(code, severity, table, field_rule.name, len(frame), [], "补充标准字段后重新预检", "缺少核心字段" if field_rule.required else "缺少非核心字段"))
            checks.append(FieldCheckResult(table, field_rule.name, field_rule.required, field_rule.nullable, field_rule.data_type, "缺失", "字段不存在"))
            return
        series = frame[field_rule.name]
        blank = series.isna() | series.astype(str).str.strip().eq("")
        if field_rule.name in table_rule.pit_fields and blank.any():
            issues.append(self._issue("W701", "一般", table, field_rule.name, int(blank.sum()), self._examples(frame.loc[blank]), "如任务要求严格 PIT，请补充独立于下载时间的源信息可得时间", "源数据未提供独立 PIT 时间，当前无法严格验证；这不代表存在未来数据"))
        elif not field_rule.nullable and blank.any():
            issues.append(self._issue("E501", "严重", table, field_rule.name, int(blank.sum()), self._examples(frame.loc[blank]), "补充核心字段缺失值", "核心字段存在缺失值"))
        valid = ~blank
        invalid = pd.Series(False, index=frame.index)
        if field_rule.data_type in {"date", "datetime"}:
            date_format = "%Y-%m-%d" if field_rule.data_type == "date" else "mixed"
            parsed = pd.to_datetime(
                series.where(valid),
                errors="coerce",
                format=date_format,
                utc=field_rule.data_type == "datetime",
            )
            invalid = valid & parsed.isna()
            if field_rule.data_type == "datetime":
                timezone_missing = valid & ~series.astype(str).str.strip().str.contains(
                    r"(?:Z|[+-]\d{2}:\d{2})$", regex=True
                )
                invalid |= timezone_missing
            parsed_dates[(table, field_rule.name)] = parsed
        elif field_rule.data_type in {"decimal", "integer"}:
            parsed_number = pd.to_numeric(series.where(valid), errors="coerce")
            invalid = valid & parsed_number.isna()
            if field_rule.data_type == "integer":
                invalid |= valid & parsed_number.notna() & (parsed_number % 1 != 0)
        elif field_rule.data_type == "boolean":
            allowed = {"true", "false", "1", "0", "是", "否"}
            invalid = valid & ~series.astype(str).str.strip().str.lower().isin(allowed)
        if invalid.any():
            issues.append(self._issue("E201", "严重", table, field_rule.name, int(invalid.sum()), self._examples(frame.loc[invalid]), "修正为标准的字段类型或日期格式", "字段类型或日期格式错误"))
        if field_rule.allowed_values is not None:
            enum_invalid = valid & ~series.astype(str).str.strip().isin(field_rule.allowed_values)
            if enum_invalid.any():
                issues.append(self._issue("E202", "严重", table, field_rule.name, int(enum_invalid.sum()), self._examples(frame.loc[enum_invalid]), "使用数据契约允许的枚举值", "分类枚举值非法"))
        has_enum_error = bool(field_rule.allowed_values is not None and enum_invalid.any())
        if field_rule.name in table_rule.pit_fields and blank.any() and not invalid.any():
            status = "警告"
        else:
            status = "通过" if not invalid.any() and not has_enum_error and (field_rule.nullable or not blank.any()) else "异常"
        checks.append(FieldCheckResult(table, field_rule.name, field_rule.required, field_rule.nullable, field_rule.data_type, status))

    def _check_primary_key(self, table: str, frame: pd.DataFrame, rule: TableRule, issues: list[ValidationIssue]) -> None:
        if not set(rule.primary_key).issubset(frame.columns):
            return
        key = frame.loc[:, list(rule.primary_key)]
        blank = key.isna() | key.astype(str).apply(lambda col: col.str.strip().eq(""))
        missing = blank.any(axis=1)
        if missing.any():
            issues.append(self._issue("E301", "严重", table, " + ".join(rule.primary_key), int(missing.sum()), self._examples(frame.loc[missing], rule.primary_key), "补全主键字段", "主键存在缺失"))
        duplicates = key.duplicated(keep=False) & ~missing
        if duplicates.any():
            issues.append(self._issue("E302", "严重", table, " + ".join(rule.primary_key), int(duplicates.sum()), self._examples(frame.loc[duplicates], rule.primary_key), "删除或更正重复主键记录，并保留源数据追溯", "主键组合重复"))

    def _check_security_codes(self, table: str, frame: pd.DataFrame, security_field: str | None, issues: list[ValidationIssue]) -> None:
        if not security_field or security_field not in frame.columns:
            return
        values = frame[security_field].astype(str).str.strip()
        invalid = ~values.str.fullmatch(_SECURITY_CODE)
        if invalid.any():
            issues.append(self._issue("E401", "严重", table, security_field, int(invalid.sum()), self._examples(frame.loc[invalid], [security_field]), "使用六位数字证券代码，可带 .XSHG/.XSHE/.XBSE 市场后缀", "股票代码格式或长度错误"))

    def _check_date_range(self, table: str, frame: pd.DataFrame, rule: TableRule, parsed_dates: dict[tuple[str, str], pd.Series], start: date, end: date, issues: list[ValidationIssue]) -> None:
        if not rule.business_date_field:
            return
        parsed = parsed_dates.get((table, rule.business_date_field))
        if parsed is None:
            return
        start_timestamp = pd.Timestamp(start)
        end_timestamp = pd.Timestamp(end) + pd.Timedelta(1, unit="D")
        outside = parsed.notna() & ((parsed < start_timestamp) | (parsed >= end_timestamp))
        if outside.any():
            issues.append(self._issue("E402", "严重", table, rule.business_date_field, int(outside.sum()), self._examples(frame.loc[outside]), "移除观察范围外数据或调整任务观察范围", "数据日期超出任务观察范围"))

    def _check_pit(self, table: str, frame: pd.DataFrame, rule: TableRule, parsed_dates: dict[tuple[str, str], pd.Series], end: date, issues: list[ValidationIssue], fetch_times: set[pd.Timestamp]) -> None:
        cutoff = pd.Timestamp(end).tz_localize("Asia/Shanghai") + pd.Timedelta(1, unit="D") - pd.Timedelta(1, unit="us")
        for field in rule.pit_fields:
            parsed = parsed_dates.get((table, field))
            if parsed is None:
                continue
            comparable = pd.to_datetime(parsed, utc=True, errors="coerce")
            acquisition_proxy = (
                comparable.isin(fetch_times)
                if fetch_times
                else pd.Series(False, index=frame.index)
            )
            if acquisition_proxy.any():
                issues.append(self._issue("W701", "一般", table, field, int(acquisition_proxy.sum()), self._examples(frame.loc[acquisition_proxy]), "如任务要求严格 PIT，请补充独立于下载时间的源信息可得时间", "该值是本次数据下载时间，不是未来信息异常；源信息可得时间尚不可验证"))
            future = comparable.notna() & ~acquisition_proxy & (comparable > cutoff.tz_convert("UTC"))
            if future.any():
                issues.append(self._issue("E701", "严重", table, field, int(future.sum()), self._examples(frame.loc[future]), "使用观察截止时点前已可得的数据版本", "存在未来信息进入当前观察范围"))
        for effective_rule in rule.effective_date_rules:
            effective_field = effective_rule.effective_field
            usage_field = effective_rule.usage_date_field
            if (table, effective_field) not in parsed_dates or (table, usage_field) not in parsed_dates:
                continue
            effective = pd.to_datetime(parsed_dates[(table, effective_field)], utc=True, errors="coerce").dt.tz_convert("Asia/Shanghai").dt.date
            business = parsed_dates[(table, usage_field)].dt.date
            late = effective.notna() & business.notna() & (effective > business)
            if late.any():
                issues.append(self._issue("E702", "严重", table, effective_field, int(late.sum()), self._examples(frame.loc[late]), "核对状态生效时点与业务使用日期", "状态生效日期晚于使用日期"))

    def _load_fetch_times(self, source: Path) -> set[pd.Timestamp]:
        manifest_path = source / "manifest.json"
        if not manifest_path.exists():
            return set()
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return set()
        fetch_times: set[pd.Timestamp] = set()
        for artifact in manifest.get("raw_artifacts", []):
            value = artifact.get("received_at")
            if value is None:
                metadata_value = artifact.get("raw_metadata_path")
                if metadata_value:
                    for metadata_path in self._manifest_path_candidates(
                        source, metadata_value
                    ):
                        try:
                            metadata = json.loads(
                                metadata_path.read_text(encoding="utf-8")
                            )
                        except (OSError, json.JSONDecodeError):
                            continue
                        value = metadata.get("received_at")
                        if value:
                            break
            if value:
                parsed = pd.to_datetime(value, utc=True, errors="coerce")
                if pd.notna(parsed):
                    fetch_times.add(parsed)
        return fetch_times

    def _manifest_path_candidates(
        self, source: Path, path_value: str | Path
    ) -> tuple[Path, ...]:
        """按新旧 manifest 约定返回去重后的候选绝对路径。"""

        path = Path(path_value)
        if path.is_absolute():
            return (path,)
        candidates = (
            source / path,
            self.repository_root / path,
            source.parent.parent / path,
            Path.cwd() / path,
        )
        unique: list[Path] = []
        seen: set[Path] = set()
        for candidate in candidates:
            resolved = candidate.resolve()
            if resolved not in seen:
                seen.add(resolved)
                unique.append(resolved)
        return tuple(unique)

    def _statistics(self, table: str, frame: pd.DataFrame, rule: TableRule, parsed_dates: dict[tuple[str, str], pd.Series]) -> TableStatistics:
        parsed = (
            parsed_dates.get((table, rule.business_date_field))
            if rule.business_date_field
            else None
        )
        valid_dates = parsed.dropna() if parsed is not None else pd.Series(dtype="datetime64[ns]")
        return TableStatistics(
            table=table,
            row_count=len(frame),
            column_count=len(frame.columns),
            min_date="" if valid_dates.empty else valid_dates.min().date().isoformat(),
            max_date="" if valid_dates.empty else valid_dates.max().date().isoformat(),
            market_count=frame["交易市场代码"].nunique(dropna=True) if "交易市场代码" in frame else 0,
            security_count=frame["证券代码"].nunique(dropna=True) if "证券代码" in frame else 0,
        )

    @staticmethod
    def _observation_range(start: str | date, end: str | date) -> tuple[date, date]:
        parsed_start = date.fromisoformat(start) if isinstance(start, str) else start
        parsed_end = date.fromisoformat(end) if isinstance(end, str) else end
        if parsed_end < parsed_start:
            raise ValueError("观察结束日期不能早于观察开始日期")
        return parsed_start, parsed_end

    @staticmethod
    def _examples(frame: pd.DataFrame, fields: Any = None) -> list[dict[str, Any]]:
        selected = frame if fields is None else frame.loc[:, [field for field in fields if field in frame.columns]]
        return selected.head(5).fillna("").astype(object).to_dict(orient="records")

    @staticmethod
    def _issue(code: str, severity: str, table: str, field: str, affected_rows: int, examples: list[dict[str, Any]], action: str, message: str) -> ValidationIssue:
        return ValidationIssue(code, severity, table, field, affected_rows, examples, action, message)

    def _result(self, issues: list[ValidationIssue], fields: list[FieldCheckResult], statistics: list[TableStatistics], start: date, end: date) -> ValidationResult:
        allow_run = not any(item.severity == "严重" for item in issues)
        status = "FAIL" if not allow_run else ("WARNING" if issues else "PASS")
        return ValidationResult(
            status,
            allow_run,
            issues,
            fields,
            statistics,
            start.isoformat(),
            end.isoformat(),
            self.pack.pack_id,
            self.pack.version,
        )
