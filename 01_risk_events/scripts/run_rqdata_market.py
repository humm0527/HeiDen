"""
文件作用：提供正式 RQData 市场数据命令行入口，接收股票列表、日期区间和输出目录并生成四张市场标准表。
编辑记录：
【首次生成：2026-08-05，复用现有市场接入服务实现正式命令行数据接入。】
【二次编辑内容：2026-08-05，增加 CSV 配置批量接入、逐任务容错和批次汇总。】
【三次改进：2026-08-05，为配置模式提供 data_ingestion 默认输出目录，兼容仅传 --config 的调用。】
【四次编辑：2026-08-11，为风险事件真实运行增加观察期前 ST 基线日和非重叠生命周期主数据保留参数。】
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Sequence


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from riskaudit.data_ingestion import (  # noqa: E402
    DataIngestionService,
    FieldMapper,
    RawStore,
    StandardStore,
)
from riskaudit.data_ingestion.adapters import (  # noqa: E402
    RQDataClientProtocol,
    RealRQDataClient,
)
from riskaudit.data_ingestion.market_data import (  # noqa: E402
    MarketDataIngestionService,
    build_market_mapping_catalog,
)


LOGGER = logging.getLogger("riskaudit.rqdata_market")
REQUIRED_CONFIG_FIELDS = ("股票代码", "开始日期", "结束日期")
_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9_-]+$")


class ConfigFileError(ValueError):
    """配置文件结构或字段值不满足批量接入契约。"""


@dataclass(frozen=True)
class ConfigTask:
    task_index: int
    security_code: str
    start_date: str
    end_date: str
    test_case: str = ""
    purpose: str = ""


@dataclass(frozen=True)
class MarketRunResult:
    run_id: str
    output_dir: Path
    raw_dataset_names: tuple[str, ...]
    standard_table_names: tuple[str, ...]
    row_counts: dict[str, int]
    test_case: str = ""
    purpose: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "output_dir": str(self.output_dir),
            "raw_dataset_names": list(self.raw_dataset_names),
            "standard_table_names": list(self.standard_table_names),
            "row_counts": self.row_counts,
            "test_case": self.test_case,
            "purpose": self.purpose,
        }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="使用 RQData 接入指定股票和日期区间的四张市场标准表。"
    )
    parser.add_argument(
        "--stocks",
        nargs="+",
        help="RQData 证券代码列表，例如 000001.XSHE 600000.XSHG。",
    )
    parser.add_argument("--start-date", help="起始日期 YYYY-MM-DD。")
    parser.add_argument("--end-date", help="结束日期 YYYY-MM-DD。")
    parser.add_argument(
        "--config",
        type=Path,
        help="批量任务 CSV，必需字段为股票代码、开始日期、结束日期。",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "data_ingestion",
        help="Raw 和 standard 子目录的输出根目录，默认使用项目 data_ingestion。",
    )
    parser.add_argument(
        "--run-id",
        help="可选运行标识；不提供时生成 rqdata_market_UTC时间。",
    )
    args = parser.parse_args(argv)
    manual_values = (args.stocks, args.start_date, args.end_date)
    if args.config is not None:
        if any(value is not None for value in manual_values):
            parser.error("--config 不能与 --stocks/--start-date/--end-date 同时使用")
    elif any(value is None for value in manual_values):
        parser.error(
            "手动模式必须同时提供 --stocks、--start-date 和 --end-date；"
            "或改用 --config"
        )
    return args


def validate_request(
    order_book_ids: Sequence[str], start_date: str, end_date: str
) -> None:
    if not order_book_ids:
        raise ValueError("At least one stock must be provided")
    if len(set(order_book_ids)) != len(order_book_ids):
        raise ValueError("Duplicate stocks are not allowed")
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("end_date must not precede start_date")


def load_config_tasks(config_path: str | Path) -> tuple[ConfigTask, ...]:
    path = Path(config_path)
    if path.suffix.lower() != ".csv":
        raise ConfigFileError("配置文件必须是 CSV")
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            fieldnames = tuple(reader.fieldnames or ())
            missing_headers = [
                field for field in REQUIRED_CONFIG_FIELDS if field not in fieldnames
            ]
            if missing_headers:
                raise ConfigFileError(
                    "CSV 缺少必需字段: " + ", ".join(missing_headers)
                )

            tasks: list[ConfigTask] = []
            for row_number, row in enumerate(reader, start=2):
                if not any(str(value or "").strip() for value in row.values()):
                    continue
                values = {
                    field: str(row.get(field) or "").strip()
                    for field in REQUIRED_CONFIG_FIELDS
                }
                missing_values = [
                    field for field, value in values.items() if not value
                ]
                if missing_values:
                    raise ConfigFileError(
                        f"CSV 第 {row_number} 行缺少值: "
                        + ", ".join(missing_values)
                    )
                try:
                    validate_request(
                        [values["股票代码"]],
                        values["开始日期"],
                        values["结束日期"],
                    )
                except ValueError as exc:
                    raise ConfigFileError(
                        f"CSV 第 {row_number} 行日期或证券参数无效: {exc}"
                    ) from exc
                tasks.append(
                    ConfigTask(
                        task_index=len(tasks) + 1,
                        security_code=values["股票代码"],
                        start_date=values["开始日期"],
                        end_date=values["结束日期"],
                        test_case=str(row.get("test_case") or "").strip(),
                        purpose=str(row.get("purpose") or "").strip(),
                    )
                )
    except UnicodeDecodeError as exc:
        raise ConfigFileError("CSV 必须使用 UTF-8 或 UTF-8 BOM 编码") from exc

    if not tasks:
        raise ConfigFileError("CSV 没有可执行任务")
    return tuple(tasks)


def run_rqdata_market(
    *,
    client: RQDataClientProtocol,
    repository_root: str | Path,
    output_dir: str | Path,
    run_id: str,
    order_book_ids: Sequence[str],
    start_date: str,
    end_date: str,
    st_baseline_date: str | None = None,
    include_non_overlapping: bool = False,
    retrieved_at: datetime | None = None,
    test_case: str = "",
    purpose: str = "",
) -> MarketRunResult:
    validate_request(order_book_ids, start_date, end_date)
    repository_root = Path(repository_root)
    output_root = Path(output_dir)
    retrieved = (retrieved_at or datetime.now(timezone.utc)).astimezone(timezone.utc)

    catalog = build_market_mapping_catalog(repository_root)
    ingestion_service = DataIngestionService(
        FieldMapper(catalog),
        RawStore(output_root / "raw"),
        StandardStore(output_root / "standard"),
    )
    market_service = MarketDataIngestionService(
        client=client,
        ingestion_service=ingestion_service,
        source_name="rqdata_real",
    )
    batch = market_service.ingest(
        order_book_ids=order_book_ids,
        start_date=start_date,
        end_date=end_date,
        run_id=run_id,
        st_baseline_date=st_baseline_date,
        include_non_overlapping=include_non_overlapping,
        retrieved_at=retrieved,
    )
    standard_output_dir = ingestion_service.save_standard_run(
        run_id,
        batch.outcomes,
        additional_raw_artifacts=batch.trace_raw_artifacts,
    )

    tables = {
        outcome.mapping_result.target_table: outcome.mapping_result.business_data
        for outcome in batch.outcomes
    }
    result = MarketRunResult(
        run_id=run_id,
        output_dir=standard_output_dir,
        raw_dataset_names=tuple(
            outcome.raw_artifact.dataset_name for outcome in batch.outcomes
        )
        + tuple(
            artifact.dataset_name for artifact in batch.trace_raw_artifacts
        ),
        standard_table_names=tuple(tables),
        row_counts={name: len(frame) for name, frame in tables.items()},
        test_case=test_case,
        purpose=purpose,
    )
    (standard_output_dir / "run_summary.json").write_text(
        json.dumps(result.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return result


def run_config_batch(
    *,
    client: RQDataClientProtocol,
    repository_root: str | Path,
    output_dir: str | Path,
    config_path: str | Path,
    batch_run_id: str,
    retrieved_at: datetime | None = None,
) -> dict[str, object]:
    tasks = load_config_tasks(config_path)
    if not _SAFE_RUN_ID.fullmatch(batch_run_id):
        raise ValueError(f"Unsafe batch run id: {batch_run_id!r}")

    output_root = Path(output_dir)
    batch_dir = output_root / "batch" / batch_run_id
    batch_dir.mkdir(parents=True, exist_ok=False)
    retrieved = (retrieved_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    task_results: list[dict[str, object]] = []

    for task in tasks:
        task_run_id = f"{batch_run_id}_{task.task_index:04d}"
        task_result: dict[str, object] = {
            "task_index": task.task_index,
            "security_code": task.security_code,
            "start_date": task.start_date,
            "end_date": task.end_date,
            "test_case": task.test_case,
            "purpose": task.purpose,
            "run_id": task_run_id,
        }
        try:
            result = run_rqdata_market(
                client=client,
                repository_root=repository_root,
                output_dir=output_root,
                run_id=task_run_id,
                order_book_ids=[task.security_code],
                start_date=task.start_date,
                end_date=task.end_date,
                retrieved_at=retrieved,
                test_case=task.test_case,
                purpose=task.purpose,
            )
        except Exception as exc:
            reason = f"{type(exc).__name__}: {exc}"
            LOGGER.warning(
                "批量任务 %s（%s）失败，继续处理后续任务：%s",
                task.task_index,
                task.security_code,
                reason,
            )
            task_result.update(
                {
                    "status": "failed",
                    "output_dir": None,
                    "failure_reason": reason,
                }
            )
        else:
            task_result.update(
                {
                    "status": "success",
                    "output_dir": str(result.output_dir),
                    "failure_reason": None,
                }
            )
        task_results.append(task_result)

    failures = [item for item in task_results if item["status"] == "failed"]
    summary_path = batch_dir / "batch_run_summary.json"
    summary: dict[str, object] = {
        "batch_run_id": batch_run_id,
        "source_config": str(Path(config_path)),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "total_tasks": len(task_results),
        "success_count": len(task_results) - len(failures),
        "failure_count": len(failures),
        "failure_reasons": [
            {
                "task_index": item["task_index"],
                "security_code": item["security_code"],
                "reason": item["failure_reason"],
            }
            for item in failures
        ],
        "tasks": task_results,
        "batch_summary_path": str(summary_path),
    }
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    now = datetime.now(timezone.utc)
    client = RealRQDataClient.from_environment()
    if args.config is not None:
        batch_run_id = args.run_id or now.strftime(
            "rqdata_market_batch_%Y%m%dT%H%M%S%fZ"
        )
        output = run_config_batch(
            client=client,
            repository_root=ROOT,
            output_dir=args.output_dir,
            config_path=args.config,
            batch_run_id=batch_run_id,
            retrieved_at=now,
        )
    else:
        run_id = args.run_id or now.strftime("rqdata_market_%Y%m%dT%H%M%S%fZ")
        output = run_rqdata_market(
            client=client,
            repository_root=ROOT,
            output_dir=args.output_dir,
            run_id=run_id,
            order_book_ids=args.stocks,
            start_date=args.start_date,
            end_date=args.end_date,
            retrieved_at=now,
        ).to_dict()
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
