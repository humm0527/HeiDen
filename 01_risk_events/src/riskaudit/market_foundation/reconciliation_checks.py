"""Frozen full-range check groups for market reconciliation."""

from __future__ import annotations

import csv
from datetime import date
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping

from .constants import MARKETS, RISK_MARKETS, SCHEMA_VERSION
from .rqdata import read_universe_plan
from .service import MarketFoundationService
from .reconciliation_support import (
    ReconciliationCheck,
    _check,
    _expected_security_days_sql,
    _file_sha256,
    _issues,
    _member_tuple,
)


DAILY_DATASETS = (
    "daily_price_unadjusted",
    "daily_st_status",
    "daily_suspension_status",
    "daily_price_limits",
)


def _universe_checks(
    service: MarketFoundationService, run: Mapping[str, Any]
) -> list[ReconciliationCheck]:
    universe_id = str(run["universe_id"])
    row = service.catalog.row(
        "SELECT * FROM market_universe_manifest WHERE universe_id=?", [universe_id]
    )
    issues: list[str] = []
    plan = None
    if row is None:
        issues.append(f"MISSING_CATALOG_MANIFEST|{universe_id}")
    else:
        if str(row["manifest_hash"]) != str(run["universe_manifest_hash"]):
            issues.append(f"HASH_MISMATCH|{universe_id}")
        try:
            plan = read_universe_plan(row["manifest_path"])
        except Exception as exc:
            issues.append(f"INVALID_MANIFEST|{type(exc).__name__}")
        if not Path(row["members_path"]).is_file():
            issues.append(f"MISSING_MEMBERS_CSV|{row['members_path']}")
    if plan is not None:
        catalog_rows = service.catalog.rows(
            """
            SELECT instrument_key, source_order_book_id, source_exchange,
                   market_code, security_code, symbol, security_type, board_code,
                   listing_date, termination_date, source_status
            FROM market_universe_member WHERE universe_id=?
            ORDER BY market_code, source_order_book_id
            """,
            [universe_id],
        )
        expected = {_member_tuple(item) for item in plan.records}
        actual = {_member_tuple(item) for item in catalog_rows}
        issues.extend(
            f"CATALOG_MEMBER_DIFF|{item[0]}" for item in sorted(expected ^ actual)
        )
        try:
            with Path(row["members_path"]).open(
                "r", encoding="utf-8-sig", newline=""
            ) as handle:
                csv_members = {_member_tuple(item) for item in csv.DictReader(handle)}
            issues.extend(
                f"CSV_MEMBER_DIFF|{item[0]}" for item in sorted(expected ^ csv_members)
            )
        except (OSError, csv.Error) as exc:
            issues.append(f"INVALID_MEMBERS_CSV|{type(exc).__name__}")
    return [
        _check(
            "MDFRQ001",
            "冻结证券清单、成员哈希与目录一致",
            "SEVERE",
            len(issues),
            "冻结证券清单或成员证据不一致",
            "冻结证券清单、CSV 和 DuckDB 成员记录可相互复验",
            "保留现有证据并重新生成新的不可变清单，禁止修改哈希迁就现状",
            issues,
        )
    ]

def _calendar_checks(
    service: MarketFoundationService, start_date: date, end_date: date
) -> list[ReconciliationCheck]:
    checks: list[ReconciliationCheck] = []
    for market in MARKETS:
        sql = """
            WITH expected AS (
                SELECT CAST(day AS DATE) AS business_date
                FROM generate_series(?::DATE, ?::DATE, INTERVAL 1 DAY) AS t(day)
            ), actual AS (
                SELECT business_date FROM current_market_calendar
                WHERE market_code=? AND business_date BETWEEN ? AND ?
            )
            SELECT 'MISSING|' || CAST(business_date AS VARCHAR) AS issue_key
            FROM (SELECT * FROM expected EXCEPT SELECT * FROM actual)
            UNION ALL
            SELECT 'DUPLICATE_OR_NULL|' || CAST(business_date AS VARCHAR)
            FROM current_market_calendar
            WHERE market_code=? AND business_date BETWEEN ? AND ?
              AND is_trading_day IS NULL
        """
        count, examples = _issues(
            service,
            sql,
            [
                start_date,
                end_date,
                market,
                start_date,
                end_date,
                market,
                start_date,
                end_date,
            ],
        )
        checks.append(
            _check(
                "MDFRQ002",
                "市场自然日日历全范围覆盖",
                "SEVERE",
                count,
                "交易日历存在缺日或空交易日标志",
                "冻结范围内每个自然日均有明确日历状态",
                "补齐日历来源事实后重新对账",
                examples,
                dataset_id="market_calendar",
                market_code=market,
            )
        )
    return checks

def _master_and_lifecycle_checks(
    service: MarketFoundationService,
    universe_id: str,
    start_date: date,
    end_date: date,
) -> list[ReconciliationCheck]:
    master_sql = """
        WITH expected AS (
            SELECT instrument_key, market_code, security_code, security_type,
                   listing_date, termination_date
            FROM market_universe_member WHERE universe_id=?
        ), actual AS (
            SELECT DISTINCT instrument_key, market_code, security_code, security_type,
                            listing_date, termination_date
            FROM current_instrument_master_history
        )
        SELECT 'MISSING_OR_MISMATCH|' || e.instrument_key AS issue_key
        FROM expected e
        LEFT JOIN actual a
          ON a.instrument_key=e.instrument_key
         AND a.market_code=e.market_code
         AND a.security_code=e.security_code
         AND a.security_type=e.security_type
         AND a.listing_date=e.listing_date
         AND a.termination_date IS NOT DISTINCT FROM e.termination_date
        WHERE a.instrument_key IS NULL
        UNION ALL
        SELECT 'EXTRA|' || a.instrument_key
        FROM actual a LEFT JOIN expected e USING (instrument_key)
        WHERE e.instrument_key IS NULL
    """
    count, examples = _issues(service, master_sql, [universe_id])
    checks = [
        _check(
            "MDFRQ003",
            "冻结证券清单与当前主数据逐证券一致",
            "SEVERE",
            count,
            "冻结证券成员与主数据/生命周期不一致",
            "冻结证券成员均有且仅有可匹配的主数据生命周期证据",
            "核对主数据来源、生命周期边界和证券清单，禁止删除历史退市证券",
            examples,
            dataset_id="instrument_master_history",
        )
    ]
    for dataset_id in DAILY_DATASETS:
        if dataset_id == "daily_price_unadjusted":
            termination_violation = """
                d.business_date > u.termination_date OR (
                    d.business_date = u.termination_date AND NOT (
                        d.volume=0 AND d.amount=0
                        AND d.open_price=d.high_price
                        AND d.open_price=d.low_price
                        AND d.open_price=d.close_price
                    )
                )
            """
        elif dataset_id == "daily_price_limits":
            termination_violation = """
                d.business_date > u.termination_date OR (
                    d.business_date = u.termination_date AND NOT EXISTS (
                        SELECT 1 FROM current_daily_price_unadjusted p
                        WHERE p.market_code=d.market_code
                          AND p.security_code=d.security_code
                          AND p.business_date=d.business_date
                          AND p.volume=0 AND p.amount=0
                          AND p.open_price=p.high_price
                          AND p.open_price=p.low_price
                          AND p.open_price=p.close_price
                    )
                )
            """
        else:
            termination_violation = "d.business_date >= u.termination_date"
        sql = f"""
            SELECT d.business_key AS issue_key
            FROM current_{dataset_id} d
            LEFT JOIN market_universe_member u
              ON u.universe_id=? AND u.instrument_key=d.instrument_key
             AND u.market_code=d.market_code AND u.security_code=d.security_code
            LEFT JOIN current_market_calendar c
              ON c.market_code=d.market_code AND c.business_date=d.business_date
            WHERE d.business_date BETWEEN ? AND ? AND (
                u.instrument_key IS NULL
                OR d.business_date < u.listing_date
                OR (u.termination_date IS NOT NULL AND ({termination_violation}))
                OR c.business_date IS NULL OR NOT c.is_trading_day
            )
        """
        count, examples = _issues(
            service, sql, [universe_id, start_date, end_date]
        )
        checks.append(
            _check(
                "MDFRQ004",
                "日事实位于证券生命周期和市场交易日内",
                "SEVERE",
                count,
                "日事实出现在清单外、生命周期外或非交易日",
                "全部日事实均落在冻结证券生命周期内的市场交易日",
                "保留来源版本并修正后追加新事实，禁止原位删除",
                examples,
                dataset_id=dataset_id,
            )
        )
        if dataset_id in {"daily_price_unadjusted", "daily_price_limits"}:
            marker_condition = (
                "d.volume=0 AND d.amount=0 AND d.open_price=d.high_price "
                "AND d.open_price=d.low_price AND d.open_price=d.close_price"
                if dataset_id == "daily_price_unadjusted"
                else "EXISTS (SELECT 1 FROM current_daily_price_unadjusted p "
                "WHERE p.market_code=d.market_code AND p.security_code=d.security_code "
                "AND p.business_date=d.business_date AND p.volume=0 AND p.amount=0 "
                "AND p.open_price=p.high_price AND p.open_price=p.low_price "
                "AND p.open_price=p.close_price)"
            )
            marker_sql = f"""
                SELECT d.business_key AS issue_key
                FROM current_{dataset_id} d
                JOIN market_universe_member u
                  ON u.universe_id=? AND u.instrument_key=d.instrument_key
                 AND u.market_code=d.market_code AND u.security_code=d.security_code
                WHERE d.business_date BETWEEN ? AND ?
                  AND u.termination_date IS NOT NULL
                  AND d.business_date=u.termination_date
                  AND {marker_condition}
            """
            marker_count, marker_examples = _issues(
                service, marker_sql, [universe_id, start_date, end_date]
            )
            checks.append(
                _check(
                    "MDFRQ019",
                    "来源退市日零成交终端标记已留痕",
                    "INFO",
                    marker_count,
                    "来源在摘牌日返回零成交平价终端标记，已从风险生命周期事实中排除",
                    "没有来源退市日终端标记",
                    "保留来源事实和审计留痕，不将终端标记解释为可交易行情",
                    marker_examples,
                    dataset_id=dataset_id,
                )
            )
    return checks

def _daily_coverage_checks(
    service: MarketFoundationService,
    universe_id: str,
    start_date: date,
    end_date: date,
) -> list[ReconciliationCheck]:
    checks: list[ReconciliationCheck] = []
    expected = _expected_security_days_sql()
    for dataset_id in ("daily_st_status", "daily_suspension_status"):
        for market in MARKETS:
            sql = f"""
                WITH expected AS ({expected})
                SELECT e.issue_key
                FROM expected e
                LEFT JOIN current_{dataset_id} a
                  ON a.market_code=e.market_code
                 AND a.security_code=e.security_code
                 AND a.business_date=e.business_date
                WHERE e.market_code=? AND a.business_key IS NULL
            """
            count, examples = _issues(
                service,
                sql,
                [universe_id, start_date, end_date, market],
            )
            checks.append(
                _check(
                    "MDFRQ005",
                    "证券×交易日状态事实完整",
                    "SEVERE",
                    count,
                    f"{dataset_id} 缺少生命周期内证券交易日记录",
                    f"{dataset_id} 覆盖全部生命周期内证券交易日",
                    "补齐状态事实后重新对账，不得从名称或其他事实推断",
                    examples,
                    dataset_id=dataset_id,
                    market_code=market,
                )
            )
    for dataset_id in ("daily_price_unadjusted", "daily_price_limits"):
        for market in RISK_MARKETS:
            sql = f"""
                WITH expected AS ({expected})
                SELECT e.issue_key
                FROM expected e
                JOIN current_daily_suspension_status s
                  ON s.market_code=e.market_code
                 AND s.security_code=e.security_code
                 AND s.business_date=e.business_date
                 AND NOT s.is_suspended
                LEFT JOIN current_{dataset_id} a
                  ON a.market_code=e.market_code
                 AND a.security_code=e.security_code
                 AND a.business_date=e.business_date
                WHERE e.market_code=? AND a.business_key IS NULL
            """
            count, examples = _issues(
                service,
                sql,
                [universe_id, start_date, end_date, market],
            )
            checks.append(
                _check(
                    "MDFRQ006",
                    "非停牌证券×交易日价格事实完整",
                    "SEVERE",
                    count,
                    f"{dataset_id} 存在无法由停牌解释的缺口",
                    f"{dataset_id} 的缺行均可由显式停牌解释",
                    "补齐来源数据或显式停牌证据，禁止把缺行默认成停牌",
                    examples,
                    dataset_id=dataset_id,
                    market_code=market,
                )
            )
    key_sql = """
        WITH prices AS (
            SELECT market_code, security_code, business_date
            FROM current_daily_price_unadjusted
            WHERE business_date BETWEEN ? AND ?
              AND market_code IN ('XSHG', 'XSHE')
        ), limits AS (
            SELECT market_code, security_code, business_date
            FROM current_daily_price_limits
            WHERE business_date BETWEEN ? AND ?
              AND market_code IN ('XSHG', 'XSHE')
        ), mismatch AS (
            (SELECT * FROM prices EXCEPT SELECT * FROM limits)
            UNION ALL
            (SELECT * FROM limits EXCEPT SELECT * FROM prices)
        )
        SELECT market_code || '|' || security_code || '|' ||
               CAST(business_date AS VARCHAR) AS issue_key FROM mismatch
    """
    count, examples = _issues(
        service, key_sql, [start_date, end_date, start_date, end_date]
    )
    checks.append(
        _check(
            "MDFRQ007",
            "未复权行情与涨跌停价格业务键一致",
            "SEVERE",
            count,
            "行情与涨跌停价格键集合不一致",
            "行情与涨跌停价格逐证券逐交易日键一致",
            "按来源请求证据补齐对应事实后重新对账",
            examples,
        )
    )
    suspended_price_sql = """
        SELECT p.business_key AS issue_key
        FROM current_daily_price_unadjusted p
        JOIN current_daily_suspension_status s USING (
            market_code, security_code, business_date
        )
        WHERE p.business_date BETWEEN ? AND ? AND s.is_suspended
          AND p.market_code IN ('XSHG', 'XSHE')
    """
    count, examples = _issues(
        service, suspended_price_sql, [start_date, end_date]
    )
    checks.append(
        _check(
            "MDFRQ008",
            "停牌事实不带伪成交行情",
            "SEVERE",
            count,
            "显式停牌日仍存在成交行情记录",
            "显式停牌缺行情得到正确解释且未复制前值",
            "核对来源停牌与行情请求，禁止用停牌前价格填充",
            examples,
            dataset_id="daily_price_unadjusted",
        )
    )
    return checks

def _daily_value_checks(
    service: MarketFoundationService,
    universe_id: str,
    start_date: date,
    end_date: date,
) -> list[ReconciliationCheck]:
    checks: list[ReconciliationCheck] = []
    price_sql = """
        SELECT business_key AS issue_key
        FROM current_daily_price_unadjusted
        WHERE business_date BETWEEN ? AND ?
          AND market_code IN ('XSHG', 'XSHE') AND (
            open_price IS NULL OR high_price IS NULL OR low_price IS NULL
            OR close_price IS NULL OR volume IS NULL OR amount IS NULL
            OR open_price < 0 OR high_price < 0 OR low_price < 0 OR close_price < 0
            OR volume < 0 OR amount < 0
            OR high_price < greatest(open_price, low_price, close_price)
            OR low_price > least(open_price, high_price, close_price)
        )
    """
    count, examples = _issues(service, price_sql, [start_date, end_date])
    checks.append(
        _check(
            "MDFRQ009",
            "未复权行情数值与 OHLC 区间有效",
            "SEVERE",
            count,
            "行情存在空价格、负值或 OHLC 区间异常",
            "行情价格和成交字段满足非空、非负与 OHLC 约束",
            "按来源修订追加新版本并保留旧版本证据",
            examples,
            dataset_id="daily_price_unadjusted",
        )
    )
    limit_sql = """
        SELECT business_key AS issue_key
        FROM current_daily_price_limits
        WHERE business_date BETWEEN ? AND ?
          AND market_code IN ('XSHG', 'XSHE') AND (
            limit_up_price IS NULL OR limit_down_price IS NULL
            OR limit_up_price < 0 OR limit_down_price < 0
            OR limit_up_price < limit_down_price
        )
    """
    count, examples = _issues(service, limit_sql, [start_date, end_date])
    checks.append(
        _check(
            "MDFRQ010",
            "涨跌停价格值有效",
            "SEVERE",
            count,
            "涨跌停价格存在空值、负值或上下限倒置",
            "涨跌停价格均非负且涨停价不低于跌停价",
            "核对来源价格并追加修订版本",
            examples,
            dataset_id="daily_price_limits",
        )
    )
    for dataset_id, value_column in (
        ("daily_st_status", "is_st"),
        ("daily_suspension_status", "is_suspended"),
    ):
        sql = f"""
            SELECT business_key AS issue_key FROM current_{dataset_id}
            WHERE business_date BETWEEN ? AND ? AND {value_column} IS NULL
        """
        count, examples = _issues(service, sql, [start_date, end_date])
        checks.append(
            _check(
                "MDFRQ011",
                "每日布尔状态值完整",
                "SEVERE",
                count,
                f"{dataset_id} 存在空状态",
                f"{dataset_id} 的状态值均明确",
                "补齐来源状态，不得从证券名称或其他数据集推断",
                examples,
                dataset_id=dataset_id,
            )
        )
    return checks

def _file_integrity_checks(
    service: MarketFoundationService,
) -> list[ReconciliationCheck]:
    parquet_issues: list[str] = []
    for item in service.catalog.rows("SELECT * FROM partition_catalog ORDER BY partition_id"):
        path = Path(item["file_path"])
        if not path.is_file():
            parquet_issues.append(f"MISSING|{item['partition_id']}")
            continue
        if _file_sha256(path) != item["file_sha256"]:
            parquet_issues.append(f"HASH|{item['partition_id']}")
            continue
        try:
            actual = service.catalog.connection.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(path)]
            ).fetchone()[0]
            bad_schema = service.catalog.connection.execute(
                "SELECT count(*) FROM read_parquet(?) WHERE schema_version<>? OR schema_version IS NULL",
                [str(path), SCHEMA_VERSION],
            ).fetchone()[0]
        except Exception:
            parquet_issues.append(f"UNREADABLE|{item['partition_id']}")
            continue
        source_rows = service.catalog.row(
            "SELECT count(*) AS count FROM source_record_catalog WHERE partition_id=?",
            [item["partition_id"]],
        )["count"]
        if int(actual) != int(item["row_count"]) or int(source_rows) != int(
            item["row_count"]
        ):
            parquet_issues.append(f"ROW_COUNT|{item['partition_id']}")
        if int(bad_schema):
            parquet_issues.append(f"SCHEMA|{item['partition_id']}")
    raw_issues: list[str] = []
    normalized_newline_issues: list[str] = []
    for item in service.catalog.rows("SELECT * FROM raw_asset ORDER BY asset_id"):
        path = Path(item["file_path"])
        metadata_path = Path(item["metadata_path"])
        if not path.is_file():
            raw_issues.append(f"MISSING_RAW|{item['asset_id']}")
            continue
        try:
            raw_bytes = path.read_bytes()
            actual_hash = sha256(raw_bytes).hexdigest()
            if actual_hash != item["file_sha256"]:
                normalized_hash = sha256(raw_bytes.replace(b"\r\n", b"\n")).hexdigest()
                if b"\r\n" in raw_bytes and normalized_hash == item["file_sha256"]:
                    normalized_newline_issues.append(
                        f"CRLF_NORMALIZED_HASH|{item['asset_id']}"
                    )
                else:
                    raw_issues.append(f"RAW_HASH|{item['asset_id']}")
            rows = sum(1 for line in raw_bytes.splitlines() if line.strip())
            if rows != int(item["row_count"]):
                raw_issues.append(f"RAW_ROWS|{item['asset_id']}")
        except OSError:
            raw_issues.append(f"RAW_READ|{item['asset_id']}")
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            for field in (
                "asset_id",
                "task_id",
                "chunk_id",
                "dataset_id",
                "market_code",
                "file_sha256",
                "row_count",
            ):
                if str(metadata.get(field)) != str(item[field]):
                    raw_issues.append(f"METADATA_{field.upper()}|{item['asset_id']}")
        except (OSError, json.JSONDecodeError):
            raw_issues.append(f"METADATA_READ|{item['asset_id']}")
    evidence_sql = """
        SELECT p.partition_id AS issue_key
        FROM partition_catalog p
        LEFT JOIN raw_asset r
          ON r.task_id=p.ingestion_task_id
         AND r.dataset_id=p.dataset_id
         AND r.market_code=p.market_code
        WHERE r.asset_id IS NULL
    """
    evidence_count, evidence_examples = _issues(service, evidence_sql, [])
    return [
        _check(
            "MDFRQ012",
            "Parquet 文件、行数、schema 与目录一致",
            "SEVERE",
            len(parquet_issues),
            "Parquet 文件完整性或目录行数不一致",
            "全部 Parquet 文件可读且 SHA-256、行数和 schema 可复验",
            "保留损坏证据并通过新任务追加新分区，禁止修改目录哈希",
            parquet_issues,
        ),
        _check(
            "MDFRQ013",
            "Raw 文件、metadata、行数与 SHA-256 一致",
            "SEVERE",
            len(raw_issues),
            "Raw 资产或 metadata 完整性不一致",
            "全部 Raw 资产及 metadata 的哈希和行数可复验",
            "保留原始证据并重新请求为新 attempt，禁止覆盖原 Raw",
            raw_issues,
        ),
        _check(
            "MDFRQ018",
            "旧 Raw 换行规范化哈希可复验",
            "INFO",
            len(normalized_newline_issues),
            "旧 Windows 写入把 LF 转为 CRLF；规范化后与目录及 metadata 哈希一致",
            "Raw 文件字节哈希与目录直接一致",
            "保留原文件、目录哈希和本次规范化证明，禁止原位改写旧 Raw",
            normalized_newline_issues,
        ),
        _check(
            "MDFRQ014",
            "每个 Parquet 分区具有对应 Raw 请求证据",
            "SEVERE",
            evidence_count,
            "Parquet 分区缺少同任务、数据集和市场的 Raw 证据",
            "全部 Parquet 分区均能关联 Raw 请求证据",
            "核对任务清单与 Raw 资产，禁止补写虚假来源证据",
            evidence_examples,
        ),
    ]

def _source_version_checks(
    service: MarketFoundationService,
) -> list[ReconciliationCheck]:
    version_sql = """
        WITH ordered AS (
            SELECT dataset_id, business_key, source_record_version,
                   supersedes_version,
                   row_number() OVER (
                       PARTITION BY dataset_id, business_key
                       ORDER BY source_record_version
                   ) AS expected_version
            FROM source_record_catalog
        )
        SELECT dataset_id || '|' || business_key || '|' ||
               CAST(source_record_version AS VARCHAR) AS issue_key
        FROM ordered
        WHERE source_record_version<>expected_version
           OR (source_record_version=1 AND supersedes_version IS NOT NULL)
           OR (source_record_version>1 AND supersedes_version<>source_record_version-1)
    """
    count, examples = _issues(service, version_sql, [])
    evidence_sql = """
        SELECT dataset_id || '|' || business_key || '|' ||
               CAST(source_record_version AS VARCHAR) AS issue_key
        FROM source_record_catalog
        WHERE payload_hash<>sha256(payload_json)
           OR source_request_id IS NULL OR trim(source_request_id)=''
           OR partition_id IS NULL
    """
    evidence_count, evidence_examples = _issues(service, evidence_sql, [])
    return [
        _check(
            "MDFRQ015",
            "来源版本连续且 supersedes 链合法",
            "SEVERE",
            count,
            "来源版本序号或 supersedes 链不连续",
            "来源版本从 1 连续递增且修订关系可追溯",
            "保留现有版本并调查登记路径，禁止重排历史版本号",
            examples,
        ),
        _check(
            "MDFRQ016",
            "来源 payload 哈希、请求和分区证据完整",
            "SEVERE",
            evidence_count,
            "来源记录哈希或请求/分区证据不完整",
            "全部来源记录 payload 哈希可复算且绑定请求与分区",
            "从 Raw 和任务清单重建证据，不得修改 payload 哈希迁就现状",
            evidence_examples,
        ),
    ]

def _blocked_capability_checks(
    service: MarketFoundationService,
    run: Mapping[str, Any],
    universe_id: str,
    start_date: date,
    end_date: date,
) -> list[ReconciliationCheck]:
    blocked = json.loads(run["blocked_capabilities_json"])
    checks: list[ReconciliationCheck] = []
    expected = _expected_security_days_sql()
    for item in blocked:
        market = str(item["market_code"])
        dataset_id = str(item["dataset_id"])
        sql = f"""
            WITH expected AS ({expected})
            SELECT e.issue_key
            FROM expected e
            JOIN current_daily_suspension_status s
              ON s.market_code=e.market_code
             AND s.security_code=e.security_code
             AND s.business_date=e.business_date
             AND NOT s.is_suspended
            LEFT JOIN current_{dataset_id} a
              ON a.market_code=e.market_code
             AND a.security_code=e.security_code
             AND a.business_date=e.business_date
            WHERE e.market_code=? AND a.business_key IS NULL
        """
        count, examples = _issues(
            service,
            sql,
            [universe_id, start_date, end_date, market],
        )
        risk_blocking = market in RISK_MARKETS
        checks.append(_check(
            "MDFRQ017",
            "风险范围来源能力可用",
            "SEVERE" if risk_blocking else "INFO",
            count,
            (
                "风险范围来源能力缺口阻断沪深风险快照"
                if risk_blocking
                else "北交所来源能力缺口已留痕，不参与当前沪深风险门禁"
            ),
            (
                "风险范围来源能力完整"
                if risk_blocking
                else "北交所缺口按非阻断能力项保留审计证据"
            ),
            (
                "补齐沪深来源数据后重新对账"
                if risk_blocking
                else "无需为当前风险计算补拉；未来业务纳入北交所时再补齐"
            ),
            examples,
            dataset_id=dataset_id,
            market_code=market,
        ))
    return checks
