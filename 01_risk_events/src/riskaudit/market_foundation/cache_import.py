"""
文件作用：将可追溯的本地风险批次缓存离线导入独立六类事实候选，禁止发布或覆盖源目录。
编辑记录：
【首次生成：2026-09-14，增加哈希计划、源副本、生命周期过滤和非正式候选入库。】
"""
from __future__ import annotations

from collections import Counter
import csv
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import shutil
from typing import Callable

import pandas as pd
import duckdb

from riskaudit.data_ingestion.adapters.rqdata import RQDataAdapter
from .constants import DATASETS
from .facts import canonical_json
from .service import MarketFoundationService
from .service_support import atomic_json

STANDARD_FILES = {
    'market_calendar': 'trading_calendar.csv',
    'instrument_master_history': 'security_status_daily.csv',
    'daily_price_unadjusted': 'stock_market_daily.csv',
    'daily_st_status': 'security_risk_status.csv',
}
MODE = 'RQDATA_OFFLINE_CACHE_CANDIDATE'


def file_hash(path: Path) -> str:
    digest = sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def _day(value: str) -> date:
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError(f'日期必须规范化：{value}')
    return parsed


def plan_cached_import(source_batch: str | Path) -> dict:
    root = Path(source_batch).resolve(strict=True)
    summary = root / 'risk_results' / root.name / 'calculation_manifest.json'
    manifest = json.loads(summary.read_text(encoding='utf-8'))
    if manifest.get('status') != 'SUCCEEDED' or manifest.get('gate_allow_run') is not True:
        raise ValueError('源批次必须具有成功且通过风险门禁的manifest')
    start, end = _day(manifest['observation_start']), _day(manifest['observation_end'])
    if start > end:
        raise ValueError('源批次观察期倒置')
    files = [summary, *(root / 'risk_input_code' / name for name in STANDARD_FILES.values())]
    metadata = sorted(root.glob('market_chunks/raw/rqdata_real/*/*.metadata.json'))
    metadata += sorted(root.glob('raw/rqdata_real/*/*.metadata.json'))
    required = {'rqdata_market_daily', 'rqdata_security_master', 'rqdata_st_status', 'rqdata_suspension_status', 'rqdata_trading_calendar'}
    if not required <= {p.parent.name for p in metadata}:
        raise ValueError('源批次缺少六类市场事实所需的独立Raw证据')
    for path in metadata:
        value = json.loads(path.read_text(encoding='utf-8'))
        raw = path.parent / Path(value['raw_data_path']).name
        if file_hash(raw) != value['sha256']:
            raise ValueError(f'Raw哈希不一致：{raw}')
        files.extend([path, raw])
    sources = {}
    for path in files:
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(root) or path.is_symlink():
            raise ValueError('源文件不能通过链接越出批次目录')
        sources[str(path.relative_to(root))] = {'sha256': file_hash(path), 'bytes': path.stat().st_size}
    with (root / 'risk_input_code/trading_calendar.csv').open(encoding='utf-8-sig', newline='') as stream:
        calendar = list(csv.DictReader(stream))
    trading = sorted(_day(row['calendar_date']) for row in calendar if row['is_trading_day'].lower() in {'true', '1'})
    baseline = max((d for d in trading if d < start), default=None)
    if baseline is None or not any(start <= d <= end for d in trading):
        raise ValueError('源交易日历缺少观察期或期初基线')
    plan = {'format': 'offline_market_cache_v1', 'execution_mode': MODE,
            'source_batch': str(root), 'observation_start': str(start), 'observation_end': str(end),
            'baseline_date': str(baseline), 'sources': sources,
            'publish_allowed': False, 'rqdata_accessed': False}
    plan['plan_hash'] = sha256(canonical_json(plan).encode()).hexdigest()
    return plan


def _common(row: dict, day: str) -> dict:
    code = row['security_code']
    if row['market_code'] not in {'XSHG', 'XSHE'} or not code.endswith('.' + row['market_code']):
        raise ValueError('离线风险候选仅接受规范的沪深证券键')
    _day(day)
    return {'market_code': row['market_code'], 'security_code': code.split('.')[0],
            'instrument_key': code, 'business_date': day}


def _csv_chunks(path: Path, dataset: str):
    """Sort on disk/inside DuckDB so source ordering cannot create thousands of tiny partitions."""
    field = 'calendar_date' if dataset == 'market_calendar' else 'status_start_date' if dataset == 'daily_st_status' else 'trading_date'
    connection = duckdb.connect()
    try:
        connection.execute("SET memory_limit='512MB'")
        connection.execute('SET threads=2')
        cursor = connection.execute(f'SELECT * FROM read_csv(?, all_varchar=true, header=true) ORDER BY "{field}", market_code', [str(path)])
        columns = [item[0] for item in cursor.description]
        while rows := cursor.fetchmany(25000):
            yield [dict(zip(columns, ('' if value is None else value for value in row), strict=True)) for row in rows]
    finally:
        connection.close()


def _records(source: Path, dataset: str, counts: Counter):
    master = pd.read_csv(source / 'risk_input_code/security_status_daily.csv', dtype=str, keep_default_na=False)
    lifecycle = {}
    for code, rows in master.groupby('security_code', sort=False):
        if rows[['listed_date', 'delisted_date']].drop_duplicates().shape[0] != 1:
            raise ValueError(f'生命周期版本冲突：{code}')
        last = rows.iloc[-1]
        lifecycle[code] = (_day(last.listed_date), _day(last.delisted_date) if last.delisted_date else date.max)

    def inside(code, day):
        if code not in lifecycle:
            raise ValueError(f'主数据缺少证券：{code}')
        start, end = lifecycle[code]
        return start <= _day(day) < end

    if dataset == 'instrument_master_history':
        for code, rows in master.groupby('security_code', sort=False):
            records = rows.sort_values('status_date').to_dict('records')
            for index, row in enumerate(records):
                effective = _day(row['status_date'])
                following = _day(records[index + 1]['status_date']) if index + 1 < len(records) else None
                if following and following <= effective:
                    raise ValueError(f'主数据状态日期重复：{code}')
                common = _common(row, row['status_date'])
                common.pop('business_date')
                yield {**common, 'security_type': row['security_type'], 'board_code': row['board_code'],
                       'listing_date': row['listed_date'], 'termination_date': row['delisted_date'] or None,
                       'effective_start': str(effective), 'effective_end': str(following - timedelta(days=1)) if following else None,
                       'standard_record': row}
        return
    if dataset == 'daily_suspension_status':
        for path in sorted(source.glob('market_chunks/raw/rqdata_real/rqdata_suspension_status/*.metadata.json')):
            metadata = json.loads(path.read_text())
            raw = path.parent / Path(metadata['raw_data_path']).name
            payload = json.loads(raw.read_text())
            for row in RQDataAdapter._long_boolean_records(payload['data'], 'is_suspended'):
                code, day = str(row['order_book_id']), str(row['date'])[:10]
                if not inside(code, day):
                    counts['outside_lifecycle'] += 1
                    continue
                yield {**_common({'market_code': code.split('.')[1], 'security_code': code}, day),
                       'is_suspended': RQDataAdapter._as_bool(row['is_suspended']),
                       'raw_sha256': metadata['sha256']}
        return
    filename = STANDARD_FILES.get(dataset, 'stock_market_daily.csv')
    for rows in _csv_chunks(source / 'risk_input_code' / filename, dataset):
        for row in rows:
            if dataset == 'market_calendar':
                if row['market_code'] != 'cn' or row['is_trading_day'].lower() not in {'true', 'false', '1', '0'}:
                    raise ValueError('缓存日历市场/布尔格式不支持')
                _day(row['calendar_date'])
                for market in ('XSHG', 'XSHE'):
                    yield {'market_code': market, 'business_date': row['calendar_date'],
                           'is_trading_day': row['is_trading_day'].lower() in {'true', '1'}, 'standard_record': row}
                continue
            day = row['status_start_date'] if dataset == 'daily_st_status' else row['trading_date']
            common = _common(row, day)
            if not inside(row['security_code'], day):
                counts['outside_lifecycle'] += 1
                continue
            if dataset == 'daily_st_status':
                if row['risk_status_type'] != 'ST' or row['status_end_date'] != day or row['risk_status_value'] not in {'生效', '未生效'}:
                    raise ValueError('只接受显式逐日ST状态，不猜测区间或未知值')
                yield {**common, 'is_st': row['risk_status_value'] == '生效', 'standard_record': row}
            elif dataset == 'daily_price_limits':
                yield {**common, 'limit_up': row['limit_up_price'] or None, 'limit_down': row['limit_down_price'] or None}
            else:
                if row['trading_status'] not in {'停牌', '正常成交'}:
                    raise ValueError('未知行情状态')
                yield {**common, 'open': row['open_price'] or None, 'high': row['high_price'] or None,
                       'low': row['low_price'] or None, 'close': row['close_price'] or None,
                       'volume': row['volume'] or None, 'amount': row['turnover'] or None,
                       'adjustment': 'NONE', 'standard_record': row}


def import_cached_market(plan: dict, target_root: str | Path, *, progress: Callable[[dict], None] | None = None) -> dict:
    """Build a diagnostic candidate. No snapshot rows or publication APIs are used."""
    unsigned = {k: v for k, v in plan.items() if k != 'plan_hash'}
    if sha256(canonical_json(unsigned).encode()).hexdigest() != plan.get('plan_hash'):
        raise ValueError('离线计划哈希无效')
    source, target = Path(plan['source_batch']).resolve(strict=True), Path(target_root).resolve()
    if target.exists() or target.is_relative_to(source) or source.is_relative_to(target):
        raise ValueError('目标必须是独立且不存在的新目录')
    for relative, evidence in plan['sources'].items():
        path = (source / relative).resolve(strict=True)
        if not path.is_relative_to(source) or file_hash(path) != evidence['sha256']:
            raise ValueError(f'计划后源文件变更：{relative}')
    target.mkdir(parents=True, exist_ok=False)
    state = {'status': 'IMPORTING', 'plan': plan, 'datasets': {}, 'publish_allowed': False, 'rqdata_accessed': False}
    state_path = target / 'cache_import_manifest.json'
    atomic_json(state_path, state)
    service = None
    try:
        copy_root = target / 'raw/cache_source'
        for relative, evidence in plan['sources'].items():
            dest = copy_root / relative
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source / relative, dest)
            if file_hash(dest) != evidence['sha256']:
                raise ValueError(f'源副本哈希不一致：{relative}')
        service = MarketFoundationService(target, execution_mode=MODE, source_system='RQDATA', allow_snapshot_candidates=False,
                                          refresh_range_end=_day(plan['observation_end']))
        # Bound DuckDB memory/threads on a local desktop; external sort may spill inside the candidate.
        service.catalog.execute("SET memory_limit='1GB'")
        service.catalog.execute('SET threads=2')
        # This envelope describes receipt from the local RQDATA_CACHE source.
        # Original RQData receipt timestamps remain unchanged in copied Raw metadata
        # and standard_record; never present the offline receipt as an API fetch.
        received = datetime.now(timezone.utc)
        task = 'cache_' + plan['plan_hash'][:24]
        for dataset in DATASETS:
            counts = Counter()
            batch = []
            def flush():
                result = service.facts.append(dataset, batch, task_id=task,
                    source_request_id='offline-cache:' + plan['plan_hash'], source_received_at=received,
                    source_system='RQDATA_CACHE', refresh_views=False)
                if result['idempotent_rows']:
                    raise ValueError(f'源数据出现重复业务键：{dataset}')
                counts['written_rows'] += result['written_rows']
                counts['partitions'] += len(result['partitions'])
                if progress:
                    progress({'dataset': dataset, **dict(counts)})
                batch.clear()
            for record in _records(copy_root, dataset, counts):
                batch.append(record)
                if len(batch) >= 25000:
                    flush()
            if batch:
                flush()
            revisions = service.catalog.row('SELECT count(*) AS n FROM source_record_catalog WHERE dataset_id=? AND source_record_version<>1', [dataset])['n']
            if revisions or not counts['written_rows']:
                raise ValueError(f'源数据存在冲突业务键或空数据集：{dataset}')
            state['datasets'][dataset] = dict(counts)
            atomic_json(state_path, state)
        service.facts.refresh_views()
        state['partitions'] = service.catalog.rows('SELECT file_path, file_sha256, row_count, dataset_id FROM partition_catalog ORDER BY file_path')
        for relative, evidence in plan['sources'].items():
            if file_hash(source / relative) != evidence['sha256']:
                raise ValueError(f'入库期间源文件变更：{relative}')
        state['status'] = 'IMPORTED_UNVALIDATED'
        state['source_files_unchanged'] = True
        atomic_json(state_path, state)
        return state
    except Exception as exc:
        state.update(status='FAILED', error=str(exc))
        atomic_json(state_path, state)
        raise
    finally:
        if service is not None:
            service.close()
