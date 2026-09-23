"""
文件作用：独立核对原始米筐响应与缓存候选的行情、涨跌停、ST、停牌和日历事实。
编辑记录：
【首次生成：2026-09-14，逐Raw分块/证券组核对，不用标准CSV作为唯一事实证据。】
"""
from __future__ import annotations
from collections import Counter
import json
from pathlib import Path

import pandas as pd

from riskaudit.data_ingestion.adapters.rqdata import RQDataAdapter
from .cache_import import file_hash
from .cache_validation import CachedCandidateReader
from .service_support import atomic_json


def validate_raw_candidate(root: Path, *, progress=None) -> dict:
    reader = CachedCandidateReader(root)
    connection = reader.connection
    checks = Counter()
    try:
        connection.execute('''CREATE TEMP VIEW life AS SELECT instrument_key, listing_date, termination_date FROM instrument_master_history
            QUALIFY row_number() OVER (PARTITION BY instrument_key ORDER BY effective_start DESC)=1''')
        expected_calendar = {str(row[0]) for row in connection.execute("SELECT business_date FROM market_calendar WHERE market_code='XSHG' AND is_trading_day").fetchall()}
        raw_root = reader.root / 'raw/cache_source/market_chunks/raw/rqdata_real'
        specs = [('rqdata_trading_calendar',None), ('rqdata_st_status','is_st'), ('rqdata_suspension_status','is_suspended'), ('rqdata_market_daily',None)]
        for source, boolean_field in specs:
            paths = sorted((raw_root / source).glob('*.metadata.json'))
            if not paths:
                raise ValueError(f'缺少独立Raw：{source}')
            for part, metadata_path in enumerate(paths, start=1):
                metadata = json.loads(metadata_path.read_text())
                raw_path = metadata_path.parent / Path(metadata['raw_data_path']).name
                if file_hash(raw_path) != metadata['sha256']:
                    raise ValueError('Raw文件哈希失配')
                payload = json.loads(raw_path.read_text())
                raw = payload['data']
                if source == 'rqdata_trading_calendar':
                    days = {str(row['date'])[:10] for row in raw}
                    checks['calendar_raw_differences'] += len(days ^ expected_calendar)
                    continue
                records = RQDataAdapter._long_boolean_records(raw, boolean_field) if boolean_field else raw
                # String conversion preserves the source JSON's declared numeric values;
                # comparison then uses the same six-decimal contract as the price engine.
                frame = pd.DataFrame([{k: None if value is None else str(value) for k,value in row.items()} for row in records])
                if not {'order_book_id','date'} <= set(frame.columns):
                    raise ValueError('Raw证券或日期字段缺失')
                codes = sorted(set(payload.get('requested_order_book_ids') or frame.order_book_id.unique()))
                for offset in range(0, len(codes), 100):
                    selected = codes[offset:offset+100]
                    chunk = frame.loc[frame.order_book_id.isin(selected)].copy()
                    connection.register('_raw_group', chunk)
                    try:
                        connection.execute('''CREATE OR REPLACE TEMP VIEW eligible_raw AS SELECT r.* FROM _raw_group r JOIN life l ON r.order_book_id=l.instrument_key
                            WHERE CAST(r.date AS DATE)>=l.listing_date AND (l.termination_date IS NULL OR CAST(r.date AS DATE)<l.termination_date)''')
                        checks[source + '_unknown_master'] += connection.execute('SELECT count(*) FROM _raw_group r LEFT JOIN life l ON r.order_book_id=l.instrument_key WHERE l.instrument_key IS NULL').fetchone()[0]
                        checks[source + '_duplicate_keys'] += connection.execute('SELECT count(*)-count(DISTINCT (order_book_id,date)) FROM eligible_raw').fetchone()[0]
                        if boolean_field:
                            dataset = 'daily_st_status' if boolean_field=='is_st' else 'daily_suspension_status'
                            query = f'''SELECT count(*) FROM eligible_raw r LEFT JOIN {dataset} p ON r.order_book_id=p.instrument_key AND CAST(r.date AS DATE)=p.business_date
                                WHERE p.business_key IS NULL OR CAST(r.{boolean_field} AS BOOLEAN) IS DISTINCT FROM p.{boolean_field}'''
                        else:
                            pairs = [('open','open_price'),('high','high_price'),('low','low_price'),('close','close_price'),('volume','volume'),('total_turnover','amount')]
                            diffs = [f'CAST(r."{raw_field}" AS DECIMAL(24,6)) IS DISTINCT FROM p.{fact_field}' for raw_field, fact_field in pairs]
                            diffs += [f'CAST(r.{raw_field} AS DECIMAL(24,6)) IS DISTINCT FROM l.{fact_field}' for raw_field,fact_field in [('limit_up','limit_up_price'),('limit_down','limit_down_price')]]
                            query = '''SELECT count(*) FROM eligible_raw r LEFT JOIN daily_price_unadjusted p ON r.order_book_id=p.instrument_key AND CAST(r.date AS DATE)=p.business_date
                                LEFT JOIN daily_price_limits l ON p.business_key=l.business_key WHERE p.business_key IS NULL OR l.business_key IS NULL OR ''' + ' OR '.join(diffs)
                        checks[source + '_value_or_key_differences'] += connection.execute(query).fetchone()[0]
                        if source == 'rqdata_market_daily':
                            # Every non-suspended fact must also have a Raw price row;
                            # explicit suspension placeholders are the only allowed extras.
                            query = '''SELECT count(*) FROM daily_price_unadjusted p JOIN daily_suspension_status s USING(business_key)
                                LEFT JOIN eligible_raw r ON r.order_book_id=p.instrument_key AND CAST(r.date AS DATE)=p.business_date
                                WHERE NOT s.is_suspended AND r.order_book_id IS NULL AND p.instrument_key IN (''' + ','.join('?' for _ in selected) + ')'
                            checks['live_fact_without_raw_price'] += connection.execute(query, selected).fetchone()[0]
                    finally:
                        connection.execute('DROP VIEW eligible_raw')
                        connection.unregister('_raw_group')
                if progress:
                    progress({'raw_dataset':source,'chunks_checked':part,'total_chunks':len(paths)})
        report = {'status':'PASS' if not any(checks.values()) else 'FAIL', 'checks':dict(checks),
                  'plan_hash':reader.manifest['plan']['plan_hash'], 'import_manifest_sha256':file_hash(reader.root/'cache_import_manifest.json'),
                  'scope':'Raw-to-fact equality on lifecycle-valid source rows; complete expected coverage separately checked in cache_quality_report.json',
                  'rqdata_accessed':False}
        atomic_json(reader.root/'cache_raw_reconciliation.json',report)
        return report
    except Exception as exc:
        atomic_json(reader.root/'cache_raw_reconciliation.json',{'status':'FAILED','checks':dict(checks),'error':str(exc)})
        raise
    finally:
        reader.close()
