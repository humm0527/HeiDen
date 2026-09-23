"""
文件作用：复验离线候选成员、六类事实键和值，并仅向隔离诊断提供候选四表读取。
编辑记录：
【首次生成：2026-09-14，新增生命周期覆盖检查和实际Parquet事实回读，不绕过正式快照门禁。】
"""
from __future__ import annotations
import csv
from hashlib import sha256
import json
import re
from pathlib import Path

import duckdb
import pandas as pd

from .cache_import import MODE, STANDARD_FILES, file_hash
from .constants import DATASETS
from .facts import canonical_json
from .service_support import atomic_json


class CachedCandidateReader:
    """Diagnostic-only reader. Never used as a fallback for a formal snapshot."""

    def __init__(self, root: str | Path):
        self.root = Path(root).resolve(strict=True)
        self.manifest = json.loads((self.root / 'cache_import_manifest.json').read_text())
        if self.manifest['status'] != 'IMPORTED_UNVALIDATED' or self.manifest['plan']['execution_mode'] != MODE:
            raise ValueError('离线导入未完整完成或不是缓存候选')
        unsigned = {k: v for k, v in self.manifest['plan'].items() if k != 'plan_hash'}
        if sha256(canonical_json(unsigned).encode()).hexdigest() != self.manifest['plan'].get('plan_hash'):
            raise ValueError('候选计划哈希不一致')
        self.connection = duckdb.connect()
        self.connection.execute("SET memory_limit='1GB'")
        self.connection.execute('SET threads=2')
        self.connection.execute('SET preserve_insertion_order=false')
        try:
            for dataset in DATASETS:
                members = [item for item in self.manifest['partitions'] if item['dataset_id'] == dataset]
                if not members:
                    raise ValueError(f'候选缺少数据集：{dataset}')
                files = []
                for member in members:
                    path = Path(member['file_path']).resolve(strict=True)
                    if not path.is_relative_to(self.root) or file_hash(path) != member['file_sha256']:
                        raise ValueError(f'候选成员路径或哈希不一致：{path.name}')
                    files.append("'" + str(path).replace("'", "''") + "'")
                self.connection.execute(f'CREATE VIEW {dataset} AS SELECT * FROM read_parquet([{",".join(files)}])')
            self.columns = {}
            for dataset, filename in STANDARD_FILES.items():
                relative = 'risk_input_code/' + filename
                path = self.root / 'raw/cache_source' / relative
                if file_hash(path) != self.manifest['plan']['sources'][relative]['sha256']:
                    raise ValueError('候选内标准来源副本哈希不一致')
                with path.open(encoding='utf-8-sig', newline='') as stream:
                    self.columns[dataset] = next(csv.reader(stream))
                if not all(re.fullmatch('[a-z][a-z0-9_]*', name) for name in self.columns[dataset]):
                    raise ValueError('标准字段名称不符合代码表契约')
        except Exception:
            self.connection.close()
            raise

    def close(self):
        self.connection.close()

    def frame(self, dataset: str, *, start: str | None = None, end: str | None = None, codes: list[str] | None = None) -> pd.DataFrame:
        columns = self.columns[dataset]
        selected = ','.join(f"json_extract_string(p.payload_json, '$.standard_record.{name}') AS \"{name}\"" for name in columns)
        filters, params = [], []
        if dataset == 'market_calendar':
            filters.append("p.market_code='XSHG'")  # Both market facts retain the same original cn row.
        elif codes is not None:
            if not codes:
                return pd.DataFrame(columns=columns)
            filters.append('p.instrument_key IN (' + ','.join('?' for _ in codes) + ')')
            params.extend(codes)
        if start and end and dataset != 'instrument_master_history':
            filters.append('p.business_date BETWEEN ? AND ?')
            params.extend([start, end])
        where = ' WHERE ' + ' AND '.join(filters) if filters else ''
        frame = self.connection.execute(f'SELECT {selected} FROM {dataset} p{where} ORDER BY p.business_key', params).fetchdf().fillna('')
        return frame

    def four_tables(self, *, start: str, end: str, codes: list[str]) -> dict[str, pd.DataFrame]:
        """Rehydrate exact source metadata, but consume the normalized six-fact values."""
        output = {dataset: self.frame(dataset, start=start, end=end, codes=codes) for dataset in STANDARD_FILES}
        prices = output['daily_price_unadjusted']
        keys = ['market_code', 'security_code', 'trading_date']
        sql = '''SELECT p.market_code, p.instrument_key AS security_code, CAST(p.business_date AS VARCHAR) AS trading_date,
            CAST(p.open_price AS VARCHAR) AS open_price, CAST(p.high_price AS VARCHAR) AS high_price,
            CAST(p.low_price AS VARCHAR) AS low_price, CAST(p.close_price AS VARCHAR) AS close_price,
            CAST(p.volume AS VARCHAR) AS volume, CAST(p.amount AS VARCHAR) AS turnover,
            CAST(l.limit_up_price AS VARCHAR) AS limit_up_price, CAST(l.limit_down_price AS VARCHAR) AS limit_down_price, s.is_suspended
            FROM daily_price_unadjusted p
            LEFT JOIN daily_price_limits l ON p.business_key=l.business_key
            LEFT JOIN daily_suspension_status s ON p.business_key=s.business_key
            WHERE p.business_date BETWEEN ? AND ? AND p.instrument_key IN (''' + ','.join('?' for _ in codes) + ')'
        typed = self.connection.execute(sql, [start, end, *codes]).fetchdf()
        if len(typed) != len(prices) or typed.is_suspended.isna().any():
            raise ValueError('候选价格、涨跌停或停牌键不一致')
        merged = prices[keys].merge(typed, on=keys, validate='one_to_one', how='left')
        for field in ['open_price', 'high_price', 'low_price', 'close_price', 'volume', 'turnover', 'limit_up_price', 'limit_down_price']:
            prices[field] = merged[field].map(lambda value: '' if pd.isna(value) else str(value))
        prices['trading_status'] = merged.is_suspended.map({True: '停牌', False: '正常成交'})
        # Typed ST and PIT master fields must also be consumed, not only their provenance copies.
        for dataset, field, mapping in [('daily_st_status', 'is_st', {True: '生效', False: '未生效'})]:
            rows = self.connection.execute(f'SELECT business_key, {field} FROM {dataset} WHERE business_date BETWEEN ? AND ? AND instrument_key IN ({",".join("?" for _ in codes)}) ORDER BY business_key', [start, end, *codes]).fetchdf()
            if len(rows) != len(output[dataset]) or rows[field].isna().any():
                raise ValueError('候选ST状态不能缺失')
            output[dataset]['risk_status_value'] = rows[field].map(mapping)
        masters = self.connection.execute('SELECT board_code FROM instrument_master_history WHERE instrument_key IN (' + ','.join('?' for _ in codes) + ') ORDER BY business_key', codes).fetchdf()
        output['instrument_master_history']['board_code'] = masters.board_code
        return output


def validate_cached_candidate(root: str | Path, *, progress=None) -> dict:
    reader = CachedCandidateReader(root)
    connection = reader.connection
    findings = {}
    try:
        plan = reader.manifest['plan']
        catalog_path = str(reader.root / 'duckdb/market_catalog.duckdb').replace("'", "''")
        connection.execute(f"ATTACH '{catalog_path}' AS cached (READ_ONLY)")
        findings['catalog_payload_hash_mismatch'] = connection.execute('SELECT count(*) FROM cached.source_record_catalog WHERE sha256(payload_json)<>payload_hash').fetchone()[0]
        for relative, evidence in plan['sources'].items():
            path = reader.root / 'raw/cache_source' / relative
            if not path.resolve().is_relative_to(reader.root) or file_hash(path) != evidence['sha256']:
                raise ValueError(f'候选内来源证据损坏：{relative}')
        for dataset in DATASETS:
            if progress:
                progress({'checking_catalog': dataset})
            row = connection.execute(f'SELECT count(*), count(DISTINCT business_key) FROM {dataset}').fetchone()
            findings[dataset + '_duplicate_keys'] = row[0] - row[1]
            findings[dataset + '_row_count_mismatch'] = abs(row[0] - reader.manifest['datasets'][dataset]['written_rows'])
            findings[dataset + '_payload_hash_mismatch'] = connection.execute(f'SELECT count(*) FROM {dataset} WHERE sha256(payload_json)<>payload_hash').fetchone()[0]
            # Once both JSON hashes are verified, compare fixed-size hashes instead of
            # materializing multi-GB payload strings inside an EXCEPT hash table.
            fields = 'business_key,source_record_version,payload_hash'
            left = f"SELECT {fields} FROM cached.source_record_catalog WHERE dataset_id='{dataset}'"
            right = f'SELECT {fields} FROM {dataset}'
            findings[dataset + '_catalog_missing'] = connection.execute(f'SELECT count(*) FROM (({right}) EXCEPT ALL ({left}))').fetchone()[0]
            findings[dataset + '_catalog_extra'] = connection.execute(f'SELECT count(*) FROM (({left}) EXCEPT ALL ({right}))').fetchone()[0]
        connection.execute('''CREATE TEMP VIEW lifecycle AS SELECT market_code, instrument_key, listing_date, termination_date
            FROM instrument_master_history QUALIFY row_number() OVER (PARTITION BY instrument_key ORDER BY effective_start DESC)=1''')
        connection.execute('''CREATE TEMP VIEW expected AS SELECT l.market_code, l.instrument_key, c.business_date,
            l.market_code || '|' || split_part(l.instrument_key, '.', 1) || '|' || CAST(c.business_date AS VARCHAR) AS business_key
            FROM lifecycle l JOIN market_calendar c ON l.market_code=c.market_code
            WHERE c.is_trading_day AND c.business_date>=l.listing_date AND (l.termination_date IS NULL OR c.business_date<l.termination_date)''')
        for dataset in ['daily_price_unadjusted', 'daily_price_limits', 'daily_st_status', 'daily_suspension_status']:
            # Only ST requires a pre-window baseline. Raw suspension may optionally
            # include it, but the risk engine does not consume pre-window suspension.
            begin = plan['baseline_date'] if dataset == 'daily_st_status' else plan['observation_start']
            scope = f"business_date BETWEEN DATE '{begin}' AND DATE '{plan['observation_end']}'"
            allowed_scope = f"business_date BETWEEN DATE '{plan['baseline_date']}' AND DATE '{plan['observation_end']}'" if dataset == 'daily_suspension_status' else scope
            findings[dataset + '_missing_keys'] = connection.execute(f'SELECT count(*) FROM (SELECT business_key FROM expected WHERE {scope} EXCEPT SELECT business_key FROM {dataset})').fetchone()[0]
            findings[dataset + '_unexpected_keys'] = connection.execute(f'SELECT count(*) FROM (SELECT business_key FROM {dataset} EXCEPT SELECT business_key FROM expected WHERE {allowed_scope})').fetchone()[0]
        findings['live_price_or_limit_missing'] = connection.execute('''SELECT count(*) FROM daily_price_unadjusted p
            JOIN daily_suspension_status s USING(business_key) JOIN daily_price_limits l USING(business_key)
            WHERE NOT s.is_suspended AND (p.open_price IS NULL OR p.high_price IS NULL OR p.low_price IS NULL OR p.close_price IS NULL
              OR p.volume IS NULL OR p.amount IS NULL OR l.limit_up_price IS NULL OR l.limit_down_price IS NULL)''').fetchone()[0]
        findings['invalid_price_relationship'] = connection.execute('''SELECT count(*) FROM daily_price_unadjusted p JOIN daily_price_limits l USING(business_key)
            WHERE p.close_price<=0 OR p.volume<0 OR p.amount<0 OR p.high_price<p.low_price OR p.close_price>p.high_price
            OR p.close_price<p.low_price OR l.limit_down_price>l.limit_up_price''').fetchone()[0]
        findings['st_unknown'] = connection.execute('SELECT count(*) FROM daily_st_status WHERE is_st IS NULL').fetchone()[0]
        findings['suspension_unknown'] = connection.execute('SELECT count(*) FROM daily_suspension_status WHERE is_suspended IS NULL').fetchone()[0]
        # Independently compare persisted typed fields against the original text carried in provenance.
        expressions = []
        for field, original in [('open_price','open_price'), ('high_price','high_price'), ('low_price','low_price'), ('close_price','close_price'), ('volume','volume'), ('amount','turnover')]:
            expressions.append(f"p.{field} IS DISTINCT FROM CAST(NULLIF(json_extract_string(p.payload_json, '$.standard_record.{original}'),'') AS DECIMAL(24,6))")
        for field in ['limit_up_price', 'limit_down_price']:
            expressions.append(f"l.{field} IS DISTINCT FROM CAST(NULLIF(json_extract_string(p.payload_json, '$.standard_record.{field}'),'') AS DECIMAL(24,6))")
        expressions.append("s.is_suspended IS DISTINCT FROM (json_extract_string(p.payload_json, '$.standard_record.trading_status')='停牌')")
        findings['normalized_prices_limits_suspension_differences'] = 0
        months = connection.execute("SELECT DISTINCT CAST(date_trunc('month',business_date) AS DATE) FROM daily_price_unadjusted ORDER BY 1").fetchall()
        for (month,) in months:
            following = month.replace(year=month.year+1, month=1) if month.month==12 else month.replace(month=month.month+1)
            select = lambda dataset: f'(SELECT * FROM {dataset} WHERE business_date>=? AND business_date<?)'
            query = f'SELECT count(*) FROM {select("daily_price_unadjusted")} p JOIN {select("daily_price_limits")} l USING(business_key) JOIN {select("daily_suspension_status")} s USING(business_key) WHERE ' + ' OR '.join(expressions)
            findings['normalized_prices_limits_suspension_differences'] += connection.execute(query, [month, following]*3).fetchone()[0]
            if progress:
                progress({'checked_normalized_month': str(month)})
        findings['normalized_st_differences'] = connection.execute("SELECT count(*) FROM daily_st_status WHERE is_st IS DISTINCT FROM (json_extract_string(payload_json,'$.standard_record.risk_status_value')='生效')").fetchone()[0]
        findings['normalized_master_differences'] = connection.execute("""SELECT count(*) FROM instrument_master_history WHERE
            board_code IS DISTINCT FROM json_extract_string(payload_json,'$.standard_record.board_code') OR
            listing_date IS DISTINCT FROM CAST(json_extract_string(payload_json,'$.standard_record.listed_date') AS DATE) OR
            termination_date IS DISTINCT FROM CAST(NULLIF(json_extract_string(payload_json,'$.standard_record.delisted_date'),'') AS DATE) OR
            effective_start IS DISTINCT FROM CAST(json_extract_string(payload_json,'$.standard_record.status_date') AS DATE)""").fetchone()[0]
        for dataset, filename in STANDARD_FILES.items():
            columns = reader.columns[dataset]
            original = ','.join(f'"{name}" := coalesce(s."{name}",\'\')' for name in columns)
            stored = ','.join(f"\"{name}\" := json_extract_string(p.payload_json,'$.standard_record.{name}')" for name in columns)
            source_path = str(reader.root / 'raw/cache_source/risk_input_code' / filename).replace("'", "''")
            source_sql = f"SELECT sha256(to_json(struct_pack({original}))) AS row_hash FROM read_csv('{source_path}', header=true, all_varchar=true) s"
            candidate_filter = " WHERE p.market_code='XSHG'" if dataset == 'market_calendar' else ''
            if dataset in {'daily_price_unadjusted', 'daily_st_status'}:
                field = 'trading_date' if dataset == 'daily_price_unadjusted' else 'status_start_date'
                source_sql += f' JOIN lifecycle l ON s.security_code=l.instrument_key WHERE CAST(s.{field} AS DATE)>=l.listing_date AND (l.termination_date IS NULL OR CAST(s.{field} AS DATE)<l.termination_date)'
            stored_sql = f'SELECT sha256(to_json(struct_pack({stored}))) AS row_hash FROM {dataset} p{candidate_filter}'
            findings[dataset + '_roundtrip_missing'] = connection.execute(f'SELECT count(*) FROM (({source_sql}) EXCEPT ALL ({stored_sql}))').fetchone()[0]
            findings[dataset + '_roundtrip_extra'] = connection.execute(f'SELECT count(*) FROM (({stored_sql}) EXCEPT ALL ({source_sql}))').fetchone()[0]
            if progress:
                progress({'dataset_roundtrip_completed': dataset})
        if progress:
            progress(findings)
        report = {'status': 'PASS' if not any(findings.values()) else 'FAIL', 'scope': MODE, 'plan_hash': plan['plan_hash'],
                  'import_manifest_sha256': file_hash(reader.root / 'cache_import_manifest.json'),
                  'findings': findings, 'publish_allowed': False, 'rqdata_accessed': False,
                  'limitations': ['Coverage relative to cached lifecycle/calendar only', 'No 2023 annual prices or adjusted-return coverage claim', 'Not a P4 publication gate']}
        atomic_json(reader.root / 'cache_quality_report.json', report)
        return report
    except Exception as exc:
        atomic_json(reader.root / 'cache_quality_report.json', {'status': 'FAILED', 'findings': findings, 'error': str(exc), 'publish_allowed': False})
        raise
    finally:
        reader.close()
