"""
文件作用：对已获用户确认范围的离线缓存执行独立版本发布门禁，保留旧P4全范围口径。
编辑记录：
【首次生成：2026-09-14，绑定六类质量、全区间结果复算、源/结果哈希和人工不可变发布确认。】
"""
from __future__ import annotations
from datetime import date
import json
from pathlib import Path

from .cache_import import file_hash
from .cache_validation import CachedCandidateReader
from .constants import DATASETS, RISK_MARKETS
from .facts import canonical_json
from .service import MarketFoundationError, MarketFoundationService
from .service_support import now, atomic_json

CACHE_RULE_VERSION = 'market_cache_reconciliation_v1'
CACHE_P4_MODE = 'RQDATA_OFFLINE_CACHE_P4'


def _evidence(root: Path) -> dict:
    reader = CachedCandidateReader(root)
    try:
        plan = reader.manifest['plan']
        quality_path = root / 'cache_quality_report.json'
        quality = json.loads(quality_path.read_text())
        required = {'catalog_payload_hash_mismatch', 'normalized_prices_limits_suspension_differences', 'normalized_st_differences', 'normalized_master_differences'}
        required.update(dataset + suffix for dataset in DATASETS for suffix in ['_duplicate_keys','_row_count_mismatch','_catalog_missing','_catalog_extra','_payload_hash_mismatch'])
        required.update(dataset + suffix for dataset in ['daily_price_unadjusted','daily_price_limits','daily_st_status','daily_suspension_status'] for suffix in ['_missing_keys','_unexpected_keys'])
        required.update(dataset + suffix for dataset in ['market_calendar','instrument_master_history','daily_price_unadjusted','daily_st_status'] for suffix in ['_roundtrip_missing','_roundtrip_extra'])
        if quality.get('status') != 'PASS' or quality.get('plan_hash') != plan['plan_hash'] or not required <= quality.get('findings', {}).keys() or any(quality['findings'].values()):
            raise ValueError('离线六类质量报告未完成或未通过')
        if quality.get('import_manifest_sha256') != file_hash(root / 'cache_import_manifest.json'):
            raise ValueError('质量报告未绑定当前导入成员清单，必须重新核验')
        raw_path = root / 'cache_raw_reconciliation.json'
        if not raw_path.is_file():
            raise ValueError('缺少Raw独立核查报告')
        raw = json.loads(raw_path.read_text())
        raw_required = {'calendar_raw_differences','live_fact_without_raw_price'}
        raw_required.update(dataset+suffix for dataset in ['rqdata_market_daily','rqdata_st_status','rqdata_suspension_status'] for suffix in ['_unknown_master','_duplicate_keys','_value_or_key_differences'])
        if (raw.get('status')!='PASS' or raw.get('plan_hash')!=plan['plan_hash']
            or raw.get('import_manifest_sha256')!=quality['import_manifest_sha256']
            or not raw_required <= raw.get('checks',{}).keys() or any(raw['checks'].values())):
            raise ValueError('Raw独立核查未完成、存在差异或未绑定候选')
        replay_paths = sorted((root / 'replays').glob('*/candidate_reconciliation.json'))
        full = None
        for path in replay_paths:
            replay = json.loads(path.read_text())
            if (replay.get('status') == 'PASS' and replay.get('candidate_plan_hash') == plan['plan_hash']
                and replay.get('observation_start') == plan['observation_start'] and replay.get('observation_end') == plan['observation_end']
                and replay.get('baseline_batch') == plan['source_batch']):
                full = (path, replay)
                break
        if full is None:
            raise ValueError('缺少候选整个观察区间的成功结果复算')
        replay_path, replay = full
        expected_files = {'risk_events.csv', 'continuous_limit_down_segments.csv'}
        output_names = {Path(path).name for path in replay.get('output_sha256', {})}
        if (not expected_files <= output_names or set(replay.get('comparisons', {})) != expected_files
            or any(item.get('business_differences') != 0 for item in replay['comparisons'].values())):
            raise ValueError('复算结果未绑定输出哈希或存在差异')
        for group in ['source_sha256', 'output_sha256']:
            for path, digest in replay[group].items():
                if file_hash(Path(path)) != digest:
                    raise ValueError('复算证据或结果在验收后发生变更')
        for relative, item in plan['sources'].items():
            path = root / 'raw/cache_source' / relative
            if not path.resolve().is_relative_to(root) or file_hash(path) != item['sha256']:
                raise ValueError('候选来源副本改变')
        return {'start_date': plan['observation_start'], 'end_date': plan['observation_end'],
                'baseline_date': plan['baseline_date'], 'plan_hash': plan['plan_hash'],
                'scope_type': 'OFFLINE_CACHE_RECONCILIATION', 'markets': list(RISK_MARKETS), 'datasets': list(DATASETS),
                'quality_path': str(quality_path), 'quality_sha256': file_hash(quality_path),
                'raw_report_path':str(raw_path), 'raw_report_sha256':file_hash(raw_path),
                'replay_path': str(replay_path), 'replay_sha256': file_hash(replay_path),
                'legacy_p4_scope_changed': False, 'rqdata_accessed': False}
    finally:
        reader.close()


def verify_cached_publication_evidence(service: MarketFoundationService, quality: dict) -> None:
    if quality['rule_version'] != CACHE_RULE_VERSION or quality['status'] != 'PASS' or quality['severe_count']:
        raise MarketFoundationError('MDFC001', '缓存候选质量版本或状态不符合发布条件')
    expected = json.loads(quality['scope_json'])
    actual = _evidence(service.root)
    if any(expected.get(key) != value for key, value in actual.items()):
        raise MarketFoundationError('MDFC002', '缓存候选发布证据已经改变')


def create_cached_publication_candidate(service: MarketFoundationService, *, approved_start: date, approved_end: date) -> dict:
    if service.execution_mode != CACHE_P4_MODE:
        raise ValueError('必须显式启用隔离缓存发布模式')
    evidence = _evidence(service.root)
    if (str(approved_start), str(approved_end)) != (evidence['start_date'], evidence['end_date']):
        raise ValueError('用户确认范围必须与已完整验收的缓存范围一致')
    evidence['approved_scope'] = {'start_date': str(approved_start), 'end_date': str(approved_end)}
    quality_id = 'mdq_cache_' + evidence['plan_hash'][:24]
    current = service.catalog.row('SELECT * FROM data_quality_run WHERE quality_run_id=?', [quality_id])
    if current:
        if current['scope_json'] != canonical_json(evidence):
            raise ValueError('已有缓存质量绑定与本次证据不一致')
        if current['candidate_id']:
            return service.get_snapshot(current['candidate_id'])
    else:
        timestamp = now()
        service.catalog.execute('INSERT INTO data_quality_run VALUES (?, ?, NULL, ?, ?, ?, 0, 0, 0, ?, ?)',
            [quality_id, 'cache_' + evidence['plan_hash'][:24], CACHE_RULE_VERSION, canonical_json(evidence), 'PASS', timestamp, timestamp])
        atomic_json(service.root / 'quality' / quality_id / 'quality_report.json',
            {'status': 'PASS', 'rule_version': CACHE_RULE_VERSION, 'scope': evidence,
             'checks': json.loads(Path(evidence['quality_path']).read_text())['findings']})
    service._refresh_watermarks('cache_' + evidence['plan_hash'][:24])
    # Match the workbench's JSON/CSV report contract, including the bound
    # source-quality and replay evidence in scope_json.
    service._write_quality_report(quality_id)
    return service._create_candidate('cache_' + evidence['plan_hash'][:24], quality_id)
