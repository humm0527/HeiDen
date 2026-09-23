"""
文件作用：从冻结正式事件和 PIT 评价导出任意子区间的风险股票拦截/9，不重算事件、不访问供应商。
编辑记录：
【首次生成：2026-09-08，区分观察期与历史范围，保留空白/未知并使用未取整日均分母。】
【二次编辑：2026-09-14，校验日期证据、行级批次和输入稳定性，CSV随附完整时间口径。】
"""

from __future__ import annotations

from collections import Counter
import csv
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from hashlib import sha256
import json
from pathlib import Path

from .descriptive_inputs import _load_history


GRADES = (*'ABCDEFG', 'BLANK', 'UNKNOWN')
EVENT_TYPES = ('NEW_ST', 'NON_ST_CONTINUOUS_LIMIT_DOWN')


def _iso_date(value, field):
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f'REPORT_DATE_INVALID: {field} 必须为 YYYY-MM-DD') from exc
    if parsed.isoformat() != value:
        raise ValueError(f'REPORT_DATE_INVALID: {field} 必须为 YYYY-MM-DD')
    return parsed


def _assessment_grade(assessment, event, manifest, metric):
    if not assessment:
        return 'UNKNOWN', 'MISSING_ASSESSMENT'
    expected = {'风险事件批次标识': manifest['calculation_batch_id'],
                '指标计算批次标识': metric['metric_batch_id'],
                '证券代码': event['证券代码'], '交易市场代码': event['交易市场代码'],
                '风险事件类型': event['事件类型'], '第一次事实日期': event['首次事实日期']}
    for field, value in expected.items():
        if field in assessment and assessment[field] != value:
            raise ValueError(f'REPORT_ASSESSMENT_IDENTITY: 评价字段与绑定事件不符: {field}')
    raw = assessment.get('原始分类值', '').strip().upper()
    selected, cutoff = assessment.get('选中分类日期', ''), assessment.get('评价截止交易日', '')
    fact = _iso_date(event['首次事实日期'], '首次事实日期')
    selected_date = _iso_date(selected, '选中分类日期') if selected else None
    cutoff_date = _iso_date(cutoff, '评价截止交易日') if cutoff else None
    usable = assessment.get('分类首次可用交易日', '')
    usable_date = _iso_date(usable, '分类首次可用交易日') if usable else None
    if ((cutoff_date and cutoff_date >= fact) or
            (selected_date and cutoff_date and selected_date > cutoff_date) or
            (usable_date and cutoff_date and usable_date > cutoff_date)):
        raise ValueError('REPORT_PIT_LOOKAHEAD: 分类、可用日期或截止日违反历史边界')
    if not selected_date or not cutoff_date:
        return 'UNKNOWN', 'MISSING_PIT_DATES'
    if raw in 'ABCDEFG' and len(raw) == 1:
        return raw, 'MATCHED'
    if not raw:
        return 'BLANK', 'MATCHED'
    return 'UNKNOWN', 'UNKNOWN_RAW_CLASSIFICATION'


def _rows(path):
    with Path(path).open(encoding='utf-8-sig', newline='') as stream:
        return list(csv.DictReader(stream))


def _hash(path):
    digest = sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024*1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _event_key(row):
    return '|'.join(row[field].strip() for field in ('交易市场代码', '证券代码', '事件类型', '首次事实日期'))


def build_period_report(*, event_csv, event_manifest, models: dict, start: str, end: str) -> dict:
    """models: {label: {assessment_csv, history_files, broker_id, metric_manifest}}.

    A subwindow is a view of one frozen event batch, not an independently rerun
    shorter observation. Events confirmed only after the subwindow may therefore
    remain included; the report explicitly labels that boundary.
    """
    first, last = _iso_date(start, 'start'), _iso_date(end, 'end')
    if not isinstance(models, dict) or not models:
        raise ValueError('REPORT_MODELS_REQUIRED: 至少指定一个模型')
    input_paths = {Path(event_csv).resolve(), Path(event_manifest).resolve()}
    for spec in models.values():
        input_paths.update(Path(spec[key]).resolve() for key in ('metric_manifest', 'assessment_csv'))
        input_paths.update(Path(path).resolve() for path in spec['history_files'].values())
    initial_hashes = {str(path): _hash(path) for path in input_paths}
    manifest = json.loads(Path(event_manifest).read_text(encoding='utf-8'))
    if (manifest.get('status') != 'SUCCEEDED' or manifest.get('gate_allow_run') is not True
            or not manifest.get('rule_sha256') or not manifest.get('calculation_batch_id')):
        raise ValueError('REPORT_FORMAL_GATE: 缺少成功的正式事件批次、规则哈希或计算门禁')
    if not date.fromisoformat(manifest['observation_start']) <= first <= last <= date.fromisoformat(manifest['observation_end']):
        raise ValueError('REPORT_PERIOD_RANGE: 请求区间必须在冻结事件批次覆盖范围内')
    all_events = _rows(event_csv)
    if len(all_events) != manifest['event_count']:
        raise ValueError('REPORT_EVENT_COUNT: 事件文件与 manifest 数量不一致')
    keys = [_event_key(row) for row in all_events]
    if len(keys) != len(set(keys)) or any(row['事件类型'] not in EVENT_TYPES for row in all_events):
        raise ValueError('REPORT_EVENT_LAYER: 正式事件重复或混入非正式观察类型')
    batch_start = _iso_date(manifest['observation_start'], '批次开始日')
    batch_end = _iso_date(manifest['observation_end'], '批次结束日')
    for row in all_events:
        if not batch_start <= _iso_date(row['首次事实日期'], '首次事实日期') <= batch_end:
            raise ValueError('REPORT_EVENT_RANGE: 事件首次事实日超出冻结批次范围')
        for field, expected in (('计算批次标识', manifest['calculation_batch_id']),
                                ('风险规则版本号', manifest['rule_version'])):
            if field in row and row[field] != expected:
                raise ValueError(f'REPORT_EVENT_IDENTITY: 事件与清单不符: {field}')
    events = [row for row in all_events if first <= date.fromisoformat(row['首次事实日期']) <= last]
    if not models:
        raise ValueError('REPORT_MODELS_REQUIRED: 至少指定一个模型')
    tables, details, model_metadata = {}, [], {}
    for label, spec in models.items():
        metric_path = Path(spec['metric_manifest'])
        metric = json.loads(metric_path.read_text(encoding='utf-8'))
        if metric.get('status') != 'SUCCEEDED' or metric.get('event_batch_id') != manifest['calculation_batch_id']:
            raise ValueError('REPORT_BATCH_MISMATCH: PIT 评价与事件必须精确绑定同一批次')
        assessment_path = Path(spec['assessment_csv'])
        expected_hash = metric.get('output_files', {}).get(assessment_path.name, {}).get('sha256')
        if not expected_hash or expected_hash != initial_hashes[str(assessment_path.resolve())]:
            raise ValueError('REPORT_ASSESSMENT_HASH: PIT 评价缺少或不匹配 manifest 哈希')
        assessments = {}
        for row in _rows(assessment_path):
            if row['券商标识'] != spec['broker_id']:
                continue
            key = row['事件唯一键']
            if key in assessments:
                raise ValueError('REPORT_DUPLICATE_ASSESSMENT: PIT 评价键重复')
            assessments[key] = row
        history = _load_history(spec['history_files'], broker_id=spec['broker_id'], start=first, end=last)
        # Each model retains its own observed source dates; no cross-model date union.
        days = len(history['daily'])
        grade_days = Counter()
        for values in history['daily'].values():
            grade_days.update(grade if grade in GRADES else 'BLANK' if not grade else 'UNKNOWN' for grade in values.values())
        counts = {event_type: Counter() for event_type in EVENT_TYPES}
        for event in events:
            key = _event_key(event)
            assessment = assessments.get(key)
            selected = (assessment or {}).get('选中分类日期', '')
            cutoff = (assessment or {}).get('评价截止交易日', '')
            grade, assessment_status = _assessment_grade(assessment, event, manifest, metric)
            counts[event['事件类型']][grade] += 1
            details.append({'分类方法': label, '事件唯一键': key, '事件类型': event['事件类型'],
                            '证券代码': event['证券代码'], '首次事实日期': event['首次事实日期'],
                            '档位': grade, '选中分类日期': selected, '评价截止交易日': cutoff,
                            '评价状态': assessment_status})
        for event_type, number in zip(EVENT_TYPES, ('table8', 'table9')):
            rows = tables.setdefault(number, [])
            for grade in GRADES:
                count = counts[event_type][grade]
                avg = Decimal(grade_days[grade]) / Decimal(days) if days else None
                ratio = Decimal(count) / avg * 100 if avg else None
                rows.append({'分类方法': label, '档位': grade, '事件类型': event_type,
                    '区间正式风险事件数量': count, '源数据有效日期数': days,
                    '档位累计股票日数': grade_days[grade],
                    '日均档位数量_未取整': str(avg) if avg is not None else '',
                    '日均档位数量_显示值': str(avg.quantize(Decimal('1'), rounding=ROUND_HALF_UP)) if avg is not None else '',
                    '风险事件数除日均数量_百分比': str(ratio.quantize(Decimal('.01'), rounding=ROUND_HALF_UP)) if ratio is not None else '',
                    '分母状态': 'DEFINED' if avg else 'ZERO' if days else 'MISSING'})
        model_metadata[label] = {'metric_batch_id': metric['metric_batch_id'], 'broker_id': spec['broker_id'],
            'mapping_version': metric.get('mapping_version'), 'source_day_count': days,
            'history_source_count': len(spec['history_files']),
            'pre_observation_security_count': len(history['initial'])}
    changed = [path for path, digest in initial_hashes.items() if _hash(path) != digest]
    if changed:
        raise ValueError('REPORT_INPUT_CHANGED: 计算期间输入文件发生改变，拒绝导出')
    return {'schema_version': 'formal_period_report_v2', 'event_layer': 'FORMAL',
        'inputs_verified_unchanged': True,
        'input_verification_policy': 'HASH_BEFORE_AND_AFTER_NOT_HISTORICAL_SIGNATURE',
        'event_batch_id': manifest['calculation_batch_id'], 'rule_version': manifest['rule_version'],
        'rule_sha256': manifest['rule_sha256'], 'market_snapshot_id': manifest.get('upstream_snapshot_id'),
        'observation_start': start, 'observation_end': end,
        'frozen_batch_observation_end': manifest['observation_end'],
        'date_field': '首次事实日期', 'event_count': len(events),
        'event_count_by_type': dict(Counter(r['事件类型'] for r in events)),
        'count_unit': 'EVENT_NOT_DISTINCT_SECURITY',
        'denominator_policy': 'EACH_MODEL_OBSERVED_WEEKDAY_SOURCE_DATES_NO_FORWARD_FILL',
        'event_window_policy': 'FILTER_FROZEN_BATCH_BY_FIRST_FACT_NOT_AS_OF_RERUN',
        'rounding_policy': 'DIVIDE_BY_UNROUNDED_AVERAGE_THEN_ROUND_HALF_UP_2DP',
        'warnings': ['仅筛选冻结批次的首次事实日期；不等同于在子区间末重新运行事件引擎。',
                     '事件数/日均档位数量不是事件命中率或证券风险概率，可超过100%。',
                     '周末分类行按既有规则忽略；两模型各用自己的实际源日期，差异必须随报告披露。'],
        'assessment_status_counts': dict(Counter(r['评价状态'] for r in details)),
        'models': model_metadata, 'source_sha256': initial_hashes, 'tables': tables, 'event_details': details}


def write_period_report(output: Path, report: dict) -> None:
    metadata = {'事件层': report['event_layer'], '事件批次': report['event_batch_id'],
                '观察开始日': report['observation_start'], '观察结束日': report['observation_end'],
                '冻结批次结束日': report['frozen_batch_observation_end'],
                '日期筛选字段': report['date_field'], '窗口口径': report['event_window_policy'],
                '分母口径': report['denominator_policy'], '舍入口径': report['rounding_policy']}
    if set(report['tables']) != {'table8', 'table9'}:
        raise ValueError('REPORT_TABLE_SET: 仅允许导出正式风险股票拦截/9')
    output.mkdir(parents=True, exist_ok=False)
    for name, rows in {**report['tables'], 'event_details': report['event_details']}.items():
        with (output / f'{name}.csv').open('w', encoding='utf-8-sig', newline='') as stream:
            fields = [*list(rows[0]), *metadata] if rows else ['事件唯一键', *metadata]
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows({**row, **metadata} for row in rows)
    report = {**report, 'output_sha256': {p.name: _hash(p) for p in output.glob('*.csv')}}
    (output / 'report_manifest.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
