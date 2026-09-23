"""Desktop handover workflow. A fresh subprocess isolates the 01 and 02 packages."""
from __future__ import annotations
import argparse
import csv
import hashlib
from datetime import date, datetime, timedelta
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import traceback
from credentials import redact, without_credentials

ROOT = Path(__file__).resolve().parents[1]
RISK = ROOT / '01_risk_events'
PEER = ROOT / '02_peer_benchmark'
TABLE_START = date(2025, 1, 15)
OVERRIDES = {}  # Add only your own explicitly approved security overrides.


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str)+'\n', encoding='utf-8')



def quarter_files(folder):
    return sorted(p.resolve() for p in folder.glob('*.csv') if re.search(r'20\d{2}Q[1-4]', p.name, re.I))


def apply_g_overrides(folder):
    """Apply the agreed two-stock rule to a generated copy, never to source Excel."""
    from itertools import groupby
    for path in quarter_files(folder):
        target = path.with_suffix('.tmp')
        with path.open(encoding='utf-8-sig', newline='') as source, target.open('w', encoding='utf-8-sig', newline='') as dest:
            reader = csv.DictReader(source)
            writer = csv.DictWriter(dest, fieldnames=reader.fieldnames)
            writer.writeheader()
            for day, rows in groupby(reader, key=lambda r: r['biz_date']):
                seen = set()
                for row in rows:
                    code = row['stk_code']; seen.add(code)
                    if code in OVERRIDES:
                        row['券商03'] = 'G'
                    writer.writerow(row)
                for code in sorted(set(OVERRIDES)-seen):
                    writer.writerow({'biz_date': day, 'stk_code': code, '证券名称': code, '券商03': 'G'})
        target.replace(path)



def validate_peer_header(upload):
    constants = load_module('handover_broker_constants', PEER/'src/riskaudit/broker_metrics/constants.py')
    with upload.open(encoding='utf-8-sig', newline='') as f:
        columns = set(next(csv.reader(f), []))
    missing = {'biz_date', 'stk_code', *constants.BUSINESS_BROKER_COLUMNS} - columns
    if missing:
        raise ValueError('全行业CSV缺少必要列：'+', '.join(sorted(missing)))


def prepare_peer_history(upload, destination):
    """Packaged history + selected folder history, with the selected file taking precedence."""
    from riskaudit.business_files import inspect_business_date_range
    selected = {}
    for folder in (PEER/'data', upload.parent):
        local = {}
        for path in quarter_files(folder):
            match = re.search(r'(20\d{2}Q[1-4])', path.name, re.I)
            key = match.group(1).upper()
            if key in local and path != upload and local[key] != upload:
                raise ValueError(f'同一目录同季度存在多个文件，请先整理：{folder} / {key}')
            local[key] = upload if path == upload or local.get(key) == upload else path
        selected.update(local)
    first, last, _ = inspect_business_date_range(upload)
    quarter = f'{last.year}Q{(last.month-1)//3+1}'
    if first.year != last.year or (first.month-1)//3 != (last.month-1)//3:
        raise ValueError('全行业请选择一个季度的集中度CSV；以前季度保留在历史目录。')
    selected[quarter] = upload
    destination.mkdir(parents=True)
    for key, path in selected.items():
        shutil.copy2(path, destination/f'集中度{key}.csv')
    return destination/f'集中度{quarter}.csv', last


def inspect_single_daily(folder):
    """Validate daily cache without loading millions of stock-days into memory."""
    days = set()
    quarters = set()
    for path in quarter_files(folder):
        quarter = re.search(r'20\d{2}Q[1-4]', path.name, re.I).group().upper()
        if quarter in quarters:
            raise ValueError(f'single 同一季度存在多个CSV：{quarter}，请先整理。')
        quarters.add(quarter)
        with path.open(encoding='utf-8-sig', newline='') as source:
            reader = csv.DictReader(source)
            if not {'biz_date', 'stk_code', '证券名称', '券商03'} <= set(reader.fieldnames or []):
                raise ValueError(f'single 文件缺少逐日分类必要列：{path.name}')
            previous = None
            codes = set()
            for row in reader:
                day = date.fromisoformat(row['biz_date'])
                if quarter != f'{day.year}Q{(day.month-1)//3+1}' or (previous and day < previous):
                    raise ValueError(f'single 日期未排序或与文件季度不符：{path.name} / {day}')
                if day != previous:
                    codes.clear()
                code = row['stk_code']
                if not code or code in codes or row['券商03'] not in set('ABCDEFG'):
                    raise ValueError(f'single 证券为空、重复或档位不是A—G：{path.name} / {day} / {code}')
                codes.add(code); days.add(day); previous = day
    return days


def stage_single_daily(source, destination, end):
    """Copy only the requested interval; never let later cached rows extend a run."""
    destination.mkdir(parents=True, exist_ok=True)
    for path in quarter_files(source):
        with path.open(encoding='utf-8-sig', newline='') as handle:
            reader = csv.DictReader(handle)
            target = None
            try:
                for row in reader:
                    if date.fromisoformat(row['biz_date']) > end:
                        break
                    if target is None:
                        day = date.fromisoformat(row['biz_date'])
                        target = (destination/f'集中度分类_{day.year}Q{(day.month-1)//3+1}.csv').open('w', encoding='utf-8-sig', newline='')
                        writer = csv.DictWriter(target, fieldnames=reader.fieldnames)
                        writer.writeheader()
                    writer.writerow(row)
            finally:
                if target is not None:
                    target.close()


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024*1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def prepare_single(folder, destination, audit, client, requested_end, *, daily_source=None):
    daily_source = daily_source if daily_source is not None else PEER/'data/single'
    cached_days = inspect_single_daily(daily_source)
    paths = sorted(folder.glob('股票打分结果*.xlsx'))
    dates = []
    for path in paths:
        match = re.fullmatch(r'股票打分结果(\d{8})', path.stem)
        if not match:
            raise ValueError(f'截面文件名不符合要求：{path.name}')
        dates.append(datetime.strptime(match.group(1), '%Y%m%d').date())
    if not dates and not cached_days:
        raise ValueError('single 没有逐日数据，所选文件夹也没有 股票打分结果YYYYMMDD.xlsx。')
    end = requested_end or max(dates or cached_days)
    if end >= date.today() or (dates and max(dates) >= date.today()):
        raise ValueError('只支持今天之前的截面及截止日期。')
    start_calendar = min([TABLE_START, *dates, *cached_days])-timedelta(days=20)
    end_calendar = max([end, *dates, *cached_days])+timedelta(days=14)
    days = {str(d)[:10] for d in client.get_trading_dates(str(start_calendar), str(end_calendar))}
    if not days:
        raise ValueError('米筐未返回交易日历。')
    calendar = audit/'转换交易日历.csv'
    with calendar.open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f); writer.writerow(['market_code', 'calendar_date', 'is_trading_day'])
        day = start_calendar
        while day <= end_calendar:
            writer.writerow(['XSHG', str(day), str(day) in days]); day += timedelta(days=1)
    required_days = {date.fromisoformat(d) for d in days if str(TABLE_START) <= d <= str(end)}
    if not required_days:
        raise ValueError('截止日之前没有2025-01-15起的交易日。')
    actual_end = max(required_days)
    missing = required_days - cached_days
    if missing:
        if not dates or min(dates) > TABLE_START:
            raise ValueError(f'single 缺少 {min(missing)} 等 {len(missing)} 个交易日；请提供2025-01-15起的完整月度文件夹以生成逐日数据。')
        print(f'single 缺少 {len(missing)} 个交易日，正在从月度截面生成逐日分类。', flush=True)
        converter = load_module('handover_monthly', PEER/'data/月截面数据转化为逐日数据/convert_monthly_to_daily.py')
        # Rebuild through existing cache end as well, so an earlier request cannot truncate it.
        conversion_end = max([end, *cached_days])
        if conversion_end >= date.today():
            raise ValueError('single 含今天或之后的数据，请先检查。')
        result = converter.convert(folder, daily_source, calendar, 'g', end=conversion_end)
        write_json(audit/'月度转换记录.json', {'action': 'CONVERTED', **result})
    else:
        print(f'single 已覆盖至 {actual_end}，跳过月度转逐日。', flush=True)
        write_json(audit/'月度转换记录.json', {'action': 'REUSED', 'source_dir': str(daily_source.resolve()),
            'requested_end': str(end), 'last_date': str(actual_end), 'trading_days': len(required_days)})
    source_hashes = {p.name: file_sha256(p) for p in quarter_files(daily_source)}
    stage_single_daily(daily_source, destination, actual_end)
    apply_g_overrides(destination)
    # Hashes in the conversion record describe the pre-override intermediate file.
    write_json(audit/'实际分类输入.json', {'overrides': OVERRIDES, 'source_dir': str(daily_source.resolve()),
        'source_files': source_hashes, 'files': {p.name: file_sha256(p) for p in quarter_files(destination)}})
    upload = destination/f'集中度分类_{actual_end.year}Q{(actual_end.month-1)//3+1}.csv'
    return upload, actual_end


def run_risk(request, run):
    sys.path.insert(0, str(RISK/'src'))
    from riskaudit.data_ingestion.adapters import RealRQDataClient
    from riskaudit.business_files import inspect_business_date_range
    engine = load_module('handover_online_risk', RISK/'scripts/run_real_risk_events.py')
    source = Path(request['input']).resolve()
    audit = run/'核对材料'; audit.mkdir()
    history = audit/'逐日分类'
    if request['mode'] == 'peer':
        if not source.is_file(): raise ValueError('请选择全行业集中度CSV。')
        validate_peer_header(source)
        upload, end = prepare_peer_history(source, history)
        if end >= date.today(): raise ValueError('输入日期必须早于今天。')
    else:
        if not source.is_dir() and not quarter_files(PEER/'data/single'):
            raise ValueError('请选择券商03原始月度截面文件夹，或先在 single 放入完整逐日分类。')
    print('正在连接米筐并准备输入。', flush=True)
    client = RealRQDataClient.from_environment()
    if request['mode'] == 'single':
        upload, end = prepare_single(source, history, audit, client, date.fromisoformat(request['end_date']) if request.get('end_date') else None)
    if request['mode'] == 'single' and end.year == 2025:
        raise ValueError('当前普通命中率报告按年初起算，而首个模型截面是2025-01-15；请使用2026年或之后的截止日。')
    print(f'输入已准备；截止日 {end}。正在计算风险事件，首次在线取数可能较久。', flush=True)
    batch = 'easy_risk'
    # Supplemental files expand the risk universe, never replace the chosen classifications.
    supplements = sorted(set(quarter_files(PEER/'data') + quarter_files(history)))
    result = engine.run_real_risk_events(client=client, business_file=upload, output_dir=audit/'risk', batch_id=batch,
        rules_path=RISK/'configs/rules/approved/rules_v2.yaml', chunk_size=500,
        risk_universe_files=supplements, broker_history_dirs=[history],
        historical_observation_start=date(2025,1,1) if request['mode']=='single' else None)
    if result['status'] != 'SUCCEEDED':
        raise ValueError(f"风险计算未通过：{result['status']}。详情见核对材料。")
    user_output = Path(result['user_output']['output_dir'])
    handoffs = list(user_output.glob('02输入清单_*.json'))
    if len(handoffs) != 1: raise ValueError('未找到唯一的01交接清单。')
    from riskaudit.tasks.broker_handoff import load_broker_input_manifest
    handoff = load_broker_input_manifest(handoffs[0])
    # One event list for the year of the uploaded file, and a cross-year list for table 9.
    export_current_events(handoff['files']['event_csv'], run, end)
    write_json(audit/'下一步.json', {'mode': request['mode'], 'end_date': str(end), 'upload': str(upload), 'history': str(history),
        'handoff': str(handoffs[0]), 'source_batch': str(audit/'risk'/batch), 'calendar': str(handoff['files']['calendar_csv'])})



def export_current_events(event_csv, run, end):
    import pandas as pd
    events = pd.read_csv(event_csv, dtype=str, keep_default_na=False)
    events.rename(columns={'market_code': '交易市场代码', 'security_code': '证券代码',
                           'first_fact_date': '首次事实日期'}, inplace=True)
    current = events[events['首次事实日期'].between(f'{end.year}-01-01', str(end))]
    current.to_csv(run/'风险事件明细.csv', index=False, encoding='utf-8-sig')
    current[['交易市场代码','证券代码']].drop_duplicates().to_csv(run/'风险股票清单.csv', index=False, encoding='utf-8-sig')


def run_broker(run):
    sys.path.insert(0, str(PEER/'src'))
    from riskaudit.tasks.runner import _run_broker
    audit = run/'核对材料'
    data = json.loads((audit/'下一步.json').read_text(encoding='utf-8'))
    print('正在计算命中率和券商评价。', flush=True)
    output = _run_broker({'risk_input_manifest': Path(data['handoff']), 'uploaded_history_file': Path(data['upload']),
        'history_dir': Path(data['history']), 'output_root': audit/'券商计算', 'batch_id': 'easy_broker'},
        peer=data['mode']=='peer', project_root=PEER)
    for path in Path(output['output_dir']).glob('*.csv'):
        shutil.copy2(path, run/path.name)
    if data['mode'] == 'single':
        print('正在生成风险股票拦截、风险股票分档。', flush=True)
        prepare = load_module('handover_event_prepare', PEER/'excel8.9/prepare_scoring_event_inputs.py')
        tables = load_module('handover_tables', PEER/'excel8.9/calculate_scoring_tables_8_9.py')
        event_dir = audit/'风险股票拦截与分档事件输入'
        prepare.prepare(Path(data['source_batch']), event_dir, Path(data['calendar']))
        result = tables.calculate(Path(data['history']), event_dir, Path(data['calendar']), end=date.fromisoformat(data['end_date']))
        report = tables.save_reports(result, audit/'风险股票拦截与分档')
        for path in report.glob('*.csv'): shutil.copy2(path, run/path.name)
        shutil.copy2(report/'核对材料/风险事件评价明细.csv', run/'风险股票拦截与分档风险事件评价明细.csv')
    write_json(run/'运行结果.json', {'status': 'SUCCEEDED', 'mode': data['mode'], 'end_date': data['end_date'],
        'return_calculation': '未启用，等待新口径', 'table9_start': str(TABLE_START) if data['mode']=='single' else None,
        'broker_result': output, 'files': sorted(p.name for p in run.glob('*.csv'))})
    print(f'全部完成。结果文件夹：{run}', flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--request', type=Path)
    parser.add_argument('--broker-stage', type=Path)
    args = parser.parse_args()
    if args.broker_stage:
        # This stage is entirely offline.
        def offline(event, values):
            if event in {'socket.connect','socket.connect_ex','socket.getaddrinfo','socket.sendto'}:
                raise RuntimeError('券商评价阶段禁止联网')
        sys.addaudithook(offline)
        run_broker(args.broker_stage.resolve()); return
    if not args.request: parser.error('需要请求文件')
    request = json.loads(args.request.read_text(encoding='utf-8'))
    if request.get('mode') not in {'single','peer'}: raise ValueError('未知运行类型')
    run = Path(request['run_dir']).resolve()
    if not run.is_dir(): raise ValueError('结果目录尚未创建')
    try:
        run_risk(request, run)
        subprocess.run([sys.executable, '-B', str(Path(__file__).resolve()), '--broker-stage', str(run)], check=True, env=without_credentials(os.environ))
    except Exception:
        write_json(run/'运行结果.json', {'status': 'FAILED', 'message': redact(traceback.format_exc()), 'returns_enabled': False})
        raise


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print(redact(traceback.format_exc()), file=sys.stderr, flush=True)
        sys.exit(1)
