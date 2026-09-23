"""User-triggered RQData increments; immutable snapshots and separate post-adjusted assets."""
from __future__ import annotations
import argparse
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import fcntl
import hashlib
import json
import math
from pathlib import Path
import os
import sys

import pandas as pd
import duckdb
from riskaudit.data_ingestion.adapters import RealRQDataClient
from .constants import DATASETS
from .facts import canonical_json
from .rqdata_provider import RQDataFoundationProvider
from .service import MarketFoundationService
from .service_support import atomic_json as _atomic_json

def atomic_json(path, payload):
    _atomic_json(path, json.loads(canonical_json(payload)))

MODE = 'RQDATA_MANUAL_INCREMENT' 
RULE = 'manual_increment_v2'
MARKETS = ('XSHG', 'XSHE')
JOIN = '''FROM market_snapshot_record m JOIN source_record_catalog s
 USING(dataset_id,business_key,source_record_version,payload_hash,partition_id)
 WHERE m.snapshot_id=? AND m.dataset_id=?'''


def read_parquet(path):
    db=duckdb.connect()
    try:return db.read_parquet(str(path)).fetchdf()
    finally:db.close()


def write_parquet(frame,path):
    db=duckdb.connect()
    try:
        db.register('price_frame',frame)
        db.execute("COPY price_frame TO ? (FORMAT PARQUET)",[str(path)])
    finally:db.close()


def digest(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(4*1024*1024),b''): h.update(b)
    return h.hexdigest()


def emit(event, **values):
    print(json.dumps({'event':event,**values},ensure_ascii=False,default=str),flush=True)


def snapshot_records(service, sid, dataset):
    return [json.loads(r['payload_json']) for r in service.catalog.rows('SELECT s.payload_json '+JOIN,[sid,dataset])]


def verify_manual_evidence(service, quality):
    scope=json.loads(quality['scope_json'])
    if quality['rule_version']!=RULE or quality['status']!='PASS' or quality['severe_count']:
        raise ValueError('手动增量质量门禁未通过')
    evidence=Path(scope['evidence_path'])
    if digest(evidence)!=scope['evidence_sha256']: raise ValueError('增量核对证据变化')
    report=json.loads(evidence.read_text())
    if report['status']!='PASS' or report['end_date']!=scope['end_date']: raise ValueError('增量范围或状态不一致')
    if report['base_snapshot_id']!=scope['base_snapshot_id'] or report['selection_path']!=scope['selection_path'] or report['assets'].get(scope['selection_path'])!=scope['selection_sha256']:raise ValueError('候选记录选取未绑定质量证据')
    base=service.get_snapshot(report['base_snapshot_id'])
    if base['status']!='PUBLISHED' or base['risk_adapter_status']!='PASSED' or base['member_hash']!=report['base_member_hash']:
        raise ValueError('增量基线不是已核验快照')
    for path,h in report['assets'].items():
        if digest(path)!=h: raise ValueError('增量资产哈希变化: '+path)


def manual_selection_sql(scope, dataset=None):
    """Exclude newer/unrelated pending versions even when a different cutoff was staged."""
    selection=Path(scope['selection_path'])
    if digest(selection)!=scope['selection_sha256']:raise ValueError('候选选取凭据变化')
    source="read_parquet('"+str(selection).replace("'","''")+"')"
    sid=str(scope['base_snapshot_id']).replace("'","''")
    dataset_filter = "" if dataset is None else " WHERE dataset_id='"+str(dataset).replace("'","''")+"'"
    selected=f"(SELECT * FROM {source}{dataset_filter})"
    base_filter = "" if dataset is None else " AND m.dataset_id='"+str(dataset).replace("'","''")+"'"
    # An explicit anti join builds only the small receipt key set, not a correlated
    # delimiter join over every historical key. Filter each side before joining.
    return f"""SELECT dataset_id,business_key,source_record_version,payload_hash,partition_id FROM {selected}
       UNION ALL SELECT m.dataset_id,m.business_key,m.source_record_version,m.payload_hash,m.partition_id
       FROM market_snapshot_record m ANTI JOIN {selected} r
       ON r.dataset_id=m.dataset_id AND r.business_key=m.business_key
       WHERE m.snapshot_id='{sid}'{base_filter}"""


def open_increment_service(root, end):
    service=MarketFoundationService(root,execution_mode=MODE,source_system='RQDATA',allow_snapshot_candidates=False,refresh_range_end=end)
    try:
        service.catalog.execute("SET memory_limit='2GB'")
        service.catalog.execute('SET threads=1')
        service.catalog.execute('SET preserve_insertion_order=false')
        spill=root/'duckdb/manual_spill'
        service.catalog.execute("SET temp_directory='"+str(spill).replace("'","''")+"'")
        return service
    except Exception:
        service.close()
        raise



def validate_batch(records, codes, start, end):
    """Fail closed on missing facts, duplicates, lifecycle or invalid traded prices."""
    indexes={}
    for ds, rows in records.items():
        keys=[(r['market_code'],r.get('instrument_key',''),str(r.get('business_date',r.get('effective_start')))) for r in rows]
        if len(keys)!=len(set(keys)): raise ValueError('重复业务键: '+ds)
        indexes[ds]={k:r for k,r in zip(keys,rows)}
    masters={r['instrument_key']:r for r in records['instrument_master_history']}
    if set(masters)!=set(codes): raise ValueError('证券主数据未覆盖请求范围')
    days=sorted({str(r['business_date']) for r in records['market_calendar'] if r['is_trading_day']})
    count=0
    for code,m in masters.items():
        for d in days:
            if d<str(m['listing_date']) or (m.get('termination_date') and d>=str(m['termination_date'])): continue
            key=(m['market_code'],code,d)
            for ds in ('daily_st_status','daily_suspension_status'):
                if key not in indexes[ds]: raise ValueError(f'{ds} 缺失: {key}')
            if not indexes['daily_suspension_status'][key]['is_suspended']:
                for ds in ('daily_price_unadjusted','daily_price_limits'):
                    if key not in indexes[ds]: raise ValueError(f'{ds} 缺失: {key}')
                p=indexes['daily_price_unadjusted'][key]
                if not all(p.get(f) is not None and math.isfinite(float(p[f])) and float(p[f])>0 for f in ['open','high','low','close']):
                    raise ValueError(f'正常交易价格无效: {key}')
            count+=1
    for ds in ('daily_price_unadjusted','daily_price_limits','daily_st_status','daily_suspension_status'):
        for r in records[ds]:
            d=str(r['business_date']);m=masters[r['instrument_key']]
            if not str(start)<=d<=str(end) or d not in days or d<str(m['listing_date']) or (m.get('termination_date') and d>=str(m['termination_date'])):
                raise ValueError(f'范围外市场事实: {ds}, {r}')
    return {'trading_days':len(days),'active_stock_days':count,'securities':len(codes)}


def normalize_master(records, old, start):
    updates=[]
    latest={}
    for r in old:
        c=r['instrument_key']
        if c not in latest or r['effective_start']>latest[c]['effective_start']: latest[c]=r
    for r in records:
        code=r['instrument_key']; previous=latest[code]
        if str(r['listing_date'])!=previous['listing_date']: raise ValueError('上市日期变化需专项核对: '+code)
        if r['board_code']!=previous['board_code']:
            raise ValueError('板块发生变化，需补齐变更日PIT证据: '+code)
        if r.get('termination_date') and str(r['termination_date'])<=str(start):
            if previous.get('termination_date')!=str(r['termination_date']):
                revised=dict(previous);revised['termination_date']=str(r['termination_date']);revised['effective_end']=str(date.fromisoformat(str(r['termination_date']))-timedelta(days=1));updates.append(revised)
            continue
        if previous.get('effective_end') is None or previous['effective_end']>=str(start):
            closed=dict(previous);closed['effective_end']=str(start-timedelta(days=1));updates.append(closed)
        current=dict(r);current['effective_start']=str(start)
        current['effective_end']=str(date.fromisoformat(str(r['termination_date']))-timedelta(days=1)) if r.get('termination_date') else None
        updates.append(current)
    return updates


def _hash_manifest_files(state):
    return {str(p):digest(p) for p in state}


def resolve_update_window(requested_end, current_end, trading_dates):
    """Dates absent from the trading calendar map to the last completed session."""
    days=sorted({date.fromisoformat(str(d)[:10]) for d in trading_dates if str(d)[:10]<=str(requested_end)})
    effective=days[-1] if days else None
    return effective, effective is not None and effective>current_end


def update(config_path, end, *, execute=False, chunk_size=500):
    config_path=Path(config_path).resolve();cfg=json.loads(config_path.read_text());root=Path(cfg['market_lake_root'])
    requested_end=end
    if requested_end>=date.today(): raise ValueError('只允许选择今天之前的日期')
    if not 1<=chunk_size<=500: raise ValueError('chunk-size必须为1—500')
    service=MarketFoundationService(root,read_only_catalog=True)
    try:
        base=service.get_snapshot(cfg['snapshot_id'])
        old=snapshot_records(service,base['snapshot_id'],'instrument_master_history')
        calendar=snapshot_records(service,base['snapshot_id'],'market_calendar')
    finally:service.close()
    old_end=date.fromisoformat(str(base['end_date']));old_start=date.fromisoformat(str(base['start_date']))
    if requested_end<old_start:raise ValueError('请求日期早于底座观察起点，需另做历史回填')
    local_days=[r['business_date'] for r in calendar if r['is_trading_day']]
    effective,_=resolve_update_window(requested_end,old_end,local_days)
    known_effective=str(effective) if requested_end<=old_end and effective else None
    codes=sorted({r['instrument_key'] for r in old})
    plan={'base_snapshot_id':base['snapshot_id'],'start_date':str(old_end+timedelta(days=1)) if requested_end>old_end else None,
          'requested_end_date':str(requested_end),'effective_end_date':known_effective,
          'calendar_resolution':'LOCAL' if known_effective else 'RESOLVE_ON_EXECUTE',
          'current_end_date':str(old_end),'risk_update_needed':requested_end>old_end,
          'security_count':len(codes),'markets':list(MARKETS),'market_lake_root':str(root)}
    emit('PLAN',**plan)
    if not execute:return plan
    if base['status']!='PUBLISHED' or base['risk_adapter_status']!='PASSED':raise ValueError('基线未通过验收')
    post_head=root/'post_adjusted/latest.json'
    if requested_end<=old_end and post_head.exists():
        head=json.loads(post_head.read_text())
        if head.get('status')=='AVAILABLE' and head.get('risk_snapshot_id')==cfg['snapshot_id'] and head.get('end_date')==str(old_end):
            if not all(digest(root/'post_adjusted'/f)==h for f,h in head['files'].items()):raise ValueError('后复权资产哈希变化')
            emit('ALREADY_COVERED',requested_end_date=str(requested_end),effective_end_date=known_effective,current_end_date=str(old_end),rqdata_accessed=False)
            return cfg
    with (root/'manual_update.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if json.loads(config_path.read_text())!=cfg:raise ValueError('连接配置已改变，请重新执行')
        client=RealRQDataClient.from_environment()
        if requested_end>old_end:
            days=client.get_trading_dates(str(old_end),str(requested_end))
            effective,needed=resolve_update_window(requested_end,old_end,days)
            if effective is None:raise ValueError('米筐未返回有效交易日历')
            emit('CUTOFF_RESOLVED',requested_end_date=str(requested_end),effective_end_date=str(effective),risk_update_needed=needed)
            if needed:
                cfg=update_risk(root,cfg,config_path,base,old,codes,old_end+timedelta(days=1),effective,client,chunk_size,requested_end=requested_end)
        update_adjusted(root,cfg,client,chunk_size)
        emit('COMPLETED',snapshot_id=cfg['snapshot_id'],requested_end_date=str(requested_end),effective_end_date=str(effective),current_end_date=cfg['approved_observation_end'],market_lake_root=str(root))
        return cfg


def update_risk(root,cfg,config_path,base,old,codes,start,end,client,chunk_size,requested_end=None):
    batch='manual_'+str(end).replace('-','')+'_'+base['snapshot_id'][-8:]
    work=root/'manual_updates'/batch;work.mkdir(parents=True,exist_ok=True)
    state_path=work/'state.json'
    state=json.loads(state_path.read_text()) if state_path.exists() else {'base_snapshot_id':base['snapshot_id'],'end_date':str(end),'chunks':{},'status':'DOWNLOADING'}
    if state['base_snapshot_id']!=base['snapshot_id'] or state['end_date']!=str(end): raise ValueError('续跑范围不一致')
    state.setdefault('requested_end_date',str(requested_end or end))
    atomic_json(state_path,state)
    # Network fetches finish and pass validation before opening the catalog for writing.
    files=[];summaries=[]
    for n,pos in enumerate(range(0,len(codes),chunk_size)):
        part=work/f'chunk_{n:04}.json';batch_codes=codes[pos:pos+chunk_size]
        if part.exists():
            expected=state['chunks'].get(str(n))
            if expected is None or digest(part)!=expected: raise ValueError('下载分块缺少或不匹配哈希记录')
            payload=json.loads(part.read_text())
        else:
            provider=RQDataFoundationProvider(client,order_book_ids=batch_codes,max_securities=500,max_calendar_days=None,phase_label='MANUAL')
            result=provider.fetch(start_date=start,end_date=end,markets=sorted({c.split('.')[1] for c in batch_codes}),datasets=DATASETS)
            records={ds:list(rows) for ds,rows in result.records_by_dataset.items()}
            # Price API may include rows outside listing interval; retain raw but exclude in normalized facts.
            masters={r['instrument_key']:r for r in records['instrument_master_history']}
            for ds in ('daily_price_unadjusted','daily_price_limits'):
                records[ds]=[r for r in records[ds] if str(r['business_date'])>=str(masters[r['instrument_key']]['listing_date']) and (not masters[r['instrument_key']].get('termination_date') or str(r['business_date'])<str(masters[r['instrument_key']]['termination_date']))]
            stats=validate_batch(records,batch_codes,start,end)
            payload={'codes':batch_codes,'records':records,'raw':result.raw_records_by_dataset,'stats':stats}
            atomic_json(part,payload);state['chunks'][str(n)]=digest(part);atomic_json(state_path,state)
        validate_batch(payload['records'],batch_codes,start,end)
        files.append(part);summaries.append(payload['stats']);emit('RISK_DOWNLOAD',chunk=n+1,chunks=math.ceil(len(codes)/chunk_size),**payload['stats'])
    service=open_increment_service(root,end)
    try:
        if state.get('snapshot_id'):
            sid=state['snapshot_id'];snapshot=service.get_snapshot(sid)
        else:
            qid=batch+'_quality_v2'
            quality=service.catalog.row('SELECT * FROM data_quality_run WHERE quality_run_id=?',[qid])
            if quality:
                verify_manual_evidence(service,quality)
                emit('RISK_IMPORT_REUSED',quality_run_id=qid)
            else:
                # Candidate membership is bound to base snapshot + verified receipts below.
                # Other interrupted cutoffs may coexist in the catalog but cannot enter this snapshot.
                received=datetime.now(timezone.utc);calendar_written=set()
                for part in files:
                    payload=json.loads(part.read_text())
                    for ds,rows in payload['records'].items():
                        if ds=='instrument_master_history': rows=normalize_master(rows,old,start)
                        if ds=='market_calendar':
                            rows=[r for r in rows if (r['market_code'],r['business_date']) not in calendar_written]
                            calendar_written.update((r['market_code'],r['business_date']) for r in rows)
                        if rows: service.facts.append(ds,rows,task_id=batch,source_request_id=str(part),source_received_at=received,source_system='RQDATA_MANUAL',refresh_views=False)
                    emit('RISK_IMPORTED',chunk=part.name)
                service.facts.refresh_views()
                from .facts import business_key,payload_hash,normalize_payload
                desired={}
                for part in files:
                    data=json.loads(part.read_text())
                    for ds,rows in data['records'].items():
                        if ds=='instrument_master_history':rows=normalize_master(rows,old,start)
                        for row in rows:desired[(ds,business_key(ds,row))]=payload_hash(normalize_payload(row))
                wanted=pd.DataFrame([{'dataset_id':ds,'business_key':key,'payload_hash':h} for (ds,key),h in desired.items()])
                with service.catalog.transaction() as db:
                    db.register('_manual_wanted',wanted)
                    try:
                        matched=db.execute("""SELECT s.dataset_id,s.business_key,s.source_record_version,s.payload_hash,s.partition_id
                          FROM source_record_catalog s JOIN _manual_wanted w USING(dataset_id,business_key,payload_hash)
                          QUALIFY row_number() OVER(PARTITION BY s.dataset_id,s.business_key ORDER BY s.source_record_version DESC)=1""").fetchdf()
                    finally:db.unregister('_manual_wanted')
                if len(matched)!=len(wanted):raise ValueError('入库后逐键值核对失败')
                selection=work/'verified_records_v2.parquet';write_parquet(matched.sort_values(['dataset_id','business_key']),selection)
                evidence={'status':'PASS','base_snapshot_id':base['snapshot_id'],'base_member_hash':base['member_hash'],'start_date':str(base['start_date']),'end_date':str(end),'increment_start':str(start),'requested_end_date':state['requested_end_date'],'selection_path':str(selection),'assets':_hash_manifest_files([*files,selection]),'chunks':summaries,'checks':['unique_keys','active_stock_day_coverage','finite_traded_prices','raw_normalized_retained','persisted_value_hashes','base_passed_snapshot']}
                ep=work/'quality.json';atomic_json(ep,evidence)
                scope={'start_date':str(base['start_date']),'end_date':str(end),'markets':list(MARKETS),'evidence_path':str(ep),'evidence_sha256':digest(ep),'base_snapshot_id':base['snapshot_id'],'selection_path':str(selection),'selection_sha256':digest(selection)}
                if not service.catalog.row('SELECT quality_run_id FROM data_quality_run WHERE quality_run_id=?',[qid]):
                    now=datetime.now(timezone.utc)
                    service.catalog.execute('INSERT INTO data_quality_run VALUES (?, ?, NULL, ?, ?, ?, 0, 0, 0, ?, ?)',[qid,batch,RULE,canonical_json(scope),'PASS',now,now])
            # Reopen after committed imports: DuckDB ART index buffers can remain
            # pinned for the lifetime of a connection, starving the snapshot query.
            service.close()
            service=open_increment_service(root,end)
            quality=service.catalog.row('SELECT * FROM data_quality_run WHERE quality_run_id=?',[qid]);verify_manual_evidence(service,quality)
            if quality['candidate_id']:candidate=service.get_snapshot(quality['candidate_id'])
            else:
                emit('RISK_CANDIDATE_CREATING')
                candidate=service._create_candidate(batch,qid)
            service.close()
            service=open_increment_service(root,end)
            emit('RISK_PUBLISHING',candidate_id=candidate['snapshot_id'])
            published=service.catalog.row('SELECT snapshot_id FROM snapshot_publish_log WHERE request_id=?',[batch])
            snapshot=service.get_snapshot(published['snapshot_id']) if published else service.publish_candidate(candidate['snapshot_id'],candidate_hash=candidate['member_hash'],quality_run_id=qid,request_id=batch,confirmed_immutable=True,note=f'用户手动增量至{end}; 沪深既有证券范围',published_by='USER_MANUAL_UPDATE')
            sid=snapshot['snapshot_id'];state['snapshot_id']=sid;state['status']='PUBLISHED_PENDING_ADAPTER';atomic_json(state_path,state)
        validate_adapter(service,sid,files,start,end,work)
        reference=write_lifecycle_reference(service,sid,cfg,work)
        cfg={**cfg,'lifecycle_reference':reference,'snapshot_id':sid,'approved_observation_end':str(end),'requested_observation_end':str(requested_end or end),'previous_snapshot_id':base['snapshot_id'],'manual_update_report':str(work/'quality.json')}
        atomic_json(work/'previous_connection.json',json.loads(config_path.read_text()))
        atomic_json(config_path,cfg)
        state['status']='ACTIVE';atomic_json(state_path,state)
        emit('RISK_ACTIVE',snapshot_id=sid,end_date=str(end))
        return cfg
    finally:service.close()


def write_lifecycle_reference(service,sid,cfg,work):
    reference=cfg.get('lifecycle_reference',{})
    values={}
    if reference:
        path=Path(reference['path'])
        if digest(path)!=reference['sha256']:raise ValueError('旧生命周期证据哈希变化')
        frame=pd.read_csv(path,dtype=str,keep_default_na=False)
        def clean(v):
            v=str(v or '')[:10]
            return '' if v in {'0000-00-00','NaT','None'} else v
        for row in frame.to_dict('records'):
            code=row.get('order_book_id',row.get('security_code',''))
            if not code.endswith(('.XSHG','.XSHE')):continue
            values[code]={'market_code':code.split('.')[1],'security_code':code,'listed_date':clean(row.get('listed_date_norm') or row.get('listed_date')),'delisted_date':clean(row.get('de_listed_date_norm') or row.get('delisted_date') or row.get('de_listed_date'))}
    for row in sorted(snapshot_records(service,sid,'instrument_master_history'),key=lambda r:r['effective_start']):
        code=row['instrument_key'];values[code]={'market_code':row['market_code'],'security_code':code,'listed_date':row['listing_date'],'delisted_date':row.get('termination_date') or ''}
    output=work/'lifecycle_reference.csv'
    pd.DataFrame([values[k] for k in sorted(values)]).to_csv(output,index=False)
    return {'path':str(output),'sha256':digest(output),'source_snapshot_id':sid,'previous_reference':reference,'purpose':'手动增量后用于01/02的生命周期证据'}


def validate_adapter(service,sid,files,start,end,work):
    from .adapter import FiveTableShadowAdapter
    path=work/'adapter.json'
    if path.exists():
        report=json.loads(path.read_text())
        for asset,h in report['assets'].items():
            if digest(asset)!=h:raise ValueError('适配证据变化')
    else:
        from uuid import uuid4
        exported=FiveTableShadowAdapter(service).export(sid,work/('exports_'+uuid4().hex[:8]),start_date=start,end_date=end)
        tables=exported['tables'];df=pd.read_csv(tables['股票日行情表']['path'],dtype=str,keep_default_na=False)
        if df.duplicated(['交易市场代码','证券代码','交易日期']).any():raise ValueError('适配行情重复键')
        got=df.set_index(['证券代码','交易日期'])
        st=pd.read_csv(tables['证券风险状态表']['path'],dtype=str,keep_default_na=False)
        master=pd.read_csv(tables['证券基础状态日表']['path'],dtype=str,keep_default_na=False)
        if master.duplicated(['证券代码','状态日期']).any():raise ValueError('生命周期区间重叠')
        if st.duplicated(['证券代码','状态开始日期']).any():raise ValueError('ST适配重复键')
        st_index=st.set_index(['证券代码','状态开始日期'])
        master_index=master.set_index(['证券代码','状态日期'])
        cal=pd.read_csv(tables['交易日历表']['path'],dtype=str,keep_default_na=False).set_index(['交易市场代码','日历日期'])
        checks=0
        for part in files:
            r=json.loads(part.read_text())['records']
            for item in r['market_calendar']:
                value=cal.loc[(item['market_code'],item['business_date']),'是否交易日']
                if str(value).lower()!=str(item['is_trading_day']).lower():raise ValueError('日历适配不一致')
            for item in r['daily_st_status']:
                key=(item['instrument_key'],item['business_date'])
                if key not in st_index.index or st_index.loc[key,'风险状态值']!=('生效' if item['is_st'] else '未生效'):raise ValueError('ST适配不一致')
            for item in r['daily_suspension_status']:
                key=(item['instrument_key'],item['business_date'])
                if key not in got.index or got.loc[key,'成交状态']!=('停牌' if item['is_suspended'] else '正常成交'):raise ValueError('停牌适配不一致')
                if key not in master_index.index:raise ValueError('适配生命周期缺失')
            for p in r['daily_price_unadjusted']:
                key=(p['instrument_key'],p['business_date'])
                if key not in got.index:raise ValueError('适配行情缺失')
                actual=got.loc[key]
                for field,column in [('close','收盘价'),('open','开盘价'),('high','最高价'),('low','最低价')]:
                    if p.get(field) is not None and not math.isclose(float(actual[column]),float(p[field]),rel_tol=1e-9,abs_tol=1e-8):raise ValueError('适配行情数值不符')
                checks+=1
        report={'status':'PASS','market_snapshot_id':sid,'checked_price_rows':checks,'st_rows':len(st),'master_rows':len(master),'assets':{t['path']:digest(t['path']) for t in tables.values()}}
        atomic_json(path,report)
    service.catalog.execute("UPDATE market_snapshot SET risk_adapter_status='PASSED' WHERE snapshot_id=?",[sid])
    service._write_snapshot_manifest(service.get_snapshot(sid))


def update_adjusted(root,cfg,client,chunk_size):
    """Immutable post-close versions, separate from the six risk facts."""
    import rqdatac
    service=MarketFoundationService(root,read_only_catalog=True)
    try:
        sid=cfg['snapshot_id'];masters=snapshot_records(service,sid,'instrument_master_history')
        calendar=snapshot_records(service,sid,'market_calendar')
    finally:service.close()
    codes=sorted({r['instrument_key'] for r in masters})
    store=root/'post_adjusted';store.mkdir(exist_ok=True)
    head_path=store/'latest.json';previous=json.loads(head_path.read_text()) if head_path.exists() else None
    end=cfg['approved_observation_end'];start=cfg['approved_observation_start']
    if previous and previous['end_date']==end and previous['risk_snapshot_id']==sid:
        for f,h in previous['files'].items():
            if digest(store/f)!=h:raise ValueError('后复权资产哈希变化')
        emit('POST_ALREADY_CURRENT',end_date=end);return previous
    baseline=str(client.get_previous_trading_date(start))[:10]
    all_days=pd.to_datetime(client.get_trading_dates(baseline,end))
    version=store/('v_'+end.replace('-','')+'_'+sid[-8:]);version.mkdir(exist_ok=True)
    manifest_path=version/'manifest.json'
    progress=json.loads(manifest_path.read_text()) if manifest_path.exists() else {'risk_snapshot_id':sid,'start_date':start,'end_date':end,'baseline_date':baseline,'files':{},'chunks':{},'status':'DOWNLOADING'}
    if progress['risk_snapshot_id']!=sid:raise ValueError('复权续跑版本不一致')
    prior_all=None
    if previous:
        for f,h in previous['files'].items():
            if digest(store/f)!=h:raise ValueError('旧后复权版本损坏')
        prior_all=pd.concat([read_parquet(store/f) for f in previous['files'] if f.endswith('.parquet')],ignore_index=True)
    for n,pos in enumerate(range(0,len(codes),chunk_size)):
        batch=codes[pos:pos+chunk_size];part=version/f'close_{n:04}.parquet'
        if str(n) in progress['chunks']:
            if digest(part)!=progress['chunks'][str(n)]['sha256']:raise ValueError('复权分块损坏')
            continue
        # Read the previous immutable version to reuse old rows and validate the overlap.
        prior=None
        if previous:
            prior=prior_all[prior_all.security_code.isin(batch)]
        fetch_start=previous['end_date'] if prior is not None and not prior.empty else baseline
        def fetch(a):
            raw=rqdatac.get_price(batch,start_date=a,end_date=end,frequency='1d',fields=['close'],adjust_type='post',skip_suspended=False,expect_df=True,market='cn')
            if raw is None or raw.empty:return pd.DataFrame(columns=['security_code','trading_date','close_price'])
            data=raw.reset_index().rename(columns={'order_book_id':'security_code','date':'trading_date','close':'close_price'})
            data['trading_date']=pd.to_datetime(data.trading_date).dt.strftime('%Y-%m-%d')
            if data.duplicated(['security_code','trading_date']).any():raise ValueError('米筐后复权重复键')
            return data[['security_code','trading_date','close_price']]
        fresh=fetch(fetch_start);refetched=False
        if prior is not None and not prior.empty:
            overlap=prior.merge(fresh,on=['security_code','trading_date'],suffixes=('_old','_new'))
            comparable=overlap.dropna()
            changed=set(comparable.loc[~comparable.apply(lambda r:math.isclose(r.close_price_old,r.close_price_new,rel_tol=1e-9,abs_tol=1e-8),axis=1),'security_code'])
            expected=set(prior.loc[prior.trading_date==fetch_start,'security_code'])
            if changed or expected-set(comparable.security_code):fresh=fetch(baseline);refetched=True
            else:fresh=pd.concat([prior[prior.trading_date<fetch_start],fresh],ignore_index=True)
        if fresh.empty:raise ValueError('后复权整块无返回')
        values=pd.to_numeric(fresh.close_price,errors='coerce').dropna()
        if (values<=0).any() or not values.map(math.isfinite).all():raise ValueError('后复权价格非正或非有限值')
        temp=part.with_suffix('.tmp');write_parquet(fresh,temp);os.replace(temp,part)
        h=digest(part);progress['files'][str(part.relative_to(store))]=h;progress['chunks'][str(n)]={'sha256':h,'rows':len(fresh),'history_refetched':refetched};atomic_json(manifest_path,progress)
        emit('POST_DOWNLOADED',chunk=n+1,chunks=math.ceil(len(codes)/chunk_size),rows=len(fresh),history_refetched=refetched)
    # Require every stored traded-price key to have a finite positive post-adjusted close.
    db=duckdb.connect(str(root/'duckdb/market_catalog.duckdb'),read_only=True)
    try:
        literals=','.join("'"+str(store/f).replace("'","''")+"'" for f in progress['files'])
        db.execute('CREATE TEMP VIEW post_check AS SELECT * FROM read_parquet(['+literals+'])')
        duplicates=db.execute('SELECT count(*) FROM (SELECT security_code,trading_date FROM post_check GROUP BY ALL HAVING count(*)>1)').fetchone()[0]
        if duplicates:raise ValueError('后复权持久数据重复证券日期')
        coverage=db.execute("""SELECT count(*),count(*) FILTER(WHERE p.close_price IS NULL OR NOT isfinite(p.close_price) OR p.close_price<=0)
          FROM market_snapshot_record m JOIN source_record_catalog s USING(dataset_id,business_key,source_record_version,payload_hash,partition_id)
          LEFT JOIN post_check p ON p.security_code=json_extract_string(s.payload_json,'$.instrument_key') AND p.trading_date=json_extract_string(s.payload_json,'$.business_date')
          WHERE m.snapshot_id=? AND m.dataset_id='daily_price_unadjusted'
           AND TRY_CAST(json_extract_string(s.payload_json,'$.close') AS DOUBLE)>0
           AND json_extract_string(s.payload_json,'$.business_date') BETWEEN ? AND ?""",[sid,start,end]).fetchone()
        progress['quality']={'expected_price_rows':coverage[0],'missing_post_close_rows':coverage[1],'duplicate_keys':duplicates}
        atomic_json(manifest_path,progress)
        if coverage[1]:raise ValueError(f'后复权行情缺少{coverage[1]}个已有成交价格键，未启用新版本；可核对分块后重取')
    finally:db.close()
    progress['status']='AVAILABLE';progress['price_adjustment']='RQData后复权价格收益（权息修复）';progress['markets']=list(MARKETS)
    atomic_json(manifest_path,progress);atomic_json(head_path,progress)
    emit('POST_ACTIVE',end_date=end,files=len(progress['files']))
    return progress


def main():
    ap=argparse.ArgumentParser(description='手动更新市场底座；不创建自动任务。默认仅显示计划。')
    ap.add_argument('--connection',type=Path,default=Path(__file__).resolve().parents[5]/'data/local_market_connection.json');ap.add_argument('--end-date',type=date.fromisoformat,required=True)
    ap.add_argument('--execute',action='store_true');ap.add_argument('--chunk-size',type=int,default=500)
    args=ap.parse_args();update(args.connection,args.end_date,execute=args.execute,chunk_size=args.chunk_size)

if __name__=='__main__':main()


def export_adjusted(connection, output_dir, start=None, end=None):
    """Export the persisted post-close dataset without importing or calling rqdatac."""
    import duckdb
    cfg=json.loads(Path(connection).read_text());root=Path(cfg['market_lake_root']);store=root/'post_adjusted'
    head=json.loads((store/'latest.json').read_text())
    if head['status']!='AVAILABLE' or head['risk_snapshot_id']!=cfg['snapshot_id']:raise ValueError('后复权版本与风险快照未对齐，请先手动更新')
    start=str(start or head['start_date']);end=str(end or head['end_date'])
    requested_end=end
    verified_cutoff=max(head['end_date'],cfg.get('requested_observation_end',head['end_date']))
    if start<head['start_date'] or end>verified_cutoff or start>end:raise ValueError('导出区间超出已保存后复权行情范围')
    end=min(end,head['end_date'])
    if start>end:raise ValueError('导出区间没有已覆盖交易日')
    files=[str(store/f) for f,h in head['files'].items() if digest(store/f)==h]
    if len(files)!=len(head['files']):raise ValueError('后复权分区哈希不一致')
    out=Path(output_dir).resolve()
    if out.exists():raise FileExistsError('导出目录已存在: '+str(out))
    out.mkdir(parents=True)
    db=duckdb.connect(str(root/'duckdb/market_catalog.duckdb'),read_only=True)
    try:
        db.execute("SET memory_limit='2GB'");db.execute('SET threads=2')
        literals=','.join("'"+f.replace("'","''")+"'" for f in files)
        db.execute('CREATE TEMP VIEW stored_post_close AS SELECT * FROM read_parquet(['+literals+'])')
        db.execute('''CREATE TEMP TABLE calendar_lag AS
         SELECT d,lag(d) OVER(ORDER BY d) previous_day FROM
         (SELECT DISTINCT json_extract_string(s.payload_json,'$.business_date') d '''+JOIN+'''
         AND json_extract_string(s.payload_json,'$.market_code')='XSHG'
         AND CAST(json_extract_string(s.payload_json,'$.is_trading_day') AS BOOLEAN))''',[cfg['snapshot_id'],'market_calendar'])
        resolved=db.execute('SELECT max(d) FROM calendar_lag WHERE d BETWEEN ? AND ?',[start,end]).fetchone()[0]
        if resolved is None:raise ValueError('导出范围没有交易日')
        end=resolved
        db.execute('''CREATE TEMP TABLE return_export AS
         SELECT json_extract_string(s.payload_json,'$.market_code') "交易市场代码",
          json_extract_string(s.payload_json,'$.instrument_key') "证券代码",
          json_extract_string(s.payload_json,'$.business_date') "交易日期",
          CASE WHEN CAST(json_extract_string(s.payload_json,'$.is_suspended') AS BOOLEAN) THEN '停牌' ELSE '正常成交' END "成交状态",
          prior.close_price "前收盘价", price.close_price "收盘价"
         FROM market_snapshot_record m JOIN source_record_catalog s
           USING(dataset_id,business_key,source_record_version,payload_hash,partition_id)
         JOIN calendar_lag cal ON cal.d=json_extract_string(s.payload_json,'$.business_date')
         LEFT JOIN stored_post_close price ON price.security_code=json_extract_string(s.payload_json,'$.instrument_key') AND price.trading_date=cal.d
         LEFT JOIN stored_post_close prior ON prior.security_code=json_extract_string(s.payload_json,'$.instrument_key') AND prior.trading_date=cal.previous_day
         WHERE m.snapshot_id=? AND m.dataset_id='daily_suspension_status' AND cal.d BETWEEN ? AND ?
         ORDER BY "交易日期","证券代码"''',[cfg['snapshot_id'],start,end])
        counts=db.execute('''SELECT count(*),count(*) FILTER(WHERE "成交状态"='正常成交'),count(*) FILTER(WHERE "成交状态"='正常成交' AND ("前收盘价" IS NULL OR "收盘价" IS NULL OR "前收盘价"<=0 OR NOT isfinite("前收盘价") OR NOT isfinite("收盘价"))) FROM return_export''').fetchone()
        if counts[0]==0:raise ValueError('导出范围没有市场数据')
        output=out/'股票日行情表_后复权.csv'
        db.execute('COPY return_export TO ? (HEADER, DELIMITER \',\')',[str(output)])
        manifest={'status':'EXPORTED','rqdata_accessed':False,'risk_snapshot_id':cfg['snapshot_id'],'post_adjusted_manifest':str(store/'latest.json'),'post_adjusted_manifest_sha256':digest(store/'latest.json'),'start_date':start,'requested_end_date':requested_end,'end_date':end,'row_count':counts[0],'normal_trading_row_count':counts[1],'missing_normal_trading_row_count':counts[2],'output_path':str(output),'output_sha256':digest(output),'price_adjustment':'RQData后复权价格收益（权息修复）','return_formula':'T日后复权收盘价/T-1交易日后复权收盘价-1'}
        atomic_json(out/'后复权行情清单.json',manifest);emit('OFFLINE_EXPORT',**manifest);return output
    finally:db.close()
