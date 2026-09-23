"""从已发布市场底座离线计算01并生成02交接清单；不调用RQData。"""
from pathlib import Path
import argparse
from datetime import date,datetime
import json
import shutil
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from riskaudit.business_files import inspect_business_date_range
from riskaudit.market_foundation.risk_shadow import run_snapshot_risk_calculation
from riskaudit.tasks.broker_handoff import write_broker_input_manifest


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--connection',type=Path,required=True)
    ap.add_argument('--business-file',type=Path,required=True)
    ap.add_argument('--broker-history-dir',type=Path,action='append',default=[])
    ap.add_argument('--output-dir',type=Path,default=ROOT/'data/outputs')
    ap.add_argument('--batch-id')
    args=ap.parse_args()
    # Offline guarantee applies to this entire command, including any accidental downstream call.
    def offline(event,values):
        if event in {'socket.connect','socket.connect_ex','socket.getaddrinfo','socket.sendto'}:raise RuntimeError('本地市场计算禁止联网')
    sys.addaudithook(offline)
    cfg=json.loads(args.connection.read_text());_,end,_=inspect_business_date_range(args.business_file);start=date(end.year,1,1)
    if str(end)>cfg['approved_observation_end']:raise ValueError('市场底座尚未覆盖上传日期，请先手动增量更新')
    batch=args.batch_id or 'local_risk_'+datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    supplemental=sorted({p.resolve() for d in args.broker_history_dir for p in d.glob('*.csv') if p.name.startswith('集中度')})
    result=run_snapshot_risk_calculation(repository_root=ROOT,market_lake_root=cfg['market_lake_root'],snapshot_id=cfg['snapshot_id'],business_file=args.business_file,output_root=args.output_dir,batch_id=batch,rules_path=ROOT/'configs/rules/approved/rules_v2.yaml',observation_start=start,observation_end=end,risk_universe_files=supplemental,lifecycle_reference_path=cfg.get('lifecycle_reference',{}).get('path'),progress_callback=lambda stage,detail:print(stage,flush=True))
    resultdir=Path(result['result_dir']);batchroot=args.output_dir.resolve()/batch
    quarter=f'{end.year}Q{(end.month-1)//3+1}'
    target=args.output_dir/'结果输出'/datetime.now().strftime('%Y%m%d_%H%M%S_%f');target.mkdir(parents=True)
    for src,name in [(resultdir/'risk_events.csv','风险事件'),(resultdir/'continuous_limit_down_segments.csv','连续跌停区段'),(Path(result['pressure_result_dir'])/'limit_down_pressure_observations.csv','低门槛压力观察')]:shutil.copy2(src,target/f'{name}_{quarter}.csv')
    market=result['broker_metric_market_inputs']['files']
    handoff=write_broker_input_manifest(target/f'02输入清单_{quarter}.json',batch_id=batch,observation_start=str(start),observation_end=str(end),files={'event_csv':resultdir/'risk_events.csv','event_manifest':resultdir/'calculation_manifest.json','calendar_csv':market['calendar']['path'],'st_csv':market['st_status']['path'],'lifecycle_csv':market['lifecycle']['path'],'universe_csv':batchroot/'risk_input_code/broker_risk_universe.csv'})
    summary={'status':result['status'],'rqdata_accessed':False,'snapshot_id':cfg['snapshot_id'],'observation_start':str(start),'observation_end':str(end),'event_count':result['result_manifest']['event_count'],'output_dir':str(target.resolve()),'risk_input_manifest':str(handoff)}
    (target/f'运行摘要_{quarter}.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n');print(json.dumps(summary,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
