"""Offline fixtures only; no real RQData initialization or market writes."""
import csv
from datetime import date, timedelta
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(HERE))
import workflow as w
sys.path.insert(0,str(w.RISK/'src'))


def csv_file(path, rows, columns=None):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w',encoding='utf-8-sig',newline='') as f:
        writer=csv.writer(f);writer.writerow(columns or ['biz_date','stk_code','证券名称','券商03']);writer.writerows(rows)


class Calendar:
    def get_trading_dates(self,start,end):
        day=date.fromisoformat(start);result=[]
        while day<=date.fromisoformat(end):
            if day.weekday()<5:result.append(day)
            day+=timedelta(days=1)
        return result


def daily_cache(folder, end, omit=None):
    quarters={}
    for day in Calendar().get_trading_dates(str(w.TABLE_START),str(end)):
        if day == omit:continue
        quarters.setdefault(f'{day.year}Q{(day.month-1)//3+1}',[]).append([str(day),'000001.SZ','示例','A'])
    for quarter,rows in quarters.items():csv_file(folder/f'集中度分类_{quarter}.csv',rows)


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        fixture = patch.object(w, "OVERRIDES", {"990001.SZ": "G", "990002.SZ": "G"})
        fixture.start(); self.addCleanup(fixture.stop)
        # Production deliberately runs 01 and 02 in different subprocesses.
        for key in list(sys.modules):
            if key == 'riskaudit' or key.startswith('riskaudit.'):
                del sys.modules[key]
        sys.path.insert(0,str(w.RISK/'src'))

    def test_chinese_risk_output_filters_year_and_deduplicates_stocks(self):
        import pandas as pd
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);source=root/'events.csv'
            csv_file(source,[['XSHE','000001.XSHE','2025-08-01'],['XSHE','000001.XSHE','2026-08-01'],['XSHE','000001.XSHE','2026-08-02']],['交易市场代码','证券代码','首次事实日期'])
            w.export_current_events(source,root,date(2026,8,31))
            self.assertEqual(len(pd.read_csv(root/'风险股票清单.csv')),1)
            self.assertEqual(len(pd.read_csv(root/'风险事件明细.csv')),2)
    def test_monthly_boundaries_and_g_correction_do_not_change_excel(self):
        from openpyxl import Workbook
        import pandas as pd
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);source=root/'input';source.mkdir();audit=root/'audit';audit.mkdir()
            for day,grade in [('20250115','A'),('20250228','C'),('20260915','B')]:
                book=Workbook();sheet=book.active;sheet.title='Sheet1';sheet.append(['证券代码','证券名称','集中度分类']);sheet.append(['000001.SZ','示例',grade]);sheet.append(['990001.SZ','示例2','A']);book.save(source/f'股票打分结果{day}.xlsx')
            original={p.name:p.read_bytes() for p in source.iterdir()}
            upload,end=w.prepare_single(source,root/'daily',audit,Calendar(),date(2026,9,13),daily_source=root/'cache')
            self.assertEqual(end,date(2026,9,11))
            data=pd.concat([pd.read_csv(p,dtype=str) for p in (root/'daily').glob('*.csv')])
            self.assertEqual(data[(data.biz_date=='2025-02-28')&(data.stk_code=='000001.SZ')]['券商03'].iloc[0],'A')
            self.assertEqual(data[(data.biz_date=='2025-03-03')&(data.stk_code=='000001.SZ')]['券商03'].iloc[0],'C')
            self.assertEqual(set(data[data.stk_code.isin(w.OVERRIDES)]['券商03']),{'G'})
            self.assertEqual(len(data[data.stk_code=='990002.SZ']),data.biz_date.nunique())
            self.assertEqual(original,{p.name:p.read_bytes() for p in source.iterdir()})
            self.assertTrue(upload.exists())
            self.assertEqual(json.loads((audit/'月度转换记录.json').read_text())['action'],'CONVERTED')
            self.assertTrue((root/'cache/集中度分类_2026Q3.csv').exists())

    def test_daily_cache_reused_without_excel_and_trimmed_without_mutation(self):
        import pandas as pd
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);cache=root/'cache';audit=root/'audit';audit.mkdir()
            daily_cache(cache,date(2026,9,15))
            before={p.name:p.read_bytes() for p in cache.iterdir()}
            with patch.object(w,'load_module',side_effect=AssertionError('must not convert')):
                upload,end=w.prepare_single(root/'missing_excel',root/'daily',audit,Calendar(),date(2026,9,13),daily_source=cache)
            self.assertEqual(end,date(2026,9,11))
            self.assertEqual(pd.read_csv(upload).biz_date.max(),'2026-09-11')
            self.assertEqual(before,{p.name:p.read_bytes() for p in cache.iterdir()})
            self.assertEqual(json.loads((audit/'月度转换记录.json').read_text())['action'],'REUSED')
            self.assertEqual(set(pd.read_csv(upload).query('stk_code == "990002.SZ"')['券商03']),{'G'})

    def test_daily_cache_missing_history_requires_monthly_input(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);audit=root/'audit';audit.mkdir()
            daily_cache(root/'cache',date(2026,9,15),omit=date(2025,2,3))
            with self.assertRaisesRegex(ValueError,'2025-02-03'):
                w.prepare_single(root/'missing',root/'daily',audit,Calendar(),date(2026,9,15),daily_source=root/'cache')

    def test_cache_extends_from_monthly_when_new_cutoff_requested(self):
        from openpyxl import Workbook
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);audit=root/'audit';audit.mkdir();source=root/'input';source.mkdir()
            daily_cache(root/'cache',date(2026,8,31))
            book=Workbook();sheet=book.active;sheet.title='Sheet1'
            sheet.append(['证券代码','证券名称','集中度分类']);sheet.append(['000001.SZ','示例','B'])
            book.save(source/'股票打分结果20250115.xlsx')
            upload,end=w.prepare_single(source,root/'daily',audit,Calendar(),date(2026,9,15),daily_source=root/'cache')
            self.assertEqual(end,date(2026,9,15))
            self.assertIn(end,w.inspect_single_daily(root/'cache'))
            self.assertEqual(json.loads((audit/'月度转换记录.json').read_text())['action'],'CONVERTED')
            self.assertIn('2026-09-15,000001.SZ,示例,B',upload.read_text(encoding='utf-8-sig'))

    def test_cache_default_end_and_invalid_grade_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);audit=root/'audit';audit.mkdir();cache=root/'cache'
            daily_cache(cache,date(2026,9,15))
            _,end=w.prepare_single(root/'missing',root/'daily',audit,Calendar(),None,daily_source=cache)
            self.assertEqual(end,date(2026,9,15))
            csv_file(cache/'集中度分类_2026Q3.csv',[['2026-09-15','000001.SZ','示例','Z']])
            with self.assertRaisesRegex(ValueError,'档位不是A—G'):w.inspect_single_daily(cache)

    def test_peer_quarter_override_and_duplicate_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);peer=root/'module';incoming=root/'incoming'
            csv_file(peer/'data/集中度2026Q1.csv',[['2026-01-05','000001.SZ','示例','A']])
            csv_file(peer/'data/集中度2026Q3.csv',[['2026-07-01','000001.SZ','示例','A']])
            upload=incoming/'新季度.csv';csv_file(upload,[['2026-09-15','000001.SZ','示例','G']])
            with patch.object(w,'PEER',peer):
                selected,end=w.prepare_peer_history(upload,root/'daily')
                self.assertEqual(end,date(2026,9,15));self.assertEqual(selected.read_bytes(),upload.read_bytes())
                self.assertEqual(len(list((root/'daily').glob('*.csv'))),2)
                csv_file(incoming/'重复2026Q1.csv',[['2026-01-05','000001.SZ','示例','A']])
                csv_file(incoming/'另一份2026Q1.csv',[['2026-01-05','000001.SZ','示例','G']])
                with self.assertRaisesRegex(ValueError,'同季度'):w.prepare_peer_history(upload,root/'bad')

    def test_cross_year_risk_scope_is_explicit_and_default_unchanged(self):
        engine=w.load_module('test_easy_risk',w.RISK/'scripts/run_real_risk_events.py')
        class Stop(Exception):pass
        for historical,expected in [(None,date(2026,1,1)),(date(2025,1,1),date(2025,1,1))]:
            calls=[]
            def rules(path,**kwargs):
                calls.append(kwargs)
                if kwargs:raise Stop()
            with patch.object(engine,'inspect_business_date_range',return_value=(date(2026,7,1),date(2026,9,15),1)),patch.object(engine,'load_approved_rules',side_effect=rules):
                with self.assertRaises(Stop):engine.run_real_risk_events(client=None,business_file='unused',output_dir='unused',batch_id='test',rules_path='unused',historical_observation_start=historical)
            self.assertEqual(calls[-1]['observation_start'],expected)
            self.assertEqual(calls[-1]['observation_end'],date(2026,9,15))

    def test_table_end_date_and_empty_events(self):
        table=w.load_module('test_easy_table',w.PEER/'excel8.9/calculate_scoring_tables_8_9.py')
        table.OVERRIDES = dict(w.OVERRIDES)
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);history=root/'history';events=root/'events';events.mkdir()
            days=['2026-09-10','2026-09-11','2026-09-14','2026-09-15']
            csv_file(history/'集中度分类_2026Q3.csv',[[d,'000001.SZ','示例','A'] for d in days])
            cal=root/'calendar.csv';csv_file(cal,[['XSHG',d,True] for d in days+['2026-09-16']],['market_code','calendar_date','is_trading_day'])
            event_csv=events/'risk_events.csv';csv_file(event_csv,[],['market_code','security_code','event_type','first_fact_date','risk_date','rule_version','calculation_batch_id'])
            manifest={'status':'SUCCEEDED','observation_start':'2026-09-10','observation_end':'2026-09-15','event_count':0,'rule_version':'v2','calculation_batch_id':'fixture','cutoff_st_evidence':{'schema_version':1,'method':'EXACT_DAILY_ST_LOOKUP_AT_PREVIOUS_TRADING_DAY','event_csv_sha256':table.sha256(event_csv),'entry_count':0,'entries':{}}}
            w.write_json(events/'calculation_manifest.json',manifest)
            result=table.calculate(history,events,cal,start=date(2026,9,10),end=date(2026,9,15),table8_start=date(2026,9,10))
            self.assertEqual(result['table9_period'],['2026-09-10','2026-09-15'])
            self.assertEqual(result['table9'][0]['trading_days'],4)
            self.assertEqual(len(result['table8']),3)
            self.assertTrue(all(r['risk_event_count']==0 for r in result['table8']))
            self.assertEqual(result['table9'][-1]['stock_days'],8)
            self.assertTrue(all(r['event_hit_rate_display'] is None for r in result['table8']))

    def test_broker_stage_collects_reports_and_never_enables_returns(self):
        from types import SimpleNamespace
        sys.path.insert(0,str(w.PEER/'src'))
        from riskaudit.tasks import runner
        with tempfile.TemporaryDirectory() as temp:
            run=Path(temp);audit=run/'核对材料';audit.mkdir();reports=audit/'stub_output';reports.mkdir()
            csv_file(reports/'券商风险命中率汇总_2026Q3.csv',[],['风险类型','事件命中率'])
            data={'mode':'single','end_date':'2026-09-15','upload':'unused.csv','history':'unused','handoff':'unused.json','source_batch':'unused','calendar':'unused.csv'}
            w.write_json(audit/'下一步.json',data)
            captured={}
            def broker(parameters,**kwargs):
                self.assertNotIn('market_price_csv',parameters)
                self.assertFalse(kwargs['peer'])
                return {'output_dir':str(reports)}
            def calculate(*args,**kwargs):
                captured.update(kwargs);return {}
            def save(result,output):
                output.mkdir();(output/'核对材料').mkdir()
                for name in ['风险股票拦截_股票打分.csv','风险股票分档_股票打分.csv','核对材料/风险事件评价明细.csv']:
                    csv_file(output/name,[],['fixture'])
                return output
            def module(name,path):
                if name=='handover_event_prepare':return SimpleNamespace(prepare=lambda *a:None)
                return SimpleNamespace(calculate=calculate,save_reports=save)
            with patch.object(runner,'_run_broker',side_effect=broker),patch.object(w,'load_module',side_effect=module):
                w.run_broker(run)
            self.assertEqual(captured['end'],date(2026,9,15))
            self.assertEqual(json.loads((run/'运行结果.json').read_text())['status'],'SUCCEEDED')
            for name in ['风险股票拦截_股票打分.csv','风险股票分档_股票打分.csv','风险股票拦截与分档风险事件评价明细.csv','券商风险命中率汇总_2026Q3.csv']:
                self.assertTrue((run/name).exists())

    def test_single_broker_file_is_rejected_before_online_peer_work(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'single.csv'
            csv_file(path,[['2026-09-15','000001.SZ','示例','A']])
            with self.assertRaisesRegex(ValueError,'全行业CSV缺少必要列'):
                w.validate_peer_header(path)

    def test_offline_environment_has_no_rqdata_credentials(self):
        from credentials import without_credentials
        env={'PATH':'keep','PYTHONUTF8':'1','RQDATA_USERNAME':'account','RQDATA_PASSWORD':'secret',
             'RQDATAC_CONF':'tcp://account:secret@host','RQDATAC_PROXY':'proxy',
             'RISKAUDIT_RQDATA_PROXY_FALLBACK':'proxy'}
        self.assertEqual(without_credentials(env),{'PATH':'keep','PYTHONUTF8':'1'})
        self.assertEqual(env['RQDATA_PASSWORD'],'secret')

    def test_uri_and_escaped_credentials_are_redacted(self):
        from credentials import redact, secret_values, configured
        env={'RQDATAC_CONF':'tcp://test-account:pass%20word%40x@localhost:16011'}
        text='test-account pass word@x pass%20word%40x '+env['RQDATAC_CONF']
        self.assertEqual(redact(text,secret_values(env)),' '.join(['[已隐藏]']*4))
        self.assertTrue(configured(env))
        self.assertFalse(configured({'RQDATA_USERNAME':'only-user'}))

    def test_redaction(self):
        with patch.dict('os.environ',{'RQDATA_PASSWORD':'private-password','RQDATA_USERNAME':'private-user'}):
            self.assertEqual(w.redact('private-user private-password'),'[已隐藏] [已隐藏]')


if __name__=='__main__':unittest.main()
