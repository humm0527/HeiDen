"""Small Tk launcher; credentials stay in memory and child environment only."""
from __future__ import annotations
import argparse
from datetime import date, datetime
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from credentials import configured, secret_values, redact

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent


def create_request(mode, source, output, end_text=''):
    if mode == 'single' and not source.strip():
        source = str(ROOT/'02_peer_benchmark/data/股票打分结果')
    source = Path(source).expanduser().resolve()
    output = Path(output).expanduser().resolve()
    if mode == 'single' and not source.is_dir():
        from workflow import quarter_files
        if not quarter_files(ROOT/'02_peer_benchmark/data/single'):
            raise ValueError('请选择股票打分结果文件夹，或在 single 放入完整逐日分类。')
    if mode == 'peer' and (not source.is_file() or source.suffix.lower() != '.csv'): raise ValueError('请选择全行业集中度CSV。')
    if source == output or source in output.parents:
        raise ValueError('结果目录请放在输入文件夹之外，避免混入源文件。')
    end = date.fromisoformat(end_text) if end_text.strip() else None
    if end and end >= date.today(): raise ValueError('截止日期必须早于今天。')
    if mode == 'peer' and end: raise ValueError('全行业截止日期跟随所选CSV。')
    name = ('券商03_' if mode == 'single' else '全行业_') + datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    run = output/name
    return {'mode': mode, 'input': str(source), 'end_date': str(end) if end else '', 'run_dir': str(run)}


class App:
    def __init__(self, window, mode):
        self.window = window; self.mode = mode; self.process = None; self.events = queue.Queue(); self.result = None
        title = '券商03：风险股票、命中率、风险股票拦截与分档' if mode == 'single' else '全行业：风险股票与券商评价'
        window.title(title); window.geometry('850x660'); window.minsize(720,560)
        frame = ttk.Frame(window, padding=16); frame.pack(fill='both', expand=True); frame.columnconfigure(1, weight=1); frame.rowconfigure(8, weight=1)
        ttk.Label(frame, text=title, font=('',14,'bold')).grid(row=0,column=0,columnspan=3,sticky='w',pady=(0,14))
        default = ROOT/'02_peer_benchmark/data'/('股票打分结果' if mode=='single' else '集中度2026Q3.csv')
        self.source=tk.StringVar(value=str(default) if mode=='single' or default.exists() else '')
        self.output=tk.StringVar(value=str(ROOT/'结果输出'))
        self.end=tk.StringVar(); self.username=tk.StringVar(); self.password=tk.StringVar()
        for row,label,value,command in [(1,'原始月度文件夹' if mode=='single' else '本季度集中度CSV',self.source,self.pick_source),(2,'结果保存位置',self.output,self.pick_output)]:
            ttk.Label(frame,text=label).grid(row=row,column=0,sticky='w',pady=6)
            ttk.Entry(frame,textvariable=value).grid(row=row,column=1,sticky='ew',padx=8)
            ttk.Button(frame,text='选择…',command=command).grid(row=row,column=2)
        ttk.Label(frame,text='截止日期（可留空）' if mode=='single' else '截止日期').grid(row=3,column=0,sticky='w',pady=6)
        if mode=='single':
            ttk.Entry(frame,textvariable=self.end).grid(row=3,column=1,sticky='ew',padx=8)
        else: ttk.Label(frame,text='自动读取CSV中的最后日期').grid(row=3,column=1,sticky='w',padx=8)
        ttk.Label(frame,text='米筐账号').grid(row=4,column=0,sticky='w',pady=6)
        ttk.Entry(frame,textvariable=self.username).grid(row=4,column=1,sticky='ew',padx=8)
        ttk.Label(frame,text='米筐密码').grid(row=5,column=0,sticky='w',pady=6)
        ttk.Entry(frame,textvariable=self.password,show='*').grid(row=5,column=1,sticky='ew',padx=8)
        note = '优先复用 single 逐日数据；覆盖不足时由完整月度截面生成。截止留空取最后截面日期，无截面时取逐日数据末日。' if mode=='single' else '程序自动结合随包历史季度及所选文件同目录的历史；所选季度文件优先。'
        ttk.Label(frame,text=note+'\n使用自己的米筐授权在线取数；已有环境授权可留空账号密码。本入口暂不计算收益率。',wraplength=770).grid(row=6,column=0,columnspan=3,sticky='w',pady=10)
        buttons=ttk.Frame(frame);buttons.grid(row=7,column=0,columnspan=3,sticky='w',pady=6)
        self.start_button=ttk.Button(buttons,text='开始计算',command=self.start);self.start_button.pack(side='left')
        self.open_button=ttk.Button(buttons,text='打开结果文件夹',command=self.open_result,state='disabled');self.open_button.pack(side='left',padx=12)
        self.log=tk.Text(frame,wrap='word',state='disabled');self.log.grid(row=8,column=0,columnspan=3,sticky='nsew',pady=8)
        self.status=tk.StringVar(value='就绪；已检测到本机授权，账号密码可留空。' if configured() else '就绪；请填写账号密码或先配置本机授权。');ttk.Label(frame,textvariable=self.status).grid(row=9,column=0,columnspan=3,sticky='w')
        window.protocol('WM_DELETE_WINDOW',self.close);window.after(150,self.poll)

    def pick_source(self):
        value=filedialog.askdirectory(title='选择完整月度截面文件夹') if self.mode=='single' else filedialog.askopenfilename(title='选择全行业本季度集中度CSV',filetypes=[('CSV','*.csv')])
        if value:self.source.set(value)
    def pick_output(self):
        value=filedialog.askdirectory(title='选择结果保存位置')
        if value:self.output.set(value)
    def start(self):
        try:
            request=create_request(self.mode,self.source.get(),self.output.get(),self.end.get().strip())
            user,password=self.username.get().strip(),self.password.get()
            if bool(user)!=bool(password):raise ValueError('账号和密码请同时填写，或同时留空使用已有授权。')
            env=os.environ.copy();env.update(PYTHONUTF8='1',PYTHONUNBUFFERED='1',PYTHONDONTWRITEBYTECODE='1')
            if user:
                env.pop('RQDATAC_CONF',None);env.pop('RQDATAC2_CONF',None)
                env.update(RQDATA_USERNAME=user,RQDATA_PASSWORD=password)
            if not user and not ((env.get('RQDATA_USERNAME') and env.get('RQDATA_PASSWORD')) or env.get('RQDATAC_CONF') or env.get('RQDATAC2_CONF')):
                raise ValueError('请填写同事自己的米筐账号密码，或先配置SDK授权环境变量。')
            run=Path(request['run_dir']);run.mkdir(parents=True,exist_ok=False)
            req=run/'运行请求.json';req.write_text(json.dumps(request,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
            self.process=subprocess.Popen([sys.executable,'-B',str(HERE/'workflow.py'),'--request',str(req)],cwd=ROOT,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,encoding='utf-8',errors='replace')
            secrets=secret_values(env)
            self.password.set('');self.result=run;self.start_button.configure(state='disabled');self.open_button.configure(state='normal');self.status.set('计算中，请保留窗口。')
            threading.Thread(target=self.read_output,args=(self.process,run,secrets),daemon=True).start()
        except Exception as exc:messagebox.showerror('无法开始',str(exc))
    def read_output(self,process,run,secrets):
        with (run/'运行日志.txt').open('w',encoding='utf-8') as log:
            for line in process.stdout:
                line=redact(line,secrets)
                log.write(line);log.flush();self.events.put(('line',line))
        self.events.put(('done',process.wait()))
    def poll(self):
        try:
            while True:
                kind,value=self.events.get_nowait()
                if kind=='line':
                    self.log.configure(state='normal');self.log.insert('end',value);self.log.see('end');self.log.configure(state='disabled')
                else:
                    self.start_button.configure(state='normal');self.process=None
                    self.status.set('全部完成，点击“打开结果文件夹”。' if value==0 else '运行未完成，请查看日志最后的提示；现有结果不会覆盖。')
        except queue.Empty:pass
        self.window.after(150,self.poll)
    def open_result(self):
        if self.result:
            if os.name=='nt':os.startfile(self.result)
            elif sys.platform=='darwin':subprocess.Popen(['open',str(self.result)])
    def close(self):
        if self.process is not None:
            messagebox.showinfo('仍在运行','请等待运行结束后关闭窗口。');return
        self.window.destroy()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--mode',choices=['single','peer'],required=True)
    parser.add_argument('--check-ui',action='store_true',help='仅验证窗口初始化，不启动计算')
    args=parser.parse_args()
    window=tk.Tk()
    if args.check_ui:window.withdraw()
    App(window,args.mode)
    if args.check_ui:
        window.update_idletasks();window.destroy()
        print('UI_CHECK_PASSED: no calculation or authentication performed')
    else:window.mainloop()


if __name__=='__main__':main()
