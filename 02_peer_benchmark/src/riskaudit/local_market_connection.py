"""
文件作用：读取仅含已验收本地市场路径的跨平台连接配置，环境变量始终优先。
编辑记录：
【首次生成：2026-09-14，为Mac启动器和服务提供持久化本地快照默认路径，不保存凭证。】
"""
from __future__ import annotations
import json
from pathlib import Path
import re
from typing import Mapping


def local_market_environment(project_root: Path, environment: Mapping[str, str]) -> dict[str, str]:
    output = dict(environment)
    if output.get('RISKAUDIT_P4_MARKET_LAKE_ROOT', '').strip():
        return output
    data_root = Path(output.get('RISKAUDIT_DATA_ROOT') or project_root / 'data').expanduser()
    if not data_root.is_absolute():
        data_root = project_root / data_root
    config_path = data_root / 'local_market_connection.json'
    if not config_path.is_file():
        return output
    config = json.loads(config_path.read_text(encoding='utf-8'))
    if config.get('schema_version') != 1 or not re.fullmatch(r'ms_[A-Za-z0-9]+', str(config.get('snapshot_id', ''))):
        raise ValueError('本地市场连接配置无效')
    lake = Path(config['market_lake_root']).expanduser()
    if not lake.is_absolute() or not (lake / 'duckdb/market_catalog.duckdb').is_file():
        raise ValueError('已配置的本地市场目录不可用，请核对路径；不会自动联网替代')
    output['RISKAUDIT_P4_MARKET_LAKE_ROOT'] = str(lake.resolve())
    output.setdefault('RISKAUDIT_MARKET_SOURCE', 'RQDATA')
    return output
