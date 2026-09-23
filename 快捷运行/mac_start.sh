#!/bin/zsh
set -eu
quick_dir="${0:A:h}"
root_dir="${quick_dir:h}"
mode="${1:-single}"
shift || true
selected_python=""
for candidate in "$quick_dir/.venv/bin/python" "$root_dir/../.venv/bin/python" "$root_dir/01_risk_events/.venv/bin/python" "$root_dir/02_peer_benchmark/.venv/bin/python"; do
  [[ -x "$candidate" ]] || continue
  # Normalize directories without dereferencing the venv's Python symlink.
  candidate="$(cd "${candidate:h}" && pwd)/${candidate:t}"
  if "$candidate" -B -c 'import sys, importlib.util; assert sys.version_info >= (3,11); assert all(importlib.util.find_spec(n) for n in ("tkinter","_tkinter","pandas","duckdb","openpyxl","yaml","rqdatac"))' >/dev/null 2>&1; then
    selected_python="$candidate"
    break
  fi
done
if [[ -z "$selected_python" ]]; then
  print '未找到包含窗口组件和米筐依赖的Python，请先双击“Mac首次安装.command”。'
  read -r '?按回车关闭。'
  exit 1
fi
export PYTHONUTF8=1 PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
cd "$root_dir"
if [[ "${1:-}" == '--check' ]]; then
  print "Python：$selected_python"
  "$selected_python" -B -c 'import os; e=os.environ; ok=bool(e.get("RQDATAC_CONF") or e.get("RQDATAC2_CONF") or (e.get("RQDATA_USERNAME") and e.get("RQDATA_PASSWORD"))); print("本机米筐授权：" + ("已配置（未连接验证）" if ok else "未配置"))'
  "$selected_python" -B "$quick_dir/launcher.py" --mode "$mode" --check-ui
else
  "$selected_python" -B "$quick_dir/launcher.py" --mode "$mode" || { read -r '?启动失败，请保留错误信息。按回车关闭。'; exit 1; }
fi
