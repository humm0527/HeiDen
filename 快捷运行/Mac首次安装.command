#!/bin/zsh -il
set -eu
unset HISTFILE
quick_dir="${0:A:h}"
root_dir="${quick_dir:h}"
selected_python=""
# Select a Python with Tk; installing rqdatac alone cannot add the Tk runtime.
for candidate in "$root_dir/../.venv/bin/python" "$root_dir/01_risk_events/.venv/bin/python" /opt/homebrew/bin/python3 /usr/local/bin/python3 "$(command -v python3 || true)"; do
  [[ -x "$candidate" ]] || continue
  if "$candidate" -B -c 'import sys, tkinter, _tkinter; assert sys.version_info >= (3,11)' >/dev/null 2>&1; then
    selected_python="$candidate"; break
  fi
done
if [[ -z "$selected_python" ]]; then
  print '请先安装带Tcl/Tk的Python 3.11或更新版本，再运行此安装入口。'
  read -r '?按回车关闭。'
  exit 1
fi
cd "$quick_dir"
"$selected_python" -m venv .venv || { read -r '?创建环境失败，按回车关闭。'; exit 1; }
.venv/bin/python -m pip install -r "$root_dir/01_risk_events/requirements.txt" -r "$root_dir/01_risk_events/requirements-rqdata.txt" || { read -r '?依赖安装失败，按回车关闭。'; exit 1; }
print '安装完成。以后双击“单模型一键运行.command”或“全行业一键运行.command”。'
read -r '?按回车关闭。'
