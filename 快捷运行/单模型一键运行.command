#!/bin/zsh -il
set -eu
unset HISTFILE
quick_dir="${0:A:h}"
exec /bin/zsh "$quick_dir/mac_start.sh" single "$@"
