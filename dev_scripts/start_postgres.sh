#!/bin/bash
if [[ "$(hostname)" == "MacBook-Pro-20" ]] && [[ "$(whoami)" == "patrickwang" ]]; then
    pg_ctl start
else
    echo "unimplemented" >&2
    exit 1
fi