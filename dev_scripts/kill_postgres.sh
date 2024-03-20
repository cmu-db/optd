#!/bin/bash
if [[ "$(whoami)" == "patrickwang" ]]; then
    # a sketchy way to kill "orphaned" postgres processes (i.e. processes whose pgdata dir has already been deleted)
    # use this script to avoid having to manually do "ps aux" and "kill [pid]" every time cardtest_integration (or something similar) fails
    # the "main" process has bin/postgres in it
    # we kill it in a loop because sometimes it's required for some reason I don't understand
    while pid=$(ps aux | grep bin/postgres | grep -v grep | head -n1 | awk '{print $2}'); do
    if [ -z "$pid" ]; then
        break
    else
        # we'll sometimes kill pids that don't exist. don't show the error in these cases
        kill $pid &>/dev/null
        sleep 1 # sleep so we don't loop too fast
    fi
    done

    # even after it's gone, wait for it to completely shut down
    sleep 1
else
    echo "unimplemented" >&2
    exit 1
fi