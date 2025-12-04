#!/bin/bash

BENCHMARK_HOME="bench/tpch-sf0_01/"
TIMEOUT=60
TPCH_QUERIES=(1 2 3)
PUSHDOWN_OPTIONS=("on" "off")

for PUSHDOWN in ${PUSHDOWN_OPTIONS[@]}; do
  for Q in ${TPCH_QUERIES[@]}; do
    echo "q=r$Q,pushdown=$PUSHDOWN"
    timeout -v $TIMEOUT cargo run -p optd-cli --release -- -f "$BENCHMARK_HOME/pushdown-$PUSHDOWN.sql" -f "$BENCHMARK_HOME/prepare.sql" -f "$BENCHMARK_HOME/r$Q.sql" 2>/dev/null |
      grep "Elapsed" |
      tail -n 1 |
      awk '{ print $2 }'
  done
done
