#!/bin/sh

# Make sure you install tpchgen-cli first:
# cargo install tpchgen-cli

mkdir -p data/tpch
rm data/tpch/*.parquet
tpchgen-cli -s 0.1 --format parquet -o data/tpch
RUST_LOG=warn cargo run -p optd-cli -- -r populate.sql
