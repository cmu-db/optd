%/bin/sh


mkdir -p data/tpch
rm data/tpch/*
tpchgen-cli -s 0.1 --format parquet -o data/tpch
cargo run -p optd-cli -- -p data/tpch
rm -rf data/tpch


