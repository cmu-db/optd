%/bin/sh


mkdir -p data/tpch
rm data/tpch/*.parquet
tpchgen-cli -s 0.1 --format parquet -o data/tpch
RUST_LOG=info cargo run -p optd-cli -- -p data/tpch
rm -rf data/tpch/*.parquet


