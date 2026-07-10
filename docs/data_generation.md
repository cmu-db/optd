# Data Generation

This repository keeps benchmark data out of git. Prepare local Parquet files under
`optd/connectors/datafusion/data/` before running SQLLogicTest suites or local benchmark
experiments that read real tables.

## TPC-H

TPC-H SF 0.1 is downloaded as pinned Parquet files from the same dataset revision used by CI.

```sh
./scripts/download_tpch_hf.sh
```

Files are written to:

```text
optd/connectors/datafusion/data/tpch/sf-0.1/
```

The connector and existing TPC-H SLTs use this location directly. The download script owns the
pinned dataset revision, and CI keys its data cache from the script so changing the revision also
invalidates the cache.

## Join Order Benchmark

The Join Order Benchmark (JOB) data is downloaded as pinned Parquet files from the same dataset
revision used by scheduled CI.

```sh
./scripts/download_job_hf.sh
```

The default output directory is:

```text
optd/connectors/datafusion/data/job/
```

The connector and JOB SLTs use this location directly. Scheduled CI keys its data cache from the
download script, so changing the pinned revision also invalidates the cache.

## Reference Generators

The repository keeps the original local generation tools as references for rebuilding or
republishing datasets. They are not the canonical test-data setup and may not reproduce the exact
bytes pinned by CI.

```sh
./scripts/generate_tpch.sh
./scripts/generate_job_parquet.sh
```

The TPC-H generator requires `tpchgen-cli`. The JOB generator requires DuckDB and uses
`scripts/job_schema_duckdb.sql` to convert the CedarDB/JOB CSV archive to Parquet.

## Running TPC-H SLT

After downloading the TPC-H dataset:

```sh
cargo nextest run --release -p optd-datafusion --test slt tpch
```
