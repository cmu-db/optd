# Data Generation

This repository keeps benchmark data out of git. Generate local Parquet files under
`optd/connectors/datafusion/data/` before running SQLLogicTest suites or local benchmark
experiments that read real tables.

## TPC-H

TPC-H data is generated directly as Parquet using `tpchgen-cli`.

```sh
./scripts/generate_tpch.sh
```

The default scale factor is `0.1`, and files are written to:

```text
optd/connectors/datafusion/data/tpch/sf-0.1/
```

To generate another scale factor or output directory:

```sh
./scripts/generate_tpch.sh 1 optd/connectors/datafusion/data/tpch/sf-1
```

The connector currently registers SF 0.1 from `optd/connectors/datafusion/data/tpch/sf-0.1`.
Use that default location for the existing TPC-H SLT tests.

## Join Order Benchmark

The Join Order Benchmark (JOB) data is derived from the IMDB dataset used by the
[CedarDB JOB example](https://cedardb.com/docs/example_datasets/job/). CedarDB documents that the
archive is about 1.2 GB compressed and about 3.7 GB after extraction. The script below downloads
that archive, imports the CSV files into a local DuckDB database, and exports one Parquet file per
JOB table.

```sh
./scripts/generate_job_parquet.sh
```

The default output directory is:

```text
optd/connectors/datafusion/data/job/
```

The default staging directory is:

```text
optd/connectors/datafusion/data/job-source/
```

To choose explicit locations:

```sh
./scripts/generate_job_parquet.sh \
  optd/connectors/datafusion/data/job \
  optd/connectors/datafusion/data/job-source
```

The script uses `scripts/job_schema_duckdb.sql`, a DuckDB-compatible version of the JOB schema.
It expects the extracted CSV files to use the CedarDB/JOB filenames, for example `title.csv`,
`cast_info.csv`, and `movie_info.csv`.

## Running TPC-H SLT

After generating the default TPC-H dataset:

```sh
cargo nextest run --release -p optd-datafusion --test slt tpch
```
