# Cost Model Cardinality Benchmarking

## Overview
You can benchmark the cardinality estimates of optd's cost model against other DBMSs using the optd-perfbench module.

All aspects of benchmarking (except for setting up comparison DBMSs) are handled automatically. This includes loading workload data, building statistics, gathering the true cardinality of workload queries, running explains on workload queries, and aggregating cardinality estimation results.

We elected not to automate the installation and setup of the DBMS in order to accomodate the needs of all users. For instance, some users prefer installing Postgres on Homebrew, others choose to install the Mac application, while others wish to create a Postgres Docker container. However, it could be feasible in the future to standardize on Docker and automatically start a container. The only difficult part in that scenario is tuning Postgres/other DBMSs to the machine being run on, as this is currently done manually using PGTune.

Additionally, our system provides **fine-grained, robust caching** for every single step of the process. After the first run of a workload, all subsequent runs will *only require running explains*, which takes in a matter of seconds for all workloads. We use "acknowledgement files" to ensure that the caching is robust in that we never cache incomplete results.

## Basic Operation
First, you need to manually install, configure, and start the DBMS(s) being compared against. Currently, only Postgres is supported. To see an example of how Postgres is installed, configured, and started on a Mac, check the `patrick/` folder in the [gungnir-experiments](https://github.com/wangpatrick57/gungnir-experiments) repository.

Once the DBMS(s) being compared against are set up, run this to quickly get started. It should take a few minutes on the first run and a few seconds on subsequent runs. This specific command that tests TPC-H with scale factor 0.01 is **run in a CI script** before every merge to main, so it should be very reliable.
```
cargo run --release --bin optd-perfbench cardbench tpch --scale-factor 0.01
```

After this, you can try out different workloads and scale factors based on the CLI options.

Roughly speaking, there are two main ways the benchmarking system is used: (a) to compare the cardinality estimates of optd against another system *in aggregate* or (b) to investigate the cardinality estimates of a small subset of queries. The command above is for use case (a). The system automatically outputs a variety of *aggregate* information about the q-error including median, p95, max, and more. Additionally, the system outputs *comparative* information which shows the # of queries in which a given DBMS performs the best or is tied for the best.

For use case (b), you will want to set the `RUST_LOG` environment variable to `info` and use the `--query-ids` parameter. Setting `RUST_LOG` to `info` will show the results of the explain commands on all DBMSs and `--query-ids` will let you only run specific queries to avoid cluttering the output.
```
RUST_LOG=info cargo run --release --bin optd-perfbench cardbench tpch --scale-factor 0.01 --query-ids 2
```

## Supporting More Queries
Currently, we are missing support for a few queries in TPC-H, JOB, and JOB-light. An *approximate* list of supported queries can be found in the `[workload].rs` files (e.g. `tpch.rs` and `job.rs`). If `--query-ids` is ommitted from the command, we use the list of supported queries as defined in the `[workload].rs` file by default. Some of these queries are not supported by DataFusion, some by optd, and some because we run into an OOM error when trying to execute them on Postgres. Because of the last point, the set of supported queries may be different on different machines. The list of queries in `[workload].rs` (at least the one in `tpch.rs`) is tested to be working on the CI machine.

The *definitive* list of supported queries on your machine can be found by running `dev_scripts/which_queries_work.sh`, which simply runs the benchmarking system for each query individually. While this script does take a long time to complete when first run, it has the nice side effect of warming up all your caches so that subsequent runs are fast. The script outputs a string to replace the `WORKING_*QUERY_IDS` variable in `[workload].rs` as well as another string to use as the `--query-ids` argument. If you are use `which_queries_work.sh` to figure out the queries that work on your machine, you probably want to use `--query-ids` instead of setting `WORKING_*QUERY_IDS`.

If you add support for more queries, you will want to rerun `dev_scripts/which_queries_work.sh`. Since you are permanently adding support for more queries, you will want to update `WORKING_*QUERY_IDS`.

## Adding More DBMSs
Currently, only Postgres is supported. Additional DBMSs can be easily added using the `CardbenchRunnerDBMSHelper` trait and optionally the `TruecardGetter` trait. `CardbenchRunnerDBMSHelper` must be implemented by all DBMSs that are supported because it has functions for gathering estimated cardinalities from DBMSs. `TruecardGetter` only needs to be implemented by at least one DBMS. The true cardinality should be the same across all DBMSs, so we only execute the queries for real on a single DBMS to drastically reduce benchmarking runtime. `TruecardGetter` is currently implemented for Postgres, so it is unnecessary to implement this for any other DBMS unless one wishes to improve the runtime of benchmarking (e.g. by gathering true cardinalities using an OLAP DBMS for OLAP workloads). Do keep in mind that true cardinalities are cached after the first run of a workload and can be shared across users (in the future, perhaps we'll even put the cached true cardinalities in the GitHub repository itself), so this optimization is not terribly important.