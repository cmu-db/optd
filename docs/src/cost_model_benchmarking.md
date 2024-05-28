# Cost Model Cardinality Benchmarking

You can benchmark the cardinality estimates of optd's cost model against other DBMSs using the optd-perftest module. To quickly get started, run:
```
cargo run --release --bin optd-perftest cardbench tpch --scale-factor 0.01
```

For this command to work, the DBMS(s) being compared against must be manually started.

Currently, we only support comparison against Postgres's cost model, but it is easy to extend the system to compare against
other DBMSs.