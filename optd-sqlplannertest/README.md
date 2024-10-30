# Usage

## Update the test cases

```shell
cargo run -p optd-sqlplannertest --bin planner_test_apply
# or, supply a list of directories to scan from
cargo run -p optd-sqlplannertest --bin planner_test_apply -- subqueries
```

## Verify the test cases

```shell
cargo test -p optd-sqlplannertest
# or use nextest
cargo nextest run -p optd-sqlplannertest
```

## Tasks

The `explain` and `execute` task will be run with datafusion's logical optimizer disabled. Each task has some toggleable flags to control its behavior.

### `execute` Task

#### Flags

| Name           | Description                           |
| -------------- | ------------------------------------- |
| use_df_logical | Enable Datafusion's logical optimizer |

### Explain Task

#### Flags

| Name           | Description                                                        |
| -------------- | ------------------------------------------------------------------ |
| use_df_logical | Enable Datafusion's logical optimizer                              |
| verbose        | Display estimated cost in physical plan                            |
| logical_rules  | Only enable these logical rules (also disable heuristic optimizer) |

Currently we have the following options for the explain task:

- `logical_datafusion`: datafusion's logical plan.
- `logical_optd`: optd's logical plan before optimization.
- `optimized_logical_optd`: optd's logical plan after heuristics optimization and before cascades optimization.
- `physical_optd`: optd's physical plan after optimization.
- `physical_datafusion`: datafusion's physical plan.
- `join_orders`: physical join orders.
- `logical_join_orders`: logical join orders.

## Tracing a query

```
RUST_LOG=optd_core=trace cargo run -p optd-sqlplannertest --bin planner_test_apply -- pushdowns &> log
```
