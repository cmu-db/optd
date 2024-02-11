= Usage

**Update the test cases**

```shell
cargo run -p optd-sqlplannertest --bin planner_test_apply
```

**Verify the test cases**

```shell
cargo test -p optd-sqlplannertest
# or use nextest
cargo nextest run -p optd-sqlplannertest
```

The `explain` and `execute` task will be run with datafusion's logical optimizer disabled. To keep using datafusion's logical optimizer, you could use the `execute_with_logical` and `explain_with_logical` tasks instead.

Currently we have the following options for the explain task:

- `logical_datafusion`: datafusion's logical plan.
- `logical_optd`: optd's logical plan before optimization.
- `physical_optd`: optd's physical plan after optimization.
- `physical_datafusion`: datafusion's physical plan.
- `join_orders`: physical join orders.
- `logical_join_orders`: logical join orders.

