# SQL Planner Tests

These test cases use the [sqlplannertest](https://crates.io/crates/sqlplannertest) crate to execute SQL queries and inspect their output.
They do not check whether a plan is correct, and instead rely on a text-based diff of the query's output to determine whether something is different than the expected output.


## Execute Test Cases

```shell
cargo test -p optd-sqlplannertest
# or use nextest
cargo nextest run -p optd-sqlplannertest
```
## Add New Test Case

To add a SQL query tests, create a YAML file in a subdir in "tests".
Each file can contain multiple tests that are executed in sequential order from top to bottom.

```yaml
- sql: |
    CREATE TABLE xxx (a INTEGER, b INTEGER);
    INSERT INTO xxx VALUES (0, 0), (1, 1), (2, 2);
  tasks:
    - execute
  desc: Database Setup
- sql: |
    SELECT * FROM xxx WHERE a = 0;
  tasks:
    - execute
    - explain:logical_optd,physical_optd
  desc: Equality predicate
```
| Name       | Description                                                        |
| ---------- | ------------------------------------------------------------------ |
| `sql`      | List of SQL statements to execute separate by newlines             |
| `tasks`    | How to execute the SQL statements. See [Tasks](#tasks) below       |
| `desc`     | (Optional) Text description of what the test cases represents      |

After adding the YAML file, you then need to use the update command to automatically create the matching SQL file that contains the expected output of the test cases.

## Regenerate Test Case Output

If you change the output of optd's `EXPLAIN` syntax, then you need to update all of the expected output files for each test cases.
The following commands will automatically update all of them for you. You should try to avoid this doing this often since if you introduce a bug, all the outputs will get overwritten and the tests will report false negatives.

```shell
# Update all test cases
cargo run -p optd-sqlplannertest --bin planner_test_apply
# or, supply a list of directories to update
cargo run -p optd-sqlplannertest --bin planner_test_apply -- subqueries
```

## Tasks

The `explain` and `execute` task will be run with datafusion's logical optimizer disabled. Each task has some toggleable flags to control its behavior.

### `execute` Task

#### Flags

| Name             | Description                           |
| ---------------- | ------------------------------------- |
| `use_df_logical` | Enable Datafusion's logical optimizer |

### Explain Task

#### Flags

| Name             | Description                                                        |
| ---------------- | ------------------------------------------------------------------ |
| `use_df_logical` | Enable Datafusion's logical optimizer                              |
| `verbose`        | Display estimated cost in physical plan                            |
| `logical_rules`  | Only enable these logical rules (also disable heuristic optimizer) |

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
RUST_BACKTRACE=1 RUST_LOG=optd_core=trace,optd_datafusion_bridge=trace cargo run -p optd-sqlplannertest --bin planner_test_apply -- pushdowns &> log
```
