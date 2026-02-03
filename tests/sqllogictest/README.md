# optd-sqlplannertest

These test cases use the [sqlplannertest](https://crates.io/crates/sqlplannertest) crate to execute SQL queries and inspect their output.
They do not check whether a plan is correct, and instead rely on a text-based diff of the query's output to determine whether something is different than the expected output.


## Execute Test Cases

**Running all test cases**

```shell
cargo test -p optd-sqlplannertest
# or use nextest
cargo nextest run -p optd-sqlplannertest
```

**Running tests in specfic modules or files**

```shell
# Running all test cases in the tpch module
cargo nextest run -p optd-sqlplannertest tpch
# Running all test cases in the tests/tpch/q1.yml
cargo nextest run -p optd-sqlplannertest tpch::q1
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
    - explain
  desc: Equality predicate
```
| Name    | Description                                                   |
| ------- | ------------------------------------------------------------- |
| `sql`   | List of SQL statements to execute separate by newlines        |
| `tasks` | How to execute the SQL statements. See [Tasks](#tasks) below  |
| `desc`  | (Optional) Text description of what the test cases represents |

After adding the YAML file, you then need to use the update command to automatically create the matching SQL file that contains the expected output of the test cases.

## Regenerate Test Case Output

If you change the output of optd's `EXPLAIN` syntax, then you need to update all of the expected output files for each test cases.
The following commands will automatically update all of them for you. You should try to avoid this doing this often since if you introduce a bug, all the outputs will get overwritten and the tests will report false negatives.

```shell
# Update all test cases
cargo run --bin planner_test_apply
# or, supply a list of modules or files to update
cargo run --bin planner_test_apply -- tpch::q1 basics::basic
```
