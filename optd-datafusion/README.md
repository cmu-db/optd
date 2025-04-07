# Demo

To run the demo, execute the following command:

```sh
$ cargo run --example -p optd-datafusion --example demo -- <path_to_sql>
$ cargo run --example -p optd-datafusion --example demo -- optd-datafusion/sql/test_join.sql
```

# Tests

To run the tests, simply run:

```sh
$ cargo test --test run_queries
```
