# optd

Query Optimizer Service.

## 15-745 Project Specific

To run the example for benchmark,

```bash
cargo run -p optd-cli --release -- -f bench/tpch-sf0_01/test.sql
```


## Get Started

To interact with the CLI, run

```bash
cargo run -p optd-cli
```

## Structure

- `optd/core`: The core optimizer implementation (IR, properties, rules, cost model, cardinality estimation).
- `optd/catalog`: A persistent catalog implementation.
- `connectors/datafusion`: Utilities needed to use optd in DataFusion.
- `cli`: command line interface based on [`datafusion-cli`](https://datafusion.apache.org/user-guide/cli/index.html).