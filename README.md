# optd

optd (pronounced as op-dee) is a database optimizer framework. It is a cost-based optimizer that searches the plan space using the rules that the user defines and derives the optimal plan based on the cost model and the physical properties.

The primary objective of optd is to explore the potential challenges involved in effectively implementing a cost-based optimizer for real-world production usage. optd implements the Columbia Cascades optimizer framework based on [Yongwen Xu's master's thesis](https://15721.courses.cs.cmu.edu/spring2019/papers/22-optimizer1/xu-columbia-thesis1998.pdf). Besides cascades, optd also provides a heuristics optimizer implementation for testing purpose.

The other key objective is to implement a flexible optimizer framework which supports adaptive query optimization (aka. reoptimization) and adaptive query execution. optd executes a query, captures runtime information, and utilizes this data to guide subsequent plan space searches and cost model estimations. This progressive optimization approach ensures that queries are continuously improved, and allows the optimizer to explore a large plan space.

Currently, optd is integrated into Apache Arrow Datafusion as a physical optimizer. It receives the logical plan from Datafusion, implements various physical optimizations (e.g., determining the join order), and subsequently converts it back into the Datafusion physical plan for execution.

optd is a research project and is still evolving. It should not be used in production. The code is licensed under MIT.

## Get Started

There are two demos you can run with optd. More information available in the [docs](docs/).

```
cargo run --release --bin optd-adaptive-tpch-q8
cargo run --release --bin optd-adaptive-three-join
```

You can also run the Datafusion cli to interactively experiment with optd.

```
cargo run --bin datafusion-optd-cli
```

## Documentation

The documentation is available in the mdbook format in the [docs](docs) directory.

## Structure

* `datafusion-optd-cli`: The patched Apache Arrow Datafusion (version=32) cli that calls into optd.
* `datafusion-optd-bridge`: Implementation of Apache Arrow Datafusion query planner as a bridge between optd and Apache Arrow Datafusion.
* `optd-core`: The core framework of optd.
* `optd-datafusion-repr`: Representation of Apache Arrow Datafusion plan nodes in optd.
* `optd-adaptive-demo`: Demo of adaptive optimization capabilities of optd. More information available in the [docs](docs/).
* `optd-sqlplannertest`: Planner test of optd based on [risinglightdb/sqlplannertest-rs](https://github.com/risinglightdb/sqlplannertest-rs).
* `optd-gungnir`: Scalable, memory-efficient, and parallelizable statistical methods for cardinality estimation (e.g. TDigest, HyperLogLog).
* `optd-perftest`: A CLI program for testing performance (cardinality, throughput, etc.) against other databases.


# Related Works

* [datafusion-dolomite](https://github.com/datafusion-contrib/datafusion-dolomite)
