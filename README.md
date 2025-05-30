# 🎯 optd

[![codecov](https://codecov.io/gh/cmu-db/optd/graph/badge.svg?token=FYM7I3R3GZ)](https://codecov.io/gh/cmu-db/optd)

optd is a high-performance, extensible optimizer-as-a-service, built to support research in cardinality estimation, adaptive planning, AI-driven optimization, and parallelism. It serves as both a prototype system and a foundation for building production-ready optimizers.

## Core Features

**🔍 Flexible Search Strategy**: Unlike traditional recursive sub-plan optimizers, optd supports broader, non-recursive search spaces for faster and better plan discovery.

**⚡ Parallelism**:
- *Inter-query*: Optimize multiple queries in parallel while sharing computation
- *Intra-query*: Explore a single plan's search space using many threads

**💾 Persistent Memoization**: The optimizer acts like a database—plans and statistics are stored and reused, enabling adaptivity through feedback from prior executions.

**📝 Rule DSL**: Define transformation rules in a high-level, expressive DSL. Our rule engine is Turing complete, enabling compact definitions of complex transformations like join order enumeration.

**Example Data Type Definition**:
```
data Logical = 
    | Join(left: Logical, right: Logical, type: JoinType, predicate: Scalar)
    | Filter(child: Logical, predicate: Scalar)
    | Project(child: Logical, expressions: [Scalar])
    | Sort(child: Logical, order_by: [Bool])
    \ Get(table_name: String)
```

**Example Transformation Rule**:
```
[transformation]
fn (expr: Logical*) join_commute(): Logical? = match expr
    | Join(left, right, Inner, predicate) ->
        let 
            left_props = left.properties(),
            right_props = right.properties(),
            left_len = left_props#schema#columns.len(),
            right_len = right_props#schema#columns.len(),
            
            right_indices = 0..right_len,
            left_indices = 0..left_len,
            
            remapping = (left_indices.map((i: I64) -> (i, i + right_len)) ++ 
                right_indices.map((i: I64) -> (i + left_len, i))).to_map(),
        in
            Project(
                Join(right, left, Inner, predicate.remap(remapping)),
                (right_indices ++ left_indices).map((i: I64) -> ColumnRef(i))
            )
    \ _ -> none
```

**🔧 Pluggable Scheduling**: Apply rules using customizable scheduling strategies—from heuristics to AI-guided decisions.

**🔍 Explainability**: Track rule application history for better debugging and plan introspection.

**🔌 Extensibility**: Define custom operators and inherit existing rules. Designed to integrate with standards like Substrait, with a smoother UX than systems like Calcite.

## 🛠️ Usage

optd is currently under development. The costing mechanism is still being implemented, but there is a small demo available. The DSL tooling is more mature.

### Running the Demo

```bash
# Run the demo test (located in optd/src/demo/mod.rs)
cargo test test_optimizer_demo -- --nocapture
```

### CLI Tool

```bash
# Compile a DSL file
cargo run --bin optd-cli -- compile path/to/file.opt

# Compile with verbose output and show intermediate representations
cargo run --bin optd-cli -- compile path/to/file.opt --verbose --show-ast --show-hir

# Compile with mock UDFs for testing
cargo run --bin optd-cli -- compile path/to/file.opt --mock-udfs map get_table_schema properties statistics optimize

# Run functions marked with [run] annotation
cargo run --bin optd-cli -- run-functions path/to/file.opt
```
