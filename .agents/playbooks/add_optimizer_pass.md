# Add Optimizer Pass

1. Create `optd/core/src/optimize/my_pass.rs`.
2. Implement `Pass` and either `OperatorRewrite` or `QueryPass`.
3. Return `Rewrite::Keep` when nothing changed; unconditional replacement can
   cause `MaxIterationsReached`.
4. Export the pass from `optd/core/src/optimize/mod.rs` and `optd/core/src/lib.rs`.
5. Register it in `optd/connectors/datafusion/src/runner.rs`.
6. Add focused core tests, and add SLT coverage if SQL-visible behavior changes.

Run:

```sh
cargo test -p optd-core
cargo nextest run --release -p optd-datafusion --test slt
```
