# Cost Model

Developers can plug their own cost models into optd. The cost must be represented as a vector of `f64`s, where the first element in the vector is the weighted cost. The optimizer will use weighted cost internally for cost comparison and select the winner for a group.

The cost model interface can be found in `optd-core/src/cost.rs`, and the core of the cost model is the cost computation process implemented in `CostModel::compute_cost`.

```rust
pub trait CostModel<T: RelNodeTyp>: 'static + Send + Sync {
    fn compute_cost(
            &self,
            node: &T,
            data: &Option<Value>,
            children: &[Cost],
            context: RelNodeContext,
    ) -> Cost;
}
```

`compute_cost` takes the cost of the children, the current plan node information, and some contexts of the current node. The context will be useful for adaptive optimization, and it contains the group ID and the expression ID of the current plan node, so that the adaptive cost model can use runtime information from the last run to compute the cost.

The optd Datafusion cost model stores 4 elements in the cost vector: weighted cost, row count, compute cost and I/O cost. The cost of the plan nodes and the SQL expressions can all be computed solely based on these information.

Contrary to other optimizer frameworks like Calcite, optd does not choose to implement the cost model as part of the plan node member functions. In optd, developers write all cost computation things in one file, so that testing and debugging the cost model all happens in one file (or in one `impl`).
