# Analysis Framework

## Goal

Provide a uniform interface for demand-driven query analyses over the `optd` IR.
Analyses are invoked through a single `AnalysisContext::get` entry point and may use any
internal computation strategy — recursive bottom-up, top-down via `ParentOf`, whole-plan
traversal, or catalog lookups.

## Core Traits

### `Analyzable`

The umbrella trait. Every analysis implements this.

```rust
pub trait Analyzable: 'static {
    type Value: Clone;

    fn get(
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        op: Operator,
    ) -> AnalysisResult<Self::Value>;
}
```

`get` is the only method callers ever invoke. The analysis decides internally how to compute
and whether to cache.

### `OperatorAnalysis`

A subtrait for analyses that compute a value per operator. Provides `compute` as the
computation entry point. `get` is typically implemented by calling `compute` and optionally
memoizing the result.

```rust
pub trait OperatorAnalysis: Analyzable {
    fn compute(
        op: Operator,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
    ) -> AnalysisResult<Self::Value>;
}
```

### `AnalysisContext`

The single entry point for all analyses.

```rust
impl AnalysisContext {
    pub fn get<A: Analyzable>(
        &mut self,
        ctx: &QueryContext,
        op: Operator,
    ) -> AnalysisResult<A::Value>;
}
```

## Computation Strategies

### Bottom-up (synthesized)

The analysis calls `analyses.get` on its operator's inputs recursively. Results flow from
leaves to root. Memoization per operator handle is safe under the append-only invariant —
an operator's payload never changes, so its bottom-up value is stable forever.

Examples: `AvailableColumns`, `CreatedColumns`, `ColumnNullability`, `FreeColumns`.

### Top-down (inherited)

The analysis uses `analyses.get::<ParentOf>` to walk from the operator up to the root,
collecting the ancestor chain, then folds the inherited context downward along that path.

```rust
fn compute(op, ctx, analyses) -> AnalysisResult<Self::Value> {
    let mut path = vec![op];
    let mut cursor = op;
    while let Some(parent) = analyses.get::<ParentOf>(ctx, cursor)? {
        path.push(parent);
        cursor = parent;
    }
    path.reverse(); // root first
    let mut value = Self::root_value(path[0], ctx);
    for ancestor in &path[1..] {
        value = Self::transfer(*ancestor, &value, ctx, analyses);
    }
    Ok(value)
}
```

Cost is O(depth) per call. If many operators need the value, the analysis can cache the
full map on first call.

Example: `DemandedColumns` (future).

### `ParentOf` analysis

`ParentOf` is a first-class analysis that returns the immediate parent of an operator in
the current reachable plan. It builds and caches a full parent map on first call.

```rust
analyses.get::<ParentOf>(ctx, op) -> AnalysisResult<Option<Operator>>
```

Returns `None` for the root. Returns `Some(parent)` for all other reachable operators.

The parent map is keyed on the root operator at time of computation. If the root changes
(due to a rewrite), the cache is stale and must be cleared via `analyses.clear()`.

## Caching Policy

Each analysis owns its caching. The framework does not mandate or provide a cache.

- Bottom-up analyses typically memoize per operator handle using `OperatorAnalysisState`.
- Top-down analyses may cache a full `HashMap<Operator, Value>` keyed on the root.
- Analyses that are cheap to recompute may skip caching entirely.

Under the append-only invariant, bottom-up cache entries for existing handles are always
valid. Top-down cache entries are valid until the reachable root changes.

## Future: Indexed Representation

For large plans with many incremental rewrites (e.g. Cascades exploration), a link-cut
tree over the reachable operator tree would support O(log n) parent, ancestor, and path
queries with O(log n) edge updates as rewrites redirect inputs. This is out of scope for
the current heuristic-rewrite optimizer but is the right data structure for a full
cost-based planner.
