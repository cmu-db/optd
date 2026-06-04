# Optimizer

## Goal

Build an optimization framework for `optd` that matches the existing IR model:
operators, expressions, and columns are arena-allocated handles owned by `QueryContext`.

The optimizer should favor append-only rewrites. A pass creates replacement nodes and redirects
parents or the query root to those replacements. Old nodes remain valid but may become unreachable.

## Core Invariant

Optimization passes should treat existing IR handles as immutable.

- Do not mutate existing `OperatorData`, `ExprData`, or `ColumnData` during normal optimization.
- Add replacement operators and expressions with `QueryContext::add_operator` and
  `QueryContext::add_expr`.
- Update the reachable plan through rewrite maps and `QueryContext::set_root`.
- Analysis cache entries for old handles remain correct because the payload behind each handle does
  not change.

This means the first optimizer interface does not need LLVM-style preserved-analysis metadata.
New handles simply compute analysis results on demand.

## Pass Interface

Passes should report whether they changed the reachable plan.

```rust
pub type OptimizeResult<T> = Result<T, OptimizeError>;

pub trait Pass {
    fn name(&self) -> &'static str;
}

pub trait QueryPass: Pass {
    fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<PassResult>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassResult {
    Unchanged,
    Changed,
}
```

`QueryPass` is the primitive pass type stored by the pass manager. Smaller pass types can be adapted
into query passes:

- `OperatorRewrite`: local relational rewrite over one operator and its rewritten inputs.
- `ExprRewrite`: local scalar-expression rewrite over one expression tree.
- Cascades/memo optimization: whole-query pass that owns its own exploration state.

Do not add more pass levels until the IR has a concrete need for them.

## Operator Rewrite Interface

```rust
pub trait OperatorRewrite: Pass {
    fn direction(&self) -> Direction {
        Direction::BottomUp
    }

    fn rewrite(
        &mut self,
        operator: Operator,
        ctx: &mut OptimizerContext,
    ) -> OptimizeResult<Rewrite>;
}

pub enum Direction {
    BottomUp,
    TopDown,
}

pub enum Rewrite {
    Keep,
    Replace(Operator),
}
```

`OperatorRewriteAdaptor` now owns traversal, rewrite-map updates, and parent/root
materialization. Rules pattern-match one operator and return `Keep`/`Replace`.

## Expression Rewrite Interface

Expression rewrites should be a separate adaptor because scalar expression traversal and relational
operator traversal have different concerns.

```rust
pub trait ExprRewrite: Pass {
    fn rewrite_expr(
        &mut self,
        expr: Expr,
        ctx: &mut OptimizerContext,
    ) -> OptimizeResult<ExprRewriteResult>;
}

pub enum ExprRewriteResult {
    Keep,
    Replace(Expr),
}
```

An expression rewrite adaptor walks expression fields inside reachable operators. If any expression
field changes, the adaptor appends a replacement operator and updates the rewrite map.

## Rewrite Map

The pass manager or traversal adaptor should maintain replacement state.

```rust
pub struct RewriteMap {
    replacements: HashMap<Operator, Operator>,
}

impl RewriteMap {
    pub fn replace(&mut self, old: Operator, new: Operator);
    pub fn resolve(&self, operator: Operator) -> Operator;
}
```

`resolve` should follow chains so a later rewrite sees the latest replacement for each operator.

## Pass Manager

The pass manager runs query passes in order and repeats while passes keep changing the plan.

```rust
pub struct PassManager {
    passes: Vec<Box<dyn QueryPass>>,
    max_iterations: usize,
}
```

Initial behavior:

- Read `ctx.query.root()`.
- Run each pass in registration order.
- Let passes or adaptors update the root when they replace it.
- Stop when a full iteration reports no changes.
- Return an error if `max_iterations` is reached.

No analysis invalidation is needed under the append-only invariant.

## Run Tracking and Scheduling

Append-only rewrites avoid stale analysis caches, but they do not automatically avoid repeated rule
work. A fixpoint pass manager can keep seeing equivalent replacement shapes unless the adaptor tracks
what has already been attempted.

The first implementation can be conservative and simple:

- Within one pass invocation, visit each reachable operator handle once.
- Across fixpoint iterations, allow revisiting new handles because new surrounding context may make
  a rule applicable.
- Use `max_iterations` to prevent accidental non-termination.

Potential later improvements:

- Track `(pass_name, operator)` attempts inside a single optimizer run.
- Track `(pass_name, structural_fingerprint)` attempts to avoid retrying the same shape after it is
  rebuilt with fresh handles.
- Let a rewrite rule return a scheduling hint when retrying a replacement would be useless.

Structural fingerprinting belongs in optimizer infrastructure, not `QueryContext`. `QueryContext`
should keep returning fresh handles from `add_operator` and `add_expr`.

## Traversal, Scope, and Boundaries

The first traversal adaptor should support bottom-up relational traversal from the query root.

It should:

- Visit each reachable operator once per pass.
- Rewrite children before parents.
- Rebuild parent operators when any child input changes.
- Update the root if the original root is replaced.

Subqueries embedded in expressions should be opt-in. They are reachable plan fragments, but some
rules may only be valid for the main relational tree.

```rust
pub enum RewriteScope {
    MainQueryOnly,
    IncludeSubqueries,
}
```

Scope only answers which fragments are traversed. It does not answer whether a rewrite may cross a
semantic boundary. CTEs, shared subplans, materialization points, and recursive queries will likely
need a separate boundary policy later.

```rust
pub struct RewriteOptions {
    pub scope: RewriteScope,
    pub boundaries: BoundaryPolicy,
}

pub enum BoundaryPolicy {
    RespectBarriers,
    Transparent,
}
```

For example, pushing a filter into a shared CTE producer may be invalid if other consumers do not
have the same filter. With append-only IR, the valid rewrite is usually to create a filtered
per-consumer alternative while leaving the shared producer unchanged.

## Cascades Direction

Cascades can be implemented as a `QueryPass` that builds an optimizer-owned memo over the reachable
`QueryContext` plan. `QueryContext` remains the source and target IR; the memo owns equivalence
classes and exploration indexes.

```rust
pub struct Memo {
    classes: Vec<EquivalenceClass>,
    exprs: Vec<MemoExpr>,
    by_operator: HashMap<Operator, MemoExprId>,
    by_fingerprint: HashMap<MemoFingerprint, MemoExprId>,
}

pub struct EquivalenceClass {
    members: Vec<MemoExprId>,
}

pub struct MemoExpr {
    operator: Operator,
    inputs: Vec<EquivalenceClassId>,
}
```

An equivalence class is a set of logically equivalent expressions. A memo expression points to child
equivalence classes, not concrete child operators. That distinction is what lets Cascades represent
alternatives cleanly.

Deduplication should live in the memo, not in `QueryContext`. `QueryContext::add_operator` and
`QueryContext::add_expr` should keep appending fresh handles.

## Initial Passes

Good first rules:

- `EliminateIdentityProjection`: remove projections that preserve input column order exactly.
- `MergeSelections`: combine adjacent selections with `AND`.
- `MergeLimits`: combine adjacent limits when fetch/offset semantics are straightforward.
- `PruneUnusedMapComputations`: remove computed columns that are not demanded by ancestors.

These should be deterministic heuristic rewrites. Cost-based planning can come later.

## Implementation Tasks

1. Add arena convenience methods.

   Add inherent methods on the arena payload types:

   ```rust
   impl OperatorData {
       pub fn add(self, ctx: &mut QueryContext) -> Operator {
           QueryContext::add_operator(ctx, self)
       }
   }

   impl ExprData {
       pub fn add(self, ctx: &mut QueryContext) -> Expr {
           QueryContext::add_expr(ctx, self)
       }
   }

   impl ColumnData {
       pub fn add(self, ctx: &mut QueryContext) -> Column {
           QueryContext::add_column(ctx, self)
       }
   }
   ```

   Keep the existing `ctx.add_operator`, `ctx.add_expr`, and `ctx.add_column` APIs. These methods
   are only ergonomic helpers for builders and optimizer rewrites. Do not use `intern` naming
   because `QueryContext` should not deduplicate payloads.

2. Add optimizer module skeleton.

   Create `optd/core/src/optimize.rs` or `optd/core/src/optimize/mod.rs` with `OptimizeError`, `OptimizeResult`,
   `Pass`, `QueryPass`, `PassResult`, and `PassManager`. Re-export stable public pieces from
   `optd/core/src/lib.rs`.

3. Implement append-only rewrite infrastructure.

   Add `RewriteMap`, root replacement handling, and helper functions for resolving replacement
   chains. The first version can be handle-based and local to one pass invocation.

4. Add traversal helpers.

   Provide bottom-up traversal from `QueryContext::root()` over relational inputs. Keep subquery
   traversal behind `RewriteScope::IncludeSubqueries`, even if the first implementation only uses
   `MainQueryOnly`.

5. Prototype operator rewrite adaptor.

   Implement an adaptor from a candidate `OperatorRewrite` interface into `QueryPass`. Treat this as
   experimental until input roles, shared subplans, and scheduling behavior are settled.

6. Add expression rewrite adaptor.

   Implement `ExprRewrite` separately from operator rewrites. The adaptor should rebuild expressions
   append-only and append replacement operators when operator expression fields change.

7. Add first deterministic cleanup passes.

   Start with narrow, easy-to-test rules:

   - `EliminateIdentityProjection`
   - `MergeSelections`
   - `MergeLimits`

   Add unit tests beside the optimizer implementation and integration tests when display or
   Substrait-visible behavior changes.

8. Add run tracking safeguards.

   Start with "visit each reachable operator once per pass invocation" and `max_iterations`.
   Consider structural fingerprints only after a real rewrite loop appears.

## Open Questions

- Should mutation APIs like `operator_mut` remain available to optimizer passes, or should pass
  implementations only receive append helpers?
- Should run tracking be handle-based, fingerprint-based, or both?
- How should expression-level rewrites compose with operator rewrites in one fixpoint pipeline?
- What boundary model should be used once CTEs or shared-subplan operators exist?
- Should unreachable arena nodes ever be compacted, or is append-only storage acceptable for all
  optimization workflows?
