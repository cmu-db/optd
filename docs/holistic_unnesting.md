# Holistic Unnesting

## Summary

`HolisticUnnesting` is the new default optd decorrelation pass for subqueries that
`SubqueryToJoin` lowers into subquery-derived joins. It replaces the old default
`Unnesting` slot in the DataFusion runner while keeping the legacy pass compiled
and exported for comparison.

The design follows the top-down approach from Thomas Neumann, "Improving
Unnesting of Complex Queries" (BTW 2025), adapted to optd's append-only IR. The
v1 pass processes dependent join sites outermost-first, keeps the proven
predicate-lifting rewrite surface, and uses a small amount of state for column
equivalence and replacement. The full paper's domain-based fallback remains
future work; v1 does not build a real domain join.

## Current IR Model

optd does not have an explicit dependent-join operator. A subquery becomes a
regular `Join` whose inner side may contain free columns:

- `EXISTS` and direct positive `IN` predicates become `LeftSemi` joins.
- `NOT EXISTS` becomes `LeftAnti`; `NOT IN` only becomes `LeftAnti` when both
  inputs are proven non-null. Nullable `NOT IN` is left as a subquery because a
  plain anti join cannot represent SQL `UNKNOWN`.
- Scalar subqueries become `Single` joins.
- `EXISTS` subqueries inside larger scalar expressions become `LeftMark` joins
  by producer invariant. Expression-context `IN`/`NOT IN` is kept as a subquery
  until optd has a tri-valued marker representation.

`FreeColumns` identifies inner-side references that are not produced by that
inner subtree. A subquery-derived join is non-trivially correlated when
`AvailableColumns(outer) intersects FreeColumns(inner or on)`.

## Adapted Algorithm

The pass starts by collecting `DependentJoinSite` records in top-down order.
Each site stores the join handle, join type, outer and inner inputs, join
condition, outer references, and depth. `AccessIndex` then records operators
inside each inner subtree whose `FreeColumns` touch those outer references.

For v1, executable rewrites are applied one reachable site at a time. After each
successful rewrite, the pass installs the new root and clears cached analyses so
the next site is discovered against the rebuilt append-only graph. The main
rewrite lifts correlated conjuncts from linear inner operators into the parent
join condition; scalar aggregate inners expose the lifted inner key as an
aggregation group key; and `Single` aggregate joins become left outer joins
where needed to preserve scalar-subquery empty-input semantics.

The pass uses these local state objects:

- `UnnestingState`: local recursion state with equivalence classes and column
  replacements.
- `ColumnEqClasses`: union-find over column handles, populated from equality
  predicates.
- `ColumnRewrite`: append-only expression cloning that replaces `ColumnRef`
  handles according to the current replacement map.

The planned domain `D` would be represented as an `Aggregation` with
`outer_refs` as keys and no aggregate expressions over the outer input. This
would be optd's stand-in for duplicate elimination because the IR has no
dedicated `Distinct` operator. v1 leaves shapes requiring `D` unchanged.

Generic input rebuilding is centralized in `OperatorData::map_inputs` and
`OperatorData::try_map_inputs`. Passes can use those helpers for structural
traversal while keeping semantic rewrites local to the pass.

## Substitution Vs Domain

The full algorithm can eliminate an outer reference either by substituting an
equivalent inner column or by introducing the domain `D`.

Substitution is preferred when equality predicates establish that every outer
reference has an equivalent column already produced by the inner subtree, or one
that can be exposed without changing semantics. This keeps the rewrite local:
the pass reuses the inner representative, avoids building a distinct outer-key
domain, and avoids an extra join.

Domain introduction is needed when there is no safe inner representative, for
example non-equality correlations, references hidden behind operators that
cannot expose the column safely, or nested correlations that need a stable set of
outer bindings. `D` can also be cheaper when many outer rows share few distinct
correlation keys because inner work can be evaluated per key instead of per row.
It can be more expensive when the outer references are high-cardinality because
the domain is nearly as large as the outer input and adds aggregation/join work.

v1 uses deterministic substitution-first behavior where applicable. A future
cost-based rule should compare the extra aggregation and join cost of `D` with
the cardinality and exposure cost of substitution.

## Supported V1 Shapes

`HolisticUnnesting` supports these v1 production shapes:

- correlated `EXISTS` and `NOT EXISTS` after lowering to semi/anti joins;
- correlated positive `IN` forms whose correlation is a liftable conjunction;
- correlated `NOT IN` forms only when `SubqueryToJoin` can prove both the left
  expression and subquery output are non-null;
- scalar aggregate subqueries, including scalar `COUNT` defaulting to `0` after
  the decorrelated left outer join in projections and predicates;
- conjunction predicates below linear wrappers;
- projections, maps, renames, selections, sorts, cross products, and ordinary
  inner joins when the correlation predicate can be moved safely.

The pass intentionally does not recurse through null-supplying or
cardinality-changing join boundaries (`LeftOuter`, `RightOuter`, `FullOuter`,
`LeftSemi`, `LeftAnti`, `LeftMark`) until those operators have operator-specific
decorrelation rules. Nested `Single` joins may recurse for ordinary correlated
predicates needed by scalar aggregate decorrelation, but outer-only predicates
are not lifted through scalar joins. Outer-only predicates are lifted only for
EXISTS-style semi/mark joins; scalar and anti shapes remain unchanged.

The verification invariant is: after a successful rewrite, a subquery-derived
join's inner input must no longer contain free columns supplied by that join's
outer input.

Domain/replacement joins use optd's `BinaryOp::IsNotDistinctFrom` so correlated
NULL bindings are compared with SQL null-safe equality rather than ordinary
`=`. The DataFusion physical bridge maps all-null-safe equijoin keys to
`NullEqualsNull`; mixed ordinary/null-safe key sets fall back to filtered joins
instead of changing semantics.

## Deferred TODOs

- Implement CTE/DAG shared-subtree splitting from the paper to avoid duplicating
  domain scans through shared reads.
- Add recursive SQL unnesting support.
- Add `ORDER BY ... LIMIT/OFFSET` decorrelation via window functions; optd first
  needs a relational window operator.
- Handle correlated subqueries inside full outer join conditions.
- Add broader tri-valued marker support so expression-context `IN`/`NOT IN` and
  nullable `NOT IN` can be decorrelated without losing SQL `UNKNOWN`.
- Implement cost-based selection between substituting equivalent columns and
  introducing a domain join; v1 uses deterministic simple rewrites.
- Add a GroupJoin-native representation if optd grows one; v1 uses existing
  joins and aggregations.
- Continue the null-semantics audit for row-valued `IN`, `NOT IN`, and imported
  generic mark joins beyond the DataFusion-observed scalar shapes.

## Test Strategy

Core tests should cover:

- top-down dependent site discovery;
- `ColumnEqClasses` union/find behavior;
- `ColumnRewrite` expression cloning;
- semi/anti decorrelation by predicate lifting;
- scalar aggregate decorrelation and grouping-key exposure;
- scalar `COUNT` decorrelation with `0` for empty correlated matches;
- scalar `COUNT` defaulting when the scalar result is used by a filter;
- unchanged behavior for unsupported `Limit` shapes;
- unchanged behavior for unsafe join boundaries and unsupported outer-only
  predicate shapes;
- focused SQL logic tests for correlated subqueries through the DataFusion
  connector, including `explain_steps` assertions that the `HolisticUnnesting`
  pass changed supported shapes and left deferred shapes unchanged.

The default runner test should assert that `HolisticUnnesting` runs immediately
after `ExprSimplify`.
