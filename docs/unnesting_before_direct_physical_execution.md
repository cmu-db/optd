# Unnesting Before Direct Physical Execution

## Summary

The DataFusion connector should keep the generated direct optd-to-DataFusion physical planner code in-tree, but normal optd-enabled SQL execution should continue to use the logical converter until optd can unnest the correlated subquery shapes needed by TPC-H.

The immediate Phase 1 goal is to decorrelate all TPC-H correlated subqueries in optd IR. The `FreeColumns` analysis is the main verification mechanism: after unnesting, subquery-derived join inputs should no longer contain outer references.

## Current Execution Path

The active optd-enabled runner path is:

```text
SQL
-> DataFusion logical plan
-> required DataFusion analysis/type coercion
-> from_df_logical
-> optd optimization
-> to_df_logical
-> DataFusion logical/physical execution
```

`to_df_physical.rs` remains experimental scaffolding. It should compile and retain converter-level tests, but normal SQL execution should not call it until unnesting is ready. There is no silent fallback from physical planning to logical planning because physical planning is inactive in the runner for now.

## Phase 1: TPC-H Unnesting

Add and grow an optimizer pass named `Unnesting` in this pass order:

```text
SubqueryToJoin
ExprSimplify
Unnesting
MarkJoinToSemiJoin
PredicatePushdown
ProjectionElimination
JoinOrdering
```

Phase 1 mirrors DataFusion decorrelation behavior where practical, with the acceptance target that all correlated subqueries in TPC-H are decorrelated into optd IR that can later be converted directly to physical plans.

Support at minimum:

- TPC-H Q2-style correlated scalar aggregate subqueries.
- Correlated `EXISTS` lowered to `LeftSemi`.
- Correlated `NOT EXISTS` lowered to `LeftAnti`.
- Correlated `IN` and `NOT IN` forms used by TPC-H.
- Simple equality correlations between outer and inner columns.
- Correlated predicates inside conjunctions after `ExprSimplify`.

Use `FreeColumns` to verify the rewrite:

- Before unnesting, identify subquery-derived join inputs whose inner side has free columns from the outer side.
- After unnesting, assert that rewritten join inputs no longer contain free outer columns.
- For scalar aggregate subqueries, lift correlated conjuncts into the parent join, add inner correlation columns as aggregate grouping keys, and project those keys through wrapper operators as needed.
- If an exposed inner correlation key conflicts with an outer qualified field name, compute a fresh unqualified key and rewrite the lifted join predicate to use it.
- Add optimizer tests that inspect `FreeColumns` on rewritten plans, not only SQL results.
- When direct physical planning is re-enabled, reject any join input whose `FreeColumns` are not satisfied by that input's own schema.

Unsupported non-TPC-H correlated shapes may continue through the logical compatibility path while direct physical execution is inactive.

## Re-Enabling Direct Physical Execution

After Phase 1 decorrelates all TPC-H correlated queries:

- Re-enable a strict physical execution mode.
- Route optd-enabled execution through `to_df_physical`.
- Require the optimized optd plan to have no correlated join inputs before physical conversion, checked with `FreeColumns`.
- Keep `to_df_logical` available for tests, debugging, and compatibility.
- Ensure physical conversion failures are loud, not silent fallbacks.

## Phase 2 Future Work

Do not implement Phase 2 now.

Later, revisit the design using the TUM/Hyper research line, especially:

- `Unnesting Arbitrary Queries`
- `The Complete Story of Joins in Hyper`
- newer work on complex and deeply nested query unnesting

Future Phase 2 goals may include arbitrary correlated subquery support, principled `Single` and `Mark` join handling, deeper nesting, and a general optd-native unnesting framework.

## Testing Strategy

For the current compatibility phase:

- Keep SQL runner tests on the logical path.
- Keep `to_df_physical` tests as converter-only coverage.
- Test that the optimizer pipeline includes `Unnesting` after `ExprSimplify`.
- Test that `Unnesting` lifts correlated semi-join predicates and scalar aggregate predicates, then verify the rewritten inner input with `FreeColumns`.

For Phase 1 implementation:

- Add optimizer tests for every correlated TPC-H shape encountered.
- Add focused tests for scalar aggregate, `EXISTS`, `NOT EXISTS`, `IN`, and `NOT IN`.
- Add tests that verify rewritten inner inputs have no outer references according to `FreeColumns`.
- Add negative tests for unsupported non-TPC-H correlated shapes.
- Add TPC-H SLT coverage proving all correlated TPC-H queries still produce expected results.

When re-enabling physical execution:

- Add tests proving optd-enabled SQL uses `to_df_physical`.
- Add tests proving DataFusion logical optimization is no longer used after optd optimization.
- Add strict-path TPC-H coverage for all queries affected by unnesting.
