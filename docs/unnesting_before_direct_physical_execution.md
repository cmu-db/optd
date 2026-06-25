# Unnesting For Direct Physical Execution

## Summary

The DataFusion connector now uses direct optd-to-DataFusion physical planning
by default. The old logical DataFusion conversion path remains available for
debugging with the `--logical` CLI/SLT opt-out.

The immediate Phase 1 goal is to decorrelate all TPC-H correlated subqueries in optd IR. The `FreeColumns` analysis is the main verification mechanism: after unnesting, subquery-derived join inputs should no longer contain outer references.

## Current Execution Path

The active optd-enabled runner path is:

```text
SQL
-> DataFusion logical plan
-> required DataFusion analysis/type coercion
-> from_df_logical
-> optd optimization
-> to_df_physical
-> DataFusion physical execution
```

`to_df_logical` is retained for tests, debugging, and compatibility. Physical
conversion failures should be loud rather than silently falling back to the
logical path.

## Phase 1: TPC-H Unnesting

The default pipeline now uses `HolisticUnnesting` in this pass order:

```text
SubqueryToJoin
ExprSimplify
HolisticUnnesting
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
- In the direct physical path, reject any join input whose `FreeColumns` are not
  satisfied by that input's own schema.

Unsupported non-TPC-H correlated shapes may continue through the logical
compatibility path when explicitly run with `--logical`.

## Direct Physical Execution Follow-Up

After Phase 1 decorrelates all TPC-H correlated queries:

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

For the current direct physical path:

- Keep focused `--logical` coverage for the compatibility path.
- Keep `to_df_physical` converter-level coverage.
- Test that the optimizer pipeline includes `HolisticUnnesting` after `ExprSimplify`.
- Test that `HolisticUnnesting` lifts correlated semi-join predicates and scalar aggregate predicates, then verify the rewritten inner input with `FreeColumns`.

For Phase 1 implementation:

- Add optimizer tests for every correlated TPC-H shape encountered.
- Add focused tests for scalar aggregate, `EXISTS`, `NOT EXISTS`, `IN`, and `NOT IN`.
- Add tests that verify rewritten inner inputs have no outer references according to `FreeColumns`.
- Add negative tests for unsupported non-TPC-H correlated shapes.
- Add TPC-H SLT coverage proving all correlated TPC-H queries still produce expected results.

For physical execution:

- Add tests proving optd-enabled SQL uses `to_df_physical`.
- Add tests proving DataFusion logical optimization is no longer used after optd optimization.
- Add strict-path TPC-H coverage for all queries affected by unnesting.
