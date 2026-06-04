# JOB Join Predicate Placement

This tracks the `InnerJoin true` shapes visible in the JOB `explain_flat` SLT
plans, especially `optd/connectors/datafusion/tests/slt/job/explain_flat/3c.slt`.

## Observation

JOB comma joins import as nested `CrossProduct` operators under one
`Selection`. `PredicatePushdown` already splits top-level `AND` predicates and
routes the pieces it can move.

In Q3c, the single-table filters can move early:

- `k.keyword LIKE '%sequel%'`
- `mi.info IN (...)`
- `t.production_year > 1990`

Some join predicates cannot move at the same point. For example, the local
`k × mi` subtree does not contain `mk`, so predicates like
`k.id = mk.keyword_id` are not attachable there. They only become valid once the
larger subtree contains all referenced columns.

## Previous Behavior

When `PredicatePushdown` moves only single-table filters below a `CrossProduct`,
it used to rebuild that node as `InnerJoin true`. Later `JoinOrdering` then saw
a real join edge with predicate `true`, not a dummy cross-product edge, so the
final plan could keep the odd-looking `InnerJoin true` shape.

This is not just a missed pushdown. The missing join predicate may be
unavailable at that binary join.

## Design Direction

Prefer sharing the hypergraph predicate splitting/classification between:

- [x] `JoinTreeNormalize`, a deterministic initial join-tree pass.
- [x] `JoinOrdering`, the existing cost-based join-ordering pass.

`JoinTreeNormalize` should build a predicate-aware tree from a join group,
attach each predicate at the earliest subtree where all referenced relations are
available, and preserve `CrossProduct` for truly disconnected joins.

All predicates should be split into conjuncts before classification. This
applies both to filter predicates and existing join `ON` predicates; otherwise a
wide `ON` condition can force independent join edges to wait for unrelated
relations.

`PredicatePushdown` should stay local: push single-input filters and predicates
that are valid for the current binary join, but not own global multi-relation
predicate placement.

## Implementation Steps

- [x] Small cleanup: if no join predicate was pushed into a `CrossProduct`, keep
  it as `CrossProduct` instead of rewriting it to `InnerJoin true`.
- [x] Larger transformation: add `JoinTreeNormalize` before cost-based
  `JoinOrdering`.
- [x] Reuse the hypergraph builder for both passes, rather than duplicating
  predicate/relation classification.
- [x] Split join `ON` conjuncts before hypergraph endpoint classification.
- [x] Preserve transient column-equality edges in `JoinTreeNormalize`; they may
  be redundant logically, but removing them can change nested join execution in
  the DataFusion backend.
