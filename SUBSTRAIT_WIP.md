# Substrait Integration WIP

This document is a handoff for the current Substrait work in `simple-graph`.

## Current State

Main implementation lives in [`src/substrait.rs`](src/substrait.rs). Public entry points:

- `substrait::from_plan(&Plan) -> Result<QueryContext, SubstraitError>`
- `substrait::from_plan_with_catalog(&Plan, Arc<dyn Catalog>) -> Result<QueryContext, SubstraitError>`
- `substrait::from_rel(&Rel) -> Result<QueryContext, SubstraitError>`
- `substrait::from_rel_with_catalog(&Rel, Arc<dyn Catalog>) -> Result<QueryContext, SubstraitError>`
- `substrait::to_plan(&QueryContext) -> Result<Plan, SubstraitError>`

The importer is substantially ahead of the exporter. It supports enough Substrait to consume a real DataFusion plan for:

```sql
SELECT id FROM users WHERE age >= 18 ORDER BY id DESC LIMIT 5
```

The exporter now handles:

- `Output`
- `Projection`
- `Selection`
- `Sort`
- `Limit`
- `Join`
- named-table `Scan`
- root output names
- basic scalar expression export (`ColumnRef`, `Literal`, `BinaryOp`, `UnaryOp`, `NaryOp`)
- scalar function anchors via `Plan.extensions`
- basic Arrow types: `Boolean`, `Int32`, `Int64`, `Float64`, `Utf8`

## Tested Interop

Integration tests are in [`tests/datafusion_substrait.rs`](tests/datafusion_substrait.rs).

Current tests cover both directions:

- DataFusion SQL -> Substrait protobuf bytes -> `simple_graph::substrait::from_plan`
- `simple_graph::substrait::to_plan` -> Substrait protobuf bytes -> DataFusion consumer

Run:

```bash
cargo test --test datafusion_substrait
cargo test
```

Important detail: DataFusion uses `substrait 0.62.x`, while this crate uses `substrait 0.63.x`. Interop tests cross the version boundary through protobuf bytes using `prost::Message`, not direct Rust type sharing.

## Importer Coverage

Importer-supported relation families include:

- `ReadRel` named tables, local files, filters, simple projections, and `emit`
- `FilterRel` -> `Selection`
- `ProjectRel` -> `Map`, plus `emit` -> `Projection`
- `JoinRel`, `HashJoinRel`, `MergeJoinRel`, `NestedLoopJoinRel`
- `CrossRel` -> `CrossProduct`
- `SortRel` -> `Sort`
- `FetchRel` -> `Limit`
- simple `AggregateRel` -> `Aggregation`

Expression import supports literals, column references, scalar functions, casts, common binary ops, `AND`/`OR`, and basic unary ops. Unsupported expression forms return explicit `SubstraitError` variants.

## Known Limitations

Exporter gaps remain:

- no export for `Map`
- no export for `CrossProduct`
- no export for `Aggregation`
- no export for `TableFunction`
- no export for complex scalar expressions beyond the basic set

Importer limitations to watch:

- aggregation grouping allows expression keys, but available-column analysis rejects expression aggregation keys for now
- `ColumnData` no longer stores nullability; nullability is analysis/catalog-derived
- Substrait type handling is still a conservative subset
- local file import maps file formats to `TableFunctionDef`, but exporter does not emit local files

## Recommended Next Steps

1. Export cross product.
   Emit `CrossRel` from `CrossProduct` preserving `outer` -> left and `inner` -> right mapping.

2. Export aggregation after expression and function export are stable.
   Need a decision on how to map `Aggregation.keys: Vec<Expr>` and `(Column, AggregateExpr)` into Substrait grouping expressions, measures, output names, and emit mappings.

## Design Notes

Keep the importer and exporter byte-compatible with DataFusion rather than type-compatible with DataFusion’s `substrait` crate version. Prefer adding narrow tests that roundtrip through DataFusion’s public Substrait APIs.

The existing converter builds `QueryContext` append-only and wraps imported roots in an explicit `Output` operator. The exporter expects callers to have set a root; if the root is `Output`, it exports the input relation and uses the output columns as `RelRoot.names`.
