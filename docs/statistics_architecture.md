# Statistics architecture

## Scope

Logical facts and cardinality estimates have separate responsibilities. Logical facts capture
value lineage, equality classes, nullability, and constraints; cardinality estimation consumes
those facts together with catalog statistics and explicit fallback policy.

## Separation of concerns

`LogicalFactsAnalysis` is demand-driven and cached with other analyses. It does not choose row
counts or selectivities. `CardinalityEstimationV1` owns row, NDV, and selectivity estimates.

## Logical caching

Facts are derived from immutable operator payloads and may be reused while the query context is
append-only. Contradictions are represented explicitly so consumers can safely produce an empty
profile.

## Join and outer-join facts

Ordinary equality can establish value equivalence and non-nullness. Outer joins retain only facts
that remain valid after null extension; their predicates do not become unconditional output facts.

## Filtered NDV propagation

Selections distinguish row thinning from restrictions to a known value domain. Join equivalence
classes cap the retained distinct-value estimates without conflating estimator policy and logical
proof.

## Non-sketch extension points

Future statistics providers can refine catalog fallback estimates through explicit estimator
interfaces without changing logical-fact propagation.
