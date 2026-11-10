# Sampling Cardinality Estimation Design

## Goal

Add catalog-backed table samples and use them for Umbra-style sample-driven
cardinality estimation, then LpBound pessimistic bounds — delivered as three
stacked branches.

## Lifecycle

1. Catalog stores past table samples (`TableSample`) collected offline or in a
   prior run. For now, tests reservoir-sample `N` rows from TPC-H/JOB Parquet
   (or synthetic batches) and install them via the catalog.
2. `CardinalityEstimationV1` prefers sample-based estimation for any operator
   subtree whose leaf scans all have samples; otherwise it keeps the existing
   catalog/sketch path for that subtree.
3. Point estimates come from evaluating operators over samples (scaled by
   `source_rows / sample_rows`). LpBound fills `Estimate::{lower, upper}` only.

## Stack

| Branch | Scope |
| --- | --- |
| `ac/collect-samples` | `optd-sampling` crate (`TableSample` wrapper + reservoir helper); catalog `table_samples` / `set_table_samples`; fixture helpers. No DataFusion scan wiring. |
| `ac/sampling-estimator` | Evaluate filters/joins/etc. on samples via Arrow APIs inside `optd-sampling`; V1 hooks when a subtree is fully sampled. |
| `ac/lpbound` | Use the sampling estimator to compute LpBound pessimistic bounds into existing estimate endpoints. |

## Crate layout

- `optd/sampling` (`optd-sampling`): sample types, reservoir sampling, and (later)
  operator evaluation over Arrow batches.
- `optd-core` depends on `optd-sampling` for catalog APIs, mirroring
  `optd-sketches`.
- Samples are **not** embedded in `TableStatistics`; they are a parallel catalog
  payload so summary stats stay independent of raw batches.

## `TableSample`

Wrapper around Arrow batches with explicit metadata:

- `schema: SchemaRef`
- `batches: Vec<RecordBatch>`
- `block_size: usize` — target reservoir capacity / rows retained
- `source_rows: usize` — table population at collection time
- `provenance: SampleProvenance` — `Past` for now (current-run left open)

## Catalog API

```rust
fn table_samples(&self, table: &TableRef) -> CatalogResult<Option<TableSample>>;
fn set_table_samples(&self, table: TableRef, samples: TableSample) -> CatalogResult<()>;
```

`MemoryCatalog` stores samples on `TableMetadata`. Schema of the sample must
match the table schema (validated on set).

## Estimator policy (branches 2–3)

- Sample path applies when every scan under the operator has samples.
- Mixed subtrees keep classic V1 math.
- Past samples are used (not samples from the current query execution).
- LpBound updates bounds; costing continues to use the sample point estimate.
- Inner equijoin upper bounds use the max-degree LpBound formula
  `min(|L|·maxdeg(R), |R|·maxdeg(L), |L|·|R|)` with degrees scaled from samples.
- Filters inherit the input upper bound and use a zero lower bound; scans use
  exact `source_rows` for both endpoints.

## Non-goals (this stack)

- Runtime reservoir collection during DataFusion scans
- Choosing current-run vs past samples
- Replacing V1 as a separate analysis type
