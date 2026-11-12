# Leaf Sample CE + Histogram Stack

## Goal

Match Umbra/Cedar: catalog table samples estimate **leaf predicates** only;
joins stay **Selinger + HLL/NDV** (existing V1). Then wire existing catalog
`MostCommonValues` + residual `Histogram` from full-scan collection.

## Locked decisions

- Rip sample-join CE from the hot path (no `sample ⨝ sample` for cardinality).
- Broad leaf predicate eval: `AND`/`OR`, eq, ordered compares, Date32, fold
  `date ± interval`, column–column compares, `LIKE`/`IN` when kernels exist;
  unsupported conjuncts fall back to classic V1 for those conjuncts only.
- Zero sample matches → estimate `0`; bounds carry uncertainty.
- Histograms: consume/collect into existing catalog types (no new ADT).
- Hist/MCV built from full column scans in the regression harness.

## Stack

| Branch | Scope |
| --- | --- |
| `ac/leaf-sample-ce` | Remove join-sample CE; leaf sample filter selectivity; joins unchanged |
| `ac/histogram-ce` | Full-scan MCV + residual equi-depth hist; V1 range/eq/overlap consumption |

## Branch 1 data flow

```
Scan  → catalog row estimate (unchanged); samples available in catalog
Sel   → split conjuncts
         ├─ sample-evaluable on leaf sample → (matches/n) * input_rows
         │    (0/n → 0); source Sample
         └─ else → classic V1 for that conjunct
Join  → existing V1 only (never sample join)
```

`try_sample_cardinality` must not estimate join trees. Prefer integrating sample
selectivity into selection estimation (per-conjunct) over whole-tree sample eval.

## Branch 2 data flow

```
Full-scan collector → MCV + residual Histogram → ColumnDistributionStatistics
V1 equality → MCV frequency when hit
V1 range / BETWEEN / join domain overlap → histogram buckets (else uniform)
```

## Non-goals

Sample-join CE, correlated join synopses, new histogram shapes, per-bucket
heavy-hitters, production online sample maintenance.

## Implementation status

- Leaf-sample CE: joins no longer use sample trees; leaf predicates expanded
  (compares, Date32, `date ± interval` fold, LIKE, OR); per-conjunct sample
  selectivity with classic fallback; 0 matches → 0.
- Histograms: full-scan collector builds catalog MCV + residual equi-depth hist;
  V1 consumes MCV for equality and hist/MCV for range filters. Join domain
  overlap via histograms is still future work.
