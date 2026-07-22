# JOB 15c Samply Profile

Captured on 2026-07-21 with `samply 0.13.1` on Apple M2 Max/macOS. The release binary retained
Rust symbols, and the sidecar contains pre-symbolicated addresses.

```bash
samply load artifacts/join_ordering_profiles/job15c/job15c-optimizer.json.gz
```

The capture used:

```bash
samply record --rate 10000 --save-only --no-open --unstable-presymbolicate \
  --profile-name job15c-optimizer \
  -o artifacts/join_ordering_profiles/job15c/job15c-optimizer.json.gz \
  target/release/optd-cli --job \
  -f artifacts/join_ordering_profiles/job15c/job15c_profile.sql
```

The preserved `job15c_profile.sql` makes the input independent of temporary files. All captures
were made on the same Apple M2 Max checkout in America/New_York: original at 2026-07-21 16:18,
phase 1 at 19:03, phase 2 at 23:16, and phase 3 at 23:38. Phase 3 used base commit `e48711c` plus
the sparse equality-state change finalized in `d109c8f` and documented in
`optimization_phases.md`.

## JoinOrdering-filtered sample summary

The profile contains 1,183 samples whose stacks include `JoinOrdering::run`. The SQL table
function caused the optimizer to be invoked on two worker threads; the percentages below combine
both invocations and describe hotspot share, not absolute wall time.

| Inclusive stack | Samples | Share |
|---|---:|---:|
| `DPhyp::solve_subset` | 1,180 | 99.7% |
| `DPhyp::emit_csg_cmp` | 1,167 | 98.6% |
| `best_join_candidate` | 1,149 | 97.1% |
| `CostModel::total_cost_from_children` | 1,147 | 97.0% |
| `DefaultCostModel::operator_cost` | 1,146 | 96.9% |
| `cost::cardinality_profile` | 1,021 | 86.3% |
| `CardinalityEstimationV1::compute` | 860 | 72.7% |
| `analysis::join_profile_from_predicates` | 697 | 58.9% |
| `analysis::combine_join_columns` | 312 | 26.4% |
| `BTreeMap::clone::clone_subtree` | 214 | 18.1% |
| `analysis::filter_equivalence_classes` | 191 | 16.1% |

The largest self-time buckets are allocator work, `join_profile_from_predicates`, SipHash,
`BTreeMap` cloning, `filter_equivalence_classes`, and `ColumnProfile` cloning. This indicates that
candidate cardinality-profile construction and cloning dominate; DPhyp connectivity enumeration
is not the primary leaf-level cost for this query.

The profiled run reported 59.95 ms inside `JoinOrdering`, versus a 46.95 ms median without
sampling. The 10 kHz sampler therefore adds material overhead and should be used for hotspot
direction, not benchmark timing.

## After shared profiles and sparse equivalence metadata

`job15c-phase1.json.gz` was captured with the same command and sampling rate after commits through
`628e65e`. Its unsampled five-run median is 22.08 ms, 53.0% below the original 46.95 ms baseline.
Under sampling, `JoinOrdering` took 31.05 ms and contained 597 matching samples, versus 1,183
samples in the original capture.

The absolute samples in the targeted allocation paths fell sharply:

| Stack/function | Before | After |
|---|---:|---:|
| `filter_equivalence_classes` inclusive | 191 | 19 |
| `filter_equivalence_classes` self | 48 | 6 |
| `ColumnProfile::clone` self | 39 | 9 |
| `BTreeMap::clone_subtree` self | 64 | 0 sampled |

Candidate evaluation remains dominant: `best_join_candidate` contains 95.8% of filtered samples,
and cardinality join-profile construction is now the main leaf-level work. This supports the next
step of deriving candidate properties directly and delaying IR materialization.

Reproduce the Markdown tables for either capture with:

```bash
python3 optd/core/benches/summarize_samply.py \
  artifacts/join_ordering_profiles/job15c/job15c-phase1.json.gz \
  --filter JoinOrdering --limit 25
```

## Deferred candidate evaluation

`job15c-phase2.json.gz` was captured after adding profile-only evaluation for the default cost
model. Only the winning `n-1` join operators are materialized, but the unsampled median remained
effectively flat at 22.26 ms. The sampled run took 27.61 ms and contains 544 matching samples.

The call-path shift proves that candidate IR and analysis-cache allocation have been removed:
`CardinalityEvaluator::evaluate_join` now contains 90.1% of filtered samples and
`join_profile_from_conjuncts` contains 89.5%. Hashing, tiny-object allocation, equivalence-class
lookup, and column-map construction dominate self time, identifying sparse/lazy equivalence state
as the next optimization target. See `job15c-phase2-summary.md` for the generated table.

## Compact equality state

`job15c-phase3.json.gz` was captured after replacing the per-candidate full-column hash DSU with a
sorted compact DSU containing only inherited nontrivial-class members and equality endpoints. Its
unsampled five-run median is 8.05 ms, 63.8% below phase 2 and 82.8% below the original 46.95 ms
baseline. The sampled run took 11.77 ms and contains 214 matching samples.

SipHash self samples fell from 61 to 2 and equivalence-state lookup self samples fell from 26 to 3.
The largest remaining buckets are `combine_join_columns` and B-tree insertion, so further work
should focus on column-profile construction rather than another equality-state representation.
See `job15c-phase3-summary.md` for the generated sample table.

## Summary artifacts

- `optimization_phases.csv` / `.md` records the cumulative JOB 15c pass-time progression.
- `candidate_evaluator_summary.csv` / `.md` compares deferred and materializing evaluators.
- `job15c-phase1-summary.md`, `job15c-phase2-summary.md`, and `job15c-phase3-summary.md` summarize
  the corresponding Samply captures.
- `manifest.json` records SHA-256 hashes for the complete evidence bundle.
