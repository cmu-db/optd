# Samply summary: job15c-phase3.json.gz

Stack filter: `JoinOrdering`. Matching samples: **214**. The sampled `JoinOrdering` invocation took
11.77 ms; its unsampled five-run median was 8.05 ms.

## Inclusive samples

| Function | Samples | Share |
|:---|---:|---:|
| `DPhyp::solve_subset` | 201 | 93.9% |
| `DPhyp::emit_csg_cmp` | 184 | 86.0% |
| `JoinSearch::best_join_candidate` | 153 | 71.5% |
| `CardinalityEvaluator::evaluate_join` | 153 | 71.5% |
| `join_profile_from_conjuncts` | 148 | 69.2% |
| `combine_join_columns` | 101 | 47.2% |
| B-tree recursive insertion | 42 | 19.6% |
| `filter_equivalence_classes` | 27 | 12.6% |

## Self samples

| Function | Samples | Share |
|:---|---:|---:|
| B-tree recursive insertion | 29 | 13.6% |
| `join_profile_from_conjuncts` | 22 | 10.3% |
| `combine_join_columns` | 21 | 9.8% |
| `JoinGraph::neighborhood_within` | 11 | 5.1% |
| `ColumnProfile::clone` | 7 | 3.3% |
| `filter_equivalence_classes` | 5 | 2.3% |
| `EquivalenceClassState::find_index` | 3 | 1.4% |
| SipHash write | 2 | 0.9% |

Generated with:

```bash
python3 optd/core/benches/summarize_samply.py \
  artifacts/join_ordering_profiles/job15c/job15c-phase3.json.gz \
  --filter JoinOrdering --limit 35
```
