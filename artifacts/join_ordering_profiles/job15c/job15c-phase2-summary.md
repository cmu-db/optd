# Samply summary: job15c-phase2.json.gz

Stack filter: `JoinOrdering`. Matching samples: **544**. The sampled `JoinOrdering` invocation took
27.61 ms; its unsampled five-run median was 22.26 ms.

## Inclusive samples

| Function | Samples | Share |
|:---|---:|---:|
| `DPhyp::solve_subset` | 538 | 98.9% |
| `DPhyp::emit_csg_cmp` | 522 | 96.0% |
| `JoinSearch::best_join_candidate` | 490 | 90.1% |
| `CardinalityEvaluator::evaluate_join` | 490 | 90.1% |
| `join_profile_from_conjuncts` | 487 | 89.5% |
| `EquivalenceClassState::find` | 72 | 13.2% |
| `HashMap::reserve_rehash` | 71 | 13.1% |
| `combine_join_columns` | 82 | 15.1% |

## Self samples

| Function | Samples | Share |
|:---|---:|---:|
| `join_profile_from_conjuncts` | 66 | 12.1% |
| SipHash write | 61 | 11.2% |
| tiny free | 42 | 7.7% |
| `BuildHasher::hash_one` | 41 | 7.5% |
| B-tree leaf insertion | 27 | 5.0% |
| `EquivalenceClassState::find` | 26 | 4.8% |
| `combine_join_columns` | 19 | 3.5% |
| `ColumnProfile::clone` | 8 | 1.5% |

Generated with:

```bash
python3 optd/core/benches/summarize_samply.py \
  artifacts/join_ordering_profiles/job15c/job15c-phase2.json.gz \
  --filter JoinOrdering --limit 35
```
