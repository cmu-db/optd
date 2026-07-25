# Join Ordering Pass Design

## References

- Moerkotte & Neumann, "Dynamic Programming Strikes Back" (DPhyp), SIGMOD 2008.
- Moerkotte & Neumann, "Analysis of Two Existing and One New DP Algorithm for Bushy Join Trees" (DPccp), VLDB 2006.
- Neumann & Radke, "Adaptive Optimization of Very Large Join Queries", SIGMOD 2018.
- Birler & Neumann, "Efficient Enumeration of the Complete Join Search Space" (CD-A/CD-E), DBPL 2025.

---

## Goal

A `JoinOrdering` optimizer pass that, given a query with one or more join groups, replaces
each join group's operator tree with a cost-optimal (or near-optimal) bushy join tree.

For each maximal contiguous join group, the pass builds a fresh `QueryHypergraph` from the current
IR and cached analyses, chooses an enumerator, and records only the winning reconstructed root in
the optimizer rewrite map.

## 2026 Adaptive Redesign

The implementation is split into four independent layers:

1. **Relation sets** — an immutable `RelationSet` has four canonical storage tiers:
   `Inline64`, `Inline128`, dense heap words, and sorted sparse members. `NodeSet` remains a
   compatibility alias at the public hypergraph boundary. Representation selection depends only
   on set contents, so equal sets always compare and hash identically.
2. **Join graph view** — a relation-to-incident-edge index supports neighborhood, connectivity,
   connecting-edge lookup, GOO component boundaries, exact budgeted DPhyp-state counting, and
   ordinary-inner-graph detection over a borrowed `QueryHypergraph`.
3. **Enumerators and plans** — exact DPhyp and interval DP generate csg-cmp pairs or interval
   splits through one `JoinSearch`. Accepted states reference compact recipes in a `PlanArena`;
   candidate evaluation, comparison, orientation, and final reconstruction are shared. Search-space
   size is measured separately before whole-group exact enumeration, while GOO/DP charges the
   unique states actually built by each inner DP invocation.
4. **Adaptive policy** — algorithm choice depends on relation count, hyperedges, and a bounded
   connected-subgraph count. Policy is configurable, and both its choice and execution telemetry
   are observable through `AlgorithmDecision`.

### Invariants

- Every DP state is a connected relation set and every emitted pair is disjoint and connected by
  at least one eligible hyperedge.
- Exact DPhyp emits each canonical csg-cmp pair once; both child states exist before costing it.
- All predicates whose TES endpoints become available at a split are attached exactly once.
- Non-commutative join orientation is preserved by the TES edge orientation.
- Whole-group exact solving is selected only after the bounded connected-subgraph counter proves
  that its state table fits policy, except for the unconditional small-query path.
- Every GOO/DP repair sees a frontier of disjoint, opaque `PlanAtom`s. Once a repaired subtree is
  contracted, later repairs can combine it but cannot reopen its internal relations.
- GOO/DP accounts for the number of unique states actually inserted by its inner solver and stops
  scheduling repairs when the global state budget is exhausted.
- Algorithm changes affect plan quality, never relational semantics; feature tests compare query
  results and unit/property tests compare exact costs with exhaustive enumeration on small graphs.

### Adaptive policy

The defaults follow Figure 8 of Neumann and Radke (SIGMOD 2018), with every threshold exposed for
deterministic tests:

- fewer than 14 relations use exact DPhyp unconditionally;
- every larger group first counts connected subgraphs, stopping as soon as the count exceeds
  10,000; a count within that budget also selects exact DPhyp, regardless of relation count;
- an over-budget ordinary inner-join graph of at most 100 relations uses selectivity-MST + IKKBZ
  linearization followed by O(n³) interval DP;
- an over-budget ordinary inner-join graph above 100 relations uses GOO/DP with linearized DP,
  `k = 100`, and a global budget of 10,000 inner-DP states;
- an over-budget graph with a true hyperedge or a non-inner join uses GOO/DP with DPhyp, `k = 10`,
  and the same 10,000-state global budget.

“Ordinary” is deliberately stricter than “singleton TES endpoints”: every edge must be an inner
join with one relation on each side. Outer, semi, anti, mark, and single joins are outside the
ASI/IKKBZ proof and therefore take the hypergraph-safe DPhyp repair path.

### Paper linearization and the ASI boundary

For a rooted selectivity tree, the IKKBZ ranker uses the paper's `C_out` surrogate. A sequence has
summary `(C, T)` and composition

```text
C(UV) = C(U) + T(U) C(V)
T(UV) = T(U) T(V)
```

where the chosen root has `C = 0` and `T = |R|`, while a non-root relation reached through an edge
of selectivity `s` has `C = T = |R|s`. Comparing `C(UV)` with `C(VU)` is equivalent to comparing
the ASI rank `(T - 1) / C`, but avoids division and handles a zero-cardinality sequence
deterministically. The implementation tests composition associativity and verifies that the
adjacent-swap preference is unchanged by arbitrary prefix and suffix summaries.

For a cyclic ordinary graph, pair output cardinalities determine edge selectivities and Kruskal's
algorithm chooses a selectivity-minimum spanning tree. IKKBZ tries every root, normalizes
rank-inverted precedence chains into compounds, merges independent chains by ascending rank, and
chooses the minimum-`C_out` rooted order with deterministic tie-breaking.

The proof is intentionally narrow: `C_out` is ASI-compatible for the multiplicative ordinary
inner-join tree, but an arbitrary optd `CostModel` need not be. Consequently IKKBZ decides only the
linear order. Interval DP evaluates every valid split of every interval with the configured
`CostModel`, including orientation-sensitive costing where applicable. Hypergraphs and
non-commutative joins never rely on the IKKBZ proof. Invalid negative/NaN estimates are mapped to
the neutral multiplicative value; exact zero and positive infinity retain cardinality semantics,
including a guarded `0 × ∞ = 0`.

### Candidate orientation

CD-E's directed TES endpoints determine orientation for non-commutative joins; the enumerator
never swaps these inputs. Inner joins are commutative. Because optd's default cost formulas are
symmetric, they cost one canonical orientation. A physical cost model can opt into orientation
sensitivity through `CostModel::is_join_orientation_cost_sensitive`, in which case both inner
orientations are costed and the cheaper one is retained.

### Disjoint-set boundaries

Connectivity algorithms share one crate-private dense disjoint-set forest with iterative path
compression and union by size:

- CD-E uses it while computing connectivity after removing one TES edge;
- IKKBZ uses it for Kruskal cycle detection; and
- GOO's `ComponentIndex` wraps it with the current append-only tree-node ID for each component.

`union` returns whether two previously separate components were merged. Callers must use that
result—not a change in representative identity—to drive fixpoints, because a balanced union can
legitimately retain either existing root. GOO resolves the surviving root after every union before
updating its tree-node metadata.

Two column-equivalence structures intentionally remain domain-specific. Cardinality analysis uses
a compact sorted forest containing only equality-participating columns; its representative owns
NDV provenance, including deterministic left-root tie behavior that is covered by profile tests
and was retained for the measured JOB 15c speedup. Holistic unnesting uses a lazy keyed forest
because columns appear incrementally while predicates are lifted; its lookup is iterative for deep
chains. Join-tree normalization no longer maintains equality classes: that state never affected
predicate attachment or output and was therefore deleted rather than generalized.

### Verification strategy and historical measurements

Correctness is checked at complementary levels:

- relation-set boundary, canonical-hash, set-algebra-oracle, promotion/demotion, and
  subset-iteration tests across all four representations;
- connected-subgraph counts against closed forms (6-chain = 21, 6-clique = 63), every one of the
  1,024 ordinary five-node graphs, every three-node hypergraph, and 256 deterministic randomized
  five-node hypergraphs, with exact budget-boundary assertions;
- exact DPhyp cost against exhaustive bushy enumeration on a 5-clique, plus a genuine
  `{0,1,2} LEFT ANTI {3,4}` GOO/DPhyp repair whose cost and orientation match an independent
  exhaustive constrained-hypergraph oracle;
- an indexed-connectivity oracle that compares incident-edge lookup with a complete edge scan,
  including regular edges and true hyperedges;
- `C_out` algebra tests and exhaustive left-deep oracles, including every labeled five-node tree,
  plus interval-DP and contracted-frontier tests that prove repaired atoms remain opaque;
- canonical GOO choice, contraction sequencing, and actual-state budget-accounting tests;
- DataFusion SLT executes the 5-relation exact result, 15-relation dense and star linearized paths,
  the 65-relation `Inline128` exact path, and the 101-relation GOO/DP path through the complete SQL
  optimization pipeline. None of these feature cases is skipped.

The performance numbers below are historical measurements from the 2026-07-16 implementation,
before selectivity-MST/IKKBZ and Figure-7 global GOO/DP were added. In particular, the `GOO/DP`
labels in these tables refer to the former per-subtree repair implementation; they are retained as
a regression baseline and must not be read as measurements of the current scheduler.

The dependency-free `cargo bench -p optd-core --bench join_ordering -- 3` benchmark measures the
full JoinOrdering pass with an enumeration-only cost model. On the development machine at that
historical milestone, after the orientation hot-path refinement:

| Shape | Selected algorithm | Mean per pass |
|---|---:|---:|
| 10-relation clique | DPhyp | 105.47 ms |
| 18-relation clique | Linearized DP | 14.78 ms |
| 65-relation chain | DPhyp (dynamic set) | 50.24 ms |
| 128-relation chain | GOO/DP | 135.27 ms |
| 128-relation chain | forced DPhyp | 948.68 ms |

Absolute values include hypergraph construction and vary by machine; the benchmark primarily
exists for repeatable before/after comparisons and algorithm-selection regressions. On the
128-relation chain the adaptive large-query path is 7.0× faster than forced exact DPhyp.

### Current paper-faithful scalability measurements

The 2026-07-24 release matrix measures commit `e604ff5` with three deterministic random trees and
three timed repetitions per tree. The control is commit `7a98797` with only the benchmark's empty
catalog fixture populated; no old optimizer code was changed. Both sides therefore analyze the
same query shapes with the same table schemas and enumeration-only cost model:

| Relations | Catalog-corrected control | Paper-faithful adaptive | Current / control |
|---:|---:|---:|---:|
| 10 | 0.266 ms | 0.274 ms | 1.029× |
| 20 | 17.630 ms | 20.889 ms | 1.185× |
| 30 | 2.592 ms | 8.985 ms | 3.466× |
| 40 | 3.424 ms | 9.149 ms | 2.672× |
| 70 | 7.923 ms | 13.116 ms | 1.655× |
| 100 | 13.161 ms | 18.919 ms | 1.437× |
| 128 | 111.691 ms | 19.513 ms | 0.175× |
| 192 | 610.952 ms | 27.135 ms | 0.044× |
| 256 | 2,223.087 ms | 40.373 ms | 0.018× |

The paper-faithful MST/IKKBZ work adds at most 6.4 ms absolute median time in this sub-100
workload, even where the relative ratio is large. The policy crossover then dominates: adaptive
planning is 5.7× faster at 128 relations, 22.5× at 192, and 55.1× at 256. Forced whole-query
linearized DP takes 86.015 ms at 256, versus 40.373 ms for GOO with bounded repair.

The same adaptive path reaches 512, 1,024, 2,000, and 5,000 relations in 77.020 ms, 157.357 ms,
466.027 ms, and 2.038 s median respectively. At 5,000 it contracts 66 subproblems and creates
10,057 inner-DP states. The slight overshoot is expected: the scheduler starts repairs only while
budget remains, but learns an invocation's actual table size after it completes. Raw measurements,
summary CSV/Markdown, five SVG/PNG figures, the corrected baseline, and a SHA-256 manifest live in
`artifacts/join_ordering_paper_faithful/`.

At the same 2026-07-16 adaptive-enumerator milestone, the DataFusion `profile_passes` workload was run
from both the untouched `54c9bdf` commit and the then-current adaptive tree with two measured runs.
The changing JoinOrdering invocation on
`sixty_four_join_sixty_four_predicates` was 4,868.10 ms baseline versus 4,896.65 ms after the
change (+0.59%); this is within the intended low-regression envelope. Smaller shapes remained
sub-2 ms.

JOB query 15c exposes a different bottleneck: candidate cardinality profiles rather than graph
enumeration. Five-run release medians on the same machine show the cumulative effect of the
profile-side changes:

| Implementation phase | JoinOrdering median | Change from original |
|---|---:|---:|
| Original materializing evaluator | 46.95 ms | — |
| Shared `Arc<CardinalityProfile>` cache | 31.62 ms | -32.7% |
| Sparse equivalence metadata | 22.57 ms | -51.9% |
| Pre-flattened join conjuncts | 22.08 ms | -53.0% |
| Compact plan recipes, before deferred costing | 22.06 ms | -53.0% |
| Deferred default-cost evaluation | 22.26 ms | -52.6% |
| Equality-participation-only compact DSU | 8.05 ms | -82.8% |

The separate `join_ordering_candidate_evaluation` benchmark compares the deferred and
materializing evaluators with identical catalog statistics, policy, fresh queries, and alternating
execution order. Across chain and clique cases, deferred evaluation reduced median time by
2.4%--22.9%. It appended only the winning `n - 1` join operators, versus 286 candidates for a
12-chain, 45,760 for a 65-chain, and 24,604 for a 9-clique. Raw profiles and reproducible summaries
live under `artifacts/join_ordering_profiles/job15c/`.

---

## Multi-Group Queries

A single SQL query can contain multiple independent join groups separated by non-join operators
such as aggregation, sort, limit, projection, selection, map, or rename. Each group is a maximal
contiguous region of `Join` and `CrossProduct` operators. `groups.rs` implements discovery;
`mod.rs` owns search orchestration and rewrite publication.

### Identifying Join Groups

Walk the complete operator tree in post-order. A `Join` or `CrossProduct` whose parent is not
join-like is a **join group root**. Traversal continues through unary boundaries so nested groups
below aggregations or projections are still discovered, while join children inside the same
contiguous region are not reported as separate roots.

```
fn collect_join_group_roots(ctx: &QueryContext, root: Operator) -> Vec<Operator>
```

Children are visited before the current node, so the returned roots are already in bottom-up
rewrite order.

For a query like:

```sql
SELECT * FROM (SELECT a.x, SUM(b.y) FROM a JOIN b ON a.id = b.id GROUP BY a.x) sub
JOIN c ON sub.x = c.x
```

The outer `JOIN c` is one group root; the inner `JOIN b` (inside the aggregation) is
another. They are independent and optimized separately.

### Processing Order

Group roots are retained in bottom-up order, but all immutable hypergraphs are built before any
search starts. This matters for the materializing compatibility evaluator: candidate operators
appended for one group cannot change the graph seen by another group. Winning replacements are
installed only after every group solves successfully, then non-join ancestors are rebuilt once.
Post-order determines group solve order and the order of `last_decisions`; ancestor rebuilding is
correct because `materialize_reachable_rewrites` itself traverses the operator graph post-order.

---

## Algorithm Selection (Adaptive, per Neumann & Radke 2018)

The pass selects the algorithm based on the complexity of each join group's hypergraph:

`choose_algorithm` first takes the unconditional small-group DPhyp path. For every larger group it
counts connected subgraphs with early termination at the configured budget. An exact count within
budget selects DPhyp at any size. For an over-budget ordinary inner-join graph, the relation
threshold chooses either whole-group linearized DP or GOO with linearized-DP repair. An
over-budget graph containing a true hyperedge or any non-inner edge uses GOO with DPhyp repair.

For ordinary graphs, the connected-subgraph counter is the iterative, explicit-stack form of
Figure 3. It preserves the paper's DFS expansion and early exit without risking Rust stack
overflow on a chain with thousands of relations. Genuine hyperedges require a stricter recurrence:
adding the canonical representative of an incomplete TES side does not itself create a valid DP
state. The hypergraph path therefore starts with singleton states and iteratively closes them under
applicable disjoint csg-cmp joins. This counts exactly the states DPhyp can build, rather than a
conservative expansion upper bound, and stops on insertion of state `budget + 1`.

`AlgorithmDecision` records the selected algorithm, the exact connected-subgraph count when it was
obtained, the number of unique DP states created during execution, and the number of GOO
subproblems optimized and contracted. For GOO this state count covers the repair solvers; the
greedy tree itself is not a DP table. `JoinOrdering::last_decisions` exposes these per-group
decisions after each pass run for diagnostics, correctness assertions, and benchmark attribution.

### GOO and globally budgeted DP

The GOO seed follows the paper's canonical rule: repeatedly join the applicable component pair
with the smallest estimated output cardinality. An indexed component frontier discovers candidate
pairs from crossing hyperedges, while a priority queue retains evaluated candidates and discards
stale entries through stable component identities. Cardinality—not the configured plan cost—is
the greedy key; relation representatives provide deterministic tie-breaking.

The improvement phase implements Figure 7 over an explicit GOO tree:

1. Select the most expensive maximal visible subtree whose frontier has at most `k` inputs. Its
   parent, if any, must have more than `k` visible inputs.
2. Pass those inputs to DPhyp or linearized DP as disjoint `PlanAtom`s. An atom may cover many
   original relations, but the inner solver can only combine atoms, never split them.
3. Charge the inner solver's actual number of unique DP-table states against one global budget.
   A repair starts only while budget remains; the final invocation may consume the remainder
   because its state count is known only after enumeration.
4. Retain the cheaper of the previous subtree and the repaired plan, then contract the subtree to
   one opaque input. Recompute ancestor costs and visible sizes before choosing the next maximal
   subtree.

True contraction is what permits a later ancestor containing more than `k` original relations to
become eligible once its already-optimized descendants count as single visible inputs.

---

## DPhyp (Moerkotte & Neumann 2008)

DPhyp enumerates all csg-cmp-pairs of the hypergraph and fills a DP table.

### Data Structures

```rust
type NodeSet = RelationSet;

struct PlanState<C> {
    plan: PlanId,
    cost: C,
    properties: PlanProperties,
}

type DPTable<C> = HashMap<NodeSet, PlanState<C>>;
```

`RelationSet` uses `Inline64` for identifiers below 64, `Inline128` for identifiers below 128,
dense boxed words for larger sufficiently dense sets, and sorted sparse members for very large
sparse sets. Because the value intentionally carries no query-universe metadata, the paper's
query-size sparse cutoff is adapted to a content-canonical rule: sparse storage is considered when
the highest member reaches 1024 and selected only when its payload is smaller than the dense word
array. Constructors and every set operation canonicalize the result, so equal sets have the same
derived equality and hash regardless of construction history. Owned `|=` still reuses dense
storage when safe. There is no 64-relation correctness limit.

`PlanId` addresses an immutable leaf/join recipe in the per-group `PlanArena`. `PlanProperties`
currently carries an optional shared cardinality profile. The `JoinTree` type is retained only
under `cfg(test)` as a lightweight witness for exhaustive and cross-evaluator assertions; it is not
part of production DP state.

### Algorithm

```
Solve(search):
    dp = {}
    for each node v in allowed nodes:
        dp[{v}] = search.leaf(v)

    for each node v in allowed nodes (descending by node index):
        EmitCsg({v})
        EnumerateCsgRec({v}, Bv)
            // Bv = all nodes with index ≤ v (exclusion set to avoid duplicates)

    return dp[allowed]


EnumerateCsgRec(S1, X):
    N = neighborhood(S1, X, hg)   // nodes reachable from S1, not in X
    for each non-empty N' ⊆ N:
        if dp[S1 ∪ N'] is set:    // S1 ∪ N' is a known connected subgraph
            EmitCsg(S1 ∪ N')
    for each non-empty N' ⊆ N:
        EnumerateCsgRec(S1 ∪ N', X ∪ N)


EmitCsg(S1):
    X = S1 ∪ B_min(S1)
    N = neighborhood(S1, X, hg)
    for each v in N (descending):
        S2 = {v}
        if ∃ hyperedge (u, v') with u ⊆ S1 and v' ⊆ S2:
            EmitCsgCmp(S1, S2)
        EnumerateCmpRec(S1, S2, X)


EnumerateCmpRec(S1, S2, X):
    N = neighborhood(S2, X, hg)
    for each non-empty N' ⊆ N:
        if dp[S2 ∪ N'] is set:
            if ∃ hyperedge connecting S1 to S2 ∪ N':
                EmitCsgCmp(S1, S2 ∪ N')
    for each non-empty N' ⊆ N:
        EnumerateCmpRec(S1, S2 ∪ N', X ∪ N)


EmitCsgCmp(S1, S2):
    left = dp[S1]
    right = dp[S2]
    edges = connecting_edge_indices(S1, S2, hypergraph)
    draft = search.best_join_candidate(S1, left, S2, right, edges)
    if draft exists and (dp[S1 ∪ S2] is empty or draft.cost is better):
        dp[S1 ∪ S2] = search.commit(draft)
```

`best_join_candidate` recovers the logical join type from the connecting edges. Directed TES
endpoints fix non-inner orientation. It costs one inner orientation for symmetric models and both
orientations only when the model opts into orientation sensitivity.

### Neighborhood

```
neighborhood(S, X, hg) -> NodeSet:
    result = {}
    for each edge e in hg.edges:
        if e.left ⊆ S and e.right ∩ S = {} and e.right ∩ X = {}:
            result |= min(e.right)   // canonical representative of the right hypernode
        if e.right ⊆ S and e.left ∩ S = {} and e.left ∩ X = {}:
            result |= min(e.left)
    return result
```

`min(hypernode)` is the lowest-indexed node in the hypernode (canonical representative
for hyperedge traversal, per §2.3 of DPhyp). The pseudocode expresses the semantics; production
`JoinGraph` first unions the incident-edge lists of the relations in `S`, then filters only those
indexed edges. The same index backs `connects`, sorted/deduplicated connecting-edge lookup, and
GOO boundary maintenance.

---

## Cost Model

`CostModel` supplies an arbitrary cloneable cost type plus `zero`, ordered cost accumulation,
comparison, local operator cost, and optional orientation sensitivity. Its default
`total_cost_from_children` validates input arity and folds local cost followed by child costs. A
custom model can override that method; this is why `with_cost_model` retains materializing
compatibility evaluation.

`DefaultCostModel` uses `CardinalityEstimationV1`, whose internal cache stores
`Arc<CardinalityProfile>`. A profile contains row estimates, per-column frequency/NDV/bounds, and
sparse nontrivial equivalence classes. Equality-join selectivity is `1 / max(NDV_left, NDV_right)`;
transitive or redundant equalities are recognized through equivalence classes and charged once.
Residual predicates use statistics-aware or conservative fallback selectivities.

The local binary cost distinguishes hash-like joins (a column-to-column equality is present) from
nested-loop-like joins:

```text
hash = left_bytes + right_bytes + (left_rows * right_rows)^0.75 + output_bytes
nested_loop = left_rows * right_rows + output_rows
total = local + outer_total + inner_total
```

During join search, `CardinalityEvaluator` computes the same output profile directly from child
profiles and pre-flattened edge conjuncts. Predicate-free inner joins use the dedicated cross-product
profile so per-column frequencies scale correctly. Equality tracking uses a sorted compact DSU over
only inherited equivalence members and equality endpoints—not every output column. Column lookup is
`O(log K)` after one sort/dedup pass, where `K` is the number of equality-participating columns.

---

## Plan Reconstruction

Every winning DP state contains a `PlanId` into the per-group `PlanArena`, not an operator-tree
copy. A recipe is either an existing leaf root or an oriented join of two earlier recipes plus its
join type and connecting hyperedge indices. Enumerators create a `CandidateDraft`, compare its
cost, and commit its recipe only if it becomes the current winner for that state.

`PlanArena::materialize` reconstructs the selected recipe with an explicit post-order stack and
memoizes each resulting operator. It conjoins all predicate-bearing connecting edges exactly once.
A predicate-free inner edge becomes `CrossProduct`; a predicate-free non-inner edge becomes a
typed `Join` with a literal `true` condition. Directed recipes preserve non-commutative outer/inner
inputs. Group discovery, general optimizer traversal, hypergraph construction, and deferred plan
reconstruction are all iterative; 10,000- or 20,000-deep unit fixtures protect the stack-safety
invariant used by the 5,000-relation benchmark.

Candidate costing is behind a private `CandidateEvaluator` boundary. The compatibility evaluator
materializes candidates before invoking an arbitrary `CostModel`, preserving custom
`total_cost_from_children` behavior exactly. Property production follows the enumerator's
requirements: linearized DP and GOO request a profile for each leaf and join because their
linearization and greedy keys use output cardinality; exact DPhyp requests none because it compares
only the configured `CostModel::Cost`. The model remains free to invoke cardinality analysis from
its own cost methods. This avoids coupling an independent exact-DP cost model to catalog
statistics merely to populate an unused property.

The default `CardinalityEvaluator` instead carries a `PlanProperties` bundle containing
`Option<Arc<CardinalityProfile>>`. It derives each join's output profile and local cost from its
two child profiles, composes cumulative cost in the same fold order as `CostModel`, and leaves the
candidate root absent. Because its own built-in cost needs cardinality, it always supplies that
profile even when exact DPhyp does not independently request one. Consequently only the final
recipe reaches `QueryContext`. `JoinOrdering::new` and `with_config` select this path;
`with_cost_model` deliberately selects compatibility evaluation, even when its argument happens
to be `DefaultCostModel`.

Differential tests compare the two evaluators bit-for-bit across every logical join type, a
frequency-scaling cross product, DPhyp, linearized DP, and GOO/DP. They also verify that a deferred
six-relation plan appends exactly five binary operators and that repeat materialization is
memoized. Empty-catalog custom-cost coverage proves that exact DPhyp does not request unused
profiles; forced linearized-DP and GOO integration cases prove that both cardinality-dependent
paths do request them.

---

## Pass Integration

```rust
// Abridged orchestration; `solve_with_stats` dispatches to the selected algorithm.
pub struct JoinOrdering<M: CostModel = DefaultCostModel> {
    cost_model: M,
    config: AdaptiveJoinOrderingConfig,
    evaluator: Box<dyn CandidateEvaluator<M>>,
    last_decisions: Vec<AlgorithmDecision>,
}

impl<M: CostModel> QueryPass for JoinOrdering<M> {
    fn mode(&self) -> PassMode {
        PassMode::Once
    }

    fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
        self.last_decisions.clear();
        let Some(root) = ctx.query.root() else {
            return Ok(PassResult::Unchanged);
        };

        ctx.analyses.clear();
        let groups = collect_join_group_roots(&ctx.query, root)
            .into_iter()
            .filter_map(|group_root| {
                let hg = build_hypergraph(&ctx.query, &mut ctx.analyses, group_root);
                (hg.nodes.len() >= 2).then_some((group_root, hg))
            })
            .collect::<Vec<_>>();
        if groups.is_empty() {
            return Ok(PassResult::Unchanged);
        }

        let mut replacements = Vec::new();
        for (group_root, hg) in &groups {
            let decision = choose_algorithm(hg, self.config);
            let mut search = JoinSearch::new(/* query, analyses, hg, model, evaluator */);
            let outcome = solve_with_stats(decision.algorithm, &mut search)?;
            self.last_decisions.push(decision.record_execution(
                outcome.dp_states_created,
                outcome.repaired_subproblems,
            ));
            if let Some(winner) = outcome.plan {
                replacements.push((*group_root, search.materialize(&winner)));
            }
        }
        if replacements.is_empty() {
            return Ok(PassResult::Unchanged);
        }

        // Publish no rewrite until every group has solved successfully.
        for (group_root, winner) in replacements {
            ctx.rewrites.replace(group_root, winner);
        }
        materialize_reachable_rewrites(root, ctx);

        Ok(PassResult::Changed)
    }
}
```

The omitted dispatch calls `DPhyp::solve_with_stats`, `linearized::solve_with_stats`, or
`goo::solve`. The pass declares `PassMode::Once`, so `PassManager` invokes it once per manager run.
Delaying rewrite-map mutation makes group processing transactional: an error cannot expose a
partially rewritten query. It also avoids the former query-pointer/run-key bookkeeping used to
suppress accidental fixed-point reinvocation.

---

## File Layout

```
optd/core/src/relation_set.rs                  # canonical inline64/inline128/dense/sparse sets
optd/core/src/disjoint_set.rs                   # shared dense connectivity partitions
optd/core/src/optimize/join_ordering/mod.rs    # public API and pass orchestration
optd/core/src/optimize/join_ordering/dphyp.rs  # exact csg-cmp enumeration and DP states
optd/core/src/optimize/join_ordering/candidate.rs # shared search state and candidate commitment
optd/core/src/optimize/join_ordering/evaluator.rs # pluggable candidate evaluation strategies
optd/core/src/optimize/join_ordering/plan.rs   # compact accepted-plan recipes and reconstruction
optd/core/src/optimize/join_ordering/groups.rs # maximal join-group discovery
optd/core/src/optimize/join_ordering/graph.rs  # indexed topology and exact budgeted state counting
optd/core/src/optimize/join_ordering/policy.rs # configurable adaptive algorithm selection
optd/core/src/optimize/join_ordering/linearized.rs # paper linearization and interval DP
optd/core/src/optimize/join_ordering/linearized/asi.rs # C_out composition and ASI rank
optd/core/src/optimize/join_ordering/linearized/ikkbz.rs # selectivity MST and IKKBZ
optd/core/src/optimize/join_ordering/goo.rs    # canonical GOO and global-budget DP repair
optd/core/src/optimize/join_ordering/tests.rs  # cross-module correctness tests
optd/core/src/optimize/mod.rs                  # public re-exports
```

The `collect_join_group_roots` helper lives in `join_ordering/groups.rs` (not in `hypergraph.rs`,
since it is a pass concern, not a hypergraph concern).

---

## Multi-Group Example

```sql
SELECT sub.x, c.z
FROM (
    SELECT a.x, SUM(b.y) AS total
    FROM a JOIN b ON a.id = b.id
    GROUP BY a.x
) sub
JOIN c ON sub.x = c.z
```

IR tree (simplified):

```
Projection
└── Join(sub ⋈ c)          ← group root 1
    ├── Aggregation
    │   └── Join(a ⋈ b)    ← group root 2
    └── Scan(c)
```

`collect_join_group_roots` returns `[Join(a ⋈ b), Join(sub ⋈ c)]` in bottom-up order.
The pass optimizes `Join(a ⋈ b)` first (trivial, 2 nodes), then `Join(sub ⋈ c)` (also
trivial). Their hypergraphs are built from the same pre-search query snapshot; the bottom-up order
determines solve and diagnostic order. One post-order reachable-rewrite traversal materializes all
published mappings into the final operator graph.

---

## Implementation Tasks

### Done
1. `optd/core/src/relation_set.rs`: canonical, immutable
   `Inline64`/`Inline128`/dense/sparse `RelationSet`, mixed-representation set algebra,
   arbitrary-size subset iteration, and boundary/property tests beyond one machine word.
2. `optd/core/src/hypergraph.rs`: `NodeSet = RelationSet`; hypergraph, connectivity, and CD-E
   analyses no longer impose a 64-relation limit.
3. `optd/core/src/hypergraph.rs`: Compatibility tables (`assoc`, `l_asscom`, `r_asscom`) corrected to match Tables 1–3 from Birler & Neumann 2025.
4. `optd/core/src/hypergraph.rs`: Builder upgraded to CD-E (Algorithm 3): uses `TES(◦_a)` instead of full subtree, gates extensions on connectivity check (Algorithm 5, union-find).
5. `optd/core/src/hypergraph.rs`: `HyperedgeJoinType::to_ir_join_type()` for plan reconstruction.
6. `optd/core/src/optimize/join_ordering/dphyp.rs`: `DPhyp` — full implementation of `Solve`/`EmitCsg`/`EnumerateCsgRec`/`EmitCsg`/`EnumerateCmpRec`/`EmitCsgCmp`.
7. `optd/core/src/optimize/join_ordering/graph.rs`: incident-edge indexing, iterative exact
   budgeted DPhyp-state counting for ordinary graphs and genuine hypergraphs, indexed component
   boundaries, and ordinary-inner-graph classification.
8. `optd/core/src/optimize/join_ordering/linearized/`: selectivity-minimum spanning tree,
   ASI-compatible `C_out` ranking, IKKBZ compound normalization, and interval DP costed by the
   configured model.
9. `optd/core/src/optimize/join_ordering/goo.rs`: output-cardinality GOO plus Figure-7
   most-expensive-maximal scheduling, opaque frontier contraction, and global actual-state
   budgeting with either DPhyp or linearized DP.
10. `optd/core/src/cost.rs`: catalog-aware cost integration and an explicit capability hook for
   orientation-sensitive physical costs.
11. `optd/core/src/optimize/join_ordering/`: direction-correct candidate reconstruction,
   bottom-up multi-group collection, public decisions, and `QueryPass` integration.
12. `optd/core/src/optimize/join_ordering/evaluator.rs`: shared `Arc<CardinalityProfile>` plan
    properties and deferred default-cost evaluation, while retaining exact custom-model behavior.
13. `optd/core/src/analysis.rs`: sparse nontrivial equivalence metadata and a compact sorted DSU
    populated only by inherited class members and equality endpoints.
14. `AlgorithmDecision`: selected algorithm, bounded connected-subgraph count, unique DP states,
    and contracted GOO-subproblem telemetry.
15. Unit, exhaustive-oracle, SQL feature, benchmark, and same-machine release-profiler evidence
    cover exactness, directed multi-node TES repair, algorithm selection, stack safety, more than
    64 relations, and regression bounds.
16. `artifacts/join_ordering_paper_faithful/`: three-query/three-repetition measurements through
    5,000 relations, a catalog-corrected `7a98797` comparison, summary tables, five inspected
    figures, and a SHA-256 manifest.
17. `optd/core/src/disjoint_set.rs`: one dense iterative union-find shared by CD-E, IKKBZ, and
    GOO, with exhaustive six-node partition tests and domain regressions for CD-E fixpoints and
    GOO representative metadata. Dead join-normalization equality state was removed; the
    metadata-bearing cardinality forest remains specialized.

### Open / Follow-ups
- **Directed-edge invariant defense**: CD-E guarantees simultaneously applicable directed edges
  have compatible join semantics. Add a defensive assertion/error for manually constructed public
  hypergraphs whose connecting edges disagree on join type or orientation.
- **Composite repair coverage**: combine a directed multi-node TES with several prior opaque
  contractions, and add an exact finite-budget boundary spanning several repairs.
- **Large GOO execution coverage**: the enabled 101-way SQL case verifies physical-plan
  construction; add a bounded-result executable case for end-to-end semantic comparison.
- **Greedy estimate hardening**: built-in cardinality estimates are nonnegative and non-NaN, but
  normalize invalid custom estimates before constructing the total-order GOO key.
- **Extended enumeration telemetry**: the pass now reports unique DP states and GOO repairs;
  candidate attempts and emitted csg-cmp-pair counts would provide finer production attribution.
- **Column-profile construction**: `combine_join_columns` and ordered-map insertion are the largest
  remaining JOB 15c profile buckets; evaluate a persistent or append-friendly column map without
  weakening deterministic profile ordering.
- **Null-rejecting predicate detection**: classify `'E`/`'K` variants using `ColumnNullability` analysis.
- **Predicate pushdown prerequisite**: WHERE-clause predicates must be pushed into join conditions before `JoinOrdering` runs.
