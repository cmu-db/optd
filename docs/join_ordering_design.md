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

1. **Relation sets** — an immutable `RelationSet` value uses an inline `u64` for up to 64
   relation identifiers and canonical heap words only when a set contains a larger identifier.
   `NodeSet` remains a compatibility alias at the public hypergraph boundary. Empty words are
   trimmed so equality and hashing are representation-independent.
2. **Join graph view** — neighborhood, connectivity, connecting-edge lookup, connected-subgraph
   counting, and hyperedge detection are pure operations over a borrowed `QueryHypergraph`.
3. **Enumerators and plans** — exact DPhyp and interval DP generate csg-cmp pairs or interval
   splits through one `JoinSearch`. Accepted states reference compact recipes in a `PlanArena`;
   candidate evaluation, comparison, orientation, and final reconstruction are shared. Search-space
   size is measured separately before exact enumeration so policy can enforce a deterministic
   budget.
4. **Adaptive policy** — algorithm choice depends on relation count, hyperedges, and a bounded
   connected-subgraph count. Policy is configurable and its decision is observable in tests.

### Invariants

- Every DP state is a connected relation set and every emitted pair is disjoint and connected by
  at least one eligible hyperedge.
- Exact DPhyp emits each canonical csg-cmp pair once; both child states exist before costing it.
- All predicates whose TES endpoints become available at a split are attached exactly once.
- Non-commutative join orientation is preserved by the TES edge orientation.
- Whole-group exact solving is selected only after the bounded connected-subgraph counter proves
  that its state table fits policy, except for the unconditional small-query path. GOO/DP may run
  exact DPhyp inside an explicitly size-bounded repair subtree without a second count.
- Algorithm changes affect plan quality, never relational semantics; feature tests compare query
  results and unit/property tests compare exact costs with exhaustive enumeration on small graphs.

### Adaptive policy

The defaults follow Neumann and Radke (SIGMOD 2018), with budgets exposed for deterministic tests:

- fewer than 14 relations: exact DPhyp;
- from 14 through 100 relations: count connected subgraphs, stopping at 10,001;
- within that range, at most 10,000 connected subgraphs selects exact DPhyp, including a
  65-relation chain that crosses the inline bitset boundary;
- an over-budget ordinary graph in that range uses greedy linearization followed by O(n^3)
  interval DP;
- a graph above 100 relations, or an over-budget graph with true hyperedges, uses GOO to construct
  a bushy seed followed by exact repair of maximal subtrees of at most 10 relations (IDP-2 style).

The first production version uses a deterministic connectivity-preserving order in place of the
full IKKBZ rank normalization. This is deliberately isolated in a private linearization helper so
IKKBZ or a cardinality-guided minimum-spanning-tree order can be added without changing interval
DP or policy.

### Candidate orientation

CD-E's directed TES endpoints determine orientation for non-commutative joins; the enumerator
never swaps these inputs. Inner joins are commutative. Because optd's default cost formulas are
symmetric, they cost one canonical orientation. A physical cost model can opt into orientation
sensitivity through `CostModel::is_join_orientation_cost_sensitive`, in which case both inner
orientations are costed and the cheaper one is retained.

### Verification and measured performance

Correctness is checked at three levels:

- relation-set boundary and subset-iteration unit tests, including identifiers 64 and 129;
- connected-subgraph counts against closed forms (6-chain = 21, 6-clique = 63) and exact DPhyp
  cost against exhaustive bushy enumeration on a 5-clique;
- DataFusion SLT executes the 5-relation exact result, 15-relation dense and star linearized paths,
  the 65-relation dynamic-set exact path, and the 101-relation GOO/DP path through the complete SQL
  optimization pipeline. None of these feature cases is skipped.

The dependency-free `cargo bench -p optd-core --bench join_ordering -- 3` benchmark measures the
full JoinOrdering pass with an enumeration-only cost model. On the development machine after the
orientation hot-path refinement:

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

At the 2026-07-16 adaptive-enumerator milestone, the DataFusion `profile_passes` workload was run
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

`choose_algorithm` first takes the unconditional small-group DPhyp path. For larger groups up to
the linearized threshold, it counts connected subgraphs with early termination at the configured
budget; an exact count within budget also selects DPhyp. A remaining ordinary graph within the
threshold uses connectivity-preserving linearization plus interval DP. Groups above the threshold,
and over-budget true hypergraphs, use GOO followed by exact DPhyp replacement of maximal bounded
subtrees.

`AlgorithmDecision` records the selected algorithm and the exact connected-subgraph count when it
was obtained. `JoinOrdering::last_decisions` exposes these per-group decisions for diagnostics and
tests.

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

`RelationSet` transparently switches from its inline word to a dense dynamic word slice.
Equality and hashing operate on the canonical representation, and set operations remain
immutable at API boundaries; owned `|=` reuses dynamic storage when capacity permits. There is no
64-relation correctness limit.

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
for hyperedge traversal, per §2.3 of DPhyp).

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

`PlanArena::materialize` recursively reconstructs the selected recipe and memoizes each resulting
operator. It conjoins all predicate-bearing connecting edges exactly once. A predicate-free inner
edge becomes `CrossProduct`; a predicate-free non-inner edge becomes a typed `Join` with a literal
`true` condition. Directed recipes preserve non-commutative outer/inner inputs.

Candidate costing is behind a private `CandidateEvaluator` boundary. The compatibility evaluator
materializes candidates before invoking an arbitrary `CostModel`, preserving custom
`total_cost_from_children` behavior exactly. The default `CardinalityEvaluator` instead carries a
`PlanProperties` bundle containing `Option<Arc<CardinalityProfile>>`. It derives each join's output
profile and local cost from its two child profiles, composes cumulative cost in the same fold order
as `CostModel`, and leaves the candidate root absent. Consequently only the final recipe reaches
`QueryContext`. `JoinOrdering::new` and `with_config` select this path; `with_cost_model` deliberately
selects compatibility evaluation, even when its argument happens to be `DefaultCostModel`.

Differential tests compare the two evaluators bit-for-bit across every logical join type, a
frequency-scaling cross product, DPhyp, linearized DP, and GOO/DP. They also verify that a deferred
six-relation plan appends exactly five binary operators and that repeat materialization is memoized.

---

## Pass Integration

```rust
// Abridged orchestration; `solve_with` is the three-way algorithm dispatch.
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
            self.last_decisions.push(decision);
            let mut search = JoinSearch::new(/* query, analyses, hg, model, evaluator */);
            let winner = solve_with(decision.algorithm, &mut search)?;
            if let Some(winner) = winner {
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

The omitted `solve_with` branch dispatches to DPhyp, linearized DP, or GOO/DP. The pass declares
`PassMode::Once`, so `PassManager` invokes it once per manager run. Delaying rewrite-map mutation
makes group processing transactional: an error cannot expose a partially rewritten query. It also
avoids the former query-pointer/run-key bookkeeping used to suppress accidental fixed-point
reinvocation.

---

## File Layout

```
optd/core/src/relation_set.rs                  # canonical inline/dynamic relation bitset
optd/core/src/optimize/join_ordering/mod.rs    # public API and pass orchestration
optd/core/src/optimize/join_ordering/dphyp.rs  # exact csg-cmp enumeration and DP states
optd/core/src/optimize/join_ordering/candidate.rs # shared search state and candidate commitment
optd/core/src/optimize/join_ordering/evaluator.rs # pluggable candidate evaluation strategies
optd/core/src/optimize/join_ordering/plan.rs   # compact accepted-plan recipes and reconstruction
optd/core/src/optimize/join_ordering/groups.rs # maximal join-group discovery
optd/core/src/optimize/join_ordering/graph.rs  # topology queries and bounded csg counting
optd/core/src/optimize/join_ordering/policy.rs # configurable adaptive algorithm selection
optd/core/src/optimize/join_ordering/linearized.rs # connected ordering and interval DP
optd/core/src/optimize/join_ordering/goo.rs    # GOO construction and bounded exact repair
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
1. `optd/core/src/relation_set.rs`: canonical, immutable inline/dynamic `RelationSet`, set algebra,
   arbitrary-size subset iteration, and boundary tests beyond one machine word.
2. `optd/core/src/hypergraph.rs`: `NodeSet = RelationSet`; hypergraph, connectivity, and CD-E
   analyses no longer impose a 64-relation limit.
3. `optd/core/src/hypergraph.rs`: Compatibility tables (`assoc`, `l_asscom`, `r_asscom`) corrected to match Tables 1–3 from Birler & Neumann 2025.
4. `optd/core/src/hypergraph.rs`: Builder upgraded to CD-E (Algorithm 3): uses `TES(◦_a)` instead of full subtree, gates extensions on connectivity check (Algorithm 5, union-find).
5. `optd/core/src/hypergraph.rs`: `HyperedgeJoinType::to_ir_join_type()` for plan reconstruction.
6. `optd/core/src/optimize/join_ordering/dphyp.rs`: `DPhyp` — full implementation of `Solve`/`EmitCsg`/`EnumerateCsgRec`/`EmitCsg`/`EnumerateCmpRec`/`EmitCsgCmp`.
7. `optd/core/src/optimize/join_ordering/`: bounded csg counting, adaptive policy,
   linearized interval DP, and GOO with exact bounded-subtree improvement.
8. `optd/core/src/cost.rs`: catalog-aware cost integration and an explicit capability hook for
   orientation-sensitive physical costs.
9. `optd/core/src/optimize/join_ordering/`: direction-correct candidate reconstruction,
   bottom-up multi-group collection, public decisions, and `QueryPass` integration.
10. `optd/core/src/optimize/join_ordering/evaluator.rs`: shared `Arc<CardinalityProfile>` plan
    properties and deferred default-cost evaluation, while retaining exact custom-model behavior.
11. `optd/core/src/analysis.rs`: sparse nontrivial equivalence metadata and a compact sorted DSU
    populated only by inherited class members and equality endpoints.
12. Unit, exhaustive-oracle, SQL feature, benchmark, and same-machine release-profiler evidence
    cover exactness, algorithm selection, more than 64 relations, and regression bounds.

### Open / Follow-ups
- **IKKBZ linearization** (Neumann & Radke §4.2): replace the deterministic connected order with
  rank-normalized IKKBZ over a selectivity-weighted minimum spanning tree.
- **GOO/DP global budget** (Neumann & Radke §4.3): choose maximal subproblems by benefit and charge
  their actual DP-table size against a global improvement budget.
- **Sparse relation sets**: add a sorted sparse representation above roughly 1024 relations if
  workloads at that scale show dense word vectors to be material.
- **Enumeration telemetry**: expose csg-cmp pair and winning-state counts alongside algorithm
  decisions for production profiling.
- **Column-profile construction**: `combine_join_columns` and ordered-map insertion are the largest
  remaining JOB 15c profile buckets; evaluate a persistent or append-friendly column map without
  weakening deterministic profile ordering.
- **Null-rejecting predicate detection**: classify `'E`/`'K` variants using `ColumnNullability` analysis.
- **Predicate pushdown prerequisite**: WHERE-clause predicates must be pushed into join conditions before `JoinOrdering` runs.
