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

The pass operates on the `QueryHypergraph` already built by `build_hypergraph` / `HypergraphOf`.
It does not re-derive the hypergraph; it consumes it.

## 2026 Adaptive Redesign

The implementation is split into four independent layers:

1. **Relation sets** — an immutable `RelationSet` value uses an inline `u64` for up to 64
   relation identifiers and canonical heap words only when a set contains a larger identifier.
   `NodeSet` remains a compatibility alias at the public hypergraph boundary. Empty words are
   trimmed so equality and hashing are representation-independent.
2. **Join graph view** — neighborhood, connectivity, connecting-edge lookup, connected-subgraph
   counting, and hyperedge detection are pure operations over a borrowed `QueryHypergraph`.
3. **Enumerators** — exact DPhyp and interval DP generate csg-cmp pairs or interval splits;
   candidate materialization/cost comparison is shared. Search-space size is measured separately
   before exact enumeration so policy can enforce a deterministic budget.
4. **Adaptive policy** — algorithm choice depends on relation count, hyperedges, and a bounded
   connected-subgraph count. Policy is configurable and its decision is observable in tests.

### Invariants

- Every DP state is a connected relation set and every emitted pair is disjoint and connected by
  at least one eligible hyperedge.
- Exact DPhyp emits each canonical csg-cmp pair once; both child states exist before costing it.
- All predicates whose TES endpoints become available at a split are attached exactly once.
- Non-commutative join orientation is preserved by the TES edge orientation.
- Exact solving is selected only after the bounded connected-subgraph counter proves that its
  state table fits policy, except for the unconditional small-query path.
- Algorithm changes affect plan quality, never relational semantics; feature tests compare query
  results and unit/property tests compare exact costs with exhaustive enumeration on small graphs.

### Adaptive policy

The defaults follow Neumann and Radke (SIGMOD 2018), with budgets exposed for deterministic tests:

- fewer than 14 relations: exact DPhyp;
- otherwise count connected subgraphs, stopping at 10,001;
- at most 10,000 connected subgraphs: exact DPhyp, including long chains beyond 64 relations;
- medium ordinary graphs (at most 100 relations): greedy linearization followed by O(n^3)
  interval DP;
- larger graphs or graphs with true hyperedges: GOO constructs a bushy seed and exact DPhyp
  improves maximal subtrees of at most 10 relations (IDP-2 style).

The first production version uses a deterministic connectivity-preserving order in place of the
full IKKBZ rank normalization. This is deliberately isolated behind the linearizer interface so
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
- DataFusion SLT executes 5-relation exact and 15-relation star/linearized result queries. Dense
  15-relation and wide 65-/101-relation stress shapes are explicitly skipped because unrelated
  SQL preprocessing/execution exceeds the SLT timeout; optimizer-level tests and the benchmark
  execute the equivalent algorithms directly.

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

The existing DataFusion `profile_passes` workload was also run from both the untouched
`54c9bdf` commit and this tree with two measured runs. The changing JoinOrdering invocation on
`sixty_four_join_sixty_four_predicates` was 4,868.10 ms baseline versus 4,896.65 ms after the
change (+0.59%); this is within the intended low-regression envelope. Smaller shapes remained
sub-2 ms.

---

## Multi-Group Queries

A single SQL query can contain multiple independent join groups separated by blocking
operators (Aggregation, Sort, Limit, Projection). Each group is a maximal subtree of
`Join` and `CrossProduct` operators. This design notes it as a follow-up; it is a
prerequisite for the pass.

### Identifying Join Groups

Walk the operator tree top-down. Every time a `Join` or `CrossProduct` is encountered
whose parent is *not* a `Join`/`CrossProduct`, that operator is the **join group root**.
Collect all such roots.

```
fn collect_join_group_roots(ctx: &QueryContext, root: Operator) -> Vec<Operator>
```

This is a pre-order traversal that stops descending into a subtree once it finds a
join group root (the root itself is collected; its join children are not separate groups).

For a query like:

```sql
SELECT * FROM (SELECT a.x, SUM(b.y) FROM a JOIN b ON a.id = b.id GROUP BY a.x) sub
JOIN c ON sub.x = c.x
```

The outer `JOIN c` is one group root; the inner `JOIN b` (inside the aggregation) is
another. They are independent and optimized separately.

### Processing Order

Process groups bottom-up: optimize inner groups before outer groups, so that the
cardinality estimates for subquery outputs are available when the outer group is
optimized.

---

## Algorithm Selection (Adaptive, per Neumann & Radke 2018)

The pass selects the algorithm based on the complexity of each join group's hypergraph:

```
fn optimize_group(hg: &QueryHypergraph, stats: &Statistics) -> JoinTree {
    let n = hg.nodes.len();

    // Always exact for tiny groups.
    if n < 14 {
        return dphyp(hg, stats);
    }

    // Count connected subgraphs up to budget to predict DP cost.
    let csg_count = count_csg(hg, budget: 10_000);
    if csg_count <= 10_000 {
        return dphyp(hg, stats);
    }

    // Linearize and run DP on the linearized order.
    if !hg.has_hyperedges() && n <= 100 {
        return linearized_dp(hg, stats);
    }

    // Large or hyperedge queries: GOO to seed, then DP on subtrees.
    let inner_dp = if hg.has_hyperedges() { dphyp } else { linearized_dp };
    let k = if hg.has_hyperedges() { 10 } else { 100 };
    return goo_dp(hg, stats, inner_dp, k, budget: 10_000);
}
```

The implementation follows this policy, with a deterministic connected ordering in the
linearized branch and GOO followed by exact DPhyp improvement of bounded maximal subtrees.

---

## DPhyp (Moerkotte & Neumann 2008)

DPhyp enumerates all csg-cmp-pairs of the hypergraph and fills a DP table.

### Data Structures

```rust
/// An immutable, canonical relation set. One word is stored inline; larger sets use
/// a heap-allocated word slice whose trailing zero words are removed.
type NodeSet = RelationSet;

/// One entry in the DP table: the best plan found for a given node set.
struct DPEntry {
    cost: f64,
    plan: JoinTree,
}

/// The DP table: maps NodeSet → DPEntry.
type DPTable = HashMap<NodeSet, DPEntry>;
```

`RelationSet` transparently switches from its inline word to a dense dynamic word slice.
Equality and hashing operate on the canonical representation, and set operations remain
immutable at API boundaries. There is no 64-relation correctness limit.

### JoinTree

```rust
enum JoinTree {
    /// A leaf: one hypergraph node.
    Leaf(NodeId),
    /// An inner node: two subtrees joined with a predicate.
    Join {
        left: Box<JoinTree>,
        right: Box<JoinTree>,
        /// Predicates from hyperedges connecting left and right.
        predicates: Vec<Expr>,
        /// Join type from the source hyperedge.
        join_type: HyperedgeJoinType,
        /// Estimated output cardinality.
        cardinality: f64,
    },
}
```

### Algorithm

```
Solve(hg, stats):
    dp = {}
    for each node v in hg.nodes (descending by node index):
        dp[{v}] = DPEntry { cost: scan_cost(v, stats), plan: Leaf(v) }

    for each node v in hg.nodes (descending by node index):
        EmitCsg({v}, hg, dp, stats)
        EnumerateCsgRec({v}, Bv, hg, dp, stats)
            // Bv = all nodes with index ≤ v (exclusion set to avoid duplicates)

    return dp[all_nodes].plan


EnumerateCsgRec(S1, X, hg, dp, stats):
    N = neighborhood(S1, X, hg)   // nodes reachable from S1, not in X
    for each non-empty N' ⊆ N:
        if dp[S1 ∪ N'] is set:    // S1 ∪ N' is a known connected subgraph
            EmitCsg(S1 ∪ N', hg, dp, stats)
    for each non-empty N' ⊆ N:
        EnumerateCsgRec(S1 ∪ N', X ∪ N, hg, dp, stats)


EmitCsg(S1, hg, dp, stats):
    X = S1 ∪ B_min(S1)
    N = neighborhood(S1, X, hg)
    for each v in N (descending):
        S2 = {v}
        if ∃ hyperedge (u, v') with u ⊆ S1 and v' ⊆ S2:
            EmitCsgCmp(S1, S2, hg, dp, stats)
        EnumerateCmpRec(S1, S2, X, hg, dp, stats)


EnumerateCmpRec(S1, S2, X, hg, dp, stats):
    N = neighborhood(S2, X, hg)
    for each non-empty N' ⊆ N:
        if dp[S2 ∪ N'] is set:
            if ∃ hyperedge connecting S1 to S2 ∪ N':
                EmitCsgCmp(S1, S2 ∪ N', hg, dp, stats)
    for each non-empty N' ⊆ N:
        EnumerateCmpRec(S1, S2 ∪ N', X ∪ N, hg, dp, stats)


EmitCsgCmp(S1, S2, hg, dp, stats):
    plan1 = dp[S1].plan
    plan2 = dp[S2].plan
    predicates = { P(e) | e ∈ hg.edges, e.left ⊆ S1, e.right ⊆ S2 }
                ∪ { P(e) | e ∈ hg.edges, e.left ⊆ S2, e.right ⊆ S1 }
    join_type = join_type_for(predicates, hg)

    // Try both orderings (commutativity for inner joins).
    for (left, right) in [(plan1, plan2), (plan2, plan1)]:
        if join_type is not commutative and (left, right) = (plan2, plan1): skip
        card = estimate_cardinality(left, right, predicates, stats)
        cost = cost(left) + cost(right) + card   // Cout: minimize intermediate sizes
        if dp[S1 ∪ S2] is empty or cost < dp[S1 ∪ S2].cost:
            dp[S1 ∪ S2] = DPEntry { cost, plan: Join { left, right, predicates, join_type, cardinality: card } }
```

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

Use **C_out** (minimize total intermediate result size), which has the ASI property
needed by IKKBZ and is standard in the literature:

```
cost(Leaf(v))         = 0
cost(Join{left,right,card,...}) = cost(left) + cost(right) + card

cardinality(Leaf(v))  = stats.base_cardinality(v)
cardinality(Join{left,right,predicates,...})
    = cardinality(left) * cardinality(right) * product(selectivity(p) for p in predicates)
```

Selectivity defaults: equality predicate = 1/max(NDV_left, NDV_right); no predicate = 1.0.

The `Statistics` trait abstracts cardinality and selectivity lookups:

```rust
trait Statistics {
    fn cardinality(&self, node: NodeId, hg: &QueryHypergraph) -> f64;
    fn selectivity(&self, edge: &Hyperedge, hg: &QueryHypergraph) -> f64;
}

/// Uniform statistics: all base relations have cardinality 1000,
/// all equality predicates have selectivity 0.01.
struct UniformStatistics;
```

The pass accepts a `Box<dyn Statistics>`. The default is `UniformStatistics`.

---

## Plan Reconstruction

After DPhyp fills the DP table, `dp[all_nodes].plan` is a `JoinTree`. Convert it back
to optd IR operators:

```rust
fn join_tree_to_ir(
    tree: &JoinTree,
    hg: &QueryHypergraph,
    ctx: &mut QueryContext,
) -> Operator {
    match tree {
        JoinTree::Leaf(nid) => hg.nodes[*nid].root,
        JoinTree::Join { left, right, predicates, join_type, .. } => {
            let outer = join_tree_to_ir(left, hg, ctx);
            let inner = join_tree_to_ir(right, hg, ctx);
            let on = conjoin(predicates, ctx);
            OperatorData::Join(Join {
                join_type: join_type.to_ir_join_type(),
                on,
                outer,
                inner,
            }).add(ctx)
        }
    }
}
```

For cross-product dummy edges (predicate = `None`), `on` = `ExprData::Literal(true)`.

---

## Pass Integration

```rust
pub struct JoinOrdering {
    stats: Box<dyn Statistics>,
}

impl Pass for JoinOrdering {
    fn name(&self) -> &'static str { "join_ordering" }
}

impl QueryPass for JoinOrdering {
    fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
        let Some(root) = ctx.query.root() else {
            return Ok(PassResult::Unchanged);
        };

        // Collect all join group roots, bottom-up order.
        let group_roots = collect_join_group_roots(&ctx.query, root);
        if group_roots.is_empty() {
            return Ok(PassResult::Unchanged);
        }

        let mut changed = false;
        for group_root in group_roots {
            let hg = build_hypergraph(&ctx.query, &mut ctx.analyses, group_root);
            if hg.nodes.len() < 2 {
                continue; // nothing to reorder
            }
            let tree = optimize_group(&hg, &*self.stats);
            let new_root = join_tree_to_ir(&tree, &hg, &mut ctx.query);
            ctx.rewrites.replace(group_root, new_root);
            changed = true;
        }

        Ok(if changed { PassResult::Changed } else { PassResult::Unchanged })
    }
}
```

The pass runs once (not in a fixed-point loop). `PassManager` resolves the rewrite map
after the pass completes.

---

## File Layout

```
optd/core/src/relation_set.rs                  # canonical inline/dynamic relation bitset
optd/core/src/optimize/join_ordering.rs        # pass integration, DPhyp, reconstruction
optd/core/src/optimize/join_ordering/graph.rs  # topology queries and bounded csg counting
optd/core/src/optimize/join_ordering/policy.rs # configurable adaptive algorithm selection
optd/core/src/optimize/join_ordering/linearized.rs # connected ordering and interval DP
optd/core/src/optimize/join_ordering/goo.rs    # GOO construction and bounded exact repair
optd/core/src/optimize/mod.rs                  # public re-exports
```

The `collect_join_group_roots` helper lives in `join_ordering.rs` (not in `hypergraph.rs`,
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
trivial). For larger groups the ordering matters for cardinality estimates.

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
6. `optd/core/src/optimize/join_ordering.rs`: `DPhyp` — full implementation of `Solve`/`EmitCsg`/`EnumerateCsgRec`/`EmitCsg`/`EnumerateCmpRec`/`EmitCsgCmp`.
7. `optd/core/src/optimize/join_ordering/`: bounded csg counting, adaptive policy,
   linearized interval DP, and GOO with exact bounded-subtree improvement.
8. `optd/core/src/cost.rs`: catalog-aware cost integration and an explicit capability hook for
   orientation-sensitive physical costs.
9. `optd/core/src/optimize/join_ordering.rs`: direction-correct plan reconstruction,
   bottom-up multi-group collection, public decisions, and `QueryPass` integration.
10. Unit, exhaustive-oracle, SQL feature, benchmark, and same-machine release-profiler evidence
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
- **Null-rejecting predicate detection**: classify `'E`/`'K` variants using `ColumnNullability` analysis.
- **Predicate pushdown prerequisite**: WHERE-clause predicates must be pushed into join conditions before `JoinOrdering` runs.
