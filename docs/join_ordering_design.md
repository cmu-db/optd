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

For the initial implementation, only `dphyp` is needed. `linearized_dp` and `goo_dp`
are follow-ups for large queries.

---

## DPhyp (Moerkotte & Neumann 2008)

DPhyp enumerates all csg-cmp-pairs of the hypergraph and fills a DP table.

### Data Structures

```rust
/// A set of node indices, represented as a sorted Vec<NodeId> for small groups
/// or a u64 bitmask for groups up to 64 nodes.
type NodeSet = u64; // bit i set ↔ node i is in the set

/// One entry in the DP table: the best plan found for a given node set.
struct DPEntry {
    cost: f64,
    plan: JoinTree,
}

/// The DP table: maps NodeSet → DPEntry.
type DPTable = HashMap<NodeSet, DPEntry>;
```

For groups larger than 64 nodes, `NodeSet` becomes a `Vec<u64>` (bitset of words).
The initial implementation uses `u64` only, covering groups up to 64 relations.

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
optd/core/src/optimize/join_ordering.rs    # JoinOrdering pass, DPhyp, JoinTree, Statistics trait
optd/core/src/optimize/mod.rs              # pub use join_ordering::JoinOrdering
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
1. `optd/core/src/hypergraph.rs`: `NodeSet = u64` type alias + `nodeset_singleton`, `nodeset_min`, `nodeset_iter` helpers.
2. `optd/core/src/hypergraph.rs`: `Hyperedge.left`/`.right` changed from `Vec<NodeId>` to `NodeSet`.
3. `optd/core/src/hypergraph.rs`: Compatibility tables (`assoc`, `l_asscom`, `r_asscom`) corrected to match Tables 1–3 from Birler & Neumann 2025.
4. `optd/core/src/hypergraph.rs`: Builder upgraded to CD-E (Algorithm 3): uses `TES(◦_a)` instead of full subtree, gates extensions on connectivity check (Algorithm 5, union-find).
5. `optd/core/src/hypergraph.rs`: `HyperedgeJoinType::to_ir_join_type()` for plan reconstruction.
6. `optd/core/src/optimize/join_ordering.rs`: `DPhyp` — full implementation of `Solve`/`EmitCsg`/`EnumerateCsgRec`/`EmitCsg`/`EnumerateCmpRec`/`EmitCsgCmp`.
7. `optd/core/src/optimize/join_ordering.rs`: `Statistics` trait + `UniformStatistics` placeholder.
8. `optd/core/src/optimize/join_ordering.rs`: `join_tree_to_ir` — converts `JoinTree` back to optd IR.
9. `optd/core/src/optimize/join_ordering.rs`: `collect_join_group_roots` — finds all join group roots bottom-up.
10. `optd/core/src/optimize/join_ordering.rs`: `JoinOrdering` pass implementing `QueryPass`.

### Open / Follow-ups
- **Linearized DP** (Neumann & Radke §4.2): IKKBZ ordering + O(n³) DP for 15–100 node groups without hyperedges.
- **GOO-DP** (Neumann & Radke §4.3): greedy seed + DP on subtrees of size k for >100 nodes.
- **NodeSet >64 nodes**: extend to `Vec<u64>` or sparse representation for very large groups.
- **Real statistics**: replace `UniformStatistics` with catalog-backed cardinality/selectivity.
- **Null-rejecting predicate detection**: classify `'E`/`'K` variants using `ColumnNullability` analysis.
- **Predicate pushdown prerequisite**: WHERE-clause predicates must be pushed into join conditions before `JoinOrdering` runs.
