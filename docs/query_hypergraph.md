# Query Hypergraph

Reference: Birler & Neumann, "Efficient Enumeration of the Complete Join Search Space",
DBPL 2025 at SIGMOD 2025. https://db.in.tum.de/~birler/papers/joinspace.pdf

## Goal

Build a `QueryHypergraph` that represents the join-reordering structure of a query, as
the input to join-ordering algorithms (DPhyp, CD-E, etc.).

- **Nodes** are the base relations eligible for reordering.
- **Hyperedges** encode join predicates and reordering constraints (TES).

The paper's Section 2.5 defines hyperedges as *pairs of sets* `(left_set, right_set)` —
not a single SES — to support efficient physical operators (e.g. hash join) for
multi-relation predicates. The current implementation computes TES bottom-up using
**CD-E** (Algorithm 3 in the paper).

---

## Key Concepts from the Paper

### SES and TES (§2.1, §2.5, §3.1)

- **SES(◦)** — syntactic eligibility set: the set of base relations referenced by the
  predicate of join operator ◦.
- **TES(◦)** — total eligibility set: the extended set of relations that must be present
  in the input of ◦ before it can be applied. TES ⊇ SES. The TES is split into
  `TES-left` and `TES-right` to form the hyperedge `(TES-left, TES-right)`.

### CD-A: TES construction (§3.1, Algorithm 1)

For each join operator ◦_b, bottom-up:

```
TES(◦_b) = SES(◦_b)

for ◦_a in STO(left(◦_b)):          // operators in left subtree
    if ¬assoc(◦_a, ◦_b):  TES(◦_b) += left(◦_a)
    if ¬l-asscom(◦_a, ◦_b): TES(◦_b) += right(◦_a)

for ◦_a in STO(right(◦_b)):         // operators in right subtree
    if ¬assoc(◦_a, ◦_b):  TES(◦_b) += right(◦_a)
    if ¬r-asscom(◦_a, ◦_b): TES(◦_b) += left(◦_a)
```

CD-A is incomplete (over-constrains). CD-E (Algorithm 3) improves it by:
1. Adding only `TES(◦_a)` instead of the full subtree `T(◦_a)`.
2. Skipping the extension when a connectivity check shows the reordering would require
   a cross product anyway (making the restriction redundant).

### Selections and Maps as faux-table nodes (§2.7)

> "Selection and map operators can be interpreted as joins with faux tables. This allows
> them to be considered as a natural part of the query graph."

The paper explicitly includes `Selection` and `Map` as nodes in the hypergraph, treating
them as joins with a single-relation "faux table". Group-by operators are explicitly
excluded from this treatment.

### Cross products (§2.6)

A disconnected query graph (cross product) is handled by adding a dummy edge between
the two disconnected components, reinterpreting `R × S` as `R ⋈_{RS} S` where `RS` is
an always-true predicate between arbitrary representatives.

### Operator compatibility tables (§2.2, Tables 1–3)

The paper provides lookup tables for `assoc(◦_a, ◦_b)`, `l-asscom(◦_a, ◦_b)`, and
`r-asscom(◦_a, ◦_b)` for all combinations of: inner join (B), group join (Z), left outer
join (E), full outer join (K), and their null-rejecting variants. These tables drive TES
construction.

---

## The Key Design Question: Which Operators Are Nodes?

The paper's answer (§2.7) is clear:

| Operator | Treatment |
|---|---|
| `Scan`, `TableFunction` | Node (base relation) |
| `Selection` | Node — treated as a join with a faux table |
| `Map` | Node — treated as a join with a faux table |
| `Rename` | Transparent — absorbed into the node it wraps |
| `Aggregation`, `Sort`, `Limit` | **Opaque barrier** — terminates the join group |
| `Projection` | Opaque barrier (changes available columns) |
| `Join`, `CrossProduct` | Join operators — connect nodes via hyperedges |

A `Selection` or `Map` between two joins is a node in the hypergraph, not a barrier.
Its predicate/computation is a local annotation on the node, not a hyperedge (since it
references only columns from its own subtree). If a `Selection` references columns from
multiple nodes (a cross-node filter), it becomes a hyperedge connecting those nodes.

---

## Data Model

```rust
/// Index into `QueryHypergraph::nodes`.
pub type NodeId = usize;

/// A base relation node in the hypergraph.
/// Wraps one IR operator (Scan, TableFunction, Selection, Map, or Rename over these).
pub struct HypergraphNode {
    /// The topmost operator of this node.
    pub root: Operator,
    /// Human-readable label (table name, alias, or operator kind).
    pub label: String,
    /// Columns available from this node.
    pub available: Vec<Column>,
}

/// A hyperedge encoding a join predicate and its reordering constraints.
///
/// Corresponds to the hyperedge (TES-left, TES-right) from the paper (§2.5).
/// For a simple two-relation inner join, left = {node_a}, right = {node_b}.
/// For a multi-relation predicate or after TES extension, the sets may be larger.
pub struct Hyperedge {
    /// The predicate expression. `None` for cross-product dummy edges.
    pub predicate: Option<Expr>,
    /// Left side of the hyperedge as a bitset.
    pub left: NodeSet,
    /// Right side of the hyperedge as a bitset.
    pub right: NodeSet,
    /// The IR operator this edge came from.
    pub source: Operator,
    /// Join type of the source operator (determines assoc/l-asscom/r-asscom).
    pub join_type: HyperedgeJoinType,
}

/// Join type classification used for compatibility table lookups.
/// Mirrors the paper's operator taxonomy (§2.1).
pub enum HyperedgeJoinType {
    /// Inner join or cross product (B / A in the paper).
    Inner,
    /// Left semi join, left anti join, or group join (Z in the paper).
    LeftSemi,
    /// Left outer join (E in the paper).
    LeftOuter,
    /// Full outer join (K in the paper).
    FullOuter,
    /// Left outer join with null-rejecting predicate on left ('E).
    LeftOuterNullRejectingLeft,
    /// Full outer join with null-rejecting predicate on left ('K).
    FullOuterNullRejectingLeft,
    /// Full outer join with null-rejecting predicate on right (K').
    FullOuterNullRejectingRight,
    /// Full outer join with null-rejecting predicates on both sides ('K').
    FullOuterNullRejectingBoth,
}

/// The hypergraph for one join group.
pub struct QueryHypergraph {
    pub nodes: Vec<HypergraphNode>,
    pub edges: Vec<Hyperedge>,
}
```

---

## TES Construction Algorithm

The implementation uses **CD-E** (Algorithm 3), which extends CD-A with:

1. extending with `TES(◦_a)` (not full subtree relations), and
2. gating each extension on connectivity checks to avoid redundant restrictions.

### CD-A (background)

```
fn compute_tes(join_op: Operator, ctx: &QueryContext) -> (NodeSet, NodeSet):
    // Start with SES split by which side of the join each column comes from
    tes_left  = ses_left(join_op)   // nodes whose columns appear in left predicate refs
    tes_right = ses_right(join_op)  // nodes whose columns appear in right predicate refs

    for ◦_a in operators_in_subtree(left_input(join_op)):
        if ¬assoc(◦_a, join_op):
            tes_left  |= left_relations(◦_a)
        if ¬l_asscom(◦_a, join_op):
            tes_left  |= right_relations(◦_a)

    for ◦_a in operators_in_subtree(right_input(join_op)):
        if ¬assoc(◦_a, join_op):
            tes_right |= right_relations(◦_a)
        if ¬r_asscom(◦_a, join_op):
            tes_right |= left_relations(◦_a)

    return (tes_left, tes_right)
```

The compatibility predicates `assoc`, `l_asscom`, `r_asscom` are looked up from the
tables in the paper (Tables 1–3). Inner-join-only groups still degenerate to TES == SES,
but mixed join types use the full compatibility checks.

### CD-E (implemented)

CD-E amends CD-A with:
1. Replace `left_relations(◦_a)` / `right_relations(◦_a)` with `TES-left(◦_a)` /
   `TES-right(◦_a)` (propagate already-computed TES, not raw subtree relations).
2. Gate each extension on a connectivity check: only extend TES if removing the edge
   for `◦_a` would still leave the relevant relation sets connected in the query graph.
   If not connected, the restriction is redundant (the reordering would require a cross
   product anyway).

---

## Hypergraph Construction

```
fn build_hypergraph(ctx, analyses, join_group_root) -> QueryHypergraph:
    nodes    = []
    edges    = []
    node_map: HashMap<Operator, NodeId> = {}

    fn collect(op):
        match ctx.operator(op):
            Join(j):
                collect(j.outer)
                collect(j.inner)
                for predicate in conjuncts(j.on, ctx):
                    (left_ses, right_ses) = split_ses(predicate, node_map, ctx)
                    (tes_l, tes_r) = cd_e(op, ctx, node_map)
                    edges.push(Hyperedge {
                        predicate,
                        left: tes_l, right: tes_r,
                        source: op,
                        join_type: classify(j.join_type),
                    })

            CrossProduct(cp):
                collect(cp.outer)
                collect(cp.inner)
                // Add a dummy always-true edge to keep the graph connected (§2.6)
                edges.push(Hyperedge {
                    predicate: None,
                    left: singleton(representative(cp.outer, node_map)),
                    right: singleton(representative(cp.inner, node_map)),
                    source: op,
                    join_type: HyperedgeJoinType::Inner,
                })

            // Selection, Map, Scan, TableFunction, Rename: all become nodes (§2.7)
            _ if is_leaf_or_unary_transparent(op):
                node_id = nodes.len()
                nodes.push(HypergraphNode {
                    root: op,
                    label: label_of(op, ctx),
                    available: available_columns(ctx, analyses, op),
                })
                node_map.insert(op, node_id)

            // Aggregation, Sort, Limit, Projection: opaque barrier
            _ (unary with blocking semantics):
                node_id = nodes.len()
                nodes.push(HypergraphNode {
                    root: op,
                    label: label_of(op, ctx),
                    available: available_columns(ctx, analyses, op),
                })
                node_map.insert(op, node_id)

    collect(join_group_root)
    QueryHypergraph { nodes, edges }
```

`conjuncts` flattens `AND`-connected `ExprData::Nary { op: NaryOp::And, .. }` and
`ExprData::Binary { op: BinaryOp::Eq/.. }` into atomic predicates (§4.5).

`split_ses` maps each free column in the predicate to the node that produces it via
`node_map`, then partitions into left/right based on which side of the join they appear.

`is_leaf_or_unary_transparent` returns true for `Scan`, `TableFunction`, `Rename`,
`Selection`, and `Map` — all treated as nodes per §2.7.

---

## Operator Compatibility Tables

Encode Tables 1–3 from the paper as a lookup function:

```rust
pub fn assoc(outer: HyperedgeJoinType, inner: HyperedgeJoinType) -> bool
pub fn l_asscom(outer: HyperedgeJoinType, inner: HyperedgeJoinType) -> bool
pub fn r_asscom(outer: HyperedgeJoinType, inner: HyperedgeJoinType) -> bool
```

The implementation includes the full compatibility tables used by TES construction.

---

## Pretty Printing

```
Hypergraph (3 nodes, 2 edges)

Nodes:
  [0] orders        @1   columns: [o_orderkey(#0), o_custkey(#1)]
  [1] customer      @3   columns: [c_custkey(#2), c_name(#3)]
  [2] lineitem      @5   columns: [l_orderkey(#4), l_suppkey(#5)]

Edges:
  e0  ({0}, {1})  @2  Inner  (o_custkey(#1) = c_custkey(#2))
  e1  ({0}, {2})  @4  Inner  (o_orderkey(#0) = l_orderkey(#4))
```

- `({0}, {1})` is `(TES-left, TES-right)` as node-index sets.
- When TES ≠ SES (outer joins), the SES is shown additionally: `SES={0}`.
- `@N` is the source `Operator` handle index.

```rust
impl QueryHypergraph {
    pub fn pretty(&self, ctx: &QueryContext) -> String
}
```

---

## File Layout

```
src/hypergraph.rs    # HypergraphNode, Hyperedge, HyperedgeJoinType, NodeId,
                     # QueryHypergraph, build_hypergraph, assoc/l_asscom/r_asscom,
                     # QueryHypergraph::pretty
```

Re-export from `lib.rs`:

```rust
pub mod hypergraph;
pub use hypergraph::{
    build_hypergraph, Hyperedge, HyperedgeJoinType, HypergraphNode, NodeId, QueryHypergraph,
};
```

---

## Tests

| Test | What it checks |
|---|---|
| `single_scan_one_node_no_edges` | Bare scan → 1 node, 0 edges |
| `inner_join_two_scans` | Two scans, inner join → 2 nodes, 1 edge, TES == SES |
| `cross_product_dummy_edge` | Cross product → 2 nodes, 1 dummy edge (§2.6) |
| `three_way_chain_join` | A ⋈ B ⋈ C → 3 nodes, 2 edges |
| `selection_between_joins_is_node` | Selection between joins becomes a node (§2.7) |
| `map_between_joins_is_node` | Map between joins becomes a node (§2.7) |
| `conjunctive_predicate_splits_edges` | `A.x=B.x AND A.y=B.y` → 2 edges (§4.5) |
| `outer_join_tes_extends_ses` | Left outer join → TES-right includes left subtree |
| `pretty_print_snapshot` | Text output matches expected snapshot |

---

## Implementation Status

- `src/hypergraph.rs` is implemented and re-exported from `src/lib.rs`.
- `NodeSet = u64` helpers (`nodeset_singleton`, `nodeset_min`, `nodeset_iter`) are in use.
- Hyperedges use `(left: NodeSet, right: NodeSet)` and optional predicates.
- TES construction uses CD-E and compatibility-table checks.
- Unit tests cover node/edge construction, predicate splitting, compatibility behavior, and pretty printing.

---

## Open Questions

- **Null-rejecting predicate detection**: The paper distinguishes `'E` (null-rejecting on
  left) from `E`. This requires inspecting predicate expressions to determine null
  rejection. Use the existing `ColumnNullability` analysis as a starting point.
- **Predicate decomposition (§4.5)**: Splitting conjunctive predicates into separate
  edges expands the search space significantly. The paper notes CD-E finds 96.3% of
  plans with decomposable predicates vs 81.3% for CD-A (Table 5).
- **Group-by as node**: The paper cites [Fent, Birler, Neumann 2022] for integrating
  group-by into the query graph but explicitly excludes it from this work. Defer.
- **Multi-group queries**: One `build_hypergraph` call covers one join group. A forest
  builder that identifies all join groups in a query is a follow-up.
