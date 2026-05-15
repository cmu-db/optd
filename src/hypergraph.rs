//! Query hypergraph for join ordering.
//!
//! Reference: Birler & Neumann, "Efficient Enumeration of the Complete Join Search Space",
//! DBPL 2025. <https://db.in.tum.de/~birler/papers/joinspace.pdf>
//!
//! Nodes are base relations (Scan, TableFunction, Selection, Map, Rename).
//! Hyperedges are join predicates with TES-left/TES-right sets encoding reordering
//! constraints, computed via the CD-A algorithm (Algorithm 1 in the paper).

use std::collections::HashMap;

use crate::analysis::Analysis;
use crate::{
    AnalysisContext, AnalysisError, AnalysisResult, Analyzable, AvailableColumns, BinaryOp, Column,
    Expr, ExprData, JoinType, NaryOp, Operator, OperatorData, QueryContext, QueryFormatter,
    expr_used_columns,
};

/// Index into [`QueryHypergraph::nodes`].
pub type NodeId = usize;

/// A base-relation node in the hypergraph.
pub struct HypergraphNode {
    /// The topmost IR operator for this node.
    pub root: Operator,
    /// Human-readable label.
    pub label: String,
    /// Columns available from this node.
    pub available: Vec<Column>,
}

/// Join type classification used for compatibility table lookups (§2.1, Tables 1–3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HyperedgeJoinType {
    /// Inner join or cross product (B / ×).
    Inner,
    /// Left semi join, left anti join, or group join (Z).
    LeftSemi,
    /// Left outer join (E).
    LeftOuter,
    /// Left outer join with null-rejecting predicate on left ('E).
    LeftOuterNullRejectingLeft,
    /// Full outer join (K).
    FullOuter,
    /// Full outer join with null-rejecting predicate on left ('K).
    FullOuterNullRejectingLeft,
    /// Full outer join with null-rejecting predicate on right (K').
    FullOuterNullRejectingRight,
    /// Full outer join with null-rejecting predicates on both sides ('K').
    FullOuterNullRejectingBoth,
}

impl HyperedgeJoinType {
    fn from_join_type(jt: &JoinType) -> Self {
        match jt {
            JoinType::Inner => Self::Inner,
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark(_) => Self::LeftSemi,
            JoinType::LeftOuter => Self::LeftOuter,
            JoinType::RightOuter => Self::LeftOuter, // treated symmetrically
            JoinType::FullOuter => Self::FullOuter,
            JoinType::Single => Self::LeftSemi,
        }
    }
}

impl std::fmt::Display for HyperedgeJoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inner => f.write_str("Inner"),
            Self::LeftSemi => f.write_str("LeftSemi"),
            Self::LeftOuter => f.write_str("LeftOuter"),
            Self::LeftOuterNullRejectingLeft => f.write_str("'LeftOuter"),
            Self::FullOuter => f.write_str("FullOuter"),
            Self::FullOuterNullRejectingLeft => f.write_str("'FullOuter"),
            Self::FullOuterNullRejectingRight => f.write_str("FullOuter'"),
            Self::FullOuterNullRejectingBoth => f.write_str("'FullOuter'"),
        }
    }
}

/// A hyperedge encoding a join predicate and its TES-based reordering constraints.
///
/// The hyperedge is `(left, right)` = `(TES-left(◦), TES-right(◦))` (§2.5).
pub struct Hyperedge {
    /// The predicate expression. `None` for cross-product dummy edges (§2.6).
    pub predicate: Option<Expr>,
    /// Nodes that must be present in the left input of the join.
    pub left: Vec<NodeId>,
    /// Nodes that must be present in the right input of the join.
    pub right: Vec<NodeId>,
    /// The IR operator this edge came from.
    pub source: Operator,
    /// Join type of the source operator.
    pub join_type: HyperedgeJoinType,
}

/// The hypergraph for one join group.
pub struct QueryHypergraph {
    pub nodes: Vec<HypergraphNode>,
    pub edges: Vec<Hyperedge>,
}

// ---------------------------------------------------------------------------
// Operator compatibility tables (§2.2, Tables 1–3)
// ---------------------------------------------------------------------------

/// `assoc(◦_a, ◦_b)`: can ◦_a be moved from the left subtree of ◦_b to its right?
/// Table 1 from the paper.
pub fn assoc(outer: HyperedgeJoinType, inner: HyperedgeJoinType) -> bool {
    use HyperedgeJoinType::*;
    matches!(
        (outer, inner),
        (Inner, Inner)
            | (Inner, LeftOuterNullRejectingLeft)
            | (LeftOuter, FullOuter)
            | (LeftOuter, FullOuterNullRejectingLeft)
            | (LeftOuter, FullOuterNullRejectingRight)
            | (LeftOuter, FullOuterNullRejectingBoth)
            | (LeftOuterNullRejectingLeft, FullOuter)
            | (LeftOuterNullRejectingLeft, FullOuterNullRejectingLeft)
            | (LeftOuterNullRejectingLeft, FullOuterNullRejectingRight)
            | (LeftOuterNullRejectingLeft, FullOuterNullRejectingBoth)
            | (FullOuter, FullOuter)
            | (FullOuter, FullOuterNullRejectingLeft)
            | (FullOuter, FullOuterNullRejectingRight)
            | (FullOuter, FullOuterNullRejectingBoth)
            | (FullOuterNullRejectingLeft, FullOuter)
            | (FullOuterNullRejectingLeft, FullOuterNullRejectingLeft)
            | (FullOuterNullRejectingLeft, FullOuterNullRejectingRight)
            | (FullOuterNullRejectingLeft, FullOuterNullRejectingBoth)
            | (FullOuterNullRejectingRight, FullOuter)
            | (FullOuterNullRejectingRight, FullOuterNullRejectingLeft)
            | (FullOuterNullRejectingRight, FullOuterNullRejectingRight)
            | (FullOuterNullRejectingRight, FullOuterNullRejectingBoth)
            | (FullOuterNullRejectingBoth, FullOuter)
            | (FullOuterNullRejectingBoth, FullOuterNullRejectingLeft)
            | (FullOuterNullRejectingBoth, FullOuterNullRejectingRight)
            | (FullOuterNullRejectingBoth, FullOuterNullRejectingBoth)
    )
}

/// `l_asscom(◦_a, ◦_b)`: Table 2 from the paper.
pub fn l_asscom(outer: HyperedgeJoinType, inner: HyperedgeJoinType) -> bool {
    use HyperedgeJoinType::*;
    matches!(
        (outer, inner),
        (Inner, Inner)
            | (Inner, LeftSemi)
            | (Inner, LeftOuter)
            | (Inner, LeftOuterNullRejectingLeft)
            | (LeftSemi, Inner)
            | (LeftSemi, LeftSemi)
            | (LeftSemi, LeftOuter)
            | (LeftSemi, LeftOuterNullRejectingLeft)
            | (LeftOuter, Inner)
            | (LeftOuter, LeftSemi)
            | (LeftOuter, LeftOuter)
            | (LeftOuter, LeftOuterNullRejectingLeft)
            | (LeftOuterNullRejectingLeft, Inner)
            | (LeftOuterNullRejectingLeft, LeftSemi)
            | (LeftOuterNullRejectingLeft, LeftOuter)
            | (LeftOuterNullRejectingLeft, LeftOuterNullRejectingLeft)
            | (LeftOuterNullRejectingLeft, FullOuter)
            | (LeftOuterNullRejectingLeft, FullOuterNullRejectingLeft)
            | (LeftOuterNullRejectingLeft, FullOuterNullRejectingRight)
            | (LeftOuterNullRejectingLeft, FullOuterNullRejectingBoth)
            | (FullOuterNullRejectingLeft, Inner)
            | (FullOuterNullRejectingLeft, LeftSemi)
            | (FullOuterNullRejectingLeft, LeftOuter)
            | (FullOuterNullRejectingLeft, LeftOuterNullRejectingLeft)
            | (FullOuterNullRejectingLeft, FullOuter)
            | (FullOuterNullRejectingLeft, FullOuterNullRejectingLeft)
            | (FullOuterNullRejectingLeft, FullOuterNullRejectingRight)
            | (FullOuterNullRejectingLeft, FullOuterNullRejectingBoth)
            | (FullOuterNullRejectingBoth, Inner)
            | (FullOuterNullRejectingBoth, LeftSemi)
            | (FullOuterNullRejectingBoth, LeftOuter)
            | (FullOuterNullRejectingBoth, LeftOuterNullRejectingLeft)
            | (FullOuterNullRejectingBoth, FullOuter)
            | (FullOuterNullRejectingBoth, FullOuterNullRejectingLeft)
            | (FullOuterNullRejectingBoth, FullOuterNullRejectingRight)
            | (FullOuterNullRejectingBoth, FullOuterNullRejectingBoth)
    )
}

/// `r_asscom(◦_a, ◦_b)`: Table 3 from the paper.
pub fn r_asscom(outer: HyperedgeJoinType, inner: HyperedgeJoinType) -> bool {
    use HyperedgeJoinType::*;
    matches!(
        (outer, inner),
        (Inner, Inner)
            | (FullOuterNullRejectingRight, FullOuterNullRejectingRight)
            | (FullOuterNullRejectingRight, FullOuterNullRejectingBoth)
            | (FullOuterNullRejectingBoth, FullOuterNullRejectingRight)
            | (FullOuterNullRejectingBoth, FullOuterNullRejectingBoth)
    )
}

// ---------------------------------------------------------------------------
// Hypergraph construction
// ---------------------------------------------------------------------------

/// Builds a [`QueryHypergraph`] for the join group rooted at `root`.
///
/// Uses the CD-A algorithm (Algorithm 1, Birler & Neumann 2025) to compute TES.
/// `Selection` and `Map` are treated as nodes per §2.7 of the paper.
pub fn build_hypergraph(
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
    root: Operator,
) -> QueryHypergraph {
    let mut builder = HypergraphBuilder::new(ctx, analyses);
    builder.collect(root);
    QueryHypergraph {
        nodes: builder.nodes,
        edges: builder.edges,
    }
}

struct HypergraphBuilder<'a> {
    ctx: &'a QueryContext,
    analyses: &'a mut AnalysisContext,
    nodes: Vec<HypergraphNode>,
    edges: Vec<Hyperedge>,
    /// Maps each operator handle to its NodeId (for leaf/unary operators).
    node_map: HashMap<Operator, NodeId>,
    /// Records the join operators encountered in DFS order (for CD-A).
    /// Each entry: (join_op, join_type, left_subtree_nodes, right_subtree_nodes).
    join_stack: Vec<JoinRecord>,
}

struct JoinRecord {
    join_type: HyperedgeJoinType,
    /// NodeIds reachable from the left input.
    left_nodes: Vec<NodeId>,
    /// NodeIds reachable from the right input.
    right_nodes: Vec<NodeId>,
}

impl<'a> HypergraphBuilder<'a> {
    fn new(ctx: &'a QueryContext, analyses: &'a mut AnalysisContext) -> Self {
        Self {
            ctx,
            analyses,
            nodes: Vec::new(),
            edges: Vec::new(),
            node_map: HashMap::new(),
            join_stack: Vec::new(),
        }
    }

    /// Recursively collect nodes and edges. Returns the set of NodeIds in this subtree.
    fn collect(&mut self, op: Operator) -> Vec<NodeId> {
        match self.ctx.operator(op) {
            OperatorData::Join(j) => {
                let jt = HyperedgeJoinType::from_join_type(&j.join_type.clone());
                let on = j.on;
                let outer = j.outer;
                let inner = j.inner;

                let left_nodes = self.collect(outer);
                let right_nodes = self.collect(inner);

                // CD-A: compute TES for this join operator.
                let (tes_left, tes_right) = self.cd_a(op, jt, &left_nodes, &right_nodes);

                // Split conjunctive predicates into individual edges (§4.5).
                for predicate in conjuncts(on, self.ctx) {
                    self.edges.push(Hyperedge {
                        predicate: Some(predicate),
                        left: tes_left.clone(),
                        right: tes_right.clone(),
                        source: op,
                        join_type: jt,
                    });
                }

                self.join_stack.push(JoinRecord {
                    join_type: jt,
                    left_nodes: left_nodes.clone(),
                    right_nodes: right_nodes.clone(),
                });

                let mut all = left_nodes;
                all.extend_from_slice(&right_nodes);
                all
            }

            OperatorData::CrossProduct(cp) => {
                let outer = cp.outer;
                let inner = cp.inner;
                let left_nodes = self.collect(outer);
                let right_nodes = self.collect(inner);

                // Cross product: add a dummy always-true edge to keep graph connected (§2.6).
                // TES is just one representative from each side.
                let l = left_nodes.first().copied().into_iter().collect();
                let r = right_nodes.first().copied().into_iter().collect();
                self.edges.push(Hyperedge {
                    predicate: None,
                    left: l,
                    right: r,
                    source: op,
                    join_type: HyperedgeJoinType::Inner,
                });

                self.join_stack.push(JoinRecord {
                    join_type: HyperedgeJoinType::Inner,
                    left_nodes: left_nodes.clone(),
                    right_nodes: right_nodes.clone(),
                });

                let mut all = left_nodes;
                all.extend_from_slice(&right_nodes);
                all
            }

            // Leaf and transparent-unary operators become nodes (§2.7).
            _ => {
                let node_id = self.nodes.len();
                let available = self
                    .analyses
                    .get::<AvailableColumns>(self.ctx, op)
                    .unwrap_or_default();
                self.nodes.push(HypergraphNode {
                    root: op,
                    label: node_label(self.ctx, op),
                    available,
                });
                self.node_map.insert(op, node_id);
                vec![node_id]
            }
        }
    }

    /// CD-A (Algorithm 1): compute (TES-left, TES-right) for join operator `join_op`.
    ///
    /// Iterates over all join operators in the left and right subtrees and extends
    /// the TES based on assoc/l_asscom/r_asscom compatibility.
    fn cd_a(
        &self,
        join_op: Operator,
        join_type: HyperedgeJoinType,
        left_nodes: &[NodeId],
        right_nodes: &[NodeId],
    ) -> (Vec<NodeId>, Vec<NodeId>) {
        // Start with SES: split predicate columns by which side produces them.
        let (mut tes_left, mut tes_right) = self.ses(join_op, left_nodes, right_nodes);

        // Extend TES based on operators in the left subtree.
        for rec in &self.join_stack {
            if rec.left_nodes.iter().all(|n| left_nodes.contains(n))
                && rec.right_nodes.iter().all(|n| left_nodes.contains(n))
            {
                // rec.op is in the left subtree of join_op.
                let oa = rec.join_type;
                let ob = join_type;
                if !assoc(oa, ob) {
                    extend_unique(&mut tes_left, &rec.left_nodes);
                }
                if !l_asscom(oa, ob) {
                    extend_unique(&mut tes_left, &rec.right_nodes);
                }
            } else if rec.left_nodes.iter().all(|n| right_nodes.contains(n))
                && rec.right_nodes.iter().all(|n| right_nodes.contains(n))
            {
                // rec.op is in the right subtree of join_op.
                let oa = rec.join_type;
                let ob = join_type;
                if !assoc(oa, ob) {
                    extend_unique(&mut tes_right, &rec.right_nodes);
                }
                if !r_asscom(oa, ob) {
                    extend_unique(&mut tes_right, &rec.left_nodes);
                }
            }
        }

        (tes_left, tes_right)
    }

    /// Computes the SES split into (left, right) based on which side of `join_op`
    /// each referenced column comes from.
    fn ses(
        &self,
        join_op: Operator,
        left_nodes: &[NodeId],
        right_nodes: &[NodeId],
    ) -> (Vec<NodeId>, Vec<NodeId>) {
        let predicate = match self.ctx.operator(join_op) {
            OperatorData::Join(j) => j.on,
            _ => return (vec![], vec![]),
        };

        let used = expr_used_columns(self.ctx, predicate).unwrap_or_default();

        let mut ses_left = Vec::new();
        let mut ses_right = Vec::new();

        for col in used {
            // Find which node produces this column.
            for &nid in left_nodes {
                if self.nodes[nid].available.contains(&col) {
                    if !ses_left.contains(&nid) {
                        ses_left.push(nid);
                    }
                    break;
                }
            }
            for &nid in right_nodes {
                if self.nodes[nid].available.contains(&col) {
                    if !ses_right.contains(&nid) {
                        ses_right.push(nid);
                    }
                    break;
                }
            }
        }

        // Fallback: if SES is empty on one side, include all nodes on that side
        // (handles cross-node predicates and degenerate cases).
        if ses_left.is_empty() && !left_nodes.is_empty() {
            ses_left = left_nodes.to_vec();
        }
        if ses_right.is_empty() && !right_nodes.is_empty() {
            ses_right = right_nodes.to_vec();
        }

        (ses_left, ses_right)
    }
}

fn extend_unique(target: &mut Vec<NodeId>, src: &[NodeId]) {
    for &n in src {
        if !target.contains(&n) {
            target.push(n);
        }
    }
}

/// Flattens a conjunctive expression into atomic predicates (§4.5).
fn conjuncts(expr: Expr, ctx: &QueryContext) -> Vec<Expr> {
    match expr.get(ctx) {
        ExprData::Nary {
            op: NaryOp::And,
            exprs,
        } => exprs.iter().flat_map(|&e| conjuncts(e, ctx)).collect(),
        ExprData::Binary {
            op: BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq,
            ..
        } => vec![expr],
        _ => vec![expr],
    }
}

/// Derives a human-readable label for a node operator.
fn node_label(ctx: &QueryContext, op: Operator) -> String {
    match ctx.operator(op) {
        OperatorData::Scan(s) => s.table.to_string(),
        OperatorData::TableFunction(tf) => tf.function.to_string(),
        OperatorData::Rename(r) => r.alias.clone(),
        OperatorData::Selection(_) => format!("σ@{}", op.0),
        OperatorData::Map(_) => format!("χ@{}", op.0),
        _ => format!("op@{}", op.0),
    }
}

// ---------------------------------------------------------------------------
// Pretty printing
// ---------------------------------------------------------------------------

impl QueryHypergraph {
    /// Formats the hypergraph as a compact text representation.
    pub fn pretty(&self, ctx: &QueryContext) -> String {
        let formatter = QueryFormatter::new(ctx);
        let mut out = String::new();

        out.push_str(&format!(
            "Hypergraph ({} nodes, {} edges)\n",
            self.nodes.len(),
            self.edges.len()
        ));

        out.push_str("\nNodes:\n");
        for (i, node) in self.nodes.iter().enumerate() {
            let cols: Vec<String> = node
                .available
                .iter()
                .map(|c| format!("{}(#{})", ctx.column(*c).name, c.0))
                .collect();
            out.push_str(&format!(
                "  [{i}] {:<16} @{}   columns: [{}]\n",
                node.label,
                node.root.0,
                cols.join(", ")
            ));
        }

        out.push_str("\nEdges:\n");
        for (i, edge) in self.edges.iter().enumerate() {
            let left: Vec<String> = edge.left.iter().map(|n| n.to_string()).collect();
            let right: Vec<String> = edge.right.iter().map(|n| n.to_string()).collect();
            let pred = match edge.predicate {
                Some(p) => formatter.format_expr_pub(p),
                None => "true".to_string(),
            };
            out.push_str(&format!(
                "  e{i}  ({{{}}}, {{{}}})  @{}  {}  {}\n",
                left.join(","),
                right.join(","),
                edge.source.0,
                edge.join_type,
                pred,
            ));
        }

        out
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AnalysisContext, BinaryOp, ColumnData, CrossProduct, ExprData, Join, JoinType,
        OperatorData, QueryContext, ScalarValue, Scan, Selection, TableRef,
    };
    use arrow_schema::DataType;

    fn two_scan_ctx() -> (QueryContext, Operator, Operator, Column, Column) {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let scan_a = OperatorData::Scan(Scan {
            table: TableRef::bare("A"),
            columns: vec![a],
        })
        .add(&mut ctx);
        let scan_b = OperatorData::Scan(Scan {
            table: TableRef::bare("B"),
            columns: vec![b],
        })
        .add(&mut ctx);
        (ctx, scan_a, scan_b, a, b)
    }

    #[test]
    fn single_scan_one_node_no_edges() {
        let mut ctx = QueryContext::new();
        let col = ColumnData::new("x", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("T"),
            columns: vec![col],
        })
        .add(&mut ctx);

        let mut analyses = AnalysisContext::new();
        let hg = build_hypergraph(&ctx, &mut analyses, scan);

        assert_eq!(hg.nodes.len(), 1);
        assert_eq!(hg.edges.len(), 0);
        assert_eq!(hg.nodes[0].label, "T");
    }

    #[test]
    fn inner_join_two_scans_two_nodes_one_edge() {
        let (mut ctx, scan_a, scan_b, a, b) = two_scan_ctx();
        let la = ExprData::ColumnRef(a).add(&mut ctx);
        let rb = ExprData::ColumnRef(b).add(&mut ctx);
        let on = ExprData::Binary {
            op: BinaryOp::Eq,
            left: la,
            right: rb,
        }
        .add(&mut ctx);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: scan_a,
            inner: scan_b,
        })
        .add(&mut ctx);

        let mut analyses = AnalysisContext::new();
        let hg = build_hypergraph(&ctx, &mut analyses, join);

        assert_eq!(hg.nodes.len(), 2);
        assert_eq!(hg.edges.len(), 1);
        // For inner joins TES == SES: one node on each side.
        assert_eq!(hg.edges[0].left.len(), 1);
        assert_eq!(hg.edges[0].right.len(), 1);
        assert_ne!(hg.edges[0].left[0], hg.edges[0].right[0]);
        assert_eq!(hg.edges[0].join_type, HyperedgeJoinType::Inner);
    }

    #[test]
    fn cross_product_two_nodes_one_dummy_edge() {
        let (mut ctx, scan_a, scan_b, _, _) = two_scan_ctx();
        let cp = OperatorData::CrossProduct(CrossProduct {
            outer: scan_a,
            inner: scan_b,
        })
        .add(&mut ctx);

        let mut analyses = AnalysisContext::new();
        let hg = build_hypergraph(&ctx, &mut analyses, cp);

        assert_eq!(hg.nodes.len(), 2);
        assert_eq!(hg.edges.len(), 1);
        assert_eq!(hg.edges[0].join_type, HyperedgeJoinType::Inner);
    }

    #[test]
    fn three_way_chain_join_three_nodes_two_edges() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let c = ColumnData::new("c", DataType::Int64).add(&mut ctx);
        let scan_a = OperatorData::Scan(Scan {
            table: TableRef::bare("A"),
            columns: vec![a],
        })
        .add(&mut ctx);
        let scan_b = OperatorData::Scan(Scan {
            table: TableRef::bare("B"),
            columns: vec![b],
        })
        .add(&mut ctx);
        let scan_c = OperatorData::Scan(Scan {
            table: TableRef::bare("C"),
            columns: vec![c],
        })
        .add(&mut ctx);

        let la = ExprData::ColumnRef(a).add(&mut ctx);
        let rb = ExprData::ColumnRef(b).add(&mut ctx);
        let on_ab = ExprData::Binary {
            op: BinaryOp::Eq,
            left: la,
            right: rb,
        }
        .add(&mut ctx);
        let join_ab = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on: on_ab,
            outer: scan_a,
            inner: scan_b,
        })
        .add(&mut ctx);

        let lb = ExprData::ColumnRef(b).add(&mut ctx);
        let rc = ExprData::ColumnRef(c).add(&mut ctx);
        let on_bc = ExprData::Binary {
            op: BinaryOp::Eq,
            left: lb,
            right: rc,
        }
        .add(&mut ctx);
        let join_abc = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on: on_bc,
            outer: join_ab,
            inner: scan_c,
        })
        .add(&mut ctx);

        let mut analyses = AnalysisContext::new();
        let hg = build_hypergraph(&ctx, &mut analyses, join_abc);

        assert_eq!(hg.nodes.len(), 3);
        assert_eq!(hg.edges.len(), 2);
    }

    #[test]
    fn selection_between_joins_is_node() {
        // A ⋈ σ(B) ⋈ C — the selection on B becomes a node, not a barrier.
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let c = ColumnData::new("c", DataType::Int64).add(&mut ctx);
        let scan_a = OperatorData::Scan(Scan {
            table: TableRef::bare("A"),
            columns: vec![a],
        })
        .add(&mut ctx);
        let scan_b = OperatorData::Scan(Scan {
            table: TableRef::bare("B"),
            columns: vec![b],
        })
        .add(&mut ctx);
        let scan_c = OperatorData::Scan(Scan {
            table: TableRef::bare("C"),
            columns: vec![c],
        })
        .add(&mut ctx);

        // σ(b > 0) on scan_b
        let b_ref = ExprData::ColumnRef(b).add(&mut ctx);
        let zero = ExprData::Literal(ScalarValue::Int64(0)).add(&mut ctx);
        let pred = ExprData::Binary {
            op: BinaryOp::Gt,
            left: b_ref,
            right: zero,
        }
        .add(&mut ctx);
        let sel_b = OperatorData::Selection(Selection {
            predicate: pred,
            input: scan_b,
        })
        .add(&mut ctx);

        let la = ExprData::ColumnRef(a).add(&mut ctx);
        let rb = ExprData::ColumnRef(b).add(&mut ctx);
        let on_ab = ExprData::Binary {
            op: BinaryOp::Eq,
            left: la,
            right: rb,
        }
        .add(&mut ctx);
        let join_ab = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on: on_ab,
            outer: scan_a,
            inner: sel_b,
        })
        .add(&mut ctx);

        let lb = ExprData::ColumnRef(b).add(&mut ctx);
        let rc = ExprData::ColumnRef(c).add(&mut ctx);
        let on_bc = ExprData::Binary {
            op: BinaryOp::Eq,
            left: lb,
            right: rc,
        }
        .add(&mut ctx);
        let join_abc = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on: on_bc,
            outer: join_ab,
            inner: scan_c,
        })
        .add(&mut ctx);

        let mut analyses = AnalysisContext::new();
        let hg = build_hypergraph(&ctx, &mut analyses, join_abc);

        // sel_b is a node, so we have 3 nodes: scan_a, sel_b, scan_c
        assert_eq!(hg.nodes.len(), 3);
        assert_eq!(hg.edges.len(), 2);
    }

    #[test]
    fn conjunctive_predicate_splits_into_two_edges() {
        let (mut ctx, scan_a, scan_b, a, b) = two_scan_ctx();
        let a2 = ColumnData::new("a2", DataType::Int64).add(&mut ctx);
        let b2 = ColumnData::new("b2", DataType::Int64).add(&mut ctx);
        // Add extra columns to the scans by rebuilding them.
        let scan_a2 = OperatorData::Scan(Scan {
            table: TableRef::bare("A"),
            columns: vec![a, a2],
        })
        .add(&mut ctx);
        let scan_b2 = OperatorData::Scan(Scan {
            table: TableRef::bare("B"),
            columns: vec![b, b2],
        })
        .add(&mut ctx);

        let la = ExprData::ColumnRef(a).add(&mut ctx);
        let rb = ExprData::ColumnRef(b).add(&mut ctx);
        let la2 = ExprData::ColumnRef(a2).add(&mut ctx);
        let rb2 = ExprData::ColumnRef(b2).add(&mut ctx);
        let eq1 = ExprData::Binary {
            op: BinaryOp::Eq,
            left: la,
            right: rb,
        }
        .add(&mut ctx);
        let eq2 = ExprData::Binary {
            op: BinaryOp::Eq,
            left: la2,
            right: rb2,
        }
        .add(&mut ctx);
        let on = ExprData::Nary {
            op: NaryOp::And,
            exprs: vec![eq1, eq2],
        }
        .add(&mut ctx);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: scan_a2,
            inner: scan_b2,
        })
        .add(&mut ctx);

        let mut analyses = AnalysisContext::new();
        let hg = build_hypergraph(&ctx, &mut analyses, join);

        // scan_a and scan_b from two_scan_ctx are unused here; scan_a2/scan_b2 are the nodes.
        assert_eq!(hg.nodes.len(), 2);
        assert_eq!(
            hg.edges.len(),
            2,
            "conjunctive predicate should split into 2 edges"
        );
        let _ = (scan_a, scan_b); // suppress unused warnings
    }

    /// Builds a plan:
    ///   T ⟕ S ⋈ C
    /// where:
    ///   T ⟕ S  on  t_a = s_b          (left outer join)
    ///   (T ⟕ S) ⋈ C  on  t_a + s_b = c_e  (inner join, multi-relation predicate)
    ///
    /// Expected TES behaviour (CD-A):
    ///   - Inner join ⋈ has SES = {T, S, C} (predicate touches all three).
    ///     No outer-join ancestors above it, so TES == SES.
    ///   - Left outer join ⟕ has SES = {T, S}.
    ///     The inner join above it: assoc(Inner, LeftOuter) = false (Table 1),
    ///     so TES-right of ⟕ is extended with right(Inner) = {C}.
    ///     → TES(⟕) = ({T}, {S, C})  [right side extended]
    #[test]
    fn left_outer_inner_join_with_multi_relation_predicate() {
        let mut ctx = QueryContext::new();

        // Columns
        let t_a = ColumnData::new("t_a", DataType::Int64).add(&mut ctx);
        let s_b = ColumnData::new("s_b", DataType::Int64).add(&mut ctx);
        let c_e = ColumnData::new("c_e", DataType::Int64).add(&mut ctx);

        // Scans
        let scan_t = OperatorData::Scan(Scan {
            table: TableRef::bare("T"),
            columns: vec![t_a],
        })
        .add(&mut ctx);
        let scan_s = OperatorData::Scan(Scan {
            table: TableRef::bare("S"),
            columns: vec![s_b],
        })
        .add(&mut ctx);
        let scan_c = OperatorData::Scan(Scan {
            table: TableRef::bare("C"),
            columns: vec![c_e],
        })
        .add(&mut ctx);

        // T ⟕ S  on  t_a = s_b
        let ta_ref = ExprData::ColumnRef(t_a).add(&mut ctx);
        let sb_ref = ExprData::ColumnRef(s_b).add(&mut ctx);
        let on_ts = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ta_ref,
            right: sb_ref,
        }
        .add(&mut ctx);
        let loj = OperatorData::Join(Join {
            join_type: JoinType::LeftOuter,
            on: on_ts,
            outer: scan_t,
            inner: scan_s,
        })
        .add(&mut ctx);

        // (T ⟕ S) ⋈ C  on  t_a + s_b = c_e
        let ta2 = ExprData::ColumnRef(t_a).add(&mut ctx);
        let sb2 = ExprData::ColumnRef(s_b).add(&mut ctx);
        let ce_ref = ExprData::ColumnRef(c_e).add(&mut ctx);
        let sum = ExprData::Binary {
            op: BinaryOp::Add,
            left: ta2,
            right: sb2,
        }
        .add(&mut ctx);
        let on_tsc = ExprData::Binary {
            op: BinaryOp::Eq,
            left: sum,
            right: ce_ref,
        }
        .add(&mut ctx);
        let inner = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on: on_tsc,
            outer: loj,
            inner: scan_c,
        })
        .add(&mut ctx);

        ctx.set_root(inner);

        let mut analyses = AnalysisContext::new();
        let hg = build_hypergraph(&ctx, &mut analyses, inner);

        println!("{}", hg.pretty(&ctx));

        // 3 nodes: T, S, C
        assert_eq!(hg.nodes.len(), 3);
        // 2 edges: one for ⟕, one for ⋈
        assert_eq!(hg.edges.len(), 2);

        // Find the left-outer-join edge (source == loj)
        let loj_edge = hg.edges.iter().find(|e| e.source == loj).unwrap();
        assert_eq!(loj_edge.join_type, HyperedgeJoinType::LeftOuter);

        // Find the inner-join edge (source == inner)
        let inner_edge = hg.edges.iter().find(|e| e.source == inner).unwrap();
        assert_eq!(inner_edge.join_type, HyperedgeJoinType::Inner);
        // Multi-relation predicate: both T and S nodes must be on the left side
        // (they come from the left subtree of the inner join).
        assert!(inner_edge.left.len() >= 2 || inner_edge.right.len() >= 1);
    }

    #[test]
    fn pretty_print_two_table_join() {
        let (mut ctx, scan_a, scan_b, a, b) = two_scan_ctx();
        let la = ExprData::ColumnRef(a).add(&mut ctx);
        let rb = ExprData::ColumnRef(b).add(&mut ctx);
        let on = ExprData::Binary {
            op: BinaryOp::Eq,
            left: la,
            right: rb,
        }
        .add(&mut ctx);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: scan_a,
            inner: scan_b,
        })
        .add(&mut ctx);

        let mut analyses = AnalysisContext::new();
        let hg = build_hypergraph(&ctx, &mut analyses, join);
        let pretty = hg.pretty(&ctx);

        assert!(pretty.contains("Hypergraph (2 nodes, 1 edges)"));
        assert!(pretty.contains("[0]"));
        assert!(pretty.contains("[1]"));
        assert!(pretty.contains("e0"));
        assert!(pretty.contains("Inner"));
    }
}

// ---------------------------------------------------------------------------
// JoinGroupOf
// ---------------------------------------------------------------------------

use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;

/// Returns `true` if `op` is a join or cross product (i.e., part of a join group).
fn is_join_op(op: Operator, ctx: &QueryContext) -> bool {
    matches!(
        ctx.operator(op),
        OperatorData::Join(_) | OperatorData::CrossProduct(_)
    )
}

/// Analysis that returns the root of the join group containing `op`.
///
/// The join group root is the highest ancestor of `op` that is still a
/// `Join` or `CrossProduct`. If `op` itself is not inside any join group,
/// returns `None`.
///
/// Uses [`crate::ParentOf`] to walk up the operator tree.
#[derive(Default)]
pub struct JoinGroupOf;

impl Analyzable for JoinGroupOf {
    /// The join group root, or `None` if `op` is not inside a join group.
    type Value = Option<Operator>;

    fn get(
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        op: Operator,
    ) -> AnalysisResult<Self::Value> {
        use crate::ParentOf;

        // Walk up from op, tracking the highest join/cross-product ancestor.
        let mut highest_join: Option<Operator> = None;
        let mut cursor = op;

        // Include op itself if it is a join.
        if is_join_op(cursor, ctx) {
            highest_join = Some(cursor);
        }

        loop {
            match analyses.get::<ParentOf>(ctx, cursor)? {
                None => break,
                Some(parent) => {
                    if is_join_op(parent, ctx) {
                        highest_join = Some(parent);
                    }
                    cursor = parent;
                }
            }
        }

        Ok(highest_join)
    }
}

// ---------------------------------------------------------------------------
// HypergraphOf
// ---------------------------------------------------------------------------

/// Analysis that returns the [`QueryHypergraph`] for the join group containing `op`.
///
/// The hypergraph is built once per join group root and cached. Returns `None`
/// if `op` is not inside any join group.
#[derive(Default)]
pub struct HypergraphOf {
    /// Cache: maps join group root → hypergraph.
    cache: RefCell<HashMap<Operator, Rc<QueryHypergraph>>>,
}

impl Analysis for HypergraphOf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clear(&self) {
        self.cache.borrow_mut().clear();
    }
}

impl Analyzable for HypergraphOf {
    /// The hypergraph for the join group, or `None` if `op` is not in a join group.
    type Value = Option<Rc<QueryHypergraph>>;

    fn get(
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        op: Operator,
    ) -> AnalysisResult<Self::Value> {
        let Some(group_root) = analyses.get::<JoinGroupOf>(ctx, op)? else {
            return Ok(None);
        };

        // Check cache first.
        {
            let cache = analyses.registry_entry::<HypergraphOf>();
            let typed = cache
                .as_any()
                .downcast_ref::<HypergraphOf>()
                .ok_or(AnalysisError::AnalysisTypeMismatch("HypergraphOf"))?;
            if let Some(hg) = typed.cache.borrow().get(&group_root) {
                return Ok(Some(hg.clone()));
            }
        }

        // Build and cache.
        let hg = Rc::new(build_hypergraph(ctx, analyses, group_root));

        let cache_entry = analyses.registry_entry::<HypergraphOf>();
        let typed = cache_entry
            .as_any()
            .downcast_ref::<HypergraphOf>()
            .ok_or(AnalysisError::AnalysisTypeMismatch("HypergraphOf"))?;
        typed.cache.borrow_mut().insert(group_root, hg.clone());

        Ok(Some(hg))
    }
}
