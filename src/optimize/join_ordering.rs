//! Join ordering pass using DPhyp (Moerkotte & Neumann, SIGMOD 2008).
//!
//! Finds the optimal bushy join tree for each join group in the query by
//! enumerating all csg-cmp-pairs of the query hypergraph and filling a DP table.

use std::collections::HashMap;

use crate::hypergraph::{NodeSet, QueryHypergraph, nodeset_min, nodeset_singleton};
use crate::{
    ExprData, Join, JoinType, NaryOp, Operator, OperatorData, QueryContext, build_hypergraph,
};

use super::{OptimizeResult, Pass, PassResult, QueryPass};
use crate::OptimizerContext;

// ---------------------------------------------------------------------------
// Statistics trait
// ---------------------------------------------------------------------------

/// Provides cardinality and selectivity estimates for cost computation.
pub trait Statistics: Send + Sync {
    fn cardinality(&self, node_idx: usize, hg: &QueryHypergraph) -> f64;
    fn selectivity(&self, left: NodeSet, right: NodeSet, hg: &QueryHypergraph) -> f64;
}

/// Uniform statistics: 1000 rows per relation, 0.1 selectivity per join.
pub struct UniformStatistics;

impl Statistics for UniformStatistics {
    fn cardinality(&self, _node_idx: usize, _hg: &QueryHypergraph) -> f64 {
        1000.0
    }
    fn selectivity(&self, _left: NodeSet, _right: NodeSet, _hg: &QueryHypergraph) -> f64 {
        0.1
    }
}

// ---------------------------------------------------------------------------
// JoinTree: the output of DPhyp
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum JoinTree {
    Leaf(usize), // node index
    Join {
        left: Box<JoinTree>,
        right: Box<JoinTree>,
        /// Predicates from connecting hyperedges (indices into hg.edges).
        edge_indices: Vec<usize>,
        cardinality: f64,
    },
}

impl JoinTree {
    fn leaf_count(&self) -> usize {
        match self {
            JoinTree::Leaf(_) => 1,
            JoinTree::Join { left, right, .. } => left.leaf_count() + right.leaf_count(),
        }
    }
}

// ---------------------------------------------------------------------------
// DPhyp
// ---------------------------------------------------------------------------

struct DPhyp<'a> {
    hg: &'a QueryHypergraph,
    stats: &'a dyn Statistics,
    /// DP table: NodeSet → (cost, JoinTree).
    dp: HashMap<NodeSet, (f64, JoinTree)>,
}

impl<'a> DPhyp<'a> {
    fn new(hg: &'a QueryHypergraph, stats: &'a dyn Statistics) -> Self {
        Self {
            hg,
            stats,
            dp: HashMap::new(),
        }
    }

    fn solve(&mut self) -> Option<JoinTree> {
        let n = self.hg.nodes.len();
        if n == 0 {
            return None;
        }

        // Initialise singletons.
        for i in 0..n {
            let s = nodeset_singleton(i);
            let cost = self.stats.cardinality(i, self.hg);
            self.dp.insert(s, (cost, JoinTree::Leaf(i)));
        }

        // Process nodes in descending order (largest index first).
        for v in (0..n).rev() {
            let sv = nodeset_singleton(v);
            self.emit_csg(sv);
            let bv: NodeSet = (1u64 << v).wrapping_sub(1) | sv; // all nodes ≤ v
            self.enumerate_csg_rec(sv, bv);
        }

        let all: NodeSet = (1u64 << n) - 1;
        self.dp.get(&all).map(|(_, t)| t.clone())
    }

    fn enumerate_csg_rec(&mut self, s1: NodeSet, x: NodeSet) {
        let nbrs = self.neighborhood(s1, x);
        // Collect non-empty subsets of nbrs.
        let subsets = non_empty_subsets(nbrs);
        // First pass: emit csgs.
        for &n_sub in &subsets {
            let candidate = s1 | n_sub;
            if self.dp.contains_key(&candidate) {
                self.emit_csg(candidate);
            }
        }
        // Second pass: recurse.
        for &n_sub in &subsets {
            self.enumerate_csg_rec(s1 | n_sub, x | nbrs);
        }
    }

    fn emit_csg(&mut self, s1: NodeSet) {
        let x = s1 | self.b_min(s1);
        let nbrs = self.neighborhood(s1, x);
        // Iterate neighbors in descending order.
        let mut v = nbrs;
        while v != 0 {
            let bit = 1u64 << (63 - v.leading_zeros() as u32);
            v &= !bit;
            let s2 = bit;
            if self.has_edge(s1, s2) {
                self.emit_csg_cmp(s1, s2);
            }
            self.enumerate_cmp_rec(s1, s2, x);
        }
    }

    fn enumerate_cmp_rec(&mut self, s1: NodeSet, s2: NodeSet, x: NodeSet) {
        let nbrs = self.neighborhood(s2, x);
        let subsets = non_empty_subsets(nbrs);
        for &n_sub in &subsets {
            let candidate = s2 | n_sub;
            if self.dp.contains_key(&candidate) && self.has_edge(s1, candidate) {
                self.emit_csg_cmp(s1, candidate);
            }
        }
        for &n_sub in &subsets {
            self.enumerate_cmp_rec(s1, s2 | n_sub, x | nbrs);
        }
    }

    fn emit_csg_cmp(&mut self, s1: NodeSet, s2: NodeSet) {
        let Some((cost1, tree1)) = self.dp.get(&s1).cloned() else {
            return;
        };
        let Some((cost2, tree2)) = self.dp.get(&s2).cloned() else {
            return;
        };

        // Collect connecting edge indices.
        let edge_indices: Vec<usize> = self
            .hg
            .edges
            .iter()
            .enumerate()
            .filter(|(_, e)| {
                (e.left & s1 == e.left && e.right & s2 == e.right)
                    || (e.left & s2 == e.left && e.right & s1 == e.right)
            })
            .map(|(i, _)| i)
            .collect();

        if edge_indices.is_empty() {
            return;
        }

        let sel = self.stats.selectivity(s1, s2, self.hg);
        let card = cost1 * cost2 * sel;
        let new_cost = cost1 + cost2 + card;
        let combined = s1 | s2;

        let better = self
            .dp
            .get(&combined)
            .map_or(true, |&(existing_cost, _)| new_cost < existing_cost);

        if better {
            self.dp.insert(
                combined,
                (
                    new_cost,
                    JoinTree::Join {
                        left: Box::new(tree1),
                        right: Box::new(tree2),
                        edge_indices,
                        cardinality: card,
                    },
                ),
            );
        }
    }

    /// Neighborhood of `s` excluding nodes in `x`.
    fn neighborhood(&self, s: NodeSet, x: NodeSet) -> NodeSet {
        let mut result: NodeSet = 0;
        for edge in &self.hg.edges {
            if edge.left & s == edge.left && edge.right & s == 0 && edge.right & x == 0 {
                result |= nodeset_singleton(nodeset_min(edge.right));
            }
            if edge.right & s == edge.right && edge.left & s == 0 && edge.left & x == 0 {
                result |= nodeset_singleton(nodeset_min(edge.left));
            }
        }
        result
    }

    /// Returns true if there is a hyperedge connecting s1 to s2.
    fn has_edge(&self, s1: NodeSet, s2: NodeSet) -> bool {
        self.hg.edges.iter().any(|e| {
            (e.left & s1 == e.left && e.right & s2 == e.right)
                || (e.left & s2 == e.left && e.right & s1 == e.right)
        })
    }

    /// B_min(s): all nodes with index ≤ min(s), used as exclusion set.
    fn b_min(&self, s: NodeSet) -> NodeSet {
        let m = nodeset_min(s);
        (1u64 << m).wrapping_sub(1) | nodeset_singleton(m)
    }
}

/// Enumerates all non-empty subsets of `s` in ascending order.
fn non_empty_subsets(s: NodeSet) -> Vec<NodeSet> {
    let mut result = Vec::new();
    let mut sub = s;
    while sub != 0 {
        result.push(sub);
        sub = sub.wrapping_sub(1) & s;
    }
    result.reverse();
    result
}

// ---------------------------------------------------------------------------
// Plan reconstruction
// ---------------------------------------------------------------------------

fn join_tree_to_ir(tree: &JoinTree, hg: &QueryHypergraph, ctx: &mut QueryContext) -> Operator {
    match tree {
        JoinTree::Leaf(nid) => hg.nodes[*nid].root,
        JoinTree::Join {
            left,
            right,
            edge_indices,
            ..
        } => {
            let outer = join_tree_to_ir(left, hg, ctx);
            let inner = join_tree_to_ir(right, hg, ctx);

            // Collect predicates from connecting edges.
            let mut preds: Vec<crate::Expr> = edge_indices
                .iter()
                .filter_map(|&i| hg.edges[i].predicate)
                .collect();

            let on = match preds.len() {
                0 => ExprData::Literal(crate::ScalarValue::Boolean(true)).add(ctx),
                1 => preds.remove(0),
                _ => ExprData::Nary {
                    op: NaryOp::And,
                    exprs: preds,
                }
                .add(ctx),
            };

            // Use the join type from the first connecting edge.
            let join_type = edge_indices
                .first()
                .map(|&i| hg.edges[i].join_type.to_ir_join_type())
                .unwrap_or(JoinType::Inner);

            OperatorData::Join(Join {
                join_type,
                on,
                outer,
                inner,
            })
            .add(ctx)
        }
    }
}

// ---------------------------------------------------------------------------
// Multi-group root collection
// ---------------------------------------------------------------------------

/// Collects all join group roots in bottom-up order.
///
/// A join group root is a `Join` or `CrossProduct` whose parent is not also
/// a `Join`/`CrossProduct`. We walk top-down and stop descending into a
/// subtree once we find a join group root.
pub fn collect_join_group_roots(ctx: &QueryContext, root: Operator) -> Vec<Operator> {
    let mut roots = Vec::new();
    collect_roots_rec(ctx, root, false, &mut roots);
    roots
}

fn collect_roots_rec(
    ctx: &QueryContext,
    op: Operator,
    parent_is_join: bool,
    out: &mut Vec<Operator>,
) {
    let is_join = matches!(
        ctx.operator(op),
        OperatorData::Join(_) | OperatorData::CrossProduct(_)
    );

    if is_join && !parent_is_join {
        out.push(op);
    }

    // Recurse into children.
    match ctx.operator(op) {
        OperatorData::Join(j) => {
            collect_roots_rec(ctx, j.outer, true, out);
            collect_roots_rec(ctx, j.inner, true, out);
        }
        OperatorData::CrossProduct(cp) => {
            collect_roots_rec(ctx, cp.outer, true, out);
            collect_roots_rec(ctx, cp.inner, true, out);
        }
        OperatorData::Output(o) => collect_roots_rec(ctx, o.input, false, out),
        OperatorData::Projection(p) => collect_roots_rec(ctx, p.input, false, out),
        OperatorData::Selection(s) => collect_roots_rec(ctx, s.input, false, out),
        OperatorData::Sort(s) => collect_roots_rec(ctx, s.input, false, out),
        OperatorData::Limit(l) => collect_roots_rec(ctx, l.input, false, out),
        OperatorData::Map(m) => collect_roots_rec(ctx, m.input, false, out),
        OperatorData::Rename(r) => collect_roots_rec(ctx, r.input, false, out),
        OperatorData::Aggregation(a) => collect_roots_rec(ctx, a.input, false, out),
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// JoinOrdering pass
// ---------------------------------------------------------------------------

pub struct JoinOrdering {
    pub stats: Box<dyn Statistics>,
}

impl JoinOrdering {
    pub fn new() -> Self {
        Self {
            stats: Box::new(UniformStatistics),
        }
    }

    pub fn with_stats(stats: Box<dyn Statistics>) -> Self {
        Self { stats }
    }
}

impl Default for JoinOrdering {
    fn default() -> Self {
        Self::new()
    }
}

impl Pass for JoinOrdering {
    fn name(&self) -> &'static str {
        "join_ordering"
    }
}

impl QueryPass for JoinOrdering {
    fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
        let Some(root) = ctx.query.root() else {
            return Ok(PassResult::Unchanged);
        };

        let group_roots = collect_join_group_roots(&ctx.query, root);
        if group_roots.is_empty() {
            return Ok(PassResult::Unchanged);
        }

        let mut changed = false;
        for group_root in group_roots {
            let hg = build_hypergraph(&ctx.query, &mut ctx.analyses, group_root);
            if hg.nodes.len() < 2 {
                continue;
            }
            assert!(
                hg.nodes.len() <= 64,
                "DPhyp requires ≤64 nodes per join group"
            );

            let mut solver = DPhyp::new(&hg, self.stats.as_ref());
            if let Some(tree) = solver.solve() {
                let new_op = join_tree_to_ir(&tree, &hg, &mut ctx.query);
                ctx.rewrites.replace(group_root, new_op);
                changed = true;
            }
        }

        // JoinOrdering is a one-shot pass; always return Unchanged to stop the fixed-point loop.
        let _ = changed;
        Ok(PassResult::Unchanged)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AnalysisContext, BinaryOp, ColumnData, ExprData, Join, JoinType, OperatorData,
        QueryContext, Scan, TableRef,
    };
    use arrow_schema::DataType;

    fn three_way_chain() -> (QueryContext, Operator) {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let c = ColumnData::new("c", DataType::Int64).add(&mut ctx);
        let sa = OperatorData::Scan(Scan {
            table: TableRef::bare("A"),
            columns: vec![a],
        })
        .add(&mut ctx);
        let sb = OperatorData::Scan(Scan {
            table: TableRef::bare("B"),
            columns: vec![b],
        })
        .add(&mut ctx);
        let sc = OperatorData::Scan(Scan {
            table: TableRef::bare("C"),
            columns: vec![c],
        })
        .add(&mut ctx);

        let on_ab = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(a).add(&mut ctx),
            right: ExprData::ColumnRef(b).add(&mut ctx),
        }
        .add(&mut ctx);
        let join_ab = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on: on_ab,
            outer: sa,
            inner: sb,
        })
        .add(&mut ctx);

        let on_bc = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(b).add(&mut ctx),
            right: ExprData::ColumnRef(c).add(&mut ctx),
        }
        .add(&mut ctx);
        let join_abc = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on: on_bc,
            outer: join_ab,
            inner: sc,
        })
        .add(&mut ctx);

        ctx.set_root(join_abc);
        (ctx, join_abc)
    }

    #[test]
    fn dphyp_three_way_chain_produces_plan() {
        let (ctx, root) = three_way_chain();
        let mut analyses = AnalysisContext::new();
        let hg = build_hypergraph(&ctx, &mut analyses, root);
        assert_eq!(hg.nodes.len(), 3);

        let mut solver = DPhyp::new(&hg, &UniformStatistics);
        let tree = solver.solve().expect("DPhyp should find a plan");
        assert_eq!(tree.leaf_count(), 3);
    }

    #[test]
    fn collect_join_group_roots_finds_root() {
        let (ctx, root) = three_way_chain();
        let roots = collect_join_group_roots(&ctx, root);
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0], root);
    }
}
