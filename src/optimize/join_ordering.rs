//! Join ordering pass using DPhyp (Moerkotte & Neumann, SIGMOD 2008).
//!
//! Finds the optimal bushy join tree for each join group in the query by
//! enumerating all csg-cmp-pairs of the query hypergraph and filling a DP table.

use std::collections::HashMap;

use crate::hypergraph::{NodeSet, QueryHypergraph, nodeset_min, nodeset_singleton};
use crate::{
    ExprData, Join, JoinType, NaryOp, Operator, OperatorData, QueryContext, build_hypergraph,
};

use super::{OptimizeError, OptimizeResult, Pass, PassResult, QueryPass};
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
    },
}

impl JoinTree {
    #[cfg(test)]
    fn leaf_count(&self) -> usize {
        match self {
            JoinTree::Leaf(_) => 1,
            JoinTree::Join { left, right, .. } => left.leaf_count() + right.leaf_count(),
        }
    }

    #[cfg(test)]
    fn leaf_set(&self) -> NodeSet {
        match self {
            JoinTree::Leaf(nid) => nodeset_singleton(*nid),
            JoinTree::Join { left, right, .. } => left.leaf_set() | right.leaf_set(),
        }
    }

    #[cfg(test)]
    fn has_join_with_leaves(&self, leaves: NodeSet) -> bool {
        match self {
            JoinTree::Leaf(_) => false,
            JoinTree::Join { left, right, .. } => {
                self.leaf_set() == leaves
                    || left.has_join_with_leaves(leaves)
                    || right.has_join_with_leaves(leaves)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// DPhyp
// ---------------------------------------------------------------------------

struct DPhyp<'a> {
    hg: &'a QueryHypergraph,
    stats: &'a dyn Statistics,
    /// DP table: NodeSet → best known plan for that subset.
    dp: HashMap<NodeSet, PlanState>,
}

#[derive(Clone)]
struct PlanState {
    cost: f64,
    cardinality: f64,
    tree: JoinTree,
}

impl<'a> DPhyp<'a> {
    fn new(hg: &'a QueryHypergraph, stats: &'a dyn Statistics) -> Self {
        Self {
            hg,
            stats,
            dp: HashMap::new(),
        }
    }

    fn solve(&mut self) -> OptimizeResult<Option<JoinTree>> {
        let n = self.hg.nodes.len();
        if n == 0 {
            return Ok(None);
        }
        if n > 64 {
            return Err(OptimizeError::PassError {
                pass: "JoinOrdering",
                message: format!("join group has {n} nodes; DPhyp supports at most 64 nodes"),
            });
        }

        // Initialise singletons.
        for i in 0..n {
            let s = nodeset_singleton(i);
            let cardinality = self.stats.cardinality(i, self.hg);
            self.dp.insert(
                s,
                PlanState {
                    cost: cardinality,
                    cardinality,
                    tree: JoinTree::Leaf(i),
                },
            );
        }

        // Process nodes in descending order (largest index first).
        for v in (0..n).rev() {
            let sv = nodeset_singleton(v);
            self.emit_csg(sv);
            let bv: NodeSet = (1u64 << v).wrapping_sub(1) | sv; // all nodes ≤ v
            self.enumerate_csg_rec(sv, bv);
        }

        Ok(self
            .dp
            .get(&all_nodes_mask(n))
            .map(|state| state.tree.clone()))
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
            let bit = 1u64 << (63 - v.leading_zeros());
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
        let Some(left) = self.dp.get(&s1).cloned() else {
            return;
        };
        let Some(right) = self.dp.get(&s2).cloned() else {
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
        let cardinality = left.cardinality * right.cardinality * sel;
        let new_cost = left.cost + right.cost + cardinality;
        let combined = s1 | s2;

        let better = self
            .dp
            .get(&combined)
            .is_none_or(|existing| new_cost < existing.cost);

        if better {
            self.dp.insert(
                combined,
                PlanState {
                    cost: new_cost,
                    cardinality,
                    tree: JoinTree::Join {
                        left: Box::new(left.tree),
                        right: Box::new(right.tree),
                        edge_indices,
                    },
                },
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

fn all_nodes_mask(n: usize) -> NodeSet {
    if n == 64 {
        NodeSet::MAX
    } else {
        (1u64 << n) - 1
    }
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
/// subtree once we find a join group root for each contiguous join group.
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

    if is_join && !parent_is_join {
        out.push(op);
    }
}

// ---------------------------------------------------------------------------
// JoinOrdering pass
// ---------------------------------------------------------------------------

pub struct JoinOrdering {
    pub stats: Box<dyn Statistics>,
    last_run: Option<(usize, u64)>,
}

impl JoinOrdering {
    pub fn new() -> Self {
        Self {
            stats: Box::new(UniformStatistics),
            last_run: None,
        }
    }

    pub fn with_stats(stats: Box<dyn Statistics>) -> Self {
        Self {
            stats,
            last_run: None,
        }
    }
}

impl Default for JoinOrdering {
    fn default() -> Self {
        Self::new()
    }
}

impl Pass for JoinOrdering {
    fn name(&self) -> &'static str {
        "JoinOrdering"
    }
}

impl QueryPass for JoinOrdering {
    fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
        let run_key = (
            (&ctx.query as *const QueryContext) as usize,
            ctx.optimizer_run_id,
        );
        if self.last_run == Some(run_key) {
            return Ok(PassResult::Unchanged);
        }

        let Some(root) = ctx.query.root() else {
            return Ok(PassResult::Unchanged);
        };

        let group_roots = collect_join_group_roots(&ctx.query, root);
        if group_roots.is_empty() {
            return Ok(PassResult::Unchanged);
        }

        let groups = group_roots
            .into_iter()
            .filter_map(|group_root| {
                let hg = build_hypergraph(&ctx.query, &mut ctx.analyses, group_root);
                (hg.nodes.len() >= 2).then_some((group_root, hg))
            })
            .collect::<Vec<_>>();
        if groups.is_empty() {
            return Ok(PassResult::Unchanged);
        }

        self.last_run = Some(run_key);

        let replacements =
            groups
                .iter()
                .try_fold(Vec::new(), |mut replacements, (group_root, hg)| {
                    let mut solver = DPhyp::new(hg, self.stats.as_ref());
                    if let Some(tree) = solver.solve()? {
                        replacements
                            .push((*group_root, join_tree_to_ir(&tree, hg, &mut ctx.query)));
                    }
                    Ok::<_, OptimizeError>(replacements)
                })?;
        if replacements.is_empty() {
            return Ok(PassResult::Unchanged);
        }

        for (group_root, new_op) in replacements {
            ctx.rewrites.replace(group_root, new_op);
        }

        super::materialize_reachable_rewrites(root, ctx);

        Ok(PassResult::Changed)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AnalysisContext, BinaryOp, ColumnData, ExprData, HypergraphNode, Join, JoinType,
        OperatorData, OptimizerContext, Output, PassManager, QueryContext, QueryHypergraph,
        ScalarValue, Scan, Selection, TableRef,
    };
    use arrow_schema::DataType;
    use std::{cell::RefCell, rc::Rc};

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
        let tree = solver
            .solve()
            .expect("solver should not error")
            .expect("DPhyp should find a plan");
        assert_eq!(tree.leaf_count(), 3);
    }

    #[test]
    fn collect_join_group_roots_finds_root() {
        let (ctx, root) = three_way_chain();
        let roots = collect_join_group_roots(&ctx, root);
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0], root);
    }

    #[test]
    fn collect_join_group_roots_returns_bottom_up_order() {
        let (mut ctx, inner) = three_way_chain();
        let predicate = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: inner,
        })
        .add(&mut ctx);
        let c = ColumnData::new("extra", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("D"),
            columns: vec![c],
        })
        .add(&mut ctx);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);
        let outer = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: selection,
            inner: scan,
        })
        .add(&mut ctx);

        let roots = collect_join_group_roots(&ctx, outer);

        assert_eq!(roots, vec![inner, outer]);
    }

    #[test]
    fn dphyp_uses_child_cardinalities_for_join_cardinality() {
        struct SkewStats;

        impl Statistics for SkewStats {
            fn cardinality(&self, node_idx: usize, _hg: &QueryHypergraph) -> f64 {
                match node_idx {
                    0 => 1000.0,
                    1 | 2 => 10.0,
                    _ => unreachable!("test graph has three nodes"),
                }
            }

            fn selectivity(&self, _left: NodeSet, _right: NodeSet, _hg: &QueryHypergraph) -> f64 {
                0.1
            }
        }

        let (ctx, root) = three_way_chain();
        let mut analyses = AnalysisContext::new();
        let hg = build_hypergraph(&ctx, &mut analyses, root);
        let mut solver = DPhyp::new(&hg, &SkewStats);

        let tree = solver
            .solve()
            .expect("solver should not error")
            .expect("DPhyp should find a plan");

        assert!(tree.has_join_with_leaves(nodeset_singleton(1) | nodeset_singleton(2)));
    }

    #[test]
    fn dphyp_solve_handles_64_node_all_mask() {
        let mut ctx = QueryContext::new();
        let mut nodes = Vec::new();
        for idx in 0..64 {
            let scan = OperatorData::Scan(Scan {
                table: TableRef::bare(format!("t{idx}")),
                columns: vec![],
            })
            .add(&mut ctx);
            nodes.push(HypergraphNode {
                root: scan,
                label: format!("t{idx}"),
                available: vec![],
            });
        }
        let hg = QueryHypergraph {
            nodes,
            edges: vec![],
        };
        let mut solver = DPhyp::new(&hg, &UniformStatistics);

        assert_eq!(all_nodes_mask(64), NodeSet::MAX);
        assert!(matches!(solver.solve(), Ok(None)));
    }

    #[test]
    fn dphyp_solve_rejects_65_node_groups() {
        let mut ctx = QueryContext::new();
        let mut nodes = Vec::new();
        for idx in 0..65 {
            let scan = OperatorData::Scan(Scan {
                table: TableRef::bare(format!("t{idx}")),
                columns: vec![],
            })
            .add(&mut ctx);
            nodes.push(HypergraphNode {
                root: scan,
                label: format!("t{idx}"),
                available: vec![],
            });
        }
        let hg = QueryHypergraph {
            nodes,
            edges: vec![],
        };
        let mut solver = DPhyp::new(&hg, &UniformStatistics);

        assert!(matches!(
            solver.solve(),
            Err(OptimizeError::PassError {
                pass: "JoinOrdering",
                ..
            })
        ));
    }

    #[test]
    fn join_ordering_reports_changed_so_pass_manager_updates_root() {
        let (ctx, root) = three_way_chain();
        let mut opt = OptimizerContext::new(ctx);
        let mut pm = PassManager::new(10);
        pm.add_pass(JoinOrdering::new());

        pm.run(&mut opt).unwrap();

        assert_ne!(opt.query.root(), Some(root));
    }

    #[test]
    fn join_ordering_rebuilds_parent_above_join_group() {
        let (mut ctx, join_root) = three_way_chain();
        let output = OperatorData::Output(Output { input: join_root }).add(&mut ctx);
        ctx.set_root(output);
        let mut opt = OptimizerContext::new(ctx);
        let mut pm = PassManager::new(10);
        pm.add_pass(JoinOrdering::new());

        pm.run(&mut opt).unwrap();

        let root = opt.query.root().unwrap();
        assert_ne!(root, output);
        let OperatorData::Output(rebuilt) = opt.query.operator(root) else {
            panic!("root should remain an output");
        };
        assert_ne!(rebuilt.input, join_root);
    }

    #[test]
    fn join_ordering_pass_manager_can_be_reused_for_another_context() {
        let (ctx1, root1) = three_way_chain();
        let (ctx2, root2) = three_way_chain();
        let mut opt1 = OptimizerContext::new(ctx1);
        let mut opt2 = OptimizerContext::new(ctx2);
        let mut pm = PassManager::new(10);
        pm.add_pass(JoinOrdering::new());

        pm.run(&mut opt1).unwrap();
        pm.run(&mut opt2).unwrap();

        assert_ne!(opt1.query.root(), Some(root1));
        assert_ne!(opt2.query.root(), Some(root2));
    }

    #[test]
    fn join_ordering_noop_before_join_exists_does_not_consume_pass() {
        struct CreateJoinAfterFirstPass {
            fired: bool,
            created_join: Rc<RefCell<Option<Operator>>>,
        }

        impl Pass for CreateJoinAfterFirstPass {
            fn name(&self) -> &'static str {
                "create_join_after_first_pass"
            }
        }

        impl QueryPass for CreateJoinAfterFirstPass {
            fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
                if self.fired {
                    return Ok(PassResult::Unchanged);
                }
                self.fired = true;

                let a = ColumnData::new("a", DataType::Int64).add(&mut ctx.query);
                let b = ColumnData::new("b", DataType::Int64).add(&mut ctx.query);
                let scan_a = OperatorData::Scan(Scan {
                    table: TableRef::bare("A"),
                    columns: vec![a],
                })
                .add(&mut ctx.query);
                let scan_b = OperatorData::Scan(Scan {
                    table: TableRef::bare("B"),
                    columns: vec![b],
                })
                .add(&mut ctx.query);
                let on = ExprData::Binary {
                    op: BinaryOp::Eq,
                    left: ExprData::ColumnRef(a).add(&mut ctx.query),
                    right: ExprData::ColumnRef(b).add(&mut ctx.query),
                }
                .add(&mut ctx.query);
                let join = OperatorData::Join(Join {
                    join_type: JoinType::Inner,
                    on,
                    outer: scan_a,
                    inner: scan_b,
                })
                .add(&mut ctx.query);

                *self.created_join.borrow_mut() = Some(join);
                if let Some(root) = ctx.query.root() {
                    ctx.rewrites.replace(root, join);
                } else {
                    ctx.query.set_root(join);
                }
                Ok(PassResult::Changed)
            }
        }

        let mut ctx = QueryContext::new();
        let seed = OperatorData::Scan(Scan {
            table: TableRef::bare("seed"),
            columns: vec![],
        })
        .add(&mut ctx);
        ctx.set_root(seed);
        let created_join = Rc::new(RefCell::new(None));
        let mut opt = OptimizerContext::new(ctx);
        let mut pm = PassManager::new(10);
        pm.add_pass(JoinOrdering::new());
        pm.add_pass(CreateJoinAfterFirstPass {
            fired: false,
            created_join: Rc::clone(&created_join),
        });

        pm.run(&mut opt).unwrap();

        let created_join = created_join.borrow().expect("join should be created");
        assert_eq!(opt.query.root(), Some(created_join));
    }
}
