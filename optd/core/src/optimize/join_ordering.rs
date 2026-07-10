//! Join ordering pass using DPhyp (Moerkotte & Neumann, SIGMOD 2008).
//!
//! Finds the optimal bushy join tree for each join group in the query by
//! enumerating all csg-cmp-pairs of the query hypergraph and filling a DP table.

use std::collections::HashMap;

use crate::analysis::{CardinalityEstimationV1, connecting_edge_indices};
use crate::cost::{CostModel, DefaultCostModel};
use crate::hypergraph::{NodeSet, QueryHypergraph, nodeset_min, nodeset_singleton};
use crate::{
    AnalysisContext, CardinalityProfile, CrossProduct, Expr, ExprData, Join, JoinType, NaryOp,
    Operator, OperatorData, QueryContext, ScalarValue, build_hypergraph,
};

use super::{OptimizeError, OptimizeResult, Pass, PassResult, QueryPass};
use crate::OptimizerContext;

#[cfg(test)]
use crate::cost::{JoinAlgorithmClass, join_algorithm_class, join_algorithm_cost};

// ---------------------------------------------------------------------------
// JoinTree: the output of DPhyp
// ---------------------------------------------------------------------------

#[cfg(test)]
#[derive(Clone)]
enum JoinTree {
    Leaf(usize), // node index
    Join {
        left: Box<JoinTree>,
        right: Box<JoinTree>,
    },
}

#[cfg(test)]
impl JoinTree {
    fn leaf_count(&self) -> usize {
        match self {
            JoinTree::Leaf(_) => 1,
            JoinTree::Join { left, right, .. } => left.leaf_count() + right.leaf_count(),
        }
    }

    fn leaf_set(&self) -> NodeSet {
        match self {
            JoinTree::Leaf(nid) => nodeset_singleton(*nid),
            JoinTree::Join { left, right, .. } => left.leaf_set() | right.leaf_set(),
        }
    }

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

struct DPhyp<'a, M: CostModel> {
    ctx: &'a mut QueryContext,
    analyses: &'a mut AnalysisContext,
    hg: &'a QueryHypergraph,
    cost_model: &'a M,
    /// DP table: NodeSet → best known plan for that subset.
    dp: HashMap<NodeSet, PlanState<M::Cost>>,
}

#[derive(Clone)]
struct PlanState<C> {
    root: Operator,
    cost: C,
    #[cfg(test)]
    tree: JoinTree,
}

impl<'a, M: CostModel> DPhyp<'a, M> {
    fn new(
        ctx: &'a mut QueryContext,
        analyses: &'a mut AnalysisContext,
        hg: &'a QueryHypergraph,
        cost_model: &'a M,
    ) -> Self {
        Self {
            ctx,
            analyses,
            hg,
            cost_model,
            dp: HashMap::new(),
        }
    }

    fn solve(&mut self) -> OptimizeResult<Option<PlanState<M::Cost>>> {
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
            let root = self.hg.nodes[i].root;
            cardinality_profile(self.ctx, self.analyses, root)?;
            let cost = self.cost_model.total_cost(root, self.ctx, self.analyses)?;
            self.dp.insert(
                s,
                PlanState {
                    root,
                    cost,
                    #[cfg(test)]
                    tree: JoinTree::Leaf(i),
                },
            );
        }

        // Process nodes in descending order (largest index first).
        for v in (0..n).rev() {
            let sv = nodeset_singleton(v);
            self.emit_csg(sv)?;
            let bv: NodeSet = (1u64 << v).wrapping_sub(1) | sv; // all nodes ≤ v
            self.enumerate_csg_rec(sv, bv)?;
        }

        Ok(self.dp.get(&all_nodes_mask(n)).cloned())
    }

    fn enumerate_csg_rec(&mut self, s1: NodeSet, x: NodeSet) -> OptimizeResult<()> {
        let nbrs = self.neighborhood(s1, x);
        // Collect non-empty subsets of nbrs.
        let subsets = non_empty_subsets(nbrs);
        // First pass: emit csgs.
        for &n_sub in &subsets {
            let candidate = s1 | n_sub;
            if self.dp.contains_key(&candidate) {
                self.emit_csg(candidate)?;
            }
        }
        // Second pass: recurse.
        for &n_sub in &subsets {
            self.enumerate_csg_rec(s1 | n_sub, x | nbrs)?;
        }
        Ok(())
    }

    fn emit_csg(&mut self, s1: NodeSet) -> OptimizeResult<()> {
        let x = s1 | self.b_min(s1);
        let nbrs = self.neighborhood(s1, x);
        // Iterate neighbors in descending order.
        let mut v = nbrs;
        while v != 0 {
            let bit = 1u64 << (63 - v.leading_zeros());
            v &= !bit;
            let s2 = bit;
            if self.has_edge(s1, s2) {
                self.emit_csg_cmp(s1, s2)?;
            }
            self.enumerate_cmp_rec(s1, s2, x)?;
        }
        Ok(())
    }

    fn enumerate_cmp_rec(&mut self, s1: NodeSet, s2: NodeSet, x: NodeSet) -> OptimizeResult<()> {
        let nbrs = self.neighborhood(s2, x);
        let subsets = non_empty_subsets(nbrs);
        for &n_sub in &subsets {
            let candidate = s2 | n_sub;
            if self.dp.contains_key(&candidate) && self.has_edge(s1, candidate) {
                self.emit_csg_cmp(s1, candidate)?;
            }
        }
        for &n_sub in &subsets {
            self.enumerate_cmp_rec(s1, s2 | n_sub, x | nbrs)?;
        }
        Ok(())
    }

    fn emit_csg_cmp(&mut self, s1: NodeSet, s2: NodeSet) -> OptimizeResult<()> {
        let Some(left) = self.dp.get(&s1).cloned() else {
            return Ok(());
        };
        let Some(right) = self.dp.get(&s2).cloned() else {
            return Ok(());
        };

        let edge_indices = connecting_edge_indices(s1, s2, self.hg);
        if edge_indices.is_empty() {
            return Ok(());
        }

        let join_type = candidate_join_type(&edge_indices, self.hg, self.ctx);
        let candidate = materialize_candidate_join(
            left.root,
            right.root,
            join_type,
            &edge_indices,
            self.hg,
            self.ctx,
        );
        cardinality_profile(self.ctx, self.analyses, candidate)?;
        let child_costs = [left.cost.clone(), right.cost.clone()];
        let new_cost = self.cost_model.total_cost_from_children(
            candidate,
            &child_costs,
            self.ctx,
            self.analyses,
        )?;
        let combined = s1 | s2;

        let better = self
            .dp
            .get(&combined)
            .is_none_or(|existing| self.cost_model.is_better(&new_cost, &existing.cost));

        if better {
            self.dp.insert(
                combined,
                PlanState {
                    root: candidate,
                    cost: new_cost,
                    #[cfg(test)]
                    tree: JoinTree::Join {
                        left: Box::new(left.tree),
                        right: Box::new(right.tree),
                    },
                },
            );
        }
        Ok(())
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

fn cardinality_profile(
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
    op: Operator,
) -> OptimizeResult<CardinalityProfile> {
    analyses
        .get::<CardinalityEstimationV1>(ctx, op)
        .map_err(|err| OptimizeError::PassError {
            pass: "JoinOrdering",
            message: err.to_string(),
        })
}

// ---------------------------------------------------------------------------
// Candidate materialization
// ---------------------------------------------------------------------------

fn materialize_candidate_join(
    outer: Operator,
    inner: Operator,
    join_type: JoinType,
    edge_indices: &[usize],
    hg: &QueryHypergraph,
    ctx: &mut QueryContext,
) -> Operator {
    let mut predicates: Vec<Expr> = edge_indices
        .iter()
        .filter_map(|&idx| hg.edges[idx].predicate)
        .collect();

    if predicates.is_empty() && join_type == JoinType::Inner {
        return OperatorData::CrossProduct(CrossProduct { outer, inner }).add(ctx);
    }

    let on = match predicates.len() {
        0 => ExprData::Literal(ScalarValue::Boolean(true)).add(ctx),
        1 => predicates.remove(0),
        _ => ExprData::Nary {
            op: NaryOp::And,
            exprs: predicates,
        }
        .add(ctx),
    };

    OperatorData::Join(Join {
        join_type,
        on,
        outer,
        inner,
    })
    .add(ctx)
}

fn candidate_join_type(
    edge_indices: &[usize],
    hg: &QueryHypergraph,
    ctx: &QueryContext,
) -> JoinType {
    edge_indices
        .first()
        .and_then(|&idx| match hg.edges[idx].source.get(ctx) {
            OperatorData::Join(join) => Some(join.join_type.clone()),
            OperatorData::CrossProduct(_) => Some(JoinType::Inner),
            _ => None,
        })
        .unwrap_or(JoinType::Inner)
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

pub struct JoinOrdering<M = DefaultCostModel> {
    cost_model: M,
    last_run: Option<(usize, u64)>,
}

impl JoinOrdering<DefaultCostModel> {
    pub fn new() -> Self {
        Self {
            cost_model: DefaultCostModel,
            last_run: None,
        }
    }

    pub fn with_cost_model<M: CostModel>(cost_model: M) -> JoinOrdering<M> {
        JoinOrdering {
            cost_model,
            last_run: None,
        }
    }
}

impl Default for JoinOrdering<DefaultCostModel> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: CostModel> Pass for JoinOrdering<M> {
    fn name(&self) -> &'static str {
        "JoinOrdering"
    }
}

impl<M: CostModel> QueryPass for JoinOrdering<M> {
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

        ctx.analyses.clear();

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
                    let mut solver =
                        DPhyp::new(&mut ctx.query, &mut ctx.analyses, hg, &self.cost_model);
                    let plan = solver.solve()?;
                    if let Some(plan) = plan {
                        replacements.push((*group_root, plan.root));
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
        AnalysisContext, BinaryOp, Catalog, Column, ColumnData, ColumnStatistics, ExprData,
        HypergraphNode, Join, JoinType, MemoryCatalog, OperatorData, OptimizerContext, Output,
        PassManager, QueryContext, QueryHypergraph, ScalarValue, Scan, Selection, TableRef,
        TableStatistics,
    };
    use arrow_schema::{DataType, Field, Schema};
    use std::{cell::RefCell, rc::Rc, sync::Arc};

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

    fn two_way_join_with_predicate(
        predicate: impl FnOnce(&mut QueryContext, Column, Column) -> crate::Expr,
    ) -> (QueryContext, Operator) {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
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
        let on = predicate(&mut ctx, a, b);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: sa,
            inner: sb,
        })
        .add(&mut ctx);
        ctx.set_root(join);
        (ctx, join)
    }

    fn binary_predicate(
        ctx: &mut QueryContext,
        op: BinaryOp,
        left: Column,
        right: Column,
    ) -> crate::Expr {
        ExprData::Binary {
            op,
            left: ExprData::ColumnRef(left).add(ctx),
            right: ExprData::ColumnRef(right).add(ctx),
        }
        .add(ctx)
    }

    fn join_algorithm_class_for_root(ctx: &QueryContext, root: Operator) -> JoinAlgorithmClass {
        let mut analyses = AnalysisContext::new();
        let hg = build_hypergraph(ctx, &mut analyses, root);
        let edge_indices = connecting_edge_indices(nodeset_singleton(0), nodeset_singleton(1), &hg);
        join_algorithm_class(&edge_indices, &hg, ctx)
    }

    #[test]
    fn dphyp_three_way_chain_produces_plan() {
        let (mut ctx, root) = three_way_chain();
        let mut analyses = AnalysisContext::new();
        let hg = build_hypergraph(&ctx, &mut analyses, root);
        assert_eq!(hg.nodes.len(), 3);

        let mut solver = DPhyp::new(&mut ctx, &mut analyses, &hg, &DefaultCostModel);
        let plan = solver
            .solve()
            .expect("solver should not error")
            .expect("DPhyp should find a plan");
        assert_eq!(plan.tree.leaf_count(), 3);
    }

    #[test]
    fn dphyp_materializes_winning_plan_in_arena() {
        let (mut ctx, root) = three_way_chain();
        let mut analyses = AnalysisContext::new();
        let hg = build_hypergraph(&ctx, &mut analyses, root);
        let before = ctx.operator_count();

        let plan = {
            let mut solver = DPhyp::new(&mut ctx, &mut analyses, &hg, &DefaultCostModel);
            solver
                .solve()
                .expect("solver should not error")
                .expect("DPhyp should find a plan")
        };

        assert!(ctx.operator_count() > before);
        assert!(matches!(
            plan.root.get(&ctx),
            OperatorData::Join(_) | OperatorData::CrossProduct(_)
        ));
    }

    #[test]
    fn dphyp_preserves_source_left_mark_join_type() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let marker = ColumnData::new("exists_mark", DataType::Boolean).add(&mut ctx);
        let left = OperatorData::Scan(Scan {
            table: TableRef::bare("A"),
            columns: vec![a],
        })
        .add(&mut ctx);
        let right = OperatorData::Scan(Scan {
            table: TableRef::bare("B"),
            columns: vec![b],
        })
        .add(&mut ctx);
        let on = binary_predicate(&mut ctx, BinaryOp::Eq, a, b);
        let root = OperatorData::Join(Join {
            join_type: JoinType::LeftMark {
                marker,
                nullable: false,
            },
            on,
            outer: left,
            inner: right,
        })
        .add(&mut ctx);
        let mut analyses = AnalysisContext::new();
        let hg = build_hypergraph(&ctx, &mut analyses, root);

        let plan = {
            let mut solver = DPhyp::new(&mut ctx, &mut analyses, &hg, &DefaultCostModel);
            solver
                .solve()
                .expect("solver should not error")
                .expect("DPhyp should find a plan")
        };

        let OperatorData::Join(join) = plan.root.get(&ctx) else {
            panic!("winning plan should be a join");
        };
        assert!(matches!(
            join.join_type,
            JoinType::LeftMark {
                marker: actual,
                nullable: false,
            } if actual == marker
        ));
    }

    #[test]
    fn dphyp_uses_generic_cost_model_composition_and_comparison() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        #[derive(Debug, Clone, PartialEq, Eq)]
        struct StructuredCost {
            units: usize,
        }

        struct StructuredCostModel {
            additions: Arc<AtomicUsize>,
            comparisons: Arc<AtomicUsize>,
        }

        impl CostModel for StructuredCostModel {
            type Cost = StructuredCost;

            fn zero(&self) -> Self::Cost {
                StructuredCost { units: 0 }
            }

            fn add(&self, left: Self::Cost, right: Self::Cost) -> Self::Cost {
                self.additions.fetch_add(1, Ordering::Relaxed);
                StructuredCost {
                    units: left.units + right.units,
                }
            }

            fn is_better(&self, candidate: &Self::Cost, existing: &Self::Cost) -> bool {
                self.comparisons.fetch_add(1, Ordering::Relaxed);
                candidate.units < existing.units
            }

            fn operator_cost(
                &self,
                op: Operator,
                ctx: &QueryContext,
                analyses: &mut AnalysisContext,
            ) -> OptimizeResult<Self::Cost> {
                let units = match op.get(ctx) {
                    OperatorData::Join(_) | OperatorData::CrossProduct(_) => 10,
                    _ => 1,
                };
                let _ = analyses
                    .get::<CardinalityEstimationV1>(ctx, op)
                    .map_err(|err| OptimizeError::PassError {
                        pass: "StructuredCostModel",
                        message: err.to_string(),
                    })?;
                Ok(StructuredCost { units })
            }
        }

        let (mut ctx, root) = three_way_chain();
        let mut analyses = AnalysisContext::new();
        let hg = build_hypergraph(&ctx, &mut analyses, root);
        let additions = Arc::new(AtomicUsize::new(0));
        let comparisons = Arc::new(AtomicUsize::new(0));
        let model = StructuredCostModel {
            additions: additions.clone(),
            comparisons: comparisons.clone(),
        };
        let mut solver = DPhyp::new(&mut ctx, &mut analyses, &hg, &model);

        let plan = solver.solve().unwrap().unwrap();

        assert_eq!(plan.cost, StructuredCost { units: 23 });
        assert!(additions.load(Ordering::Relaxed) > 0);
        assert!(comparisons.load(Ordering::Relaxed) > 0);
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
    fn equi_join_is_hash_like_for_costing() {
        let (ctx, root) =
            two_way_join_with_predicate(|ctx, a, b| binary_predicate(ctx, BinaryOp::Eq, a, b));

        assert_eq!(
            join_algorithm_class_for_root(&ctx, root),
            JoinAlgorithmClass::HashLike
        );
    }

    #[test]
    fn is_not_distinct_from_join_is_hash_like_for_costing() {
        let (ctx, root) = two_way_join_with_predicate(|ctx, a, b| {
            binary_predicate(ctx, BinaryOp::IsNotDistinctFrom, a, b)
        });

        assert_eq!(
            join_algorithm_class_for_root(&ctx, root),
            JoinAlgorithmClass::HashLike
        );
    }

    #[test]
    fn mixed_equi_and_residual_join_is_hash_like_for_costing() {
        let (ctx, root) = two_way_join_with_predicate(|ctx, a, b| {
            let eq = binary_predicate(ctx, BinaryOp::Eq, a, b);
            let residual = binary_predicate(ctx, BinaryOp::Gt, a, b);
            ExprData::Nary {
                op: NaryOp::And,
                exprs: vec![eq, residual],
            }
            .add(ctx)
        });

        assert_eq!(
            join_algorithm_class_for_root(&ctx, root),
            JoinAlgorithmClass::HashLike
        );
    }

    #[test]
    fn pure_non_equi_join_is_nested_loop_like_for_costing() {
        let (ctx, root) =
            two_way_join_with_predicate(|ctx, a, b| binary_predicate(ctx, BinaryOp::Gt, a, b));

        assert_eq!(
            join_algorithm_class_for_root(&ctx, root),
            JoinAlgorithmClass::NestedLoopLike
        );
    }

    #[test]
    fn hash_like_cost_does_not_use_pairwise_input_product() {
        assert_eq!(
            join_algorithm_cost(
                1_000_000.0,
                1,
                1_000_000.0,
                1,
                10.0,
                1,
                JoinAlgorithmClass::HashLike
            ),
            1_002_000_010.0
        );
        assert_eq!(
            join_algorithm_cost(
                1_000_000.0,
                1,
                1_000_000.0,
                1,
                10.0,
                1,
                JoinAlgorithmClass::NestedLoopLike
            ),
            1_000_000_000_010.0
        );
        assert_eq!(
            join_algorithm_cost(10.0, 3, 20.0, 4, 5.0, 7, JoinAlgorithmClass::HashLike),
            30.0 + 80.0 + f64::powf(200.0, 0.75) + 35.0
        );
    }

    #[test]
    fn cached_cardinality_analysis_guides_candidate_costing() {
        let (mut ctx, root) = three_way_chain();
        let catalog = MemoryCatalog::new("memory", "public");
        for (table, rows, distinct) in [("A", 10, 10), ("B", 10, 10), ("C", 1_000_000, 1)] {
            catalog
                .create_table(TableRef::bare(table), single_i64_schema(), None)
                .unwrap();
            catalog
                .set_table_statistics(
                    TableRef::bare(table),
                    table_stats(table_column(table), rows, distinct),
                )
                .unwrap();
        }
        let mut analyses = AnalysisContext::with_catalog(Arc::new(catalog));
        let hg = build_hypergraph(&ctx, &mut analyses, root);
        let mut solver = DPhyp::new(&mut ctx, &mut analyses, &hg, &DefaultCostModel);

        let plan = solver
            .solve()
            .expect("solver should not error")
            .expect("DPhyp should find a plan");

        assert!(
            plan.tree
                .has_join_with_leaves(nodeset_singleton(0) | nodeset_singleton(1))
        );
    }

    fn single_i64_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
    }

    fn table_column(table: &str) -> &'static str {
        match table {
            "A" => "a",
            "B" => "b",
            "C" => "c",
            _ => unreachable!("unexpected test table"),
        }
    }

    fn table_stats(column: &str, rows: usize, distinct: usize) -> TableStatistics {
        TableStatistics {
            row_count: Some(rows),
            size_bytes: None,
            column_statistics: [(
                column.to_string(),
                ColumnStatistics {
                    lower_bound: None,
                    upper_bound: None,
                    frequency: Some(rows),
                    distinct: Some(distinct),
                },
            )]
            .into_iter()
            .collect(),
        }
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
        let mut analyses = AnalysisContext::new();
        let mut solver = DPhyp::new(&mut ctx, &mut analyses, &hg, &DefaultCostModel);

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
        let mut analyses = AnalysisContext::new();
        let mut solver = DPhyp::new(&mut ctx, &mut analyses, &hg, &DefaultCostModel);

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
        let mut pm = PassManager::new();
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
        let mut pm = PassManager::new();
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
        let mut pm = PassManager::new();
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
        let mut pm = PassManager::new();
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
