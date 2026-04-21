//! Bottom-up join-ordering pass.
//!
//! The pass currently optimizes maximal inner-join regions. Non-inner logical
//! joins remain as boundaries for reordering, but their children are still
//! visited recursively so inner regions underneath them can be optimized.

use std::{
    collections::HashMap,
    sync::Arc,
};

use bitvec::prelude::*;

use crate::{
    error::Result,
    ir::{
        ColumnSet, IRContext, Operator, OperatorKind, Scalar,
        convert::IntoOperator,
        cost::Cost,
        explain::quick_explain,
        operator::{
            Join, JoinImplementation, JoinSide, JoinType, split_equi_and_non_equi_conditions,
        },
    },
};

use super::{
    VertexSet,
    dphyp::{DPHyp, EnumeratedPair, QueryHypergraph},
    island::is_inner_logical_join,
};

/// Reorders maximal inner-join regions by enumerating pairs with DPHyp and
/// costing concrete join implementations.
pub struct JoinOrderingPass;

impl Default for JoinOrderingPass {
    fn default() -> Self {
        Self::new()
    }
}

impl JoinOrderingPass {
    /// Creates the pass.
    pub fn new() -> Self {
        Self
    }

    /// Applies join ordering to `root` and returns the rebuilt best plan.
    pub fn apply(&self, root: Arc<Operator>, ctx: &IRContext) -> Result<Arc<Operator>> {
        self.optimize_subtree(root, ctx, false)
    }

    /// Rewrites the tree bottom-up and only optimizes maximal inner-join roots.
    fn optimize_subtree(
        &self,
        op: Arc<Operator>,
        ctx: &IRContext,
        parent_is_inner_region: bool,
    ) -> Result<Arc<Operator>> {
        let is_inner_region_root = is_inner_logical_join(op.as_ref()) && !parent_is_inner_region;
        let child_parent_is_inner = is_inner_logical_join(op.as_ref());

        let new_inputs = op
            .input_operators()
            .iter()
            .map(|input| self.optimize_subtree(input.clone(), ctx, child_parent_is_inner))
            .collect::<Result<Vec<_>>>()?;

        let rebuilt = if new_inputs.as_slice() == op.input_operators() {
            op
        } else {
            Arc::new(op.clone_with_inputs(Some(Arc::from(new_inputs)), None))
        };

        if is_inner_region_root {
            self.optimize_inner_region(rebuilt.clone(), ctx)
        } else {
            Ok(rebuilt)
        }
    }

    /// Extracts, enumerates, costs, and rebuilds one inner-join region.
    fn optimize_inner_region(&self, root: Arc<Operator>, ctx: &IRContext) -> Result<Arc<Operator>> {
        let region = InnerJoinRegion::extract(root.clone(), ctx)?;
        if region.leaves.len() <= 1 || region.predicates.is_empty() {
            self.write_debug_dump(
                DebugDump::for_skipped_region(root.clone(), region, "region is too small to reorder"),
                ctx,
            );
            return Ok(root);
        }

        let mut dphyp = DPHyp::new();
        if !dphyp.solve(&region.hypergraph) {
            self.write_debug_dump(
                DebugDump::for_skipped_region(root.clone(), region, "DPHyp could not find a connected plan"),
                ctx,
            );
            return Ok(root);
        }

        let mut best_plans = region.initialize_leaf_plans(ctx)?;
        for pair in dphyp.get_pairs() {
            region.consider_pair(&mut best_plans, pair, ctx)?;
        }

        let root_set = bitvec![1; region.leaves.len()];
        let chosen = best_plans.get(&root_set).cloned();
        self.write_debug_dump(
            DebugDump::for_completed_region(root.clone(), &region, &best_plans, &dphyp, chosen.clone()),
            ctx,
        );

        Ok(chosen.map(|entry| entry.plan).unwrap_or(root))
    }

    /// Emits the always-on debug dump for one region.
    fn write_debug_dump(&self, dump: DebugDump, ctx: &IRContext) {
        println!("{}", dump.render(ctx));
    }
}

/// Cheapest plan found so far for one vertex subset.
#[derive(Debug, Clone)]
struct BestPlan {
    plan: Arc<Operator>,
    cost: Cost,
}

/// Opaque leaf subtree in an extracted inner-join region.
#[derive(Debug, Clone)]
struct RegionLeaf {
    op: Arc<Operator>,
    output_columns: Arc<ColumnSet>,
}

/// Predicate that mentions only one extracted leaf.
#[derive(Debug, Clone)]
struct UnaryPredicate {
    scalar: Arc<Scalar>,
    leaf_index: usize,
}

/// Join predicate stored in region-level form.
///
/// `refs` tracks every referenced leaf, which is useful for debugging and for
/// future legality checks. The hypergraph edge retains the two-sided split used
/// by DPHyp.
#[derive(Debug, Clone)]
struct RegionPredicate {
    scalar: Arc<Scalar>,
    refs: VertexSet,
}

/// Flattened inner-join region used by the production join-ordering pass.
#[derive(Debug)]
struct InnerJoinRegion {
    leaves: Vec<RegionLeaf>,
    unary_predicates: Vec<UnaryPredicate>,
    predicates: Vec<RegionPredicate>,
    hypergraph: QueryHypergraph<usize, usize>,
}

/// Rendered debug snapshot for one region, either skipped or optimized.
#[derive(Debug)]
struct DebugDump {
    root_before: Arc<Operator>,
    leaves: Vec<RegionLeaf>,
    unary_predicates: Vec<UnaryPredicate>,
    predicates: Vec<RegionPredicate>,
    edge_predicates: Vec<usize>,
    dphyp_pairs: Vec<EnumeratedPair>,
    best_plans: Vec<(VertexSet, BestPlan)>,
    chosen: Option<BestPlan>,
    skipped_reason: Option<&'static str>,
}

impl DebugDump {
    /// Builds a dump for a region that was not optimized.
    fn for_skipped_region(
        root_before: Arc<Operator>,
        region: InnerJoinRegion,
        skipped_reason: &'static str,
    ) -> Self {
        Self {
            root_before,
            leaves: region.leaves,
            unary_predicates: region.unary_predicates,
            predicates: region.predicates,
            edge_predicates: region
                .hypergraph
                .edges
                .iter()
                .map(|edge| edge.info)
                .collect(),
            dphyp_pairs: Vec::new(),
            best_plans: Vec::new(),
            chosen: None,
            skipped_reason: Some(skipped_reason),
        }
    }

    /// Builds a dump for a completed optimization.
    fn for_completed_region(
        root_before: Arc<Operator>,
        region: &InnerJoinRegion,
        best_plans: &HashMap<VertexSet, BestPlan>,
        dphyp: &DPHyp,
        chosen: Option<BestPlan>,
    ) -> Self {
        let mut entries = best_plans
            .iter()
            .map(|(set, plan)| (set.clone(), plan.clone()))
            .collect::<Vec<_>>();
        entries.sort_by(|(left_set, _), (right_set, _)| {
            left_set
                .count_ones()
                .cmp(&right_set.count_ones())
                .then_with(|| format_vertex_set(left_set).cmp(&format_vertex_set(right_set)))
        });

        Self {
            root_before,
            leaves: region.leaves.clone(),
            unary_predicates: region.unary_predicates.clone(),
            predicates: region.predicates.clone(),
            edge_predicates: region
                .hypergraph
                .edges
                .iter()
                .map(|edge| edge.info)
                .collect(),
            dphyp_pairs: dphyp.get_pairs().to_vec(),
            best_plans: entries,
            chosen,
            skipped_reason: None,
        }
    }

    /// Renders a compact, human-readable debugging view.
    fn render(&self, ctx: &IRContext) -> String {
        let mut out = String::new();
        out.push_str("===== JOIN ORDERING DEBUG DUMP BEGIN =====\n");
        if let Some(reason) = self.skipped_reason {
            out.push_str(&format!("status: skipped ({reason})\n"));
        } else {
            out.push_str("status: optimized\n");
        }
        out.push_str("\n== Before Join Ordering ==\n");
        out.push_str(&format_plan_tree(self.root_before.as_ref(), ctx, 0));
        out.push_str("\n\n== Region Leaves ==\n");
        for (leaf_index, leaf) in self.leaves.iter().enumerate() {
            out.push_str(&format!(
                "leaf[{leaf_index}]: {}\n",
                format_plan_atom(leaf.op.as_ref(), ctx)
            ));
        }

        out.push_str("\n== Unary Predicates ==\n");
        if self.unary_predicates.is_empty() {
            out.push_str("<none>\n");
        } else {
            for predicate in &self.unary_predicates {
                out.push_str(&format!(
                    "leaf[{}]: {}\n",
                    predicate.leaf_index,
                    format_scalar_compact(predicate.scalar.as_ref(), ctx)
                ));
            }
        }

        out.push_str("\n== Join Predicates ==\n");
        if self.predicates.is_empty() {
            out.push_str("<none>\n");
        } else {
            for predicate in &self.predicates {
                out.push_str(&format!(
                    "refs={} predicate={}\n",
                    format_vertex_set(&predicate.refs),
                    format_scalar_compact(predicate.scalar.as_ref(), ctx)
                ));
            }
        }

        out.push_str("\n== DPHyp Pairs ==\n");
        if self.dphyp_pairs.is_empty() {
            out.push_str("<none>\n");
        } else {
            for pair in &self.dphyp_pairs {
                let predicates = pair
                    .join_edges
                    .iter_ones()
                    .filter_map(|edge_index| {
                        self.edge_predicates
                            .get(edge_index)
                            .and_then(|predicate_index| self.predicates.get(*predicate_index))
                    })
                    .map(|predicate| format_scalar_compact(predicate.scalar.as_ref(), ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                out.push_str(&format!(
                    "{} x {} on {}\n",
                    format_vertex_set(&pair.left),
                    format_vertex_set(&pair.right),
                    predicates
                ));
            }
        }

        out.push_str("\n== DP Table ==\n");
        if self.best_plans.is_empty() {
            out.push_str("<none>\n");
        } else {
            for (set, entry) in &self.best_plans {
                out.push_str(&format!(
                    "set={} cost={:.2} atom={}\n",
                    format_vertex_set(set),
                    entry.cost.as_f64(),
                    format_plan_atom(entry.plan.as_ref(), ctx)
                ));
            }
        }

        out.push_str("\n== Final Chosen Plan ==\n");
        match &self.chosen {
            Some(entry) => {
                out.push_str(&format!("cost={:.2}\n", entry.cost.as_f64()));
                out.push_str(&format_plan_tree(entry.plan.as_ref(), ctx, 0));
            }
            None => out.push_str("<none>\n"),
        }
        out.push('\n');
        out.push_str("===== JOIN ORDERING DEBUG DUMP END =====\n");
        out
    }
}

impl InnerJoinRegion {
    /// Extracts a maximal inner-join region rooted at `root`.
    fn extract(root: Arc<Operator>, ctx: &IRContext) -> Result<Self> {
        let mut leaves = Vec::new();
        let tree = InnerRegionTree::build(root, ctx, &mut leaves)?;
        let mut raw_join_predicates = Vec::new();
        let mut unary_predicates = Vec::new();

        tree.collect_predicates(&leaves, &mut raw_join_predicates, &mut unary_predicates);

        let hypergraph = Self::build_hypergraph(leaves.len(), &raw_join_predicates);
        let predicates = raw_join_predicates
            .into_iter()
            .map(|predicate| RegionPredicate {
                scalar: predicate.scalar,
                refs: to_vertex_set(&predicate.refs, leaves.len()),
            })
            .collect();

        Ok(Self {
            leaves,
            unary_predicates,
            predicates,
            hypergraph,
        })
    }

    /// Builds the hypergraph consumed by DPHyp.
    ///
    /// The edge payload is the predicate index, so later stages can recover the
    /// exact scalar without rescanning every predicate.
    fn build_hypergraph(leaf_count: usize, predicates: &[RawJoinPredicate]) -> QueryHypergraph<usize, usize> {
        let mut graph = QueryHypergraph::new();
        for vertex_index in 0..leaf_count {
            graph.add_vertex(vertex_index);
        }
        for (predicate_index, predicate) in predicates.iter().enumerate() {
            graph.add_edge(
                predicate_index,
                to_vertex_set(&predicate.left_refs, leaf_count),
                to_vertex_set(&predicate.right_refs, leaf_count),
            );
        }
        graph
    }

    /// Seeds the DP memo with singleton leaf plans, pushing unary predicates
    /// into a local `Select` when needed.
    fn initialize_leaf_plans(&self, ctx: &IRContext) -> Result<HashMap<VertexSet, BestPlan>> {
        let mut best = HashMap::new();
        for (leaf_index, leaf) in self.leaves.iter().enumerate() {
            let filters = self
                .unary_predicates
                .iter()
                .filter(|predicate| predicate.leaf_index == leaf_index)
                .map(|predicate| predicate.scalar.clone())
                .collect::<Vec<_>>();
            let plan = if filters.is_empty() {
                leaf.op.clone()
            } else {
                leaf.op
                    .clone()
                    .with_ctx(ctx)
                    .select(Scalar::combine_conjuncts(filters))
                    .build()
            };

            let mut key = bitvec![0; self.leaves.len()];
            key.set(leaf_index, true);
            let cost = ctx.cm.compute_total_cost(plan.as_ref(), ctx)?;
            best.insert(key, BestPlan { plan, cost });
        }
        Ok(best)
    }

    /// Costs one emitted DPHyp pair against the current DP memo.
    ///
    /// The pair already tells us which join predicates are relevant, so this
    /// step avoids rescanning the whole predicate list.
    fn consider_pair(
        &self,
        best: &mut HashMap<VertexSet, BestPlan>,
        pair: &EnumeratedPair,
        ctx: &IRContext,
    ) -> Result<()> {
        let left = &pair.left;
        let right = &pair.right;
        let Some(left_entry) = best.get(left).cloned() else {
            return Ok(());
        };
        let Some(right_entry) = best.get(right).cloned() else {
            return Ok(());
        };

        let union = left.clone() | right;
        let predicates = pair
            .join_edges
            .iter_ones()
            .map(|edge_index| {
                let predicate_index = *self
                    .hypergraph
                    .get_edge_info(edge_index)
                    .expect("edge index should reference a predicate");
                self.predicates[predicate_index].scalar.clone()
            })
            .collect::<Vec<_>>();

        if predicates.is_empty() {
            return Ok(());
        }

        for (outer, outer_cost, inner, inner_cost) in [
            (
                left_entry.plan.clone(),
                left_entry.cost,
                right_entry.plan.clone(),
                right_entry.cost,
            ),
            (
                right_entry.plan.clone(),
                right_entry.cost,
                left_entry.plan.clone(),
                left_entry.cost,
            ),
        ] {
            for candidate in self.make_join_candidates(
                outer,
                outer_cost,
                inner,
                inner_cost,
                predicates.clone(),
                ctx,
            )? {
                let input_costs = [candidate.outer_cost, candidate.inner_cost];
                let total_cost = ctx.cm.compute_total_with_input_costs(
                    candidate.plan.as_ref(),
                    &input_costs,
                    ctx,
                )?;

                match best.get_mut(&union) {
                    Some(existing) if existing.cost <= total_cost => {}
                    Some(existing) => {
                        *existing = BestPlan {
                            plan: candidate.plan,
                            cost: total_cost,
                        };
                    }
                    None => {
                        best.insert(
                            union.clone(),
                            BestPlan {
                                plan: candidate.plan,
                                cost: total_cost,
                            },
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Produces physical join candidates for one logical combine.
    fn make_join_candidates(
        &self,
        outer: Arc<Operator>,
        outer_cost: Cost,
        inner: Arc<Operator>,
        inner_cost: Cost,
        predicates: Vec<Arc<Scalar>>,
        ctx: &IRContext,
    ) -> Result<Vec<CandidatePlan>> {
        let join_cond = Scalar::combine_conjuncts(predicates);
        let logical = Join::new(
            JoinType::Inner,
            outer.clone(),
            inner.clone(),
            join_cond.clone(),
            None,
        )
        .into_operator();

        let mut candidates = vec![CandidatePlan {
            plan: Join::new(
                JoinType::Inner,
                outer.clone(),
                inner.clone(),
                join_cond.clone(),
                Some(JoinImplementation::nested_loop()),
            )
            .into_operator(),
            outer_cost,
            inner_cost,
        }];

        let logical_join = logical.try_borrow::<Join>().unwrap();
        let (equi_conds, _) = split_equi_and_non_equi_conditions(&logical_join, ctx)?;
        if !equi_conds.is_empty() {
            let keys: Arc<[(crate::ir::Column, crate::ir::Column)]> = equi_conds.into();
            for build_side in [JoinSide::Outer, JoinSide::Inner] {
                candidates.push(CandidatePlan {
                    plan: Join::new(
                        JoinType::Inner,
                        outer.clone(),
                        inner.clone(),
                        join_cond.clone(),
                        Some(JoinImplementation::hash(build_side, keys.clone())),
                    )
                    .into_operator(),
                    outer_cost,
                    inner_cost,
                });
            }
        }

        Ok(candidates)
    }
}

/// Candidate physical plan for one combine, along with already-computed child
/// costs needed by the cost model.
#[derive(Debug)]
struct CandidatePlan {
    plan: Arc<Operator>,
    outer_cost: Cost,
    inner_cost: Cost,
}

/// Temporary tree form used while extracting an inner-join region.
///
/// Non-inner operators become leaves immediately; only logical inner joins are
/// expanded.
#[derive(Debug)]
enum InnerRegionTree {
    Leaf {
        leaf_index: usize,
    },
    Join {
        join_cond: Arc<Scalar>,
        leaf_ids: Vec<usize>,
        outer: Box<InnerRegionTree>,
        inner: Box<InnerRegionTree>,
    },
}

impl InnerRegionTree {
    /// Builds an extraction tree while collecting opaque leaves.
    fn build(op: Arc<Operator>, ctx: &IRContext, leaves: &mut Vec<RegionLeaf>) -> Result<Self> {
        if let OperatorKind::Join(meta) = &op.kind
            && meta.implementation.is_none()
            && meta.join_type == JoinType::Inner
        {
            let join = Join::borrow_raw_parts(meta, &op.common);
            let outer = Self::build(join.outer().clone(), ctx, leaves)?;
            let inner = Self::build(join.inner().clone(), ctx, leaves)?;
            let mut leaf_ids = outer.leaf_ids().to_vec();
            leaf_ids.extend_from_slice(inner.leaf_ids());
            return Ok(Self::Join {
                join_cond: join.join_cond().clone(),
                leaf_ids,
                outer: Box::new(outer),
                inner: Box::new(inner),
            });
        }

        let leaf_index = leaves.len();
        leaves.push(RegionLeaf {
            output_columns: op.output_columns(ctx)?,
            op,
        });
        Ok(Self::Leaf { leaf_index })
    }

    /// Returns the leaf ids that appear under this subtree.
    fn leaf_ids(&self) -> &[usize] {
        match self {
            Self::Leaf { leaf_index } => std::slice::from_ref(leaf_index),
            Self::Join { leaf_ids, .. } => leaf_ids.as_slice(),
        }
    }

    /// Splits join conditions into unary predicates and cross-subtree join
    /// predicates.
    fn collect_predicates(
        &self,
        leaves: &[RegionLeaf],
        join_predicates: &mut Vec<RawJoinPredicate>,
        unary_predicates: &mut Vec<UnaryPredicate>,
    ) {
        let Self::Join {
            join_cond,
            outer,
            inner,
            ..
        } = self
        else {
            return;
        };

        outer.collect_predicates(leaves, join_predicates, unary_predicates);
        inner.collect_predicates(leaves, join_predicates, unary_predicates);

        for conjunct in split_conjuncts(join_cond.clone()) {
            let refs = predicate_leaf_refs(leaves, conjunct.as_ref());
            assign_predicate(
                conjunct,
                refs,
                outer,
                inner,
                join_predicates,
                unary_predicates,
            );
        }
    }
}

/// Join predicate in extraction-time form.
///
/// This keeps the exact left/right split only long enough to build the
/// hypergraph, after which the long-lived region stores a de-duplicated form.
#[derive(Debug)]
struct RawJoinPredicate {
    scalar: Arc<Scalar>,
    refs: Vec<usize>,
    left_refs: Vec<usize>,
    right_refs: Vec<usize>,
}

/// Classifies one conjunct relative to the current inner join node.
fn assign_predicate(
    scalar: Arc<Scalar>,
    refs: Vec<usize>,
    outer: &InnerRegionTree,
    inner: &InnerRegionTree,
    join_predicates: &mut Vec<RawJoinPredicate>,
    unary_predicates: &mut Vec<UnaryPredicate>,
) {
    if refs.is_empty() {
        return;
    }

    let left_refs = intersect_refs(&refs, outer.leaf_ids());
    let right_refs = intersect_refs(&refs, inner.leaf_ids());

    match (left_refs.is_empty(), right_refs.is_empty()) {
        (false, false) => {
            join_predicates.push(RawJoinPredicate {
                scalar,
                refs,
                left_refs,
                right_refs,
            });
        }
        (false, true) => push_into_subtree(scalar, refs, outer, join_predicates, unary_predicates),
        (true, false) => push_into_subtree(scalar, refs, inner, join_predicates, unary_predicates),
        (true, true) => {}
    }
}

/// Pushes a predicate deeper until it either becomes unary or crosses a join.
fn push_into_subtree(
    scalar: Arc<Scalar>,
    refs: Vec<usize>,
    subtree: &InnerRegionTree,
    join_predicates: &mut Vec<RawJoinPredicate>,
    unary_predicates: &mut Vec<UnaryPredicate>,
) {
    match subtree {
        InnerRegionTree::Leaf { leaf_index } => {
            if refs.contains(leaf_index) {
                unary_predicates.push(UnaryPredicate {
                    scalar,
                    leaf_index: *leaf_index,
                });
            }
        }
        InnerRegionTree::Join { outer, inner, .. } => assign_predicate(
            scalar,
            refs,
            outer,
            inner,
            join_predicates,
            unary_predicates,
        ),
    }
}

/// Returns the extracted leaves referenced by a scalar.
fn predicate_leaf_refs(leaves: &[RegionLeaf], scalar: &Scalar) -> Vec<usize> {
    let used = scalar.used_columns();
    leaves
        .iter()
        .enumerate()
        .filter_map(|(leaf_index, leaf)| {
            let intersection = used.clone() & leaf.output_columns.as_ref();
            (!intersection.is_empty()).then_some(leaf_index)
        })
        .collect()
}

/// Intersects a reference list with one candidate leaf set.
fn intersect_refs(refs: &[usize], candidates: &[usize]) -> Vec<usize> {
    refs.iter()
        .copied()
        .filter(|leaf_index| candidates.contains(leaf_index))
        .collect()
}

/// Flattens a conjunction into individual conjuncts.
fn split_conjuncts(predicate: Arc<Scalar>) -> Vec<Arc<Scalar>> {
    if let Ok(and) = predicate.try_borrow::<crate::ir::scalar::NaryOp>()
        && and.is_and()
    {
        and.terms()
            .iter()
            .flat_map(|term| split_conjuncts(term.clone()))
            .collect()
    } else {
        vec![predicate]
    }
}

/// Converts a list of indices into a fixed-width bitset.
fn to_vertex_set(indices: &[usize], len: usize) -> VertexSet {
    let mut set = bitvec![0; len];
    for index in indices {
        set.set(*index, true);
    }
    set
}

/// Formats a vertex set as `[i, j, ...]` for debugging.
fn format_vertex_set(set: &VertexSet) -> String {
    let entries = set.iter_ones().map(|index| index.to_string()).collect::<Vec<_>>();
    format!("[{}]", entries.join(", "))
}

/// Produces a one-line scalar rendering for debug output.
fn format_scalar_compact(scalar: &Scalar, ctx: &IRContext) -> String {
    quick_explain(Arc::new(scalar.clone()), ctx)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Formats only the current operator, not its children.
fn format_plan_atom(op: &Operator, ctx: &IRContext) -> String {
    if let Ok(join) = op.try_borrow::<Join>() {
        let implementation = format_join_implementation(join.implementation());
        let join_type = format!("{:?}", join.join_type());
        let cond = format_scalar_compact(join.join_cond().as_ref(), ctx);
        return format!("join[{join_type}, {implementation}] on {cond}");
    }

    if let Ok(get) = op.try_borrow::<crate::ir::operator::Get>() {
        let table = ctx
            .get_binding(get.table_index())
            .map(|binding| binding.table_ref().table().to_string())
            .unwrap_or_else(|_| format!("table#{}", get.table_index()));
        return format!("get[{table}]");
    }

    if let Ok(select) = op.try_borrow::<crate::ir::operator::Select>() {
        let predicate = format_scalar_compact(select.predicate().as_ref(), ctx);
        return format!("select[{predicate}]");
    }

    if let Ok(remap) = op.try_borrow::<crate::ir::operator::Remap>() {
        let table = ctx
            .get_binding(remap.table_index())
            .map(|binding| binding.table_ref().table().to_string())
            .unwrap_or_else(|_| format!("table#{}", remap.table_index()));
        return format!("remap[{table}]");
    }

    format!("{:?}", op.kind)
}

/// Formats a subtree using one compact line per operator.
fn format_plan_tree(op: &Operator, ctx: &IRContext, indent: usize) -> String {
    let prefix = "  ".repeat(indent);
    let mut out = format!("{prefix}{}\n", format_plan_atom(op, ctx));
    for input in op.input_operators() {
        out.push_str(&format_plan_tree(input.as_ref(), ctx, indent + 1));
    }
    out
}

/// Formats the join implementation summary shown in debug dumps.
fn format_join_implementation(implementation: &Option<JoinImplementation>) -> String {
    match implementation {
        None => "logical".to_string(),
        Some(JoinImplementation::NestedLoop) => "nested_loop".to_string(),
        Some(JoinImplementation::Hash { build_side, .. }) => {
            format!("hash(build={build_side:?})")
        }
    }
}
