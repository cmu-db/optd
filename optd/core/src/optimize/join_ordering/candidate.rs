//! Shared candidate orientation, costing, and IR reconstruction.

#[cfg(test)]
use super::dphyp::JoinTree;
use super::{OptimizeResult, dphyp::PlanState};
use crate::cost::CostModel;
use crate::hypergraph::{NodeSet, QueryHypergraph};
use crate::{
    AnalysisContext, CrossProduct, Expr, ExprData, Join, JoinType, NaryOp, Operator, OperatorData,
    QueryContext, ScalarValue,
};

#[allow(clippy::too_many_arguments)]
/// Materializes and costs the valid orientation(s) of a candidate join.
///
/// Directed TES endpoints fix non-inner orientation. Inner joins use the supplied orientation
/// unless the cost model explicitly declares orientation sensitivity, in which case both
/// alternatives are compared. `None` means no connecting edge was supplied.
pub(super) fn best_join_candidate<M: CostModel>(
    ctx: &mut QueryContext,
    analyses: &mut AnalysisContext,
    hg: &QueryHypergraph,
    cost_model: &M,
    left_nodes: &NodeSet,
    left: &PlanState<M::Cost>,
    right_nodes: &NodeSet,
    right: &PlanState<M::Cost>,
    edge_indices: &[usize],
) -> OptimizeResult<Option<PlanState<M::Cost>>> {
    if edge_indices.is_empty() {
        return Ok(None);
    }

    let join_type = candidate_join_type(edge_indices, hg, ctx);
    if join_type != JoinType::Inner {
        let first_edge = &hg.edges[edge_indices[0]];
        return if first_edge.left.is_subset(left_nodes) && first_edge.right.is_subset(right_nodes) {
            cost_join_orientation(
                ctx,
                analyses,
                hg,
                cost_model,
                left,
                right,
                join_type,
                edge_indices,
            )
            .map(Some)
        } else {
            cost_join_orientation(
                ctx,
                analyses,
                hg,
                cost_model,
                right,
                left,
                join_type,
                edge_indices,
            )
            .map(Some)
        };
    }

    let forward = cost_join_orientation(
        ctx,
        analyses,
        hg,
        cost_model,
        left,
        right,
        JoinType::Inner,
        edge_indices,
    )?;
    if !cost_model.is_join_orientation_cost_sensitive() {
        return Ok(Some(forward));
    }
    let reverse = cost_join_orientation(
        ctx,
        analyses,
        hg,
        cost_model,
        right,
        left,
        JoinType::Inner,
        edge_indices,
    )?;
    Ok(Some(
        if cost_model.is_better(&reverse.cost, &forward.cost) {
            reverse
        } else {
            forward
        },
    ))
}

#[allow(clippy::too_many_arguments)]
/// Builds one oriented join and composes its total cost from cached child costs.
fn cost_join_orientation<M: CostModel>(
    ctx: &mut QueryContext,
    analyses: &mut AnalysisContext,
    hg: &QueryHypergraph,
    cost_model: &M,
    outer: &PlanState<M::Cost>,
    inner: &PlanState<M::Cost>,
    join_type: JoinType,
    edge_indices: &[usize],
) -> OptimizeResult<PlanState<M::Cost>> {
    let root = materialize_candidate_join(outer.root, inner.root, join_type, edge_indices, hg, ctx);
    let cost = cost_model.total_cost_from_children(
        root,
        &[outer.cost.clone(), inner.cost.clone()],
        ctx,
        analyses,
    )?;
    Ok(PlanState {
        root,
        cost,
        #[cfg(test)]
        tree: JoinTree::Join {
            left: Box::new(outer.tree.clone()),
            right: Box::new(inner.tree.clone()),
        },
    })
}

// ---------------------------------------------------------------------------
// Candidate materialization
// ---------------------------------------------------------------------------

/// Appends a candidate join operator to the query arena.
///
/// All predicate-bearing connecting edges become one conjunctive `ON` expression. A predicate-
/// free inner edge is represented as [`CrossProduct`]; predicate-free non-inner joins retain an
/// explicit `true` condition so their semantics and join type remain visible in the IR.
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

/// Recovers the logical join type represented by a set of connecting hyperedges.
///
/// CD-E compatibility guarantees that simultaneously applicable edges agree on semantics. Dummy
/// cross-product edges and defensive fallbacks are treated as inner joins.
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
