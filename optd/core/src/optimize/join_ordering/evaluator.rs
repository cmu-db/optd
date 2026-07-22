//! Candidate-cost evaluation strategies.
//!
//! Stage 1 contains only [`MaterializingEvaluator`], the exact compatibility path for arbitrary
//! [`CostModel`] implementations. The private trait separates search enumeration from candidate
//! representation so a later default evaluator can cost borrowed cardinality profiles without
//! first appending an IR operator.

use super::OptimizeResult;
use super::plan::materialize_candidate_join;
use crate::cost::CostModel;
use crate::hypergraph::QueryHypergraph;
use crate::{AnalysisContext, JoinType, Operator, QueryContext};

pub(super) struct EvaluatedPlan<C> {
    pub(super) cost: C,
    pub(super) materialized: Option<Operator>,
    /// GOO compares the local work of a candidate rather than its cumulative subtree cost. The
    /// compatibility evaluator leaves this absent so GOO performs its historical second call to
    /// `operator_cost`; a future evaluator can reuse a local cost it already computed.
    pub(super) immediate_cost: Option<C>,
}

pub(super) struct CandidateInput<'a, C> {
    pub(super) cost: &'a C,
    pub(super) materialized: Option<Operator>,
}

pub(super) struct JoinSpec<'a> {
    pub(super) join_type: &'a JoinType,
    pub(super) edge_indices: &'a [usize],
    pub(super) hypergraph: &'a QueryHypergraph,
}

pub(super) trait CandidateEvaluator<M: CostModel>: Send + Sync {
    fn evaluate_leaf(
        &self,
        cost_model: &M,
        root: Operator,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<EvaluatedPlan<M::Cost>>;

    /// Whether candidate evaluation must first recover concrete child operators.
    fn requires_materialized_inputs(&self) -> bool;

    fn evaluate_join(
        &self,
        cost_model: &M,
        spec: JoinSpec<'_>,
        outer: CandidateInput<'_, M::Cost>,
        inner: CandidateInput<'_, M::Cost>,
        ctx: &mut QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<EvaluatedPlan<M::Cost>>;
}

/// Compatibility evaluator that preserves the original materialize-then-cost behavior.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct MaterializingEvaluator;

impl<M: CostModel> CandidateEvaluator<M> for MaterializingEvaluator {
    fn evaluate_leaf(
        &self,
        cost_model: &M,
        root: Operator,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<EvaluatedPlan<M::Cost>> {
        Ok(EvaluatedPlan {
            cost: cost_model.total_cost(root, ctx, analyses)?,
            materialized: Some(root),
            immediate_cost: None,
        })
    }

    fn requires_materialized_inputs(&self) -> bool {
        true
    }

    fn evaluate_join(
        &self,
        cost_model: &M,
        spec: JoinSpec<'_>,
        outer: CandidateInput<'_, M::Cost>,
        inner: CandidateInput<'_, M::Cost>,
        ctx: &mut QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<EvaluatedPlan<M::Cost>> {
        let outer_root = outer
            .materialized
            .expect("the materializing evaluator requires an outer operator");
        let inner_root = inner
            .materialized
            .expect("the materializing evaluator requires an inner operator");
        let root = materialize_candidate_join(
            outer_root,
            inner_root,
            spec.join_type.clone(),
            spec.edge_indices,
            spec.hypergraph,
            ctx,
        );
        let cost = cost_model.total_cost_from_children(
            root,
            &[outer.cost.clone(), inner.cost.clone()],
            ctx,
            analyses,
        )?;
        Ok(EvaluatedPlan {
            cost,
            materialized: Some(root),
            immediate_cost: None,
        })
    }
}
