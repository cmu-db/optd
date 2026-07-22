//! Shared search state, candidate orientation, costing, and recipe commitment.

use super::OptimizeResult;
use super::evaluator::{CandidateEvaluator, CandidateInput, EvaluatedPlan, JoinSpec};
#[cfg(test)]
use super::plan::JoinTree;
use super::plan::{PlanArena, PlanId, PlanRecipe, PlanState};
use crate::cost::CostModel;
use crate::hypergraph::{NodeSet, QueryHypergraph};
use crate::optimize::OptimizeError;
use crate::{AnalysisContext, JoinType, Operator, OperatorData, QueryContext};

/// Per-group coordinator shared by every join enumerator.
///
/// It owns accepted plan recipes and centralizes the boundary between enumeration and candidate
/// evaluation. Keeping DPhyp, linearized DP, and GOO on this one interface prevents their costing
/// and reconstruction behavior from drifting apart.
pub(super) struct JoinSearch<'a, M: CostModel> {
    ctx: &'a mut QueryContext,
    analyses: &'a mut AnalysisContext,
    hypergraph: &'a QueryHypergraph,
    cost_model: &'a M,
    evaluator: &'a dyn CandidateEvaluator<M>,
    plans: PlanArena,
    leaf_plans: Vec<Option<PlanId>>,
}

/// Evaluated but not yet accepted join alternative.
///
/// Delaying recipe commitment until the enumerator chooses this candidate keeps the recipe arena
/// proportional to accepted DP states. The default evaluator can drop a draft without having
/// appended any IR; compatibility evaluators preserve the historical eager-allocation behavior.
pub(super) struct CandidateDraft<C> {
    recipe: PlanRecipe,
    evaluated: EvaluatedPlan<C>,
    #[cfg(test)]
    tree: JoinTree,
}

struct OrientedCandidate<C> {
    outer: PlanId,
    inner: PlanId,
    join_type: JoinType,
    evaluated: EvaluatedPlan<C>,
    #[cfg(test)]
    tree: JoinTree,
}

impl<C> CandidateDraft<C> {
    pub(super) fn cost(&self) -> &C {
        &self.evaluated.cost
    }
}

impl<'a, M: CostModel> JoinSearch<'a, M> {
    pub(super) fn new(
        ctx: &'a mut QueryContext,
        analyses: &'a mut AnalysisContext,
        hypergraph: &'a QueryHypergraph,
        cost_model: &'a M,
        evaluator: &'a dyn CandidateEvaluator<M>,
    ) -> Self {
        Self {
            ctx,
            analyses,
            hypergraph,
            cost_model,
            evaluator,
            plans: PlanArena::default(),
            leaf_plans: vec![None; hypergraph.nodes.len()],
        }
    }

    pub(super) fn hypergraph(&self) -> &'a QueryHypergraph {
        self.hypergraph
    }

    pub(super) fn leaf(&mut self, node: usize) -> OptimizeResult<PlanState<M::Cost>> {
        let root = self.hypergraph.nodes[node].root;
        let evaluated =
            self.evaluator
                .evaluate_leaf(self.cost_model, root, self.ctx, self.analyses)?;
        let plan = match self.leaf_plans[node] {
            Some(plan) => plan,
            None => {
                let plan = self
                    .plans
                    .commit(PlanRecipe::Leaf { root }, evaluated.materialized);
                self.leaf_plans[node] = Some(plan);
                plan
            }
        };
        Ok(PlanState {
            plan,
            cost: evaluated.cost,
            properties: evaluated.properties,
            #[cfg(test)]
            tree: JoinTree::Leaf(node),
        })
    }

    pub(super) fn is_better(&self, candidate: &M::Cost, existing: &M::Cost) -> bool {
        self.cost_model.is_better(candidate, existing)
    }

    pub(super) fn commit(&mut self, candidate: CandidateDraft<M::Cost>) -> PlanState<M::Cost> {
        let plan = self
            .plans
            .commit(candidate.recipe, candidate.evaluated.materialized);
        PlanState {
            plan,
            cost: candidate.evaluated.cost,
            properties: candidate.evaluated.properties,
            #[cfg(test)]
            tree: candidate.tree,
        }
    }

    /// Returns the local operator cost used by GOO's greedy comparison.
    pub(super) fn immediate_cost(
        &mut self,
        candidate: &CandidateDraft<M::Cost>,
    ) -> OptimizeResult<M::Cost> {
        if let Some(cost) = &candidate.evaluated.immediate_cost {
            return Ok(cost.clone());
        }
        let root = candidate
            .evaluated
            .materialized
            .ok_or_else(|| OptimizeError::PassError {
                pass: "JoinOrdering",
                message:
                    "candidate evaluator returned neither an immediate cost nor a materialized root"
                        .to_string(),
            })?;
        self.cost_model.operator_cost(root, self.ctx, self.analyses)
    }

    /// Materializes an accepted plan, recursively reconstructing a deferred recipe when needed.
    pub(super) fn materialize(&mut self, plan: &PlanState<M::Cost>) -> Operator {
        self.plans.materialize(plan.plan, self.hypergraph, self.ctx)
    }

    /// Evaluates the valid orientation(s) of one connected candidate join.
    ///
    /// Directed TES endpoints fix non-inner orientation. Inner joins use the supplied orientation
    /// unless the cost model declares orientation sensitivity, in which case both alternatives
    /// are evaluated. `None` means no connecting edge was supplied.
    pub(super) fn best_join_candidate(
        &mut self,
        left_nodes: &NodeSet,
        left: &PlanState<M::Cost>,
        right_nodes: &NodeSet,
        right: &PlanState<M::Cost>,
        edge_indices: Vec<usize>,
    ) -> OptimizeResult<Option<CandidateDraft<M::Cost>>> {
        if edge_indices.is_empty() {
            return Ok(None);
        }

        let join_type = candidate_join_type(&edge_indices, self.hypergraph, self.ctx);
        let candidate = if join_type != JoinType::Inner {
            let first_edge = &self.hypergraph.edges[edge_indices[0]];
            if first_edge.left.is_subset(left_nodes) && first_edge.right.is_subset(right_nodes) {
                self.cost_join_orientation(left, right, join_type, &edge_indices)?
            } else {
                self.cost_join_orientation(right, left, join_type, &edge_indices)?
            }
        } else {
            let forward =
                self.cost_join_orientation(left, right, JoinType::Inner, &edge_indices)?;
            if !self.cost_model.is_join_orientation_cost_sensitive() {
                forward
            } else {
                let reverse =
                    self.cost_join_orientation(right, left, JoinType::Inner, &edge_indices)?;
                if self
                    .cost_model
                    .is_better(&reverse.evaluated.cost, &forward.evaluated.cost)
                {
                    reverse
                } else {
                    forward
                }
            }
        };

        Ok(Some(CandidateDraft {
            recipe: PlanRecipe::Join {
                outer: candidate.outer,
                inner: candidate.inner,
                join_type: candidate.join_type,
                edge_indices,
            },
            evaluated: candidate.evaluated,
            #[cfg(test)]
            tree: candidate.tree,
        }))
    }

    fn cost_join_orientation(
        &mut self,
        outer: &PlanState<M::Cost>,
        inner: &PlanState<M::Cost>,
        join_type: JoinType,
        edge_indices: &[usize],
    ) -> OptimizeResult<OrientedCandidate<M::Cost>> {
        let (outer_root, inner_root) = if self.evaluator.requires_materialized_inputs() {
            (
                Some(
                    self.plans
                        .materialize(outer.plan, self.hypergraph, self.ctx),
                ),
                Some(
                    self.plans
                        .materialize(inner.plan, self.hypergraph, self.ctx),
                ),
            )
        } else {
            (None, None)
        };
        let evaluated = self.evaluator.evaluate_join(
            self.cost_model,
            JoinSpec {
                join_type: &join_type,
                edge_indices,
                hypergraph: self.hypergraph,
            },
            CandidateInput {
                cost: &outer.cost,
                properties: &outer.properties,
                materialized: outer_root,
            },
            CandidateInput {
                cost: &inner.cost,
                properties: &inner.properties,
                materialized: inner_root,
            },
            self.ctx,
            self.analyses,
        )?;

        Ok(OrientedCandidate {
            outer: outer.plan,
            inner: inner.plan,
            join_type,
            evaluated,
            #[cfg(test)]
            tree: JoinTree::Join {
                left: Box::new(outer.tree.clone()),
                right: Box::new(inner.tree.clone()),
            },
        })
    }
}

/// Recovers the exact logical join type represented by a set of connecting hyperedges.
///
/// CD-E compatibility guarantees that simultaneously applicable edges agree on semantics. Dummy
/// cross-product edges and defensive fallbacks are treated as inner joins.
fn candidate_join_type(
    edge_indices: &[usize],
    hypergraph: &QueryHypergraph,
    ctx: &QueryContext,
) -> JoinType {
    edge_indices
        .first()
        .and_then(|&index| match hypergraph.edges[index].source.get(ctx) {
            OperatorData::Join(join) => Some(join.join_type.clone()),
            OperatorData::CrossProduct(_) => Some(JoinType::Inner),
            _ => None,
        })
        .unwrap_or(JoinType::Inner)
}
