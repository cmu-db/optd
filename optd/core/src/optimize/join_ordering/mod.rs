//! Adaptive cost-based join ordering over query hypergraphs.
//!
//! This module is the integration layer between optd's relational IR, the query-hypergraph
//! builder, the cost model, and three join enumerators:
//!
//! - `DPhyp` performs complete csg-cmp enumeration and produces an optimal bushy tree for the
//!   configured cost model.
//! - `linearized` builds a selectivity-minimum spanning tree, derives an ASI-compatible `C_out`
//!   order with IKKBZ, and runs polynomial-time interval DP using the configured cost model.
//! - `goo` builds the canonical minimum-output-cardinality bushy tree, then spends one global
//!   DP-state budget repairing and contracting bounded frontiers.
//!
//! [`JoinOrdering`] discovers every maximal contiguous join group, builds one
//! [`crate::QueryHypergraph`] per group, asks the `policy` module to choose an enumerator, and
//! records the resulting root in the optimizer rewrite map. Join groups are handled bottom-up so
//! an inner group separated by aggregation, projection, or another non-join boundary is available
//! before its enclosing group is rewritten.
//!
//! # Search-space invariants
//!
//! A DP key is a [`crate::hypergraph::NodeSet`] containing exactly the hypergraph nodes represented
//! by a partial plan. Every candidate combines two disjoint connected sets for which at least one
//! hyperedge is applicable. All newly applicable edge predicates are attached at that join, so
//! reconstruction neither loses nor duplicates predicates.
//!
//! DPhyp's exclusion sets provide canonical enumeration: `B_min(S)` prevents an equivalent
//! connected-subgraph/complement pair from being reached through a different seed. The
//! [`crate::RelationSet`] backing [`crate::hypergraph::NodeSet`] has no machine-word relation
//! limit. It canonicalizes equal values across inline-64, inline-128, dense, and sparse storage,
//! so the same algorithms apply from ordinary queries through thousand-way joins.
//!
//! # Join orientation
//!
//! Inner joins may be costed in one canonical orientation because the default cost model is
//! symmetric. Models that override [`CostModel::is_join_orientation_cost_sensitive`] cause both
//! orientations to be costed. Non-commutative joins are never freely swapped: their directed
//! hyperedge endpoints determine which partial plan is the outer and which is the inner input.
//!
//! # Adaptive policy
//!
//! [`AdaptiveJoinOrderingConfig`] controls the exact-state budget and relation-count thresholds.
//! Matching Figure 8 of Neumann and Radke, the default policy uses exact DPhyp for fewer than 14
//! relations or at most 10,000 connected subgraphs. Other ordinary graphs use direct linearized
//! DP through 100 relations and GOO with linearized-DP repair above that; hypergraphs use GOO with
//! exact DPhyp repair. [`JoinOrdering::last_decisions`] exposes both the choice and actual DP-state
//! and repair counts from the most recent run.
//!
//! # Arena allocation
//!
//! The default evaluator carries shared cardinality profiles through compact plan recipes, so
//! rejected alternatives never append operators to [`crate::QueryContext`]. Once enumeration
//! finishes, only the winning recipe is materialized in iterative post-order. Custom cost models
//! retain the compatibility evaluator: it materializes candidates before costing because
//! arbitrary models may inspect concrete operator handles. That evaluator builds cardinality
//! profiles only when the selected enumerator needs them: linearized DP and GOO rank candidates by
//! output cardinality, while exact DPhyp needs only the configured plan cost. Analyses are
//! explicitly cleared before group construction because both leaf costing and compatibility
//! evaluation are demand-driven.

mod candidate;
mod dphyp;
mod evaluator;
mod goo;
mod graph;
mod groups;
mod linearized;
mod plan;
mod policy;

#[cfg(test)]
mod tests;

pub use goo::{GooDpConfig, GooInnerSolver};
pub use groups::collect_join_group_roots;
pub use policy::{AdaptiveJoinOrderingConfig, AlgorithmDecision, JoinOrderAlgorithm};

use crate::cost::{CostModel, DefaultCostModel};
use crate::{OptimizerContext, build_hypergraph};

use super::{OptimizeResult, Pass, PassMode, PassResult, QueryPass};
use candidate::JoinSearch;
use dphyp::DPhyp;
use evaluator::{CandidateEvaluator, CardinalityEvaluator, MaterializingEvaluator};
use policy::choose_algorithm;

// ---------------------------------------------------------------------------
// JoinOrdering pass
// ---------------------------------------------------------------------------

/// Cost-based join-ordering pass with adaptive exact and bounded search.
///
/// The pass optimizes each maximal join group independently. For every group it:
///
/// 1. constructs a query hypergraph using CD-E test-edge sets;
/// 2. selects an algorithm with [`AdaptiveJoinOrderingConfig`];
/// 3. materializes the cheapest plan found by that algorithm; and
/// 4. installs the winning root through the optimizer's rewrite map.
///
/// The default type parameter uses [`DefaultCostModel`]. Supply another [`CostModel`] with
/// [`JoinOrdering::with_cost_model`] when plan comparison needs a different cost algebra.
///
/// Join ordering declares [`PassMode::Once`]: enumeration is final for this pipeline position, so
/// a fixed-point reinvocation would repeat work without exposing a new optimization opportunity.
pub struct JoinOrdering<M: CostModel = DefaultCostModel> {
    /// Cost model shared by all enumerators.
    cost_model: M,
    /// Thresholds controlling adaptive algorithm selection.
    config: AdaptiveJoinOrderingConfig,
    /// Candidate representation and costing strategy selected with the cost model.
    evaluator: Box<dyn CandidateEvaluator<M>>,
    /// Per-group decisions from the most recent attempted run.
    last_decisions: Vec<AlgorithmDecision>,
}

impl JoinOrdering<DefaultCostModel> {
    /// Creates a pass with the default cost model and adaptive thresholds.
    pub fn new() -> Self {
        Self {
            cost_model: DefaultCostModel,
            config: AdaptiveJoinOrderingConfig::default(),
            evaluator: Box::new(CardinalityEvaluator),
            last_decisions: Vec::new(),
        }
    }

    /// Creates a default-cost pass with explicitly configured adaptive thresholds.
    pub fn with_config(config: AdaptiveJoinOrderingConfig) -> Self {
        Self {
            config,
            ..Self::new()
        }
    }

    /// Creates a pass with a custom cost model and default adaptive thresholds.
    ///
    /// This constructor selects the materializing compatibility evaluator because an arbitrary
    /// model may inspect concrete operator handles or override `total_cost_from_children`. Use
    /// [`JoinOrdering::new`] for deferred evaluation with the built-in model.
    ///
    /// Use [`JoinOrdering::adaptive_config`] on the returned value when both the model and policy
    /// should be customized.
    pub fn with_cost_model<M: CostModel>(cost_model: M) -> JoinOrdering<M> {
        JoinOrdering {
            cost_model,
            config: AdaptiveJoinOrderingConfig::default(),
            evaluator: Box::new(MaterializingEvaluator),
            last_decisions: Vec::new(),
        }
    }
}

impl<M: CostModel> JoinOrdering<M> {
    /// Overrides adaptive thresholds while preserving the configured cost model.
    ///
    /// This builder is useful after [`JoinOrdering::with_cost_model`]. It consumes and returns the
    /// pass so configuration remains immutable once optimization begins.
    pub fn adaptive_config(mut self, config: AdaptiveJoinOrderingConfig) -> Self {
        self.config = config;
        self
    }

    /// Returns decisions made for join groups in the most recent attempted optimizer run.
    ///
    /// Entries follow [`collect_join_group_roots`] order. The slice is empty when the query had no
    /// optimizable join group.
    pub fn last_decisions(&self) -> &[AlgorithmDecision] {
        &self.last_decisions
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
    fn mode(&self) -> PassMode {
        PassMode::Once
    }

    fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
        self.last_decisions.clear();

        let Some(root) = ctx.query.root() else {
            return Ok(PassResult::Unchanged);
        };

        // Cached analyses may refer to operator roots that earlier passes just rewrote.
        ctx.analyses.clear();

        // Groups are returned bottom-up; build all immutable hypergraphs before appending any
        // candidate operators to the query arena.
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

        // Default-cost enumeration keeps candidates as recipes. Custom evaluators may append
        // candidates for compatibility, but only winning roots enter replacements.
        let mut replacements = Vec::new();
        for (group_root, hg) in &groups {
            let decision = choose_algorithm(hg, self.config);
            let cardinality_required = !matches!(decision.algorithm, JoinOrderAlgorithm::DpHyp);
            let mut search = JoinSearch::with_cardinality_requirement(
                &mut ctx.query,
                &mut ctx.analyses,
                hg,
                &self.cost_model,
                self.evaluator.as_ref(),
                cardinality_required,
            );
            let (plan, dp_states_created, repaired_subproblems) = match decision.algorithm {
                JoinOrderAlgorithm::DpHyp => {
                    let outcome = DPhyp::new(&mut search).solve_with_stats()?;
                    (outcome.plan, outcome.dp_states_created, 0)
                }
                JoinOrderAlgorithm::LinearizedDp => {
                    let outcome = linearized::solve_with_stats(&mut search)?;
                    (outcome.plan, outcome.dp_states_created, 0)
                }
                JoinOrderAlgorithm::GooDp(config) => {
                    let outcome = goo::solve(&mut search, config)?;
                    (
                        outcome.plan,
                        outcome.dp_states_created,
                        outcome.repaired_subproblems,
                    )
                }
            };
            self.last_decisions
                .push(decision.record_execution(dp_states_created, repaired_subproblems));
            if let Some(plan) = plan {
                let root = search.materialize(&plan);
                replacements.push((*group_root, root));
            }
        }
        if replacements.is_empty() {
            return Ok(PassResult::Unchanged);
        }

        // Defer rewrite-map mutation until every group has been solved successfully. This keeps an
        // error from exposing a partially rewritten query.
        for (group_root, new_op) in replacements {
            ctx.rewrites.replace(group_root, new_op);
        }

        // Rebuild non-join ancestors so the query root reaches the new join-group roots.
        super::materialize_reachable_rewrites(root, ctx);

        Ok(PassResult::Changed)
    }
}
