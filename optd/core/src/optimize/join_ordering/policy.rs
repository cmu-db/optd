//! Adaptive algorithm selection for small, medium, and very large join groups.

use super::goo::{GooDpConfig, GooInnerSolver};
use super::graph::{BoundedCount, JoinGraph};
use crate::QueryHypergraph;

/// Join enumeration algorithm selected for one join group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinOrderAlgorithm {
    /// Complete DPhyp enumeration.
    DpHyp,
    /// O(n³) interval DP over an IKKBZ linearization.
    LinearizedDp,
    /// Greedy Operator Ordering with globally budgeted DP repair.
    GooDp(GooDpConfig),
}

impl JoinOrderAlgorithm {
    /// Stable, delimiter-free label for diagnostics and benchmark output.
    pub const fn label(self) -> &'static str {
        match self {
            Self::DpHyp => "dphyp",
            Self::LinearizedDp => "linearized_dp",
            Self::GooDp(GooDpConfig {
                inner: GooInnerSolver::DpHyp,
                ..
            }) => "goo_dphyp",
            Self::GooDp(GooDpConfig {
                inner: GooInnerSolver::LinearizedDp,
                ..
            }) => "goo_linearized_dp",
        }
    }
}

/// Tunable limits for adaptive join enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveJoinOrderingConfig {
    /// Groups smaller than this always use exact DPhyp.
    pub exact_relation_threshold: usize,
    /// Maximum connected DP states allowed for exact DPhyp.
    pub connected_subgraph_budget: usize,
    /// Largest group for O(n³) linearized DP.
    pub linearized_relation_threshold: usize,
    /// Largest contracted regular-graph frontier optimized by linearized DP.
    pub goo_linearized_subproblem_size: usize,
    /// Largest contracted hypergraph frontier optimized exactly.
    pub goo_dphyp_subproblem_size: usize,
    /// Global number of inner-DP table entries available to GOO/DP.
    pub goo_dp_state_budget: usize,
}

impl Default for AdaptiveJoinOrderingConfig {
    fn default() -> Self {
        Self {
            exact_relation_threshold: 14,
            connected_subgraph_budget: 10_000,
            linearized_relation_threshold: 100,
            goo_linearized_subproblem_size: 100,
            goo_dphyp_subproblem_size: 10,
            goo_dp_state_budget: 10_000,
        }
    }
}

/// Observable explanation of one adaptive choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlgorithmDecision {
    pub algorithm: JoinOrderAlgorithm,
    /// Exact count when it stayed within budget; `None` means counting exceeded the budget or
    /// was skipped by the unconditional small-query path.
    pub connected_subgraphs: Option<usize>,
    /// Unique DP-table states built by the selected algorithm.
    pub dp_states_created: usize,
    /// GOO subproblems optimized and contracted; zero for non-GOO algorithms.
    pub repaired_subproblems: usize,
}

impl AlgorithmDecision {
    pub(super) fn record_execution(
        mut self,
        dp_states_created: usize,
        repaired_subproblems: usize,
    ) -> Self {
        self.dp_states_created = dp_states_created;
        self.repaired_subproblems = repaired_subproblems;
        self
    }
}

pub(super) fn choose_algorithm(
    hypergraph: &QueryHypergraph,
    config: AdaptiveJoinOrderingConfig,
) -> AlgorithmDecision {
    let relation_count = hypergraph.nodes.len();
    if relation_count < config.exact_relation_threshold {
        return AlgorithmDecision {
            algorithm: JoinOrderAlgorithm::DpHyp,
            connected_subgraphs: None,
            dp_states_created: 0,
            repaired_subproblems: 0,
        };
    }

    let graph = JoinGraph::new(hypergraph);
    match graph.count_connected_subgraphs(config.connected_subgraph_budget) {
        BoundedCount::Within(count) => {
            return AlgorithmDecision {
                algorithm: JoinOrderAlgorithm::DpHyp,
                connected_subgraphs: Some(count),
                dp_states_created: 0,
                repaired_subproblems: 0,
            };
        }
        BoundedCount::Exceeded => {}
    }

    let algorithm = if graph.supports_ikkbz_linearization() {
        if relation_count <= config.linearized_relation_threshold {
            JoinOrderAlgorithm::LinearizedDp
        } else {
            JoinOrderAlgorithm::GooDp(GooDpConfig {
                inner: GooInnerSolver::LinearizedDp,
                max_subproblem_relations: config.goo_linearized_subproblem_size,
                dp_state_budget: config.goo_dp_state_budget,
            })
        }
    } else {
        JoinOrderAlgorithm::GooDp(GooDpConfig {
            inner: GooInnerSolver::DpHyp,
            max_subproblem_relations: config.goo_dphyp_subproblem_size,
            dp_state_budget: config.goo_dp_state_budget,
        })
    };

    AlgorithmDecision {
        algorithm,
        connected_subgraphs: None,
        dp_states_created: 0,
        repaired_subproblems: 0,
    }
}
