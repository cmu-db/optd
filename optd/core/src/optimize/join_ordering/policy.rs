//! Adaptive algorithm selection for small, medium, and very large join groups.

use super::graph::{BoundedCount, JoinGraph};
use crate::QueryHypergraph;

/// Join enumeration algorithm selected for one join group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinOrderAlgorithm {
    /// Complete DPhyp enumeration.
    DpHyp,
    /// O(n³) interval DP over a connectivity-preserving linearization.
    LinearizedDp,
    /// Greedy Operator Ordering with exact DP inside bounded subtrees.
    GooDp,
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
    /// Largest greedy subtree replaced by an exact DPhyp solution.
    pub goo_exact_subproblem_size: usize,
}

impl Default for AdaptiveJoinOrderingConfig {
    fn default() -> Self {
        Self {
            exact_relation_threshold: 14,
            connected_subgraph_budget: 10_000,
            linearized_relation_threshold: 100,
            goo_exact_subproblem_size: 10,
        }
    }
}

/// Observable explanation of one adaptive choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlgorithmDecision {
    pub algorithm: JoinOrderAlgorithm,
    /// Exact count when it stayed within budget; `None` means counting exceeded the budget or
    /// was deliberately skipped for a very large group.
    pub connected_subgraphs: Option<usize>,
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
        };
    }

    if relation_count <= config.linearized_relation_threshold {
        match JoinGraph::new(hypergraph).count_connected_subgraphs(config.connected_subgraph_budget)
        {
            BoundedCount::Within(count) => {
                return AlgorithmDecision {
                    algorithm: JoinOrderAlgorithm::DpHyp,
                    connected_subgraphs: Some(count),
                };
            }
            BoundedCount::Exceeded => {}
        }
    }

    let graph = JoinGraph::new(hypergraph);
    AlgorithmDecision {
        algorithm: if relation_count <= config.linearized_relation_threshold
            && !graph.has_hyperedges()
        {
            JoinOrderAlgorithm::LinearizedDp
        } else {
            JoinOrderAlgorithm::GooDp
        },
        connected_subgraphs: None,
    }
}
