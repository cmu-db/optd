//! Greedy Operator Ordering with globally budgeted dynamic-programming repair.
//!
//! This implements Figure 7 of Neumann and Radke, “Adaptive Optimization of Very Large Join
//! Queries” (SIGMOD 2018):
//!
//! 1. GOO constructs a bushy tree by repeatedly joining the connected pair with the smallest
//!    estimated output cardinality.
//! 2. The most expensive maximal subtree with at most `k` visible inputs is optimized by an inner
//!    DP solver.
//! 3. The optimized subtree becomes one opaque input. This contraction can expose a larger
//!    ancestor as the next bounded repair problem without reopening work already optimized.
//! 4. The global budget is charged by the number of unique DP states actually created.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::OptimizeResult;
use super::candidate::{CandidateDraft, JoinSearch};
use super::dphyp;
use super::graph::{EdgeBoundary, JoinGraph};
use super::linearized;
use super::plan::{PlanAtom, PlanState, SolveOutcome};
use crate::OptimizeError;
use crate::cost::CostModel;
use crate::hypergraph::{NodeSet, nodeset_min, nodeset_singleton};

/// Inner optimizer used for each contracted GOO subtree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GooInnerSolver {
    /// Complete bushy enumeration over at most `k` opaque inputs.
    DpHyp,
    /// IKKBZ linearization followed by interval DP over at most `k` opaque inputs.
    LinearizedDp,
}

/// Configuration for one paper-style GOO/DP invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GooDpConfig {
    /// Dynamic-programming algorithm used for each bounded repair.
    pub inner: GooInnerSolver,
    /// Maximum number of opaque frontier inputs visible to one repair.
    pub max_subproblem_relations: usize,
    /// Global number of inner-DP table entries available across all repairs.
    pub dp_state_budget: usize,
}

/// Result and budget telemetry for one GOO/DP invocation.
pub(super) struct GooDpOutcome<C> {
    pub(super) plan: Option<PlanState<C>>,
    /// Unique inner-DP states charged across all repair calls.
    pub(super) dp_states_created: usize,
    /// Subproblems successfully optimized and contracted into opaque inputs.
    pub(super) repaired_subproblems: usize,
    #[cfg(test)]
    repair_trace: Vec<RepairTrace>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct RepairTrace {
    nodes: NodeSet,
    original_relations: usize,
    frontier_relations: usize,
}

struct GooNode<C> {
    /// Original hypergraph relations covered by this subtree.
    nodes: NodeSet,
    state: PlanState<C>,
    children: Option<(usize, usize)>,
    parent: Option<usize>,
    /// Number of currently visible opaque inputs below this node.
    effective_size: usize,
    /// An atomic node is an opaque input for every later ancestor repair.
    atomic: bool,
    /// Descendants of an atomic node remain in the arena for reconstruction but leave the
    /// scheduler's visible tree.
    hidden: bool,
}

struct GooTree<C> {
    nodes: Vec<GooNode<C>>,
    root: usize,
}

/// Maps every original relation to its current active GOO component.
///
/// GOO tree nodes use append-only IDs, so updating every relation whenever a parent is appended
/// would make a long chain quadratic. This small disjoint-set forest instead changes only two
/// roots and maps the surviving root to the new tree node.
struct ComponentIndex {
    parents: Vec<usize>,
    sizes: Vec<usize>,
    tree_nodes: Vec<usize>,
}

impl ComponentIndex {
    fn new(relation_count: usize) -> Self {
        Self {
            parents: (0..relation_count).collect(),
            sizes: vec![1; relation_count],
            tree_nodes: (0..relation_count).collect(),
        }
    }

    fn find(&mut self, relation: usize) -> usize {
        let mut root = relation;
        while self.parents[root] != root {
            root = self.parents[root];
        }

        let mut current = relation;
        while self.parents[current] != current {
            let parent = self.parents[current];
            self.parents[current] = root;
            current = parent;
        }
        root
    }

    fn component_covering(&mut self, relations: &NodeSet) -> Option<usize> {
        let mut relations = relations.iter();
        let root = self.find(relations.next()?);
        relations
            .all(|relation| self.find(relation) == root)
            .then_some(self.tree_nodes[root])
    }

    fn applicable_pair(
        &mut self,
        graph: &JoinGraph<'_>,
        edge_index: usize,
    ) -> Option<(usize, usize)> {
        let (left_endpoint, right_endpoint) = graph.edge_endpoints(edge_index);
        let left = self.component_covering(left_endpoint)?;
        let right = self.component_covering(right_endpoint)?;
        (left != right).then_some(if left < right {
            (left, right)
        } else {
            (right, left)
        })
    }

    fn merge(
        &mut self,
        left_relations: &NodeSet,
        right_relations: &NodeSet,
        parent_tree_node: usize,
    ) {
        let mut left = self.find(nodeset_min(left_relations));
        let mut right = self.find(nodeset_min(right_relations));
        debug_assert_ne!(left, right);
        if self.sizes[left] < self.sizes[right] {
            std::mem::swap(&mut left, &mut right);
        }
        self.parents[right] = left;
        self.sizes[left] += self.sizes[right];
        self.tree_nodes[left] = parent_tree_node;
    }
}

/// Edge-indexed view of the active greedy frontier.
///
/// A component retains only the hyperedges crossing its boundary. Merging consumes both child
/// sets, discards newly internal edges, and probes only the resulting boundary for candidate
/// neighbors.
struct GreedyFrontier {
    components: ComponentIndex,
    boundaries: Vec<Option<EdgeBoundary>>,
}

struct FrontierMerge<'a> {
    left: usize,
    right: usize,
    parent: usize,
    left_relations: &'a NodeSet,
    right_relations: &'a NodeSet,
    merged_relations: &'a NodeSet,
}

impl GreedyFrontier {
    fn new(graph: &JoinGraph<'_>, relation_count: usize) -> Self {
        Self {
            components: ComponentIndex::new(relation_count),
            boundaries: (0..relation_count)
                .map(|relation| Some(graph.singleton_boundary(relation)))
                .collect(),
        }
    }

    fn initial_candidate_pairs(&mut self, graph: &JoinGraph<'_>) -> Vec<(usize, usize)> {
        self.candidate_pairs_for_edges(graph, 0..graph.edge_count())
    }

    fn merge(&mut self, graph: &JoinGraph<'_>, merge: FrontierMerge<'_>) -> Vec<(usize, usize)> {
        let left_boundary = self.boundaries[merge.left]
            .take()
            .expect("an active GOO child has a boundary");
        let right_boundary = self.boundaries[merge.right]
            .take()
            .expect("an active GOO child has a boundary");
        let parent_boundary =
            graph.merge_boundaries(left_boundary, right_boundary, merge.merged_relations);

        self.components
            .merge(merge.left_relations, merge.right_relations, merge.parent);
        debug_assert_eq!(merge.parent, self.boundaries.len());
        self.boundaries.push(Some(parent_boundary));

        let boundary = self.boundaries[merge.parent]
            .as_ref()
            .expect("a new GOO parent has a boundary");
        let components = &mut self.components;
        let mut pairs = boundary
            .iter()
            .filter_map(|edge_index| components.applicable_pair(graph, edge_index))
            .collect::<Vec<_>>();
        pairs.sort_unstable();
        pairs.dedup();
        debug_assert!(
            pairs
                .iter()
                .all(|(left, right)| *left == merge.parent || *right == merge.parent)
        );
        pairs
    }

    fn candidate_pairs_for_edges(
        &mut self,
        graph: &JoinGraph<'_>,
        edge_indices: impl IntoIterator<Item = usize>,
    ) -> Vec<(usize, usize)> {
        let mut pairs = edge_indices
            .into_iter()
            .filter_map(|edge_index| self.components.applicable_pair(graph, edge_index))
            .collect::<Vec<_>>();
        pairs.sort_unstable();
        pairs.dedup();
        pairs
    }
}

/// Total ordering key for GOO's min-cardinality priority queue.
///
/// [`BinaryHeap`] is a max-heap, so both comparisons are reversed: fewer output rows win, followed
/// by the lexicographically smaller pair of canonical relation representatives.
#[derive(Debug, Clone, Copy)]
struct GreedyKey {
    output_rows: f64,
    first_relation: usize,
    second_relation: usize,
}

impl PartialEq for GreedyKey {
    fn eq(&self, other: &Self) -> bool {
        self.output_rows.total_cmp(&other.output_rows) == Ordering::Equal
            && self.first_relation == other.first_relation
            && self.second_relation == other.second_relation
    }
}

impl Eq for GreedyKey {}

impl PartialOrd for GreedyKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GreedyKey {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .output_rows
            .total_cmp(&self.output_rows)
            .then_with(|| other.first_relation.cmp(&self.first_relation))
            .then_with(|| other.second_relation.cmp(&self.second_relation))
    }
}

struct GreedyChoice<C> {
    key: GreedyKey,
    left: usize,
    right: usize,
    candidate: CandidateDraft<C>,
}

impl<C> PartialEq for GreedyChoice<C> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<C> Eq for GreedyChoice<C> {}

impl<C> PartialOrd for GreedyChoice<C> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<C> Ord for GreedyChoice<C> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

/// Builds the GOO tree and spends a global DP-state budget improving contracted subproblems.
pub(super) fn solve<M: CostModel>(
    search: &mut JoinSearch<'_, M>,
    config: GooDpConfig,
) -> OptimizeResult<GooDpOutcome<M::Cost>> {
    let Some(mut tree) = build_greedy_tree(search)? else {
        return Ok(GooDpOutcome {
            plan: None,
            dp_states_created: 0,
            repaired_subproblems: 0,
            #[cfg(test)]
            repair_trace: Vec::new(),
        });
    };

    let mut budget_remaining = config.dp_state_budget;
    let mut dp_states_created = 0usize;
    let mut repaired_subproblems = 0usize;
    #[cfg(test)]
    let mut repair_trace = Vec::new();

    while budget_remaining > 0 && config.max_subproblem_relations >= 2 {
        let Some(node) = tree.most_expensive_repair(search, config.max_subproblem_relations) else {
            break;
        };
        let frontier = tree.frontier(node);
        debug_assert_eq!(frontier.len(), tree.nodes[node].effective_size);
        debug_assert!(frontier.len() <= config.max_subproblem_relations);

        #[cfg(test)]
        repair_trace.push(RepairTrace {
            nodes: tree.nodes[node].nodes.clone(),
            original_relations: tree.nodes[node].nodes.len(),
            frontier_relations: frontier.len(),
        });

        let outcome = solve_frontier(search, config.inner, &frontier)?;
        dp_states_created = dp_states_created.saturating_add(outcome.dp_states_created);
        budget_remaining = budget_remaining.saturating_sub(outcome.dp_states_created);

        let optimized = outcome.plan.ok_or_else(|| OptimizeError::PassError {
            pass: "JoinOrdering",
            message: format!(
                "GOO {:?} repair produced no plan for a connected frontier of {} inputs",
                config.inner,
                frontier.len()
            ),
        })?;

        // Exact DPhyp should never be worse, but linearized DP and custom cost models need this
        // guard. Even when the candidate is rejected, the retained subtree becomes one opaque
        // input so the iterative scheduler still makes progress.
        let existing_is_better = search.is_better(&tree.nodes[node].state.cost, &optimized.cost);
        if !existing_is_better {
            tree.nodes[node].state = optimized;
            tree.rebuild_ancestors(node, search)?;
        }
        tree.make_atomic(node);
        repaired_subproblems += 1;
    }

    Ok(GooDpOutcome {
        plan: Some(tree.nodes[tree.root].state.clone()),
        dp_states_created,
        repaired_subproblems,
        #[cfg(test)]
        repair_trace,
    })
}

fn solve_frontier<M: CostModel>(
    search: &mut JoinSearch<'_, M>,
    inner: GooInnerSolver,
    frontier: &[PlanAtom<M::Cost>],
) -> OptimizeResult<SolveOutcome<M::Cost>> {
    match inner {
        GooInnerSolver::DpHyp => dphyp::solve_frontier_with_stats(search, frontier),
        GooInnerSolver::LinearizedDp => linearized::solve_frontier_with_stats(search, frontier),
    }
}

/// Constructs the canonical GOO tree.
///
/// Candidate joins are evaluated once and retained in a priority queue. After a merge, only pairs
/// involving the newly created component can have changed applicability. Stable component IDs make
/// every heap entry self-invalidating as soon as either input is consumed.
fn build_greedy_tree<M: CostModel>(
    search: &mut JoinSearch<'_, M>,
) -> OptimizeResult<Option<GooTree<M::Cost>>> {
    let hypergraph = search.hypergraph();
    if hypergraph.nodes.is_empty() {
        return Ok(None);
    }

    let graph = JoinGraph::new(hypergraph);
    let mut tree = GooTree {
        nodes: Vec::with_capacity(hypergraph.nodes.len().saturating_mul(2).saturating_sub(1)),
        root: 0,
    };
    for node in 0..hypergraph.nodes.len() {
        tree.nodes.push(GooNode {
            nodes: nodeset_singleton(node),
            state: search.leaf(node)?,
            children: None,
            parent: None,
            effective_size: 1,
            atomic: true,
            hidden: false,
        });
    }

    let mut active = vec![true; tree.nodes.len()];
    let mut frontier = GreedyFrontier::new(&graph, tree.nodes.len());
    let mut choices = BinaryHeap::new();
    for (left, right) in frontier.initial_candidate_pairs(&graph) {
        push_greedy_choice(search, &graph, &tree, &mut choices, left, right)?;
    }

    let mut active_count = tree.nodes.len();
    while active_count > 1 {
        let best = loop {
            let Some(choice) = choices.pop() else {
                return Ok(None);
            };
            if active[choice.left] && active[choice.right] {
                break choice;
            }
        };

        active[best.left] = false;
        active[best.right] = false;
        active_count -= 1;

        let parent = tree.nodes.len();
        let nodes = &tree.nodes[best.left].nodes | &tree.nodes[best.right].nodes;
        let state = search.commit(best.candidate);
        tree.nodes[best.left].parent = Some(parent);
        tree.nodes[best.right].parent = Some(parent);
        tree.nodes.push(GooNode {
            nodes,
            state,
            children: Some((best.left, best.right)),
            parent: None,
            effective_size: tree.nodes[best.left].effective_size
                + tree.nodes[best.right].effective_size,
            atomic: false,
            hidden: false,
        });
        active.push(true);

        let candidate_pairs = frontier.merge(
            &graph,
            FrontierMerge {
                left: best.left,
                right: best.right,
                parent,
                left_relations: &tree.nodes[best.left].nodes,
                right_relations: &tree.nodes[best.right].nodes,
                merged_relations: &tree.nodes[parent].nodes,
            },
        );
        for (left, right) in candidate_pairs {
            push_greedy_choice(search, &graph, &tree, &mut choices, left, right)?;
        }
    }

    tree.root = active
        .iter()
        .position(|is_active| *is_active)
        .expect("a non-empty GOO tree has one active root");
    Ok(Some(tree))
}

/// Pair-scan implementation retained as a small-graph correctness oracle for indexed discovery.
#[cfg(test)]
fn build_greedy_tree_pair_scan_reference<M: CostModel>(
    search: &mut JoinSearch<'_, M>,
) -> OptimizeResult<Option<GooTree<M::Cost>>> {
    let hypergraph = search.hypergraph();
    if hypergraph.nodes.is_empty() {
        return Ok(None);
    }

    let graph = JoinGraph::new(hypergraph);
    let mut tree = GooTree {
        nodes: Vec::with_capacity(hypergraph.nodes.len().saturating_mul(2).saturating_sub(1)),
        root: 0,
    };
    for node in 0..hypergraph.nodes.len() {
        tree.nodes.push(GooNode {
            nodes: nodeset_singleton(node),
            state: search.leaf(node)?,
            children: None,
            parent: None,
            effective_size: 1,
            atomic: true,
            hidden: false,
        });
    }

    let mut active = vec![true; tree.nodes.len()];
    let mut choices = BinaryHeap::new();
    for right in 0..tree.nodes.len() {
        for left in 0..right {
            push_greedy_choice(search, &graph, &tree, &mut choices, left, right)?;
        }
    }

    let mut active_count = tree.nodes.len();
    while active_count > 1 {
        let best = loop {
            let Some(choice) = choices.pop() else {
                return Ok(None);
            };
            if active[choice.left] && active[choice.right] {
                break choice;
            }
        };

        active[best.left] = false;
        active[best.right] = false;
        active_count -= 1;

        let parent = tree.nodes.len();
        let nodes = &tree.nodes[best.left].nodes | &tree.nodes[best.right].nodes;
        let state = search.commit(best.candidate);
        tree.nodes[best.left].parent = Some(parent);
        tree.nodes[best.right].parent = Some(parent);
        tree.nodes.push(GooNode {
            nodes,
            state,
            children: Some((best.left, best.right)),
            parent: None,
            effective_size: tree.nodes[best.left].effective_size
                + tree.nodes[best.right].effective_size,
            atomic: false,
            hidden: false,
        });
        active.push(true);

        for (other, is_active) in active.iter().copied().enumerate().take(parent) {
            if is_active {
                push_greedy_choice(search, &graph, &tree, &mut choices, other, parent)?;
            }
        }
    }

    tree.root = active
        .iter()
        .position(|is_active| *is_active)
        .expect("a non-empty GOO tree has one active root");
    Ok(Some(tree))
}

fn push_greedy_choice<M: CostModel>(
    search: &mut JoinSearch<'_, M>,
    graph: &JoinGraph<'_>,
    tree: &GooTree<M::Cost>,
    choices: &mut BinaryHeap<GreedyChoice<M::Cost>>,
    first: usize,
    second: usize,
) -> OptimizeResult<()> {
    let (left, right) = if first <= second {
        (first, second)
    } else {
        (second, first)
    };
    let left_node = &tree.nodes[left];
    let right_node = &tree.nodes[right];
    let edge_indices = graph.connecting_edge_indices(&left_node.nodes, &right_node.nodes);
    let Some(candidate) = search.best_join_candidate(
        &left_node.nodes,
        &left_node.state,
        &right_node.nodes,
        &right_node.state,
        edge_indices,
    )?
    else {
        return Ok(());
    };

    let left_min = nodeset_min(&left_node.nodes);
    let right_min = nodeset_min(&right_node.nodes);
    choices.push(GreedyChoice {
        key: GreedyKey {
            output_rows: candidate.output_rows()?,
            first_relation: left_min.min(right_min),
            second_relation: left_min.max(right_min),
        },
        left,
        right,
        candidate,
    });
    Ok(())
}

impl<C: Clone> GooTree<C> {
    /// Selects Figure 7's maximal-cost subtree whose contracted frontier contains at most `k`
    /// inputs and whose visible parent still exceeds `k`.
    fn most_expensive_repair<M>(&self, search: &JoinSearch<'_, M>, k: usize) -> Option<usize>
    where
        M: CostModel<Cost = C>,
    {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| {
                !node.hidden
                    && !node.atomic
                    && node.children.is_some()
                    && node.effective_size <= k
                    && node
                        .parent
                        .is_none_or(|parent| self.nodes[parent].effective_size > k)
            })
            .map(|(node, _)| node)
            .reduce(|best, candidate| {
                let best_cost = &self.nodes[best].state.cost;
                let candidate_cost = &self.nodes[candidate].state.cost;
                if search.is_better(best_cost, candidate_cost) {
                    candidate
                } else if search.is_better(candidate_cost, best_cost) {
                    best
                } else if nodeset_min(&self.nodes[candidate].nodes)
                    < nodeset_min(&self.nodes[best].nodes)
                {
                    candidate
                } else {
                    best
                }
            })
    }

    /// Collects the visible opaque inputs that partition `node`.
    fn frontier(&self, node: usize) -> Vec<PlanAtom<C>> {
        let mut frontier = Vec::with_capacity(self.nodes[node].effective_size);
        let mut pending = vec![node];
        while let Some(node) = pending.pop() {
            let current = &self.nodes[node];
            if current.atomic || current.children.is_none() {
                frontier.push(PlanAtom {
                    nodes: current.nodes.clone(),
                    state: current.state.clone(),
                });
                continue;
            }
            let (left, right) = current
                .children
                .expect("a non-atomic internal GOO node has children");
            // LIFO traversal retains the stable left-to-right GOO frontier.
            pending.push(right);
            pending.push(left);
        }
        frontier
    }

    /// Re-costs the unique path from a replaced subtree to the root.
    fn rebuild_ancestors<M: CostModel<Cost = C>>(
        &mut self,
        node: usize,
        search: &mut JoinSearch<'_, M>,
    ) -> OptimizeResult<()> {
        let graph = JoinGraph::new(search.hypergraph());
        let mut ancestor = self.nodes[node].parent;
        while let Some(parent) = ancestor {
            let (left, right) = self.nodes[parent]
                .children
                .expect("a GOO ancestor is an internal node");
            let edge_indices =
                graph.connecting_edge_indices(&self.nodes[left].nodes, &self.nodes[right].nodes);
            let candidate = search
                .best_join_candidate(
                    &self.nodes[left].nodes,
                    &self.nodes[left].state,
                    &self.nodes[right].nodes,
                    &self.nodes[right].state,
                    edge_indices,
                )?
                .ok_or_else(|| OptimizeError::PassError {
                    pass: "JoinOrdering",
                    message: "GOO repair lost an ancestor's connecting edge".to_string(),
                })?;
            self.nodes[parent].state = search.commit(candidate);
            ancestor = self.nodes[parent].parent;
        }
        Ok(())
    }

    /// Contracts one repaired subtree into an opaque input and updates ancestor frontier sizes.
    fn make_atomic(&mut self, node: usize) {
        if let Some((left, right)) = self.nodes[node].children {
            self.hide_subtree(left);
            self.hide_subtree(right);
        }
        self.nodes[node].atomic = true;
        self.nodes[node].effective_size = 1;

        let mut ancestor = self.nodes[node].parent;
        while let Some(parent) = ancestor {
            let (left, right) = self.nodes[parent]
                .children
                .expect("a GOO ancestor is an internal node");
            self.nodes[parent].effective_size =
                self.nodes[left].effective_size + self.nodes[right].effective_size;
            ancestor = self.nodes[parent].parent;
        }
    }

    fn hide_subtree(&mut self, node: usize) {
        let mut pending = vec![node];
        while let Some(node) = pending.pop() {
            if self.nodes[node].hidden {
                continue;
            }
            self.nodes[node].hidden = true;
            // A prior contraction already hid everything below this opaque node.
            if self.nodes[node].atomic {
                continue;
            }
            if let Some((left, right)) = self.nodes[node].children {
                pending.push(left);
                pending.push(right);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost::DefaultCostModel;
    use crate::hypergraph::{Hyperedge, HyperedgeJoinType, HypergraphNode, QueryHypergraph};
    use crate::optimize::join_ordering::evaluator::{CardinalityEvaluator, MaterializingEvaluator};
    use crate::{
        AnalysisContext, ConstScan, CrossProduct, ExprData, Join, JoinType, Operator, OperatorData,
        OptimizeResult, QueryContext, ScalarValue,
    };

    fn chain(row_counts: &[usize]) -> (QueryContext, QueryHypergraph) {
        let mut ctx = QueryContext::new();
        let roots = row_counts
            .iter()
            .map(|row_count| {
                OperatorData::ConstScan(ConstScan {
                    columns: Vec::new(),
                    rows: vec![Vec::new(); *row_count],
                })
                .add(&mut ctx)
            })
            .collect::<Vec<_>>();
        let nodes = roots
            .iter()
            .enumerate()
            .map(|(node, root)| HypergraphNode {
                root: *root,
                label: format!("r{node}"),
                available: Vec::new(),
            })
            .collect();
        let edges = (1..roots.len())
            .map(|right| {
                let left = right - 1;
                Hyperedge {
                    predicate: None,
                    left: nodeset_singleton(left),
                    right: nodeset_singleton(right),
                    source: OperatorData::CrossProduct(CrossProduct {
                        outer: roots[left],
                        inner: roots[right],
                    })
                    .add(&mut ctx),
                    join_type: HyperedgeJoinType::Inner,
                }
            })
            .collect();
        (ctx, QueryHypergraph { nodes, edges })
    }

    fn solve_chain(
        row_counts: &[usize],
        config: GooDpConfig,
    ) -> GooDpOutcome<<DefaultCostModel as CostModel>::Cost> {
        let (mut ctx, hypergraph) = chain(row_counts);
        let mut analyses = crate::test_analyses(&ctx);
        let evaluator = MaterializingEvaluator;
        let mut search = JoinSearch::new(
            &mut ctx,
            &mut analyses,
            &hypergraph,
            &DefaultCostModel,
            &evaluator,
        );
        solve(&mut search, config).expect("a connected chain has a GOO plan")
    }

    fn graph_with_edges(
        row_counts: &[usize],
        endpoints: &[(Vec<usize>, Vec<usize>)],
    ) -> (QueryContext, QueryHypergraph) {
        let mut ctx = QueryContext::new();
        let roots = row_counts
            .iter()
            .map(|row_count| {
                OperatorData::ConstScan(ConstScan {
                    columns: Vec::new(),
                    rows: vec![Vec::new(); *row_count],
                })
                .add(&mut ctx)
            })
            .collect::<Vec<_>>();
        let nodes = roots
            .iter()
            .enumerate()
            .map(|(node, root)| HypergraphNode {
                root: *root,
                label: format!("r{node}"),
                available: Vec::new(),
            })
            .collect();
        let edges = endpoints
            .iter()
            .map(|(left, right)| Hyperedge {
                predicate: None,
                left: left.iter().copied().collect(),
                right: right.iter().copied().collect(),
                source: OperatorData::CrossProduct(CrossProduct {
                    outer: roots[left[0]],
                    inner: roots[right[0]],
                })
                .add(&mut ctx),
                join_type: HyperedgeJoinType::Inner,
            })
            .collect();
        (ctx, QueryHypergraph { nodes, edges })
    }

    fn assert_indexed_discovery_matches_pair_scan(
        row_counts: &[usize],
        endpoints: &[(Vec<usize>, Vec<usize>)],
    ) {
        let (mut ctx, hypergraph) = graph_with_edges(row_counts, endpoints);
        let indexed = {
            let mut analyses = crate::test_analyses(&ctx);
            let mut search = JoinSearch::new(
                &mut ctx,
                &mut analyses,
                &hypergraph,
                &DefaultCostModel,
                &CardinalityEvaluator,
            );
            build_greedy_tree(&mut search)
                .expect("indexed GOO discovery succeeds")
                .map(|tree| {
                    let root = &tree.nodes[tree.root].state;
                    (root.tree.clone(), root.cost)
                })
        };
        let pair_scan = {
            let mut analyses = crate::test_analyses(&ctx);
            let mut search = JoinSearch::new(
                &mut ctx,
                &mut analyses,
                &hypergraph,
                &DefaultCostModel,
                &CardinalityEvaluator,
            );
            build_greedy_tree_pair_scan_reference(&mut search)
                .expect("reference GOO discovery succeeds")
                .map(|tree| {
                    let root = &tree.nodes[tree.root].state;
                    (root.tree.clone(), root.cost)
                })
        };

        assert_eq!(indexed, pair_scan, "different result for {endpoints:?}");
    }

    #[test]
    fn greedy_queue_orders_by_output_cardinality_then_relation_ids() {
        let mut heap = BinaryHeap::from([
            GreedyKey {
                output_rows: 10.0,
                first_relation: 2,
                second_relation: 3,
            },
            GreedyKey {
                output_rows: 1.0,
                first_relation: 1,
                second_relation: 3,
            },
            GreedyKey {
                output_rows: 1.0,
                first_relation: 0,
                second_relation: 2,
            },
        ]);

        assert_eq!(heap.pop().unwrap().first_relation, 0);
        assert_eq!(heap.pop().unwrap().first_relation, 1);
        assert_eq!(heap.pop().unwrap().output_rows, 10.0);
    }

    #[test]
    fn canonical_goo_joins_the_smallest_result_first() {
        let outcome = solve_chain(
            &[100, 10, 1],
            GooDpConfig {
                inner: GooInnerSolver::DpHyp,
                max_subproblem_relations: 3,
                dp_state_budget: 0,
            },
        );
        let plan = outcome.plan.expect("a connected chain has a plan");
        let smallest_pair = &nodeset_singleton(1) | &nodeset_singleton(2);

        assert!(plan.tree.has_join_with_leaves(&smallest_pair));
    }

    #[test]
    fn canonical_goo_preserves_directed_join_orientation() {
        let mut ctx = QueryContext::new();
        let left = OperatorData::ConstScan(ConstScan {
            columns: Vec::new(),
            rows: vec![Vec::new(); 10],
        })
        .add(&mut ctx);
        let right = OperatorData::ConstScan(ConstScan {
            columns: Vec::new(),
            rows: vec![Vec::new(); 2],
        })
        .add(&mut ctx);
        let predicate = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);
        let source = OperatorData::Join(Join {
            join_type: JoinType::LeftAnti,
            on: predicate,
            outer: left,
            inner: right,
        })
        .add(&mut ctx);
        let hypergraph = QueryHypergraph {
            nodes: vec![
                HypergraphNode {
                    root: left,
                    label: "left".to_string(),
                    available: Vec::new(),
                },
                HypergraphNode {
                    root: right,
                    label: "right".to_string(),
                    available: Vec::new(),
                },
            ],
            edges: vec![Hyperedge {
                predicate: Some(predicate),
                left: nodeset_singleton(0),
                right: nodeset_singleton(1),
                source,
                join_type: HyperedgeJoinType::LeftSemi,
            }],
        };
        let mut analyses = crate::test_analyses(&ctx);
        let evaluator = MaterializingEvaluator;
        let plan_root = {
            let mut search = JoinSearch::new(
                &mut ctx,
                &mut analyses,
                &hypergraph,
                &DefaultCostModel,
                &evaluator,
            );
            let outcome = solve(
                &mut search,
                GooDpConfig {
                    inner: GooInnerSolver::DpHyp,
                    max_subproblem_relations: 2,
                    dp_state_budget: 10,
                },
            )
            .unwrap();
            assert_eq!(outcome.repaired_subproblems, 1);
            let plan = outcome.plan.expect("the directed edge is connected");
            search.materialize(&plan)
        };

        let OperatorData::Join(join) = plan_root.get(&ctx) else {
            panic!("a non-inner GOO edge must remain a join");
        };
        assert_eq!(join.join_type, JoinType::LeftAnti);
        assert_eq!((join.outer, join.inner), (left, right));
    }

    #[test]
    fn indexed_discovery_matches_pair_scan_for_every_four_node_regular_graph() {
        const PAIRS: [(usize, usize); 6] = [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)];
        for edge_mask in 0..(1 << PAIRS.len()) {
            let endpoints = PAIRS
                .iter()
                .enumerate()
                .filter(|(edge, _)| edge_mask & (1 << edge) != 0)
                .map(|(_, &(left, right))| (vec![left], vec![right]))
                .collect::<Vec<_>>();
            assert_indexed_discovery_matches_pair_scan(&[2, 3, 5, 7], &endpoints);
        }
    }

    #[test]
    fn indexed_discovery_matches_pair_scan_for_staged_hyperedges() {
        let cases = [
            vec![
                (vec![0], vec![1]),
                (vec![0, 1], vec![2]),
                (vec![0, 1, 2], vec![3]),
                (vec![0, 1, 2, 3], vec![4]),
            ],
            vec![
                (vec![0], vec![1]),
                (vec![2], vec![3]),
                (vec![0, 1], vec![2, 3]),
                (vec![0, 1, 2, 3], vec![4]),
            ],
            vec![
                (vec![0], vec![1]),
                (vec![1], vec![2]),
                (vec![2], vec![3]),
                (vec![3], vec![4]),
                (vec![0, 1], vec![2]),
                (vec![1, 2], vec![3, 4]),
                (vec![0, 1, 2], vec![3, 4]),
            ],
        ];

        for endpoints in cases {
            assert_indexed_discovery_matches_pair_scan(&[11, 2, 7, 3, 5], &endpoints);
        }
    }

    #[test]
    fn indexed_discovery_matches_pair_scan_for_randomized_hypergraphs() {
        const RELATIONS: usize = 5;
        let mut possible_hyperedges = Vec::new();
        for mut assignment in 0..3usize.pow(RELATIONS as u32) {
            let mut left = Vec::new();
            let mut right = Vec::new();
            for relation in 0..RELATIONS {
                match assignment % 3 {
                    1 => left.push(relation),
                    2 => right.push(relation),
                    _ => {}
                }
                assignment /= 3;
            }
            if !left.is_empty() && !right.is_empty() && left[0] < right[0] {
                possible_hyperedges.push((left, right));
            }
        }

        let chain = (1..RELATIONS)
            .map(|right| (vec![right - 1], vec![right]))
            .collect::<Vec<_>>();
        let mut random = 0x9e37_79b9_u32;
        for _ in 0..128 {
            let mut endpoints = chain.clone();
            for edge in &possible_hyperedges {
                random ^= random << 13;
                random ^= random >> 17;
                random ^= random << 5;
                if random.is_multiple_of(11) {
                    endpoints.push(edge.clone());
                }
            }
            assert_indexed_discovery_matches_pair_scan(&[11, 2, 7, 3, 5], &endpoints);
        }
    }

    #[test]
    fn merged_boundaries_drop_internal_edges_and_retain_the_frontier() {
        let (_, hypergraph) = graph_with_edges(
            &[1; 4],
            &[
                (vec![0], vec![1]),
                (vec![1], vec![2]),
                (vec![2], vec![3]),
                (vec![0, 1], vec![2]),
            ],
        );
        let graph = JoinGraph::new(&hypergraph);
        let boundary = graph.merge_boundaries(
            graph.singleton_boundary(0),
            graph.singleton_boundary(1),
            &(&nodeset_singleton(0) | &nodeset_singleton(1)),
        );

        assert_eq!(boundary.len(), 2);
        assert_eq!(boundary.iter().collect::<Vec<_>>(), vec![1, 3]);
    }

    #[test]
    fn sparse_chain_frontier_avoids_quadratic_pair_probes() {
        const RELATIONS: usize = 512;
        let endpoints = (1..RELATIONS)
            .map(|right| (vec![right - 1], vec![right]))
            .collect::<Vec<_>>();
        let (_, hypergraph) = graph_with_edges(&vec![1; RELATIONS], &endpoints);
        let graph = JoinGraph::new(&hypergraph);
        let mut frontier = GreedyFrontier::new(&graph, RELATIONS);

        let initial = frontier.initial_candidate_pairs(&graph);
        assert_eq!(initial.len(), RELATIONS - 1);
        let mut indexed_probes = initial.len();
        let mut merged_relations = nodeset_singleton(0);

        for right in 1..RELATIONS {
            let left = if right == 1 { 0 } else { RELATIONS + right - 2 };
            let parent = RELATIONS + right - 1;
            let right_relations = nodeset_singleton(right);
            let next_relations = &merged_relations | &right_relations;
            let pairs = frontier.merge(
                &graph,
                FrontierMerge {
                    left,
                    right,
                    parent,
                    left_relations: &merged_relations,
                    right_relations: &right_relations,
                    merged_relations: &next_relations,
                },
            );
            indexed_probes += pairs.len();
            if right + 1 < RELATIONS {
                assert_eq!(pairs, vec![(right + 1, parent)]);
            } else {
                assert!(pairs.is_empty());
            }
            merged_relations = next_relations;
        }

        let pair_scan_probes = (RELATIONS - 1).pow(2);
        assert_eq!(indexed_probes, 2 * RELATIONS - 3);
        assert!(pair_scan_probes / indexed_probes > 200);
    }

    #[test]
    fn repaired_subtrees_contract_before_follow_on_repairs() {
        let outcome = solve_chain(
            &[1; 7],
            GooDpConfig {
                inner: GooInnerSolver::DpHyp,
                max_subproblem_relations: 3,
                dp_state_budget: usize::MAX,
            },
        );

        assert!(outcome.repaired_subproblems > 1);
        assert!(
            outcome
                .repair_trace
                .iter()
                .all(|repair| repair.frontier_relations <= 3)
        );
        assert!(
            outcome
                .repair_trace
                .iter()
                .any(|repair| repair.original_relations > 3),
            "contraction should expose an ancestor covering more than k original relations"
        );
        assert_eq!(
            outcome
                .repair_trace
                .last()
                .expect("the root is eventually repaired")
                .nodes,
            NodeSet::all(7),
            "contraction should eventually make the complete root eligible"
        );
    }

    #[test]
    fn dp_budget_is_global_and_charged_by_actual_table_size() {
        let outcome = solve_chain(
            &[1; 7],
            GooDpConfig {
                inner: GooInnerSolver::DpHyp,
                max_subproblem_relations: 3,
                dp_state_budget: 1,
            },
        );

        assert_eq!(outcome.repaired_subproblems, 1);
        assert!(outcome.dp_states_created > 1);
    }

    #[test]
    fn scheduler_repairs_the_most_expensive_eligible_subtree_first() {
        let outcome = solve_chain(
            &[1, 1, 1_000, 1, 2, 1_000],
            GooDpConfig {
                inner: GooInnerSolver::DpHyp,
                max_subproblem_relations: 2,
                dp_state_budget: 1,
            },
        );
        let expensive_pair = &nodeset_singleton(3) | &nodeset_singleton(4);

        assert_eq!(outcome.repair_trace.len(), 1);
        assert_eq!(outcome.repair_trace[0].nodes, expensive_pair);
    }

    /// Cost model deliberately at odds with GOO's cardinality key.
    ///
    /// GOO joins relations 1 and 2 first because their cross product is smallest, while this model
    /// strongly prefers joining 0 and 1. An exact repair must therefore replace the greedy seed.
    struct TesPenaltyCost {
        leaves: Vec<Operator>,
    }

    impl TesPenaltyCost {
        fn leaf_mask(&self, op: Operator, ctx: &QueryContext) -> u32 {
            if let Some(leaf) = self.leaves.iter().position(|candidate| *candidate == op) {
                return 1 << leaf;
            }

            match op.get(ctx) {
                OperatorData::Join(join) => {
                    self.leaf_mask(join.outer, ctx) | self.leaf_mask(join.inner, ctx)
                }
                OperatorData::CrossProduct(product) => {
                    self.leaf_mask(product.outer, ctx) | self.leaf_mask(product.inner, ctx)
                }
                data => panic!("unexpected operator in test join tree: {data:?}"),
            }
        }

        fn local_cost(mask: u32) -> usize {
            match mask {
                0b0_0110 => 1_000,
                0b0_0101 => 10,
                0b0_0011 => 1,
                _ => 0,
            }
        }

        fn contains_subtree(&self, op: Operator, target: u32, ctx: &QueryContext) -> bool {
            if self.leaf_mask(op, ctx) == target {
                return true;
            }
            match op.get(ctx) {
                OperatorData::Join(join) => {
                    self.contains_subtree(join.outer, target, ctx)
                        || self.contains_subtree(join.inner, target, ctx)
                }
                OperatorData::CrossProduct(product) => {
                    self.contains_subtree(product.outer, target, ctx)
                        || self.contains_subtree(product.inner, target, ctx)
                }
                _ => false,
            }
        }
    }

    impl CostModel for TesPenaltyCost {
        type Cost = usize;

        fn zero(&self) -> Self::Cost {
            0
        }

        fn add(&self, left: Self::Cost, right: Self::Cost) -> Self::Cost {
            left + right
        }

        fn is_better(&self, candidate: &Self::Cost, existing: &Self::Cost) -> bool {
            candidate < existing
        }

        fn operator_cost(
            &self,
            op: Operator,
            ctx: &QueryContext,
            _analyses: &mut AnalysisContext,
        ) -> OptimizeResult<Self::Cost> {
            Ok(Self::local_cost(self.leaf_mask(op, ctx)))
        }
    }

    #[derive(Clone)]
    struct OraclePlan {
        cost: usize,
        top_join: Option<(u32, u32, JoinType)>,
    }

    fn relation_mask(relations: &NodeSet) -> u32 {
        relations
            .iter()
            .fold(0, |mask, relation| mask | (1 << relation))
    }

    /// Exhaustively enumerates every unordered bushy split independently of DPhyp/JoinSearch.
    ///
    /// Inner joins use one representative input order because `TesPenaltyCost` is orientation
    /// insensitive. Directed joins are oriented strictly from the hyperedge's left TES to its
    /// right TES.
    fn exhaustive_tes_oracle(ctx: &QueryContext, hypergraph: &QueryHypergraph) -> Vec<OraclePlan> {
        let relation_count = hypergraph.nodes.len();
        let all = (1_u32 << relation_count) - 1;
        let mut table = vec![Vec::<OraclePlan>::new(); all as usize + 1];
        for relation in 0..relation_count {
            table[1 << relation].push(OraclePlan {
                cost: 0,
                top_join: None,
            });
        }

        for size in 2..=relation_count {
            for joined in 1..=all {
                if joined.count_ones() as usize != size {
                    continue;
                }
                let anchor = joined & joined.wrapping_neg();
                let mut left = (joined - 1) & joined;
                while left != 0 {
                    let right = joined ^ left;
                    if right != 0 && left & anchor != 0 {
                        let connecting = hypergraph
                            .edges
                            .iter()
                            .filter_map(|edge| {
                                let edge_left = relation_mask(&edge.left);
                                let edge_right = relation_mask(&edge.right);
                                let forward = edge_left & left == edge_left
                                    && edge_right & right == edge_right;
                                let reverse = edge_left & right == edge_left
                                    && edge_right & left == edge_right;
                                (forward || reverse).then_some((edge, forward))
                            })
                            .collect::<Vec<_>>();

                        if !connecting.is_empty() {
                            let directed = connecting
                                .iter()
                                .copied()
                                .filter(|(edge, _)| edge.join_type != HyperedgeJoinType::Inner)
                                .collect::<Vec<_>>();
                            assert!(
                                directed.len() <= 1,
                                "the oracle fixture has at most one directed edge per split"
                            );
                            let (outer, inner, join_type) =
                                if let Some((edge, forward)) = directed.first().copied() {
                                    let OperatorData::Join(source) = edge.source.get(ctx) else {
                                        panic!("a directed hyperedge must retain its source join");
                                    };
                                    if forward {
                                        (left, right, source.join_type.clone())
                                    } else {
                                        (right, left, source.join_type.clone())
                                    }
                                } else {
                                    (left, right, JoinType::Inner)
                                };
                            let local_cost = TesPenaltyCost::local_cost(joined);
                            let mut candidates = Vec::new();
                            for left_plan in &table[left as usize] {
                                for right_plan in &table[right as usize] {
                                    candidates.push(OraclePlan {
                                        cost: left_plan.cost + right_plan.cost + local_cost,
                                        top_join: Some((outer, inner, join_type.clone())),
                                    });
                                }
                            }
                            table[joined as usize].extend(candidates);
                        }
                    }
                    left = (left - 1) & joined;
                }
            }
        }

        table.pop().expect("the full-set oracle entry exists")
    }

    fn multi_node_anti_hypergraph() -> (QueryContext, QueryHypergraph, TesPenaltyCost) {
        let mut ctx = QueryContext::new();
        let leaves = [100, 10, 1, 2, 3]
            .into_iter()
            .map(|rows| {
                OperatorData::ConstScan(ConstScan {
                    columns: Vec::new(),
                    rows: vec![Vec::new(); rows],
                })
                .add(&mut ctx)
            })
            .collect::<Vec<_>>();
        let nodes = leaves
            .iter()
            .enumerate()
            .map(|(relation, root)| HypergraphNode {
                root: *root,
                label: format!("r{relation}"),
                available: Vec::new(),
            })
            .collect::<Vec<_>>();
        let mut edges = [(0, 1), (0, 2), (1, 2), (3, 4)]
            .into_iter()
            .map(|(left, right)| Hyperedge {
                predicate: None,
                left: nodeset_singleton(left),
                right: nodeset_singleton(right),
                source: OperatorData::CrossProduct(CrossProduct {
                    outer: leaves[left],
                    inner: leaves[right],
                })
                .add(&mut ctx),
                join_type: HyperedgeJoinType::Inner,
            })
            .collect::<Vec<_>>();
        let predicate = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);
        let anti_source = OperatorData::Join(Join {
            join_type: JoinType::LeftAnti,
            on: predicate,
            outer: leaves[0],
            inner: leaves[3],
        })
        .add(&mut ctx);
        edges.push(Hyperedge {
            predicate: Some(predicate),
            left: [0, 1, 2].into_iter().collect(),
            right: [3, 4].into_iter().collect(),
            source: anti_source,
            join_type: HyperedgeJoinType::LeftSemi,
        });

        (
            ctx,
            QueryHypergraph { nodes, edges },
            TesPenaltyCost { leaves },
        )
    }

    #[test]
    fn dphyp_repair_matches_exhaustive_oracle_on_directed_multi_node_hyperedge() {
        let (mut ctx, hypergraph, cost_model) = multi_node_anti_hypergraph();
        let directed = hypergraph
            .edges
            .last()
            .expect("the fixture has a directed hyperedge");
        assert!(directed.left.len() > 1 && directed.right.len() > 1);

        let oracle_plans = exhaustive_tes_oracle(&ctx, &hypergraph);
        assert!(
            oracle_plans.len() >= 3,
            "the oracle must compare multiple valid bushy trees"
        );
        let oracle = oracle_plans
            .iter()
            .min_by_key(|plan| plan.cost)
            .expect("the constrained hypergraph has a complete plan");
        assert_eq!(oracle.cost, 1);

        let greedy_cost = {
            let mut analyses = crate::test_analyses(&ctx);
            let mut search = JoinSearch::new(
                &mut ctx,
                &mut analyses,
                &hypergraph,
                &cost_model,
                &MaterializingEvaluator,
            );
            solve(
                &mut search,
                GooDpConfig {
                    inner: GooInnerSolver::DpHyp,
                    max_subproblem_relations: 5,
                    dp_state_budget: 0,
                },
            )
            .unwrap()
            .plan
            .expect("GOO finds the staged hypergraph plan")
            .cost
        };
        assert!(
            greedy_cost > oracle.cost,
            "the fixture must require exact repair rather than accepting the GOO seed"
        );

        let (repaired_cost, root) = {
            let mut analyses = crate::test_analyses(&ctx);
            let mut search = JoinSearch::new(
                &mut ctx,
                &mut analyses,
                &hypergraph,
                &cost_model,
                &MaterializingEvaluator,
            );
            let outcome = solve(
                &mut search,
                GooDpConfig {
                    inner: GooInnerSolver::DpHyp,
                    max_subproblem_relations: 5,
                    dp_state_budget: usize::MAX,
                },
            )
            .unwrap();
            assert_eq!(outcome.repaired_subproblems, 1);
            assert!(outcome.dp_states_created > hypergraph.nodes.len());
            assert_eq!(outcome.repair_trace[0].nodes, NodeSet::all(5));
            let plan = outcome.plan.expect("DPhyp repair finds the complete plan");
            let cost = plan.cost;
            let root = search.materialize(&plan);
            (cost, root)
        };

        assert_eq!(repaired_cost, oracle.cost);
        let OperatorData::Join(join) = root.get(&ctx) else {
            panic!("the directed top edge must materialize as a join");
        };
        let (oracle_outer, oracle_inner, oracle_join_type) =
            oracle.top_join.clone().expect("the oracle root is a join");
        assert_eq!(join.join_type, oracle_join_type);
        assert_eq!(join.join_type, JoinType::LeftAnti);
        assert_eq!(cost_model.leaf_mask(join.outer, &ctx), oracle_outer);
        assert_eq!(cost_model.leaf_mask(join.inner, &ctx), oracle_inner);
        assert_eq!(oracle_outer, 0b0_0111);
        assert_eq!(oracle_inner, 0b1_1000);
        assert!(
            cost_model.contains_subtree(join.outer, 0b0_0011, &ctx),
            "the exact repair should choose the oracle's low-cost left subtree"
        );
    }
}
