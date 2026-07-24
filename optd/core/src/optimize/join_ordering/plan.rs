//! Compact search-plan recipes and their materialization into the query arena.

use std::sync::Arc;

use crate::CardinalityProfile;
#[cfg(test)]
use crate::hypergraph::nodeset_singleton;
use crate::hypergraph::{NodeSet, QueryHypergraph};
use crate::{
    CrossProduct, Expr, ExprData, Join, JoinType, NaryOp, Operator, OperatorData, QueryContext,
    ScalarValue,
};

/// Stable handle to a recipe in [`PlanArena`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct PlanId(usize);

/// Winning search state for one relation subset.
///
/// The operator root lives in the plan arena rather than in every cloned DP state. The default
/// evaluator leaves join roots absent until the winning recipe is reconstructed; compatibility
/// evaluators may cache eagerly materialized roots in the same arena.
#[derive(Clone)]
pub(super) struct PlanState<C> {
    pub(super) plan: PlanId,
    pub(super) cost: C,
    pub(super) properties: PlanProperties,
    #[cfg(test)]
    pub(super) tree: JoinTree,
}

/// One atomic input to a contracted GOO/DP repair problem.
///
/// `nodes` records the covered original hypergraph relations while `state` is the already
/// optimized plan that must remain opaque to the inner solver. Atoms in one frontier are
/// pairwise disjoint and partition the selected GOO subtree.
#[derive(Clone)]
pub(super) struct PlanAtom<C> {
    pub(super) nodes: NodeSet,
    pub(super) state: PlanState<C>,
}

/// Result of one dynamic-programming invocation.
///
/// GOO/DP uses the number of unique table entries as its global optimization-budget currency.
/// Keeping that accounting beside the plan prevents individual solvers from inventing subtly
/// different budget semantics.
pub(super) struct SolveOutcome<C> {
    pub(super) plan: Option<PlanState<C>>,
    pub(super) dp_states_created: usize,
}

/// Derived properties carried by a search state without requiring a concrete IR operator.
///
/// Keeping this bundle separate from the cost makes it straightforward to add more properties
/// needed by future evaluators while preserving the enumerators' compact state representation.
#[derive(Clone, Default)]
pub(super) struct PlanProperties {
    pub(super) cardinality: Option<Arc<CardinalityProfile>>,
}

/// Immutable recipe for one leaf or oriented join.
#[derive(Clone)]
pub(super) enum PlanRecipe {
    Leaf {
        root: Operator,
    },
    Join {
        outer: PlanId,
        inner: PlanId,
        join_type: JoinType,
        edge_indices: Vec<usize>,
    },
}

struct PlanNode {
    recipe: PlanRecipe,
    materialized: Option<Operator>,
}

/// Append-only storage for accepted search recipes.
///
/// `materialized` remains empty for default-cost join nodes until final reconstruction. The
/// compatibility evaluator populates it eagerly because arbitrary cost models may inspect IR.
#[derive(Default)]
pub(super) struct PlanArena {
    nodes: Vec<PlanNode>,
}

impl PlanArena {
    pub(super) fn commit(&mut self, recipe: PlanRecipe, materialized: Option<Operator>) -> PlanId {
        let id = PlanId(self.nodes.len());
        self.nodes.push(PlanNode {
            recipe,
            materialized,
        });
        id
    }

    /// Materializes a recipe in post-order and memoizes the resulting operator handle.
    ///
    /// This is a cache hit for compatibility-evaluated plans and after the first reconstruction.
    /// An explicit stack keeps deferred reconstruction safe for highly skewed plans containing
    /// thousands of joins.
    pub(super) fn materialize(
        &mut self,
        plan: PlanId,
        hypergraph: &QueryHypergraph,
        ctx: &mut QueryContext,
    ) -> Operator {
        if let Some(root) = self.nodes[plan.0].materialized {
            return root;
        }

        let mut pending = vec![(plan, false)];
        while let Some((current, expanded)) = pending.pop() {
            if self.nodes[current.0].materialized.is_some() {
                continue;
            }

            if !expanded {
                pending.push((current, true));
                if let PlanRecipe::Join { outer, inner, .. } = self.nodes[current.0].recipe.clone()
                {
                    // The inner child is pushed first so the outer child is materialized first,
                    // matching the former recursive traversal and arena append order.
                    pending.push((inner, false));
                    pending.push((outer, false));
                }
                continue;
            }

            let root = match self.nodes[current.0].recipe.clone() {
                PlanRecipe::Leaf { root } => root,
                PlanRecipe::Join {
                    outer,
                    inner,
                    join_type,
                    edge_indices,
                } => {
                    let outer = self.nodes[outer.0]
                        .materialized
                        .expect("a join's outer recipe is materialized first");
                    let inner = self.nodes[inner.0]
                        .materialized
                        .expect("a join's inner recipe is materialized first");
                    materialize_candidate_join(
                        outer,
                        inner,
                        join_type,
                        &edge_indices,
                        hypergraph,
                        ctx,
                    )
                }
            };
            self.nodes[current.0].materialized = Some(root);
        }

        self.nodes[plan.0]
            .materialized
            .expect("the requested recipe was materialized")
    }
}

/// Appends one oriented candidate join to the query arena.
///
/// All predicate-bearing connecting edges become one conjunctive `ON` expression. A predicate-
/// free inner edge is represented as [`CrossProduct`]; predicate-free non-inner joins retain an
/// explicit `true` condition so their semantics and join type remain visible in the IR.
pub(super) fn materialize_candidate_join(
    outer: Operator,
    inner: Operator,
    join_type: JoinType,
    edge_indices: &[usize],
    hypergraph: &QueryHypergraph,
    ctx: &mut QueryContext,
) -> Operator {
    let mut predicates: Vec<Expr> = edge_indices
        .iter()
        .filter_map(|&index| hypergraph.edges[index].predicate)
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

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
/// Lightweight witness tree retained only in tests.
pub(super) enum JoinTree {
    Leaf(usize),
    Join {
        left: Box<JoinTree>,
        right: Box<JoinTree>,
    },
}

#[cfg(test)]
impl JoinTree {
    pub(super) fn leaf_count(&self) -> usize {
        match self {
            Self::Leaf(_) => 1,
            Self::Join { left, right } => left.leaf_count() + right.leaf_count(),
        }
    }

    pub(super) fn leaf_set(&self) -> NodeSet {
        match self {
            Self::Leaf(node) => nodeset_singleton(*node),
            Self::Join { left, right } => &left.leaf_set() | &right.leaf_set(),
        }
    }

    pub(super) fn has_join_with_leaves(&self, leaves: &NodeSet) -> bool {
        match self {
            Self::Leaf(_) => false,
            Self::Join { left, right } => {
                self.leaf_set() == *leaves
                    || left.has_join_with_leaves(leaves)
                    || right.has_join_with_leaves(leaves)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hypergraph::{
        Hyperedge, HyperedgeJoinType, HypergraphNode, QueryHypergraph, nodeset_singleton,
    };
    use crate::{ExprData, Join, OperatorData, Scan, TableRef};

    fn scans(ctx: &mut QueryContext) -> (Operator, Operator) {
        let left = OperatorData::Scan(Scan {
            table: TableRef::bare("left"),
            columns: vec![],
        })
        .add(ctx);
        let right = OperatorData::Scan(Scan {
            table: TableRef::bare("right"),
            columns: vec![],
        })
        .add(ctx);
        (left, right)
    }

    fn nodes(left: Operator, right: Operator) -> Vec<HypergraphNode> {
        vec![
            HypergraphNode {
                root: left,
                label: "left".to_string(),
                available: vec![],
            },
            HypergraphNode {
                root: right,
                label: "right".to_string(),
                available: vec![],
            },
        ]
    }

    #[test]
    fn deferred_recipe_materializes_and_memoizes_non_commutative_join() {
        let mut ctx = QueryContext::new();
        let (left, right) = scans(&mut ctx);
        let predicate = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);
        let source = OperatorData::Join(Join {
            join_type: JoinType::LeftAnti,
            on: predicate,
            outer: left,
            inner: right,
        })
        .add(&mut ctx);
        let hypergraph = QueryHypergraph {
            nodes: nodes(left, right),
            edges: vec![Hyperedge {
                predicate: Some(predicate),
                left: nodeset_singleton(0),
                right: nodeset_singleton(1),
                source,
                join_type: HyperedgeJoinType::LeftSemi,
            }],
        };
        let mut plans = PlanArena::default();
        let left_plan = plans.commit(PlanRecipe::Leaf { root: left }, Some(left));
        let right_plan = plans.commit(PlanRecipe::Leaf { root: right }, Some(right));
        let join_plan = plans.commit(
            PlanRecipe::Join {
                outer: left_plan,
                inner: right_plan,
                join_type: JoinType::LeftAnti,
                edge_indices: vec![0],
            },
            None,
        );
        let before = ctx.operator_count();

        let root = plans.materialize(join_plan, &hypergraph, &mut ctx);
        assert_eq!(ctx.operator_count(), before + 1);
        assert_eq!(plans.materialize(join_plan, &hypergraph, &mut ctx), root);
        assert_eq!(ctx.operator_count(), before + 1);

        let OperatorData::Join(join) = root.get(&ctx) else {
            panic!("a non-inner recipe must remain an explicit join");
        };
        assert_eq!(join.join_type, JoinType::LeftAnti);
        assert_eq!(join.on, predicate);
        assert_eq!((join.outer, join.inner), (left, right));
    }

    #[test]
    fn deferred_predicate_free_inner_recipe_materializes_cross_product() {
        let mut ctx = QueryContext::new();
        let (left, right) = scans(&mut ctx);
        let source = OperatorData::CrossProduct(CrossProduct {
            outer: left,
            inner: right,
        })
        .add(&mut ctx);
        let hypergraph = QueryHypergraph {
            nodes: nodes(left, right),
            edges: vec![Hyperedge {
                predicate: None,
                left: nodeset_singleton(0),
                right: nodeset_singleton(1),
                source,
                join_type: HyperedgeJoinType::Inner,
            }],
        };
        let mut plans = PlanArena::default();
        let left_plan = plans.commit(PlanRecipe::Leaf { root: left }, Some(left));
        let right_plan = plans.commit(PlanRecipe::Leaf { root: right }, Some(right));
        let join_plan = plans.commit(
            PlanRecipe::Join {
                outer: left_plan,
                inner: right_plan,
                join_type: JoinType::Inner,
                edge_indices: vec![0],
            },
            None,
        );

        let root = plans.materialize(join_plan, &hypergraph, &mut ctx);
        let OperatorData::CrossProduct(cross_product) = root.get(&ctx) else {
            panic!("a predicate-free inner recipe must be a cross product");
        };
        assert_eq!((cross_product.outer, cross_product.inner), (left, right));
    }

    #[test]
    fn deferred_materialization_is_stack_safe_for_deep_plans() {
        const RELATIONS: usize = 10_000;

        let mut ctx = QueryContext::new();
        let leaves = (0..RELATIONS)
            .map(|relation| {
                OperatorData::Scan(Scan {
                    table: TableRef::bare(format!("t{relation}")),
                    columns: vec![],
                })
                .add(&mut ctx)
            })
            .collect::<Vec<_>>();
        let nodes = leaves
            .iter()
            .enumerate()
            .map(|(relation, root)| HypergraphNode {
                root: *root,
                label: format!("t{relation}"),
                available: vec![],
            })
            .collect::<Vec<_>>();
        let edges = (1..RELATIONS)
            .map(|relation| Hyperedge {
                predicate: None,
                left: nodeset_singleton(relation - 1),
                right: nodeset_singleton(relation),
                source: leaves[relation],
                join_type: HyperedgeJoinType::Inner,
            })
            .collect::<Vec<_>>();
        let hypergraph = QueryHypergraph { nodes, edges };

        let mut plans = PlanArena::default();
        let leaves = leaves
            .into_iter()
            .map(|root| plans.commit(PlanRecipe::Leaf { root }, None))
            .collect::<Vec<_>>();
        let root = leaves.iter().copied().enumerate().skip(1).fold(
            leaves[0],
            |outer, (relation, inner)| {
                plans.commit(
                    PlanRecipe::Join {
                        outer,
                        inner,
                        join_type: JoinType::Inner,
                        edge_indices: vec![relation - 1],
                    },
                    None,
                )
            },
        );

        let before = ctx.operator_count();
        plans.materialize(root, &hypergraph, &mut ctx);
        assert_eq!(ctx.operator_count() - before, RELATIONS - 1);
    }
}
