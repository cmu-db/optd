//! Cross-module correctness and integration tests for join ordering.

use super::dphyp::all_nodes_mask;
use super::graph::JoinGraph;
use super::*;
use crate::analysis::connecting_edge_indices;
use crate::cost::{JoinAlgorithmClass, join_algorithm_class, join_algorithm_cost};
use crate::{
    AnalysisContext, BinaryOp, CardinalityEstimationV1, Catalog, Column, ColumnData,
    ColumnStatistics, CrossProduct, ExprData, Hyperedge, HyperedgeJoinType, HypergraphNode, Join,
    JoinType, MemoryCatalog, NaryOp, NodeSet, Operator, OperatorData, OptimizerContext, Output,
    PassManager, QueryContext, QueryHypergraph, Relation, ScalarValue, Scan, Selection, TableRef,
    TableStatistics, nodeset_singleton,
};
use arrow_schema::{DataType, Field, Schema};
use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};

struct UnitCost;

impl CostModel for UnitCost {
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
        Ok(matches!(
            op.get(ctx),
            OperatorData::Join(_) | OperatorData::CrossProduct(_)
        ) as usize)
    }
}

/// Test-only evaluator that exercises recipe reconstruction without appending candidate IR.
struct DeferredUnitEvaluator;

impl evaluator::CandidateEvaluator<UnitCost> for DeferredUnitEvaluator {
    fn evaluate_leaf(
        &self,
        _cost_model: &UnitCost,
        _root: Operator,
        _ctx: &QueryContext,
        _analyses: &mut AnalysisContext,
    ) -> OptimizeResult<evaluator::EvaluatedPlan<usize>> {
        Ok(evaluator::EvaluatedPlan {
            cost: 0,
            properties: plan::PlanProperties::default(),
            materialized: None,
        })
    }

    fn requires_materialized_inputs(&self) -> bool {
        false
    }

    fn evaluate_join(
        &self,
        _cost_model: &UnitCost,
        _spec: evaluator::JoinSpec<'_>,
        outer: evaluator::CandidateInput<'_, usize>,
        inner: evaluator::CandidateInput<'_, usize>,
        _ctx: &mut QueryContext,
        _analyses: &mut AnalysisContext,
    ) -> OptimizeResult<evaluator::EvaluatedPlan<usize>> {
        assert!(outer.materialized.is_none());
        assert!(inner.materialized.is_none());
        Ok(evaluator::EvaluatedPlan {
            cost: outer.cost + inner.cost + 1,
            properties: plan::PlanProperties::default(),
            materialized: None,
        })
    }
}

struct OrientationCost;

impl CostModel for OrientationCost {
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

    fn is_join_orientation_cost_sensitive(&self) -> bool {
        true
    }

    fn operator_cost(
        &self,
        op: Operator,
        ctx: &QueryContext,
        _analyses: &mut AnalysisContext,
    ) -> OptimizeResult<Self::Cost> {
        let outer = match op.get(ctx) {
            OperatorData::Join(join) => Some(join.outer),
            OperatorData::CrossProduct(cross) => Some(cross.outer),
            _ => None,
        };
        Ok(outer
            .and_then(|outer| match outer.get(ctx) {
                OperatorData::Scan(scan) => Some((scan.table.to_string() != "t1") as usize),
                _ => None,
            })
            .unwrap_or(0))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SubsetCost {
    relations: NodeSet,
    total: u64,
}

struct SubsetCostModel {
    weights: Vec<u64>,
}

impl SubsetCostModel {
    fn relations(&self, op: Operator, ctx: &QueryContext) -> NodeSet {
        match op.get(ctx) {
            OperatorData::Scan(scan) => {
                let label = scan.table.to_string();
                let node = label
                    .strip_prefix('t')
                    .expect("synthetic scan labels start with t")
                    .parse()
                    .expect("synthetic scan labels end in a node id");
                nodeset_singleton(node)
            }
            data => data
                .inputs()
                .into_iter()
                .fold(NodeSet::EMPTY, |set, input| {
                    &set | &self.relations(input, ctx)
                }),
        }
    }

    fn intermediate_size(&self, relations: &NodeSet) -> u64 {
        relations.iter().map(|node| self.weights[node]).product()
    }
}

impl CostModel for SubsetCostModel {
    type Cost = SubsetCost;

    fn zero(&self) -> Self::Cost {
        SubsetCost {
            relations: NodeSet::EMPTY,
            total: 0,
        }
    }

    fn add(&self, left: Self::Cost, right: Self::Cost) -> Self::Cost {
        SubsetCost {
            relations: &left.relations | &right.relations,
            total: left.total + right.total,
        }
    }

    fn is_better(&self, candidate: &Self::Cost, existing: &Self::Cost) -> bool {
        candidate.total < existing.total
    }

    fn operator_cost(
        &self,
        op: Operator,
        ctx: &QueryContext,
        _analyses: &mut AnalysisContext,
    ) -> OptimizeResult<Self::Cost> {
        let relations = self.relations(op, ctx);
        let total = if matches!(
            op.get(ctx),
            OperatorData::Join(_) | OperatorData::CrossProduct(_)
        ) {
            self.intermediate_size(&relations)
        } else {
            0
        };
        Ok(SubsetCost { relations, total })
    }
}

fn synthetic_graph(
    relation_count: usize,
    edge_pairs: impl IntoIterator<Item = (usize, usize)>,
) -> (QueryContext, QueryHypergraph) {
    let mut ctx = QueryContext::new();
    let roots = (0..relation_count)
        .map(|node| {
            OperatorData::Scan(Scan {
                table: TableRef::bare(format!("t{node}")),
                columns: vec![],
            })
            .add(&mut ctx)
        })
        .collect::<Vec<_>>();
    let nodes = roots
        .iter()
        .enumerate()
        .map(|(node, root)| HypergraphNode {
            root: *root,
            label: format!("t{node}"),
            available: vec![],
        })
        .collect();
    let edges = edge_pairs
        .into_iter()
        .map(|(left, right)| Hyperedge {
            predicate: None,
            left: nodeset_singleton(left),
            right: nodeset_singleton(right),
            source: OperatorData::CrossProduct(CrossProduct {
                outer: roots[left],
                inner: roots[right],
            })
            .add(&mut ctx),
            join_type: HyperedgeJoinType::Inner,
        })
        .collect();
    (ctx, QueryHypergraph { nodes, edges })
}

fn synthetic_hypergraph(
    relation_count: usize,
    edge_masks: &[(u64, u64)],
) -> (QueryContext, QueryHypergraph) {
    let (mut ctx, mut hypergraph) = synthetic_graph(relation_count, []);
    let roots = hypergraph
        .nodes
        .iter()
        .map(|node| node.root)
        .collect::<Vec<_>>();
    hypergraph.edges = edge_masks
        .iter()
        .map(|&(left, right)| {
            let left_node = left.trailing_zeros() as usize;
            let right_node = right.trailing_zeros() as usize;
            Hyperedge {
                predicate: None,
                left: (0..relation_count)
                    .filter(|node| left & (1_u64 << node) != 0)
                    .collect(),
                right: (0..relation_count)
                    .filter(|node| right & (1_u64 << node) != 0)
                    .collect(),
                source: OperatorData::CrossProduct(CrossProduct {
                    outer: roots[left_node],
                    inner: roots[right_node],
                })
                .add(&mut ctx),
                join_type: HyperedgeJoinType::Inner,
            }
        })
        .collect();
    (ctx, hypergraph)
}

fn chain_graph(relation_count: usize) -> (QueryContext, QueryHypergraph) {
    synthetic_graph(
        relation_count,
        (1..relation_count).map(|node| (node - 1, node)),
    )
}

fn clique_graph(relation_count: usize) -> (QueryContext, QueryHypergraph) {
    synthetic_graph(
        relation_count,
        (0..relation_count)
            .flat_map(|left| (left + 1..relation_count).map(move |right| (left, right))),
    )
}

fn connected_subgraph_count_oracle(relation_count: usize, edges: &[(usize, usize)]) -> usize {
    (1_u64..1_u64 << relation_count)
        .filter(|subset| {
            let mut reached = 1_u64 << subset.trailing_zeros();
            loop {
                let expanded = edges.iter().fold(reached, |expanded, &(left, right)| {
                    let left_bit = 1_u64 << left;
                    let right_bit = 1_u64 << right;
                    if subset & left_bit != 0
                        && subset & right_bit != 0
                        && reached & (left_bit | right_bit) != 0
                    {
                        expanded | left_bit | right_bit
                    } else {
                        expanded
                    }
                });
                if expanded == reached {
                    return reached == *subset;
                }
                reached = expanded;
            }
        })
        .count()
}

fn dphyp_state_count_oracle(relation_count: usize, edges: &[(u64, u64)]) -> usize {
    let limit = 1_u64 << relation_count;
    let mut buildable = vec![false; limit as usize];
    for relation in 0..relation_count {
        buildable[1 << relation] = true;
    }

    for size in 2..=relation_count {
        for subset in 1_u64..limit {
            if subset.count_ones() as usize != size {
                continue;
            }
            let canonical = subset & subset.wrapping_neg();
            let mut left = subset.wrapping_sub(1) & subset;
            while left != 0 {
                let right = subset ^ left;
                if left & canonical != 0
                    && buildable[left as usize]
                    && buildable[right as usize]
                    && edges.iter().any(|&(edge_left, edge_right)| {
                        (edge_left & left == edge_left && edge_right & right == edge_right)
                            || (edge_left & right == edge_left && edge_right & left == edge_right)
                    })
                {
                    buildable[subset as usize] = true;
                    break;
                }
                left = left.wrapping_sub(1) & subset;
            }
        }
    }

    buildable.into_iter().filter(|state| *state).count()
}

fn possible_hyperedges(relation_count: usize) -> Vec<(u64, u64)> {
    let mut result = Vec::new();
    for mut assignment in 0..3_usize.pow(relation_count as u32) {
        let mut left = 0_u64;
        let mut right = 0_u64;
        for relation in 0..relation_count {
            match assignment % 3 {
                1 => left |= 1 << relation,
                2 => right |= 1 << relation,
                _ => {}
            }
            assignment /= 3;
        }
        if left != 0 && right != 0 && left < right {
            result.push((left, right));
        }
    }
    result
}

fn assert_hypergraph_count_matches_oracle(relation_count: usize, edges: &[(u64, u64)]) {
    let expected = dphyp_state_count_oracle(relation_count, edges);
    let (_, hypergraph) = synthetic_hypergraph(relation_count, edges);
    let graph = JoinGraph::new(&hypergraph);

    assert_eq!(
        graph.count_connected_subgraphs(expected),
        graph::BoundedCount::Within(expected),
        "edges {edges:?}",
    );
    assert_eq!(
        graph.count_connected_subgraphs(expected - 1),
        graph::BoundedCount::Exceeded,
        "budget boundary for edges {edges:?}",
    );
}

fn exhaustive_clique_cost(subset: u64, weights: &[u64], memo: &mut HashMap<u64, u64>) -> u64 {
    if subset.count_ones() == 1 {
        return 0;
    }
    if let Some(cost) = memo.get(&subset) {
        return *cost;
    }
    let canonical_bit = subset & subset.wrapping_neg();
    let intermediate_size = (0..weights.len())
        .filter(|node| subset & (1 << node) != 0)
        .map(|node| weights[node])
        .product::<u64>();
    let mut best = u64::MAX;
    let mut left = subset.wrapping_sub(1) & subset;
    while left != 0 {
        let right = subset ^ left;
        if right != 0 && left & canonical_bit != 0 {
            best = best.min(
                exhaustive_clique_cost(left, weights, memo)
                    + exhaustive_clique_cost(right, weights, memo)
                    + intermediate_size,
            );
        }
        left = left.wrapping_sub(1) & subset;
    }
    memo.insert(subset, best);
    best
}

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
    let mut analyses = crate::test_analyses(ctx);
    let hg = build_hypergraph(ctx, &mut analyses, root);
    let edge_indices = connecting_edge_indices(&nodeset_singleton(0), &nodeset_singleton(1), &hg);
    join_algorithm_class(&edge_indices, &hg, ctx)
}

fn solve_exact<M: CostModel>(
    ctx: &mut QueryContext,
    analyses: &mut AnalysisContext,
    hypergraph: &QueryHypergraph,
    cost_model: &M,
) -> OptimizeResult<Option<(plan::PlanState<M::Cost>, Operator)>> {
    let evaluator = MaterializingEvaluator;
    let mut search = JoinSearch::new(ctx, analyses, hypergraph, cost_model, &evaluator);
    let Some(plan) = DPhyp::new(&mut search).solve()? else {
        return Ok(None);
    };
    let root = search.materialize(&plan);
    Ok(Some((plan, root)))
}

fn solve_linearized<M: CostModel>(
    ctx: &mut QueryContext,
    analyses: &mut AnalysisContext,
    hypergraph: &QueryHypergraph,
    cost_model: &M,
) -> OptimizeResult<Option<(plan::PlanState<M::Cost>, Operator)>> {
    let evaluator = MaterializingEvaluator;
    let mut search = JoinSearch::new(ctx, analyses, hypergraph, cost_model, &evaluator);
    let Some(plan) = linearized::solve(&mut search)? else {
        return Ok(None);
    };
    let root = search.materialize(&plan);
    Ok(Some((plan, root)))
}

fn solve_goo<M: CostModel>(
    ctx: &mut QueryContext,
    analyses: &mut AnalysisContext,
    hypergraph: &QueryHypergraph,
    cost_model: &M,
    exact_subproblem_size: usize,
) -> OptimizeResult<Option<(plan::PlanState<M::Cost>, Operator)>> {
    let evaluator = MaterializingEvaluator;
    let mut search = JoinSearch::new(ctx, analyses, hypergraph, cost_model, &evaluator);
    let Some(plan) = goo::solve(
        &mut search,
        GooDpConfig {
            inner: GooInnerSolver::DpHyp,
            max_subproblem_relations: exact_subproblem_size,
            dp_state_budget: usize::MAX,
        },
    )?
    .plan
    else {
        return Ok(None);
    };
    let root = search.materialize(&plan);
    Ok(Some((plan, root)))
}

#[derive(Debug, Clone, Copy)]
enum TestSearchAlgorithm {
    DpHyp,
    Linearized,
    GooDp,
}

fn run_default_algorithm(
    algorithm: TestSearchAlgorithm,
    deferred: bool,
) -> (plan::JoinTree, f64, usize) {
    let (mut ctx, hypergraph) = chain_graph(6);
    let mut analyses = crate::test_analyses(&ctx);
    let operators_before = ctx.operator_count();
    let model = DefaultCostModel;
    let cardinality_evaluator = evaluator::CardinalityEvaluator;
    let materializing_evaluator = MaterializingEvaluator;
    let selected: &dyn evaluator::CandidateEvaluator<DefaultCostModel> = if deferred {
        &cardinality_evaluator
    } else {
        &materializing_evaluator
    };

    let (tree, cost, root, expected_profile) = {
        let mut search = JoinSearch::new(&mut ctx, &mut analyses, &hypergraph, &model, selected);
        let plan = match algorithm {
            TestSearchAlgorithm::DpHyp => DPhyp::new(&mut search).solve(),
            TestSearchAlgorithm::Linearized => linearized::solve(&mut search),
            TestSearchAlgorithm::GooDp => goo::solve(
                &mut search,
                GooDpConfig {
                    inner: GooInnerSolver::DpHyp,
                    max_subproblem_relations: 3,
                    dp_state_budget: usize::MAX,
                },
            )
            .map(|outcome| outcome.plan),
        }
        .unwrap()
        .expect("a connected chain has a complete plan");
        let tree = plan.tree.clone();
        let cost = plan.cost;
        let expected_profile = plan.properties.cardinality.clone();
        let root = search.materialize(&plan);
        assert_eq!(
            search.materialize(&plan),
            root,
            "materialization is memoized"
        );
        (tree, cost, root, expected_profile)
    };

    let analyzed = CardinalityEstimationV1::get_shared(&ctx, &mut analyses, root).unwrap();
    assert_eq!(
        expected_profile
            .as_deref()
            .expect("every built-in evaluator carries cardinality"),
        analyzed.as_ref(),
    );

    (tree, cost, ctx.operator_count() - operators_before)
}

#[test]
fn dphyp_three_way_chain_produces_plan() {
    let (mut ctx, root) = three_way_chain();
    let mut analyses = crate::test_analyses(&ctx);
    let hg = build_hypergraph(&ctx, &mut analyses, root);
    assert_eq!(hg.nodes.len(), 3);

    let (plan, _) = solve_exact(&mut ctx, &mut analyses, &hg, &DefaultCostModel)
        .expect("solver should not error")
        .expect("DPhyp should find a plan");
    assert_eq!(plan.tree.leaf_count(), 3);
}

#[test]
fn dphyp_materializes_winning_plan_in_arena() {
    let (mut ctx, root) = three_way_chain();
    let mut analyses = crate::test_analyses(&ctx);
    let hg = build_hypergraph(&ctx, &mut analyses, root);
    let before = ctx.operator_count();

    let (_, plan_root) = solve_exact(&mut ctx, &mut analyses, &hg, &DefaultCostModel)
        .expect("solver should not error")
        .expect("DPhyp should find a plan");

    assert!(ctx.operator_count() > before);
    assert!(matches!(
        plan_root.get(&ctx),
        OperatorData::Join(_) | OperatorData::CrossProduct(_)
    ));
}

#[test]
fn dphyp_reconstructs_deferred_non_commutative_join_with_all_predicates() {
    let mut ctx = QueryContext::new();
    let a1 = ColumnData::new("a1", DataType::Int64).add(&mut ctx);
    let a2 = ColumnData::new("a2", DataType::Int64).add(&mut ctx);
    let b1 = ColumnData::new("b1", DataType::Int64).add(&mut ctx);
    let b2 = ColumnData::new("b2", DataType::Int64).add(&mut ctx);
    let left = OperatorData::Scan(Scan {
        table: TableRef::bare("A"),
        columns: vec![a1, a2],
    })
    .add(&mut ctx);
    let right = OperatorData::Scan(Scan {
        table: TableRef::bare("B"),
        columns: vec![b1, b2],
    })
    .add(&mut ctx);
    let equality = binary_predicate(&mut ctx, BinaryOp::Eq, a1, b1);
    let residual = binary_predicate(&mut ctx, BinaryOp::Gt, a2, b2);
    let on = ExprData::Nary {
        op: NaryOp::And,
        exprs: vec![equality, residual],
    }
    .add(&mut ctx);
    let root = OperatorData::Join(Join {
        join_type: JoinType::LeftAnti,
        on,
        outer: left,
        inner: right,
    })
    .add(&mut ctx);
    let mut analyses = crate::test_analyses(&ctx);
    let hypergraph = build_hypergraph(&ctx, &mut analyses, root);
    assert_eq!(hypergraph.edges.len(), 2);
    let operators_before_search = ctx.operator_count();

    let plan_root = {
        let evaluator = DeferredUnitEvaluator;
        let mut search =
            JoinSearch::new(&mut ctx, &mut analyses, &hypergraph, &UnitCost, &evaluator);
        let plan = DPhyp::new(&mut search)
            .solve()
            .expect("solver should not error")
            .expect("two connected relations have a plan");
        assert_eq!(plan.cost, 1);
        search.materialize(&plan)
    };
    let OperatorData::Join(join) = plan_root.get(&ctx) else {
        panic!("deferred recipe should reconstruct a join");
    };
    assert_eq!(join.join_type, JoinType::LeftAnti);
    assert_eq!(join.outer, left, "non-commutative outer input changed");
    assert_eq!(join.inner, right, "non-commutative inner input changed");
    let ExprData::Nary {
        op: NaryOp::And,
        exprs,
    } = join.on.get(&ctx)
    else {
        panic!("all connecting predicates should be reconstructed");
    };
    assert_eq!(exprs, &vec![equality, residual]);
    assert_eq!(ctx.operator_count(), operators_before_search + 1);
}

#[test]
fn cardinality_evaluator_matches_all_enumerators_and_materializes_only_the_winner() {
    for algorithm in [
        TestSearchAlgorithm::DpHyp,
        TestSearchAlgorithm::Linearized,
        TestSearchAlgorithm::GooDp,
    ] {
        let (deferred_tree, deferred_cost, deferred_operators) =
            run_default_algorithm(algorithm, true);
        let (materialized_tree, materialized_cost, materialized_operators) =
            run_default_algorithm(algorithm, false);

        assert_eq!(deferred_tree, materialized_tree, "{algorithm:?}");
        assert_eq!(
            deferred_cost.to_bits(),
            materialized_cost.to_bits(),
            "{algorithm:?}",
        );
        assert_eq!(
            deferred_operators, 5,
            "a six-relation winner has exactly five joins: {algorithm:?}",
        );
        assert!(
            materialized_operators > deferred_operators,
            "compatibility costing should expose rejected candidate IR: {algorithm:?}",
        );
    }
}

#[test]
fn join_ordering_constructors_select_deferred_and_compatibility_evaluators() {
    let (query, _) = three_way_chain();
    let mut default_ctx = crate::test_optimizer_context(query);
    let default_before = default_ctx.query.operator_count();
    JoinOrdering::new().run(&mut default_ctx).unwrap();
    let default_appended = default_ctx.query.operator_count() - default_before;

    let (query, _) = three_way_chain();
    let mut compatibility_ctx = crate::test_optimizer_context(query);
    let compatibility_before = compatibility_ctx.query.operator_count();
    JoinOrdering::with_cost_model(DefaultCostModel)
        .run(&mut compatibility_ctx)
        .unwrap();
    let compatibility_appended = compatibility_ctx.query.operator_count() - compatibility_before;

    assert_eq!(default_appended, 2, "only the two winning joins are new");
    assert!(
        compatibility_appended > default_appended,
        "with_cost_model preserves eager compatibility semantics",
    );
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
    let mut analyses = crate::test_analyses(&ctx);
    let hg = build_hypergraph(&ctx, &mut analyses, root);

    let (_, plan_root) = solve_exact(&mut ctx, &mut analyses, &hg, &DefaultCostModel)
        .expect("solver should not error")
        .expect("DPhyp should find a plan");

    let OperatorData::Join(join) = plan_root.get(&ctx) else {
        panic!("winning plan should be a join");
    };
    assert!(matches!(
        join.join_type,
        JoinType::LeftMark {
            marker: actual,
            nullable: false,
        } if actual == marker
    ));
    assert_eq!(join.outer, left, "non-commutative outer input changed");
    assert_eq!(join.inner, right, "non-commutative inner input changed");
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
            _analyses: &mut AnalysisContext,
        ) -> OptimizeResult<Self::Cost> {
            let units = match op.get(ctx) {
                OperatorData::Join(_) | OperatorData::CrossProduct(_) => 10,
                _ => 1,
            };
            Ok(StructuredCost { units })
        }
    }

    let (mut ctx, root) = three_way_chain();
    let mut analyses = crate::test_analyses(&ctx);
    let hg = build_hypergraph(&ctx, &mut analyses, root);
    let additions = Arc::new(AtomicUsize::new(0));
    let comparisons = Arc::new(AtomicUsize::new(0));
    let model = StructuredCostModel {
        additions: additions.clone(),
        comparisons: comparisons.clone(),
    };
    let (plan, _) = solve_exact(&mut ctx, &mut analyses, &hg, &model)
        .unwrap()
        .unwrap();

    assert_eq!(plan.cost, StructuredCost { units: 23 });
    assert!(additions.load(Ordering::Relaxed) > 0);
    assert!(comparisons.load(Ordering::Relaxed) > 0);
}

#[test]
fn materializing_evaluator_preserves_total_cost_from_children_override() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct OverrideCostModel {
        total_calls: Arc<AtomicUsize>,
        operator_calls: Arc<AtomicUsize>,
    }

    impl CostModel for OverrideCostModel {
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
            _op: Operator,
            _ctx: &QueryContext,
            _analyses: &mut AnalysisContext,
        ) -> OptimizeResult<Self::Cost> {
            self.operator_calls.fetch_add(1, Ordering::Relaxed);
            Ok(10_000)
        }

        fn total_cost_from_children(
            &self,
            op: Operator,
            child_costs: &[Self::Cost],
            ctx: &QueryContext,
            _analyses: &mut AnalysisContext,
        ) -> OptimizeResult<Self::Cost> {
            self.total_calls.fetch_add(1, Ordering::Relaxed);
            Ok(match op.get(ctx) {
                OperatorData::Join(_) | OperatorData::CrossProduct(_) => {
                    100 + child_costs.iter().sum::<usize>()
                }
                _ => 7,
            })
        }
    }

    let (mut ctx, root) = three_way_chain();
    let mut analyses = crate::test_analyses(&ctx);
    let hypergraph = build_hypergraph(&ctx, &mut analyses, root);
    let total_calls = Arc::new(AtomicUsize::new(0));
    let operator_calls = Arc::new(AtomicUsize::new(0));
    let model = OverrideCostModel {
        total_calls: Arc::clone(&total_calls),
        operator_calls: Arc::clone(&operator_calls),
    };

    let (plan, _) = solve_exact(&mut ctx, &mut analyses, &hypergraph, &model)
        .unwrap()
        .expect("the chain is connected");

    assert_eq!(plan.cost, 221);
    assert!(total_calls.load(Ordering::Relaxed) > 3);
    assert_eq!(operator_calls.load(Ordering::Relaxed), 0);
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
    let mut analyses = AnalysisContext::new(Arc::new(catalog));
    let hg = build_hypergraph(&ctx, &mut analyses, root);
    let (plan, _) = solve_exact(&mut ctx, &mut analyses, &hg, &DefaultCostModel)
        .expect("solver should not error")
        .expect("DPhyp should find a plan");

    assert!(
        plan.tree
            .has_join_with_leaves(&(&nodeset_singleton(0) | &nodeset_singleton(1)))
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
    let mut analyses = crate::test_analyses(&ctx);

    assert_eq!(all_nodes_mask(64), NodeSet::all(64));
    assert!(matches!(
        solve_exact(&mut ctx, &mut analyses, &hg, &DefaultCostModel),
        Ok(None)
    ));
}

#[test]
fn dphyp_accepts_relation_ids_beyond_one_machine_word() {
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
    let mut analyses = crate::test_analyses(&ctx);

    assert!(matches!(
        solve_exact(&mut ctx, &mut analyses, &hg, &DefaultCostModel),
        Ok(None)
    ));
    assert!(all_nodes_mask(65).contains(64));
}

#[test]
fn join_ordering_reports_changed_so_pass_manager_updates_root() {
    let (ctx, root) = three_way_chain();
    let mut opt = crate::test_optimizer_context(ctx);
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
    let mut opt = crate::test_optimizer_context(ctx);
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
    let mut opt1 = crate::test_optimizer_context(ctx1);
    let mut opt2 = crate::test_optimizer_context(ctx2);
    let mut pm = PassManager::new();
    pm.add_pass(JoinOrdering::new());

    pm.run(&mut opt1).unwrap();
    assert_eq!(pm.profiles().len(), 1);
    assert_eq!(pm.profiles()[0].result, Some(PassResult::Changed));
    pm.run(&mut opt2).unwrap();

    assert_ne!(opt1.query.root(), Some(root1));
    assert_ne!(opt2.query.root(), Some(root2));
    assert_eq!(pm.profiles().len(), 1);
    assert_eq!(pm.profiles()[0].result, Some(PassResult::Changed));
}

#[test]
fn join_ordering_once_mode_does_not_revisit_after_a_later_pass_creates_a_join() {
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
    let mut opt = crate::test_optimizer_context(ctx);
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

#[test]
fn connected_subgraph_counter_matches_chain_and_clique_search_spaces() {
    let (_, chain) = chain_graph(6);
    let (_, clique) = clique_graph(6);

    assert_eq!(
        JoinGraph::new(&chain).count_connected_subgraphs(100),
        graph::BoundedCount::Within(21)
    );
    assert_eq!(
        JoinGraph::new(&clique).count_connected_subgraphs(100),
        graph::BoundedCount::Within(63)
    );
    assert_eq!(
        JoinGraph::new(&clique).count_connected_subgraphs(20),
        graph::BoundedCount::Exceeded
    );
}

#[test]
fn connected_subgraph_counter_matches_oracle_for_every_five_node_graph() {
    const RELATIONS: usize = 5;
    let possible_edges = (0..RELATIONS)
        .flat_map(|left| (left + 1..RELATIONS).map(move |right| (left, right)))
        .collect::<Vec<_>>();

    for graph_bits in 0_u64..1_u64 << possible_edges.len() {
        let edges = possible_edges
            .iter()
            .enumerate()
            .filter_map(|(edge, endpoints)| {
                (graph_bits & (1_u64 << edge) != 0).then_some(*endpoints)
            })
            .collect::<Vec<_>>();
        let expected = connected_subgraph_count_oracle(RELATIONS, &edges);
        let (_, hypergraph) = synthetic_graph(RELATIONS, edges);
        let graph = JoinGraph::new(&hypergraph);

        assert_eq!(
            graph.count_connected_subgraphs(expected),
            graph::BoundedCount::Within(expected),
            "graph bits {graph_bits:#014b}",
        );
        assert_eq!(
            graph.count_connected_subgraphs(expected - 1),
            graph::BoundedCount::Exceeded,
            "budget boundary for graph bits {graph_bits:#014b}",
        );
    }
}

#[test]
fn connected_subgraph_counter_does_not_count_partial_hyperedge_sides() {
    // `{0,1}` is reachable by the representative-based graph traversal through
    // `{0}<->{1,2}`, but it has no valid final join and therefore is not a DPhyp state.
    let edges = [(0b010, 0b100), (0b011, 0b100), (0b001, 0b110)];

    assert_eq!(dphyp_state_count_oracle(3, &edges), 5);
    assert_hypergraph_count_matches_oracle(3, &edges);
}

#[test]
fn connected_subgraph_counter_matches_every_three_node_hypergraph() {
    const RELATIONS: usize = 3;
    let possible_edges = possible_hyperedges(RELATIONS);
    assert_eq!(possible_edges.len(), 6);

    for graph_bits in 0_u64..1_u64 << possible_edges.len() {
        let edges = possible_edges
            .iter()
            .enumerate()
            .filter_map(|(edge, endpoints)| {
                (graph_bits & (1_u64 << edge) != 0).then_some(*endpoints)
            })
            .collect::<Vec<_>>();
        assert_hypergraph_count_matches_oracle(RELATIONS, &edges);
    }
}

#[test]
fn connected_subgraph_counter_matches_randomized_five_node_hypergraphs() {
    const RELATIONS: usize = 5;
    let possible_edges = possible_hyperedges(RELATIONS);
    let chain = (1..RELATIONS)
        .map(|right| (1_u64 << (right - 1), 1_u64 << right))
        .collect::<Vec<_>>();
    let mut random = 0x9e37_79b9_7f4a_7c15_u64;

    for _ in 0..256 {
        let mut edges = chain.clone();
        // Force the exact hypergraph path even when this sample selects no other multi-node edge.
        edges.push((0b00011, 0b00100));
        for &edge in &possible_edges {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            if random.is_multiple_of(13) {
                edges.push(edge);
            }
        }
        assert_hypergraph_count_matches_oracle(RELATIONS, &edges);
    }
}

#[test]
fn indexed_connectivity_matches_edge_scan_for_regular_and_hyperedges() {
    let (_, mut hypergraph) = clique_graph(5);
    hypergraph.edges[0].left = &nodeset_singleton(0) | &nodeset_singleton(1);
    hypergraph.edges[0].right = nodeset_singleton(2);
    let graph = JoinGraph::new(&hypergraph);
    let subsets = NodeSet::all(5).non_empty_subsets().collect::<Vec<_>>();

    for left in &subsets {
        for right in subsets.iter().filter(|right| left.is_disjoint(right)) {
            let expected = connecting_edge_indices(left, right, &hypergraph);
            assert_eq!(
                graph.connecting_edge_indices(left, right),
                expected,
                "left={left:?}, right={right:?}",
            );
            assert_eq!(graph.connects(left, right), !expected.is_empty());
        }
    }
}

#[test]
fn dphyp_matches_exhaustive_bushy_enumeration_on_small_clique() {
    let weights = vec![2, 3, 5, 7, 11];
    let (mut ctx, clique) = clique_graph(weights.len());
    let mut analyses = crate::test_analyses(&ctx);
    let model = SubsetCostModel {
        weights: weights.clone(),
    };
    let (plan, _) = solve_exact(&mut ctx, &mut analyses, &clique, &model)
        .unwrap()
        .expect("a clique is connected");
    let expected = exhaustive_clique_cost((1 << weights.len()) - 1, &weights, &mut HashMap::new());

    assert_eq!(plan.cost.total, expected);
    assert_eq!(plan.cost.relations, NodeSet::all(weights.len()));
}

#[test]
fn dphyp_costs_both_inner_orientations_when_model_opts_in() {
    let (mut ctx, graph) = clique_graph(2);
    let preferred_outer = graph.nodes[1].root;
    let mut analyses = crate::test_analyses(&ctx);
    let (plan, plan_root) = solve_exact(&mut ctx, &mut analyses, &graph, &OrientationCost)
        .unwrap()
        .expect("two connected relations have a plan");
    let OperatorData::CrossProduct(cross) = plan_root.get(&ctx) else {
        panic!("a dummy edge should materialize as a cross product");
    };

    assert_eq!(cross.outer, preferred_outer);
    assert_eq!(plan.cost, 0);
}

#[test]
fn adaptive_policy_uses_graph_complexity_and_hyperedges() {
    let config = AdaptiveJoinOrderingConfig {
        exact_relation_threshold: 4,
        connected_subgraph_budget: 100,
        linearized_relation_threshold: 20,
        goo_linearized_subproblem_size: 8,
        goo_dphyp_subproblem_size: 4,
        goo_dp_state_budget: 100,
    };
    let (_, chain) = chain_graph(15);
    let (_, clique) = clique_graph(15);
    let (_, very_large) = chain_graph(21);

    assert_eq!(
        choose_algorithm(&chain, config),
        AlgorithmDecision {
            algorithm: JoinOrderAlgorithm::LinearizedDp,
            connected_subgraphs: None,
            dp_states_created: 0,
            repaired_subproblems: 0,
        }
    );
    assert_eq!(
        choose_algorithm(&clique, config).algorithm,
        JoinOrderAlgorithm::LinearizedDp
    );
    assert_eq!(
        choose_algorithm(&very_large, config).algorithm,
        JoinOrderAlgorithm::GooDp(GooDpConfig {
            inner: GooInnerSolver::LinearizedDp,
            max_subproblem_relations: 8,
            dp_state_budget: 100,
        })
    );

    let (_, mut hypergraph) = clique_graph(15);
    hypergraph.edges[0].left = &nodeset_singleton(0) | &nodeset_singleton(1);
    hypergraph.edges[0].right = nodeset_singleton(2);
    assert_eq!(
        choose_algorithm(&hypergraph, config).algorithm,
        JoinOrderAlgorithm::GooDp(GooDpConfig {
            inner: GooInnerSolver::DpHyp,
            max_subproblem_relations: 4,
            dp_state_budget: 100,
        })
    );
}

#[test]
fn default_policy_matches_the_paper_boundaries_and_solver_pairing() {
    let config = AdaptiveJoinOrderingConfig::default();
    let (_, cheap_large_chain) = chain_graph(140);
    let (_, expensive_large_chain) = chain_graph(141);
    let cheap_decision = choose_algorithm(&cheap_large_chain, config);
    let expensive_decision = choose_algorithm(&expensive_large_chain, config);

    assert_eq!(cheap_decision.algorithm, JoinOrderAlgorithm::DpHyp);
    assert_eq!(cheap_decision.connected_subgraphs, Some(9_870));
    assert_eq!(
        expensive_decision.algorithm,
        JoinOrderAlgorithm::GooDp(GooDpConfig {
            inner: GooInnerSolver::LinearizedDp,
            max_subproblem_relations: 100,
            dp_state_budget: 10_000,
        })
    );

    let (_, mut hypergraph) = clique_graph(15);
    hypergraph.edges[0].left = &nodeset_singleton(0) | &nodeset_singleton(1);
    hypergraph.edges[0].right = nodeset_singleton(2);
    assert_eq!(
        choose_algorithm(&hypergraph, config).algorithm,
        JoinOrderAlgorithm::GooDp(GooDpConfig {
            inner: GooInnerSolver::DpHyp,
            max_subproblem_relations: 10,
            dp_state_budget: 10_000,
        })
    );

    let (_, mut non_inner) = clique_graph(15);
    non_inner.edges[0].join_type = HyperedgeJoinType::LeftSemi;
    assert_eq!(
        choose_algorithm(&non_inner, config).algorithm,
        JoinOrderAlgorithm::GooDp(GooDpConfig {
            inner: GooInnerSolver::DpHyp,
            max_subproblem_relations: 10,
            dp_state_budget: 10_000,
        })
    );
}

#[test]
fn join_ordering_reports_actual_dp_work_in_its_decision() {
    let (ctx, _) = three_way_chain();
    let mut optimizer = crate::test_optimizer_context(ctx);
    let config = AdaptiveJoinOrderingConfig {
        exact_relation_threshold: 0,
        connected_subgraph_budget: 0,
        linearized_relation_threshold: 0,
        goo_linearized_subproblem_size: 2,
        goo_dphyp_subproblem_size: 2,
        goo_dp_state_budget: 3,
    };
    let mut pass = JoinOrdering::with_config(config);

    assert_eq!(pass.run(&mut optimizer).unwrap(), PassResult::Changed);
    let [decision] = pass.last_decisions() else {
        panic!("one join group should produce one decision");
    };
    assert_eq!(
        decision.algorithm,
        JoinOrderAlgorithm::GooDp(GooDpConfig {
            inner: GooInnerSolver::LinearizedDp,
            max_subproblem_relations: 2,
            dp_state_budget: 3,
        })
    );
    assert_eq!(decision.connected_subgraphs, None);
    assert_eq!(decision.dp_states_created, 3);
    assert_eq!(decision.repaired_subproblems, 1);
}

#[test]
fn exact_policy_accepts_chain_with_more_than_64_relations() {
    let (_, chain) = chain_graph(65);
    let decision = choose_algorithm(&chain, AdaptiveJoinOrderingConfig::default());

    assert_eq!(decision.algorithm, JoinOrderAlgorithm::DpHyp);
    assert_eq!(decision.connected_subgraphs, Some(2_145));
}

#[test]
fn dphyp_solves_connected_chain_beyond_one_machine_word() {
    let (mut ctx, chain) = chain_graph(65);
    let mut analyses = crate::test_analyses(&ctx);
    let (plan, _) = solve_exact(&mut ctx, &mut analyses, &chain, &UnitCost)
        .expect("DPhyp should support dynamic relation sets")
        .expect("a chain is connected");

    assert_eq!(plan.tree.leaf_count(), 65);
    assert_eq!(plan.tree.leaf_set(), NodeSet::all(65));
}

#[test]
fn contracted_dphyp_keeps_atoms_opaque_and_counts_frontier_states() {
    let (mut ctx, chain) = chain_graph(6);
    let mut analyses = crate::test_analyses(&ctx);
    let evaluator = MaterializingEvaluator;
    let mut search = JoinSearch::new(&mut ctx, &mut analyses, &chain, &UnitCost, &evaluator);
    let atom_nodes = NodeSet::all(3);
    let atom_state = DPhyp::new(&mut search)
        .solve_subset(&atom_nodes)
        .unwrap()
        .expect("the first three chain nodes form an exact atom");
    let mut atoms = vec![plan::PlanAtom {
        nodes: atom_nodes.clone(),
        state: atom_state,
    }];
    for node in 3..6 {
        atoms.push(plan::PlanAtom {
            nodes: nodeset_singleton(node),
            state: search.leaf(node).unwrap(),
        });
    }

    let outcome = dphyp::solve_frontier_with_stats(&mut search, &atoms).unwrap();
    let plan = outcome.plan.expect("the contracted chain is connected");

    assert_eq!(outcome.dp_states_created, 10);
    assert_eq!(plan.cost, 5);
    assert_eq!(plan.tree.leaf_set(), NodeSet::all(6));
    assert!(plan.tree.has_join_with_leaves(&atom_nodes));
}

#[test]
fn linearized_dp_and_goo_dp_each_produce_complete_plans() {
    let (mut linear_ctx, clique) = clique_graph(9);
    let mut linear_analyses = crate::test_analyses(&linear_ctx);
    let (linear_plan, _) =
        solve_linearized(&mut linear_ctx, &mut linear_analyses, &clique, &UnitCost)
            .unwrap()
            .expect("linearized DP should solve an ordinary graph");
    assert_eq!(linear_plan.tree.leaf_set(), NodeSet::all(9));

    let (mut goo_ctx, chain) = chain_graph(20);
    let mut goo_analyses = crate::test_analyses(&goo_ctx);
    let (goo_plan, _) = solve_goo(&mut goo_ctx, &mut goo_analyses, &chain, &UnitCost, 4)
        .unwrap()
        .expect("GOO/DP should solve a large connected graph");
    assert_eq!(goo_plan.tree.leaf_set(), NodeSet::all(20));
}
