//! Cross-module correctness and integration tests for join ordering.

use super::dphyp::all_nodes_mask;
use super::graph::JoinGraph;
use super::*;
use crate::analysis::connecting_edge_indices;
use crate::cost::{JoinAlgorithmClass, join_algorithm_class, join_algorithm_cost};
use crate::{
    AnalysisContext, BinaryOp, Catalog, Column, ColumnData, ColumnStatistics, CrossProduct,
    ExprData, Hyperedge, HyperedgeJoinType, HypergraphNode, Join, JoinType, MemoryCatalog, NaryOp,
    NodeSet, Operator, OperatorData, OptimizerContext, Output, PassManager, QueryContext,
    QueryHypergraph, Relation, ScalarValue, Scan, Selection, TableRef, TableStatistics,
    nodeset_singleton,
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

#[test]
fn dphyp_three_way_chain_produces_plan() {
    let (mut ctx, root) = three_way_chain();
    let mut analyses = crate::test_analyses(&ctx);
    let hg = build_hypergraph(&ctx, &mut analyses, root);
    assert_eq!(hg.nodes.len(), 3);

    let mut solver = DPhyp::new(&mut ctx, &mut analyses, &hg, &DefaultCostModel);
    let plan = solver
        .solve()
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

    let plan = {
        let mut solver = DPhyp::new(&mut ctx, &mut analyses, &hg, &DefaultCostModel);
        solver
            .solve()
            .expect("solver should not error")
            .expect("DPhyp should find a plan")
    };

    assert!(ctx.operator_count() > before);
    assert!(matches!(
        plan.root.get(&ctx),
        OperatorData::Join(_) | OperatorData::CrossProduct(_)
    ));
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

    let plan = {
        let mut solver = DPhyp::new(&mut ctx, &mut analyses, &hg, &DefaultCostModel);
        solver
            .solve()
            .expect("solver should not error")
            .expect("DPhyp should find a plan")
    };

    let OperatorData::Join(join) = plan.root.get(&ctx) else {
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
    let mut solver = DPhyp::new(&mut ctx, &mut analyses, &hg, &model);

    let plan = solver.solve().unwrap().unwrap();

    assert_eq!(plan.cost, StructuredCost { units: 23 });
    assert!(additions.load(Ordering::Relaxed) > 0);
    assert!(comparisons.load(Ordering::Relaxed) > 0);
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
    let mut solver = DPhyp::new(&mut ctx, &mut analyses, &hg, &DefaultCostModel);

    let plan = solver
        .solve()
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
    let mut solver = DPhyp::new(&mut ctx, &mut analyses, &hg, &DefaultCostModel);

    assert_eq!(all_nodes_mask(64), NodeSet::all(64));
    assert!(matches!(solver.solve(), Ok(None)));
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
    let mut solver = DPhyp::new(&mut ctx, &mut analyses, &hg, &DefaultCostModel);

    assert!(matches!(solver.solve(), Ok(None)));
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
fn dphyp_matches_exhaustive_bushy_enumeration_on_small_clique() {
    let weights = vec![2, 3, 5, 7, 11];
    let (mut ctx, clique) = clique_graph(weights.len());
    let mut analyses = crate::test_analyses(&ctx);
    let model = SubsetCostModel {
        weights: weights.clone(),
    };
    let plan = DPhyp::new(&mut ctx, &mut analyses, &clique, &model)
        .solve()
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
    let plan = DPhyp::new(&mut ctx, &mut analyses, &graph, &OrientationCost)
        .solve()
        .unwrap()
        .expect("two connected relations have a plan");
    let OperatorData::CrossProduct(cross) = plan.root.get(&ctx) else {
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
        goo_exact_subproblem_size: 4,
    };
    let (_, chain) = chain_graph(15);
    let (_, clique) = clique_graph(15);
    let (_, very_large) = chain_graph(21);

    assert_eq!(
        choose_algorithm(&chain, config),
        AlgorithmDecision {
            algorithm: JoinOrderAlgorithm::LinearizedDp,
            connected_subgraphs: None,
        }
    );
    assert_eq!(
        choose_algorithm(&clique, config).algorithm,
        JoinOrderAlgorithm::LinearizedDp
    );
    assert_eq!(
        choose_algorithm(&very_large, config).algorithm,
        JoinOrderAlgorithm::GooDp
    );

    let (_, mut hypergraph) = clique_graph(15);
    hypergraph.edges[0].left = &nodeset_singleton(0) | &nodeset_singleton(1);
    hypergraph.edges[0].right = nodeset_singleton(2);
    assert_eq!(
        choose_algorithm(&hypergraph, config).algorithm,
        JoinOrderAlgorithm::GooDp
    );
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
    let plan = DPhyp::new(&mut ctx, &mut analyses, &chain, &UnitCost)
        .solve()
        .expect("DPhyp should support dynamic relation sets")
        .expect("a chain is connected");

    assert_eq!(plan.tree.leaf_count(), 65);
    assert_eq!(plan.tree.leaf_set(), NodeSet::all(65));
}

#[test]
fn linearized_dp_and_goo_dp_each_produce_complete_plans() {
    let (mut linear_ctx, clique) = clique_graph(9);
    let mut linear_analyses = crate::test_analyses(&linear_ctx);
    let linear_plan = linearized::solve(&mut linear_ctx, &mut linear_analyses, &clique, &UnitCost)
        .unwrap()
        .expect("linearized DP should solve an ordinary graph");
    assert_eq!(linear_plan.tree.leaf_set(), NodeSet::all(9));

    let (mut goo_ctx, chain) = chain_graph(20);
    let mut goo_analyses = crate::test_analyses(&goo_ctx);
    let goo_plan = goo::solve(&mut goo_ctx, &mut goo_analyses, &chain, &UnitCost, 4)
        .unwrap()
        .expect("GOO/DP should solve a large connected graph");
    assert_eq!(goo_plan.tree.leaf_set(), NodeSet::all(20));
}
