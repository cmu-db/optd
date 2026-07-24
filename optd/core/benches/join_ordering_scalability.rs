//! Reproducible scalability measurements for adaptive join ordering.
//!
//! The output is intentionally plain CSV so figures can be regenerated without a benchmark
//! framework dependency. Timings cover one complete `JoinOrdering::run` call while excluding
//! query construction and cloning.

use std::fs::{self, File};
use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arrow_schema::{DataType, Field, Schema};
use optd_core::{
    AdaptiveJoinOrderingConfig, AnalysisContext, BinaryOp, Catalog, ColumnData, CostModel,
    ExprData, Join, JoinOrdering, JoinType, MemoryCatalog, NaryOp, Operator, OperatorData,
    OptimizeResult, OptimizerContext, QueryContext, QueryPass, RelationSet, Scan, TableRef,
};

const RANDOM_TREE_SIZES: &[usize] = &[10, 20, 30, 40, 70, 100, 128, 192, 256];
const VERY_LARGE_RANDOM_TREE_SIZES: &[usize] = &[512, 1_024, 2_000, 5_000];
const EXACT_SIZES: &[usize] = &[10, 12, 14, 16, 18];
const BOUNDARY_SIZES: &[usize] = &[32, 63, 64, 65, 127, 128, 129, 256, 512, 1_024];
const SPARSE_UNIVERSES: &[usize] = &[1_025, 4_097, 16_385];

#[derive(Clone, Copy)]
enum RequestedAlgorithm {
    Adaptive,
    DpHyp,
    LinearizedDp,
    GooDp,
}

impl RequestedAlgorithm {
    fn label(self) -> &'static str {
        match self {
            Self::Adaptive => "adaptive",
            Self::DpHyp => "dphyp",
            Self::LinearizedDp => "linearized_dp",
            Self::GooDp => "goo_dp",
        }
    }

    fn config(self) -> AdaptiveJoinOrderingConfig {
        match self {
            Self::Adaptive => AdaptiveJoinOrderingConfig::default(),
            Self::DpHyp => AdaptiveJoinOrderingConfig {
                exact_relation_threshold: usize::MAX,
                ..AdaptiveJoinOrderingConfig::default()
            },
            Self::LinearizedDp => AdaptiveJoinOrderingConfig {
                exact_relation_threshold: 0,
                connected_subgraph_budget: 0,
                linearized_relation_threshold: usize::MAX,
                ..AdaptiveJoinOrderingConfig::default()
            },
            Self::GooDp => AdaptiveJoinOrderingConfig {
                exact_relation_threshold: 0,
                connected_subgraph_budget: 0,
                linearized_relation_threshold: 0,
                ..AdaptiveJoinOrderingConfig::default()
            },
        }
    }
}

struct EnumerationCost;

impl CostModel for EnumerationCost {
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let output = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("join_ordering_scalability.csv"));
    let query_count = args
        .next()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(10);
    let repetitions = args
        .next()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(3);

    if let Some(parent) = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let mut output = BufWriter::new(File::create(output)?);
    writeln!(
        output,
        "suite,shape,relations,variant,selected_algorithm,query_id,repetition,operations,duration_ns,candidate_operators,connected_subgraphs,dp_states_created,repaired_subproblems"
    )?;

    measure_random_tree_algorithms(&mut output, query_count, repetitions)?;
    measure_topology_sweep(&mut output, repetitions)?;
    measure_relation_set_boundaries(&mut output, repetitions.max(5))?;
    measure_sparse_relation_sets(&mut output, repetitions.max(5))?;
    Ok(())
}

fn measure_random_tree_algorithms(
    output: &mut impl Write,
    query_count: usize,
    repetitions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    for &relations in RANDOM_TREE_SIZES {
        for query_id in 0..query_count {
            let query = random_tree_query(relations, query_id as u64 + 1);
            for requested in [
                RequestedAlgorithm::Adaptive,
                RequestedAlgorithm::LinearizedDp,
                RequestedAlgorithm::GooDp,
            ] {
                measure_join_ordering(
                    output,
                    "random_tree_algorithms",
                    "random_tree",
                    relations,
                    requested,
                    query_id,
                    repetitions,
                    &query,
                )?;
            }
        }
    }

    for &relations in EXACT_SIZES {
        for query_id in 0..query_count {
            let query = random_tree_query(relations, query_id as u64 + 1);
            measure_join_ordering(
                output,
                "random_tree_algorithms",
                "random_tree",
                relations,
                RequestedAlgorithm::DpHyp,
                query_id,
                repetitions,
                &query,
            )?;
        }
    }

    // The paper reports scaling through 5,000-way joins. Limit the largest cases to three
    // deterministic trees even when the caller requests a wider small-query distribution.
    for &relations in VERY_LARGE_RANDOM_TREE_SIZES {
        for query_id in 0..query_count.min(3) {
            let query = random_tree_query(relations, query_id as u64 + 1);
            for requested in [RequestedAlgorithm::Adaptive, RequestedAlgorithm::GooDp] {
                measure_join_ordering(
                    output,
                    "random_tree_algorithms",
                    "random_tree",
                    relations,
                    requested,
                    query_id,
                    repetitions,
                    &query,
                )?;
            }
        }
    }
    Ok(())
}

fn measure_topology_sweep(
    output: &mut impl Write,
    repetitions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    for &relations in RANDOM_TREE_SIZES.iter().filter(|&&size| size <= 192) {
        for (shape, query) in [
            ("chain", tree_query(relations, |node, _| node - 1)),
            ("star", tree_query(relations, |_, _| 0)),
        ] {
            measure_join_ordering(
                output,
                "adaptive_topologies",
                shape,
                relations,
                RequestedAlgorithm::Adaptive,
                0,
                repetitions,
                &query,
            )?;
        }
    }

    for &relations in RANDOM_TREE_SIZES.iter().filter(|&&size| size <= 70) {
        let query = clique_query(relations);
        measure_join_ordering(
            output,
            "adaptive_topologies",
            "clique",
            relations,
            RequestedAlgorithm::Adaptive,
            0,
            repetitions,
            &query,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn measure_join_ordering(
    output: &mut impl Write,
    suite: &str,
    shape: &str,
    relations: usize,
    requested: RequestedAlgorithm,
    query_id: usize,
    repetitions: usize,
    template: &QueryContext,
) -> Result<(), Box<dyn std::error::Error>> {
    for repetition in 0..repetitions {
        let query = template.clone();
        let before = query.operator_count();
        let catalog = benchmark_catalog(relations)?;
        let mut optimizer = OptimizerContext::new(query, catalog);
        let mut pass =
            JoinOrdering::with_cost_model(EnumerationCost).adaptive_config(requested.config());
        let started = Instant::now();
        black_box(pass.run(&mut optimizer)?);
        let duration = started.elapsed();
        let decision = pass
            .last_decisions()
            .first()
            .expect("a multi-relation benchmark has one join-ordering decision");
        let selected = decision.algorithm.label();
        let connected_subgraphs = decision
            .connected_subgraphs
            .map(|count| count.to_string())
            .unwrap_or_default();
        let candidates = optimizer.query.operator_count() - before;
        writeln!(
            output,
            "{suite},{shape},{relations},{},{selected},{query_id},{repetition},1,{},{},{connected_subgraphs},{},{}",
            requested.label(),
            duration.as_nanos(),
            candidates,
            decision.dp_states_created,
            decision.repaired_subproblems,
        )?;
    }
    Ok(())
}

fn benchmark_catalog(
    relation_count: usize,
) -> Result<Arc<MemoryCatalog>, Box<dyn std::error::Error>> {
    let catalog = Arc::new(MemoryCatalog::new("bench", "public"));
    for relation in 0..relation_count {
        catalog.create_table(
            TableRef::bare(format!("t{relation}")),
            Arc::new(Schema::new(vec![Field::new(
                format!("c{relation}"),
                DataType::Int64,
                false,
            )])),
            None,
        )?;
    }
    Ok(catalog)
}

fn measure_relation_set_boundaries(
    output: &mut impl Write,
    repetitions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    const BUILD_OPERATIONS: usize = 500;
    for &relations in BOUNDARY_SIZES {
        let members = (0..relations)
            .filter(|relation| relation % 2 == 0)
            .collect::<Vec<_>>();
        let evens: RelationSet = (0..relations)
            .filter(|relation| relation % 2 == 0)
            .collect();
        let thirds: RelationSet = (0..relations)
            .filter(|relation| relation % 3 == 0)
            .collect();
        let all = RelationSet::all(relations);
        measure_set_operations(
            output,
            "mixed_set_ops",
            relations,
            relation_set_tier(relations),
            &evens,
            &thirds,
            &all,
            repetitions,
        )?;

        for repetition in 0..repetitions {
            // Models the old `FromIterator`: repeatedly insert by constructing a singleton and
            // allocating a new union. The optimized implementation builds one word vector.
            let started = Instant::now();
            for _ in 0..BUILD_OPERATIONS {
                let set = members
                    .iter()
                    .copied()
                    .fold(RelationSet::EMPTY, |set, relation| set.with(relation));
                black_box(set);
            }
            write_relation_set_measurement(
                output,
                "set_build",
                "incremental_with",
                relations,
                relation_set_tier(relations),
                repetition,
                BUILD_OPERATIONS,
                started.elapsed(),
            )?;

            let started = Instant::now();
            for _ in 0..BUILD_OPERATIONS {
                black_box(members.iter().copied().collect::<RelationSet>());
            }
            write_relation_set_measurement(
                output,
                "set_build",
                "from_iter",
                relations,
                relation_set_tier(relations),
                repetition,
                BUILD_OPERATIONS,
                started.elapsed(),
            )?;
        }
    }
    Ok(())
}

fn measure_sparse_relation_sets(
    output: &mut impl Write,
    repetitions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    for &universe in SPARSE_UNIVERSES {
        let left: RelationSet = [0, universe - 1].into_iter().collect();
        let right: RelationSet = [1, universe / 2].into_iter().collect();
        let all = &left | &right;
        measure_set_operations(
            output,
            "sparse_set_ops",
            universe,
            "Sparse",
            &left,
            &right,
            &all,
            repetitions,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn measure_set_operations(
    output: &mut impl Write,
    shape: &str,
    relations: usize,
    representation: &str,
    left: &RelationSet,
    right: &RelationSet,
    all: &RelationSet,
    repetitions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    const SET_OPERATIONS: usize = 20_000;

    for repetition in 0..repetitions {
        let started = Instant::now();
        let mut value = RelationSet::EMPTY;
        for _ in 0..SET_OPERATIONS {
            value = black_box(left | right);
            black_box(value.is_subset(all));
            black_box(value.is_disjoint(&RelationSet::EMPTY));
        }
        black_box(value);
        write_relation_set_measurement(
            output,
            shape,
            "borrowed_union",
            relations,
            representation,
            repetition,
            SET_OPERATIONS,
            started.elapsed(),
        )?;

        // Models the old `BitOrAssign` implementation: construct a fresh union and replace the
        // owned value on every iteration.
        let mut allocating = left.clone();
        let started = Instant::now();
        for _ in 0..SET_OPERATIONS {
            allocating = black_box(allocating.union(right));
            black_box(allocating.is_subset(all));
            black_box(allocating.is_disjoint(&RelationSet::EMPTY));
        }
        black_box(allocating);
        write_relation_set_measurement(
            output,
            shape,
            "allocating_assign",
            relations,
            representation,
            repetition,
            SET_OPERATIONS,
            started.elapsed(),
        )?;

        let mut in_place = left.clone();
        let started = Instant::now();
        for _ in 0..SET_OPERATIONS {
            in_place |= black_box(right);
            black_box(in_place.is_subset(all));
            black_box(in_place.is_disjoint(&RelationSet::EMPTY));
        }
        black_box(in_place);
        write_relation_set_measurement(
            output,
            shape,
            "in_place_assign",
            relations,
            representation,
            repetition,
            SET_OPERATIONS,
            started.elapsed(),
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_relation_set_measurement(
    output: &mut impl Write,
    shape: &str,
    variant: &str,
    relations: usize,
    representation: &str,
    repetition: usize,
    operations: usize,
    duration: std::time::Duration,
) -> std::io::Result<()> {
    writeln!(
        output,
        "relation_set,{shape},{relations},{variant},{representation},0,{repetition},{operations},{},0,,0,0",
        duration.as_nanos(),
    )
}

fn relation_set_tier(relations: usize) -> &'static str {
    match relations {
        0..=64 => "Inline64",
        65..=128 => "Inline128",
        _ => "Dense",
    }
}

fn random_tree_query(relation_count: usize, seed: u64) -> QueryContext {
    let mut state = seed;
    tree_query(relation_count, |node, _| {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((state >> 32) as usize) % node
    })
}

fn tree_query(
    relation_count: usize,
    mut parent: impl FnMut(usize, &mut QueryContext) -> usize,
) -> QueryContext {
    assert!(relation_count >= 2);
    let mut ctx = QueryContext::new();
    let columns = (0..relation_count)
        .map(|node| ColumnData::new(format!("c{node}"), DataType::Int64).add(&mut ctx))
        .collect::<Vec<_>>();
    let scans = columns
        .iter()
        .enumerate()
        .map(|(node, column)| {
            OperatorData::Scan(Scan {
                table: TableRef::bare(format!("t{node}")),
                columns: vec![*column],
            })
            .add(&mut ctx)
        })
        .collect::<Vec<_>>();

    let mut root = scans[0];
    for node in 1..relation_count {
        let parent = parent(node, &mut ctx);
        let on = equality(&mut ctx, columns[parent], columns[node]);
        root = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: root,
            inner: scans[node],
        })
        .add(&mut ctx);
    }
    ctx.set_root(root);
    ctx
}

fn clique_query(relation_count: usize) -> QueryContext {
    assert!(relation_count >= 2);
    let mut ctx = QueryContext::new();
    let columns = (0..relation_count)
        .map(|node| ColumnData::new(format!("c{node}"), DataType::Int64).add(&mut ctx))
        .collect::<Vec<_>>();
    let scans = columns
        .iter()
        .enumerate()
        .map(|(node, column)| {
            OperatorData::Scan(Scan {
                table: TableRef::bare(format!("t{node}")),
                columns: vec![*column],
            })
            .add(&mut ctx)
        })
        .collect::<Vec<_>>();

    let mut root = scans[0];
    for node in 1..relation_count {
        let predicates = (0..node)
            .map(|other| equality(&mut ctx, columns[other], columns[node]))
            .collect::<Vec<_>>();
        let on = match predicates.as_slice() {
            [predicate] => *predicate,
            _ => ExprData::Nary {
                op: NaryOp::And,
                exprs: predicates,
            }
            .add(&mut ctx),
        };
        root = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: root,
            inner: scans[node],
        })
        .add(&mut ctx);
    }
    ctx.set_root(root);
    ctx
}

fn equality(
    ctx: &mut QueryContext,
    left: optd_core::Column,
    right: optd_core::Column,
) -> optd_core::Expr {
    ExprData::Binary {
        op: BinaryOp::Eq,
        left: ExprData::ColumnRef(left).add(ctx),
        right: ExprData::ColumnRef(right).add(ctx),
    }
    .add(ctx)
}
