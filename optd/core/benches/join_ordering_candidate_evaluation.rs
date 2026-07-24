//! Compares deferred and materializing join-candidate evaluation on wide joins.
//!
//! The benchmark is dependency-free and emits TSV to stdout. Timings cover only one
//! [`JoinOrdering::run`] call; fixture construction, query cloning, catalog registration, and pass
//! construction are excluded.
//!
//! Run `cargo bench -p optd-core --bench join_ordering_candidate_evaluation -- [repetitions]`.

use std::collections::BTreeMap;
use std::error::Error;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use arrow_schema::{DataType, Field, Schema};
use optd_core::{
    AdaptiveJoinOrderingConfig, BinaryOp, Catalog, Column, ColumnData, ColumnStatistics,
    DefaultCostModel, Expr, ExprData, Join, JoinOrderAlgorithm, JoinOrdering, JoinType,
    MemoryCatalog, NaryOp, OperatorData, OptimizerContext, QueryContext, QueryPass, Scan, TableRef,
    TableStatistics,
};

const DEFAULT_REPETITIONS: usize = 6;
const CASES: &[CaseSpec] = &[
    CaseSpec::new(Shape::Chain, 12, 16),
    CaseSpec::new(Shape::Chain, 12, 64),
    // Exercises a large transitive equality class as well as the dynamic relation-set path.
    CaseSpec::new(Shape::Chain, 65, 1),
    CaseSpec::new(Shape::Clique, 9, 16),
    CaseSpec::new(Shape::Clique, 9, 64),
];

#[derive(Clone, Copy)]
struct CaseSpec {
    shape: Shape,
    relations: usize,
    width: usize,
}

impl CaseSpec {
    const fn new(shape: Shape, relations: usize, width: usize) -> Self {
        Self {
            shape,
            relations,
            width,
        }
    }
}

#[derive(Clone, Copy)]
enum Shape {
    Chain,
    Clique,
}

impl Shape {
    const fn label(self) -> &'static str {
        match self {
            Self::Chain => "chain",
            Self::Clique => "clique",
        }
    }
}

#[derive(Clone, Copy)]
enum Evaluator {
    Deferred,
    Materializing,
}

impl Evaluator {
    const fn label(self) -> &'static str {
        match self {
            Self::Deferred => "deferred",
            Self::Materializing => "materializing",
        }
    }
}

struct Fixture {
    query: QueryContext,
    catalog: Arc<MemoryCatalog>,
}

struct Measurement {
    evaluator: Evaluator,
    algorithm: JoinOrderAlgorithm,
    duration_ns: u128,
    appended_operators: usize,
}

fn main() -> Result<(), Box<dyn Error>> {
    let repetitions = parse_repetitions()?;
    println!(
        "shape\trelations\twidth\tevaluator\trepetition\tselected_algorithm\tduration_ns\tappended_operator_count"
    );

    for &case in CASES {
        let fixture = build_fixture(case)?;
        // Pay one-time code/data warming symmetrically and outside every reported measurement.
        for evaluator in [Evaluator::Deferred, Evaluator::Materializing] {
            black_box(measure(&fixture, evaluator)?);
        }
        for repetition in 0..repetitions {
            let evaluator_order = if repetition % 2 == 0 {
                [Evaluator::Deferred, Evaluator::Materializing]
            } else {
                [Evaluator::Materializing, Evaluator::Deferred]
            };
            let measurements = evaluator_order
                .map(|evaluator| measure(&fixture, evaluator))
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;

            let expected_algorithm = measurements[0].algorithm;
            if measurements
                .iter()
                .any(|measurement| measurement.algorithm != expected_algorithm)
            {
                return Err(format!(
                    "candidate evaluators selected different algorithms for {}-{}x{}",
                    case.shape.label(),
                    case.relations,
                    case.width
                )
                .into());
            }

            for measurement in measurements {
                println!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    case.shape.label(),
                    case.relations,
                    case.width,
                    measurement.evaluator.label(),
                    repetition,
                    measurement.algorithm.label(),
                    measurement.duration_ns,
                    measurement.appended_operators,
                );
            }
        }
    }

    Ok(())
}

fn parse_repetitions() -> Result<usize, Box<dyn Error>> {
    let repetitions = std::env::args()
        .nth(1)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(DEFAULT_REPETITIONS);
    if repetitions == 0 {
        return Err("repetitions must be greater than zero".into());
    }
    Ok(repetitions)
}

fn measure(fixture: &Fixture, evaluator: Evaluator) -> Result<Measurement, Box<dyn Error>> {
    let query = fixture.query.clone();
    let operators_before = query.operator_count();
    let mut optimizer = OptimizerContext::new(query, fixture.catalog.clone());
    let config = AdaptiveJoinOrderingConfig::default();
    let mut pass: JoinOrdering<DefaultCostModel> = match evaluator {
        Evaluator::Deferred => JoinOrdering::with_config(config),
        Evaluator::Materializing => {
            JoinOrdering::with_cost_model(DefaultCostModel).adaptive_config(config)
        }
    };

    let started = Instant::now();
    black_box(pass.run(&mut optimizer)?);
    let duration_ns = started.elapsed().as_nanos();
    let algorithm = pass
        .last_decisions()
        .first()
        .expect("a multi-relation fixture must produce one join-ordering decision")
        .algorithm;
    let appended_operators = optimizer
        .query
        .operator_count()
        .checked_sub(operators_before)
        .expect("the append-only query arena cannot shrink");
    black_box(optimizer.query.root());

    Ok(Measurement {
        evaluator,
        algorithm,
        duration_ns,
        appended_operators,
    })
}

fn build_fixture(case: CaseSpec) -> Result<Fixture, Box<dyn Error>> {
    assert!(case.relations >= 2, "a join benchmark needs two relations");
    assert!(case.width > 0, "a wide table needs at least one column");

    let mut query = QueryContext::new();
    let catalog = Arc::new(MemoryCatalog::new("bench", "public"));
    let mut relation_columns = Vec::with_capacity(case.relations);
    let mut scans = Vec::with_capacity(case.relations);

    for relation in 0..case.relations {
        let table = TableRef::bare(format!("t{relation}"));
        let rows = 10_000 + relation * 1_000;
        let columns = (0..case.width)
            .map(|column| ColumnData::new(column_name(column), DataType::Int64).add(&mut query))
            .collect::<Vec<_>>();
        let fields = (0..case.width)
            .map(|column| Field::new(column_name(column), DataType::Int64, false))
            .collect::<Vec<_>>();
        catalog.create_table(table.clone(), Arc::new(Schema::new(fields)), None)?;
        catalog.set_table_statistics(table.clone(), table_statistics(rows, case.width))?;
        scans.push(
            OperatorData::Scan(Scan {
                table,
                columns: columns.clone(),
            })
            .add(&mut query),
        );
        relation_columns.push(columns);
    }

    let root = (1..case.relations).fold(scans[0], |outer, relation| {
        let predicates = predecessors(case.shape, relation)
            .map(|predecessor| {
                let key = predecessor % case.width;
                equality(
                    &mut query,
                    relation_columns[predecessor][key],
                    relation_columns[relation][key],
                )
            })
            .collect::<Vec<_>>();
        let on = conjunction(&mut query, predicates);
        OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer,
            inner: scans[relation],
        })
        .add(&mut query)
    });
    query.set_root(root);

    Ok(Fixture { query, catalog })
}

fn predecessors(shape: Shape, relation: usize) -> Box<dyn Iterator<Item = usize>> {
    match shape {
        Shape::Chain => Box::new(std::iter::once(relation - 1)),
        Shape::Clique => Box::new(0..relation),
    }
}

fn column_name(column: usize) -> String {
    format!("c{column}")
}

fn table_statistics(rows: usize, width: usize) -> TableStatistics {
    let column_statistics = (0..width)
        .map(|column| {
            let distinct = (rows / (column % 8 + 1)).max(1);
            (
                column_name(column),
                ColumnStatistics {
                    lower_bound: None,
                    upper_bound: None,
                    frequency: Some(rows),
                    distinct: Some(distinct),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    TableStatistics {
        row_count: Some(rows),
        size_bytes: Some(rows * width * std::mem::size_of::<i64>()),
        column_statistics,
    }
}

fn equality(ctx: &mut QueryContext, left: Column, right: Column) -> Expr {
    let left = ExprData::ColumnRef(left).add(ctx);
    let right = ExprData::ColumnRef(right).add(ctx);
    ExprData::Binary {
        op: BinaryOp::Eq,
        left,
        right,
    }
    .add(ctx)
}

fn conjunction(ctx: &mut QueryContext, mut predicates: Vec<Expr>) -> Expr {
    match predicates.len() {
        0 => unreachable!("every benchmark join has a predecessor"),
        1 => predicates.remove(0),
        _ => ExprData::Nary {
            op: NaryOp::And,
            exprs: predicates,
        }
        .add(ctx),
    }
}
