use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_schema::DataType;
use optd_core::{
    AdaptiveJoinOrderingConfig, AnalysisContext, BinaryOp, Column, ColumnData, CostModel, Expr,
    ExprData, Join, JoinOrdering, JoinType, MemoryCatalog, NaryOp, Operator, OperatorData,
    OptimizeResult, OptimizerContext, QueryContext, QueryPass, Scan, TableRef,
};

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
    let iterations = std::env::args()
        .nth(1)
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(5);

    println!("case\titerations\talgorithm\ttotal_ms\tns_per_iteration");
    run_case("exact_clique_10", iterations, clique_query(10))?;
    run_case("linearized_clique_18", iterations, clique_query(18))?;
    run_case("exact_dynamic_chain_65", iterations, chain_query(65))?;
    run_case("goo_dynamic_chain_128", iterations, chain_query(128))?;
    run_case_with_config(
        "forced_exact_dynamic_chain_128",
        iterations,
        chain_query(128),
        AdaptiveJoinOrderingConfig {
            exact_relation_threshold: usize::MAX,
            ..AdaptiveJoinOrderingConfig::default()
        },
    )?;
    Ok(())
}

fn run_case(
    name: &str,
    iterations: usize,
    template: QueryContext,
) -> Result<(), Box<dyn std::error::Error>> {
    run_case_with_config(
        name,
        iterations,
        template,
        AdaptiveJoinOrderingConfig::default(),
    )
}

fn run_case_with_config(
    name: &str,
    iterations: usize,
    template: QueryContext,
    config: AdaptiveJoinOrderingConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut elapsed = Duration::ZERO;
    let mut algorithm = String::new();
    for _ in 0..iterations {
        let query = template.clone();
        let catalog = Arc::new(MemoryCatalog::new("bench", "public"));
        let mut optimizer = OptimizerContext::new(query, catalog);
        let mut pass = JoinOrdering::with_cost_model(EnumerationCost).adaptive_config(config);
        let started = Instant::now();
        black_box(pass.run(&mut optimizer)?);
        elapsed += started.elapsed();
        algorithm = format!("{:?}", pass.last_decisions()[0].algorithm);
        black_box(optimizer.query.root());
    }

    println!(
        "{name}\t{iterations}\t{algorithm}\t{:.3}\t{}",
        elapsed.as_secs_f64() * 1_000.0,
        elapsed.as_nanos() / iterations as u128,
    );
    Ok(())
}

fn chain_query(relation_count: usize) -> QueryContext {
    join_query(relation_count, false)
}

fn clique_query(relation_count: usize) -> QueryContext {
    join_query(relation_count, true)
}

fn join_query(relation_count: usize, clique: bool) -> QueryContext {
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

    let root = (1..relation_count).fold(scans[0], |outer, node| {
        let predecessors: Box<dyn Iterator<Item = usize>> = if clique {
            Box::new(0..node)
        } else {
            Box::new(std::iter::once(node - 1))
        };
        let predicates = predecessors
            .map(|predecessor| equality(&mut ctx, columns[predecessor], columns[node]))
            .collect::<Vec<_>>();
        let on = conjunction(&mut ctx, predicates);
        OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer,
            inner: scans[node],
        })
        .add(&mut ctx)
    });
    ctx.set_root(root);
    ctx
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
    if predicates.len() == 1 {
        predicates.remove(0)
    } else {
        ExprData::Nary {
            op: NaryOp::And,
            exprs: predicates,
        }
        .add(ctx)
    }
}
