//! Profiling helpers for optd optimizer passes using DataFusion SQL input.

use std::sync::Arc;

use datafusion::{
    arrow::{
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::MemTable,
    prelude::SessionContext,
};
use optd::{
    JoinOrdering, OperatorRewriteAdaptor, OptimizerContext, PassManager, PassProfile,
    PredicatePushdown, QueryContext,
};
use optd::{Operator, OperatorData, optimize::join_ordering::collect_join_group_roots};

use crate::from_df::{FromDFError, from_logical_plan};

/// Error produced while building or profiling a optd optimizer input.
#[derive(Debug)]
pub enum ProfilingError {
    DataFusion(datafusion::error::DataFusionError),
    FromDataFusion(FromDFError),
    Optimize(optd::OptimizeError),
}

impl std::fmt::Display for ProfilingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DataFusion(error) => write!(f, "DataFusion error: {error}"),
            Self::FromDataFusion(error) => write!(f, "DataFusion import error: {error}"),
            Self::Optimize(error) => write!(f, "optimizer error: {error}"),
        }
    }
}

impl std::error::Error for ProfilingError {}

impl From<datafusion::error::DataFusionError> for ProfilingError {
    fn from(error: datafusion::error::DataFusionError) -> Self {
        Self::DataFusion(error)
    }
}

impl From<FromDFError> for ProfilingError {
    fn from(error: FromDFError) -> Self {
        Self::FromDataFusion(error)
    }
}

impl From<optd::OptimizeError> for ProfilingError {
    fn from(error: optd::OptimizeError) -> Self {
        Self::Optimize(error)
    }
}

pub type ProfilingResult<T> = Result<T, ProfilingError>;

/// A SQL workload item to profile.
#[derive(Debug, Clone)]
pub struct ProfileQuery {
    pub name: String,
    pub sql: String,
}

/// Creates a DataFusion session with empty synthetic tables for optimizer profiling.
pub fn synthetic_session() -> ProfilingResult<SessionContext> {
    let session = SessionContext::new();
    for idx in 1..=100 {
        register_empty_table(&session, &format!("t{idx}"), synthetic_schema())?;
    }
    Ok(session)
}

/// Query shapes with increasing predicate-pushdown pressure.
pub fn predicate_pushdown_queries() -> Vec<ProfileQuery> {
    let mut queries = vec![
        ProfileQuery {
            name: "scan_filter".to_string(),
            sql: "SELECT a, b FROM t1 WHERE a > 10 AND c < 50".to_string(),
        },
        ProfileQuery {
            name: "projection_filter".to_string(),
            sql: "SELECT a FROM (SELECT a, b, c, d FROM t1) q WHERE b = 7 AND c > 1".to_string(),
        },
        ProfileQuery {
            name: "join_filter".to_string(),
            sql: "SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.b > 5 AND t2.c < 20"
                .to_string(),
        },
        ProfileQuery {
            name: "three_join_conjuncts".to_string(),
            sql: "SELECT t1.a, t2.b, t3.c \
                  FROM t1 \
                  JOIN t2 ON t1.a = t2.a \
                  JOIN t3 ON t2.b = t3.b \
                  WHERE t1.c > 10 AND t2.d = 3 AND t3.e < 100"
                .to_string(),
        },
        ProfileQuery {
            name: "derived_join_filter".to_string(),
            sql: "SELECT q.a, t3.c \
                  FROM (SELECT t1.a, t1.b, t2.c FROM t1 JOIN t2 ON t1.a = t2.a) q \
                  JOIN t3 ON q.b = t3.b \
                  WHERE q.c > 2 AND t3.d < 9"
                .to_string(),
        },
        ProfileQuery {
            name: "five_join_conjuncts".to_string(),
            sql: "SELECT t1.a, t2.b, t3.c, t4.d, t5.e \
                  FROM t1 \
                  JOIN t2 ON t1.a = t2.a \
                  JOIN t3 ON t2.b = t3.b \
                  JOIN t4 ON t3.c = t4.c \
                  JOIN t5 ON t4.d = t5.d \
                  WHERE t1.b > 1 \
                    AND t2.c > 2 \
                    AND t3.d > 3 \
                    AND t4.e > 4 \
                    AND t5.a < 900"
                .to_string(),
        },
        ProfileQuery {
            name: "eight_join_conjuncts".to_string(),
            sql: "SELECT t1.a, t2.b, t3.c, t4.d, t5.e, t6.a, t7.b, t8.c \
                  FROM t1 \
                  JOIN t2 ON t1.a = t2.a \
                  JOIN t3 ON t2.b = t3.b \
                  JOIN t4 ON t3.c = t4.c \
                  JOIN t5 ON t4.d = t5.d \
                  JOIN t6 ON t5.e = t6.e \
                  JOIN t7 ON t6.a = t7.a \
                  JOIN t8 ON t7.b = t8.b \
                  WHERE t1.b > 1 \
                    AND t2.c > 2 \
                    AND t3.d > 3 \
                    AND t4.e > 4 \
                    AND t5.a < 900 \
                    AND t6.b < 800 \
                    AND t7.c < 700 \
                    AND t8.d < 600"
                .to_string(),
        },
        ProfileQuery {
            name: "nested_derived_eight_join".to_string(),
            sql: "SELECT q1.a, q2.c \
                  FROM ( \
                      SELECT t1.a, t2.b, t3.c, t4.d \
                      FROM t1 \
                      JOIN t2 ON t1.a = t2.a \
                      JOIN t3 ON t2.b = t3.b \
                      JOIN t4 ON t3.c = t4.c \
                  ) q1 \
                  JOIN ( \
                      SELECT t5.a, t6.b, t7.c, t8.d \
                      FROM t5 \
                      JOIN t6 ON t5.e = t6.e \
                      JOIN t7 ON t6.a = t7.a \
                      JOIN t8 ON t7.b = t8.b \
                  ) q2 ON q1.b = q2.b \
                  WHERE q1.c > 10 AND q1.d < 90 AND q2.c > 20 AND q2.d < 80"
                .to_string(),
        },
    ];
    queries.push(sixty_four_join_sixty_four_predicates_query());
    queries.push(hundred_join_hundred_predicates_query());
    queries
}

fn sixty_four_join_sixty_four_predicates_query() -> ProfileQuery {
    n_join_n_predicates_query(
        64,
        "sixty_four_join_sixty_four_predicates",
        "SELECT t1.a, t2.b, t3.c, t4.d, t5.e, t6.a, t7.b, t8.c FROM t1",
    )
}

fn hundred_join_hundred_predicates_query() -> ProfileQuery {
    n_join_n_predicates_query(
        100,
        "hundred_join_hundred_predicates",
        "SELECT t1.a FROM t1",
    )
}

fn n_join_n_predicates_query(n: usize, name: &str, select: &str) -> ProfileQuery {
    let mut sql = select.to_string();
    for idx in 2..=n {
        sql.push_str(&format!(" JOIN t{idx} ON t{}.a = t{idx}.a", idx - 1));
    }

    sql.push_str(" WHERE ");
    for idx in 1..=n {
        if idx > 1 {
            sql.push_str(" AND ");
        }
        let column = match idx % 5 {
            0 => "a",
            1 => "b",
            2 => "c",
            3 => "d",
            _ => "e",
        };
        sql.push_str(&format!("t{idx}.{column} > {idx}"));
    }

    ProfileQuery {
        name: name.to_string(),
        sql,
    }
}

/// Profiles optd PredicatePushdown + JoinOrdering for one DataFusion SQL query.
///
/// The SQL is planned by DataFusion but not optimized by DataFusion before
/// conversion, so the measured pass sees the unoptimized logical shape.
pub async fn profile_predicate_pushdown_sql(
    session: &SessionContext,
    sql: &str,
) -> ProfilingResult<Vec<PassProfile>> {
    let plan = session.state().create_logical_plan(sql).await?;

    let mut query = QueryContext::new();
    let root = from_logical_plan(&plan, &mut query)?;
    query.set_root(root);

    let mut opt = OptimizerContext::new(query);
    let mut pass_manager = PassManager::new(100);
    pass_manager.add_pass(OperatorRewriteAdaptor::new(PredicatePushdown));
    if supports_join_ordering(&opt.query) {
        pass_manager.add_pass(JoinOrdering::new());
    }
    pass_manager.run(&mut opt)?;

    Ok(pass_manager.profiles().to_vec())
}

fn supports_join_ordering(query: &QueryContext) -> bool {
    let Some(root) = query.root() else {
        return true;
    };
    collect_join_group_roots(query, root)
        .into_iter()
        .map(|group_root| join_group_terminal_count(query, group_root))
        .all(|count| count <= 64)
}

fn join_group_terminal_count(query: &QueryContext, op: Operator) -> usize {
    match query.operator(op) {
        OperatorData::Join(j) => {
            join_group_terminal_count(query, j.outer) + join_group_terminal_count(query, j.inner)
        }
        OperatorData::CrossProduct(cp) => {
            join_group_terminal_count(query, cp.outer) + join_group_terminal_count(query, cp.inner)
        }
        _ => 1,
    }
}

fn synthetic_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
        Field::new("d", DataType::Int64, true),
        Field::new("e", DataType::Int64, true),
    ]))
}

fn register_empty_table(
    session: &SessionContext,
    name: &str,
    schema: SchemaRef,
) -> ProfilingResult<()> {
    let batch = RecordBatch::new_empty(schema.clone());
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    session.register_table(name, Arc::new(table))?;
    Ok(())
}
