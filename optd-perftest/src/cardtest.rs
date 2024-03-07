use optd_sqlplannertest::DatafusionDb;
use anyhow::Result;

/// This struct performs cardinality testing across one or more databases.
/// Another design would be for the CardtestRunnerHelper trait to expose a function
///   to evaluate the Q-error. However, I chose not to do this design for reasons
///   described in the comments of the CardtestRunnerHelper trait. This is why
///   you would use CardtestRunner even for computing the Q-error of a single database.
pub struct CardtestRunner {}

impl CardtestRunner {
    /// Get the Q-error of a query using the cost models of all databases being tested
    /// Q-error is defined in [Leis 2015](https://15721.courses.cs.cmu.edu/spring2024/papers/16-costmodels/p204-leis.pdf)
    /// One detail not specified in the paper is that Q-error is based on the ratio of true and estimated cardinality
    ///   of the entire query, not of a subtree of the query. This detail is specified in Section 7.1 of
    ///   [Yang 2020](https://arxiv.org/pdf/2006.08109.pdf)
    async fn eval_qerrors(&self, sql: &str) -> Result<Vec<f64>> {
        todo!()
    }
}

/// This trait defines helper functions to enable cardinality testing on a database
/// The reason a "get_qerror()" function is not exposed is because of the potential
///   for inconsistency. Computing Q-error requires knowing the true cardinality of
///   the SQL query, which theoretically should stay the same when the query is
///   executed across different databases but may not be the same in practice. It's
///   good to assert that the true cardinalities across different databases is
///   indeed the same when doing cardinality testing.
/// If you want to compute the Q-error of a single database, just create a
///   CardtestRunner with a single database as input.
trait CardtestRunnerHelper {
    async fn eval_true_card(&self, sql: &str) -> Result<usize>;
    async fn eval_est_card(&self, sql: &str) -> Result<usize>;
}

impl CardtestRunnerHelper for DatafusionDb {
    async fn eval_true_card(&self, sql: &str) -> Result<usize> {
        // think about await vs block_on
        let datafusion_db = DatafusionDb::new().await?;
        let rows = datafusion_db.execute(sql, true).await?;
        let num_rows = rows.len();
        Ok(num_rows)
    }

    async fn eval_est_card(&self, _sql: &str) -> Result<usize> {
        Ok(0)
    }
}

struct PostgresDb {}

impl PostgresDb {
    async fn new() -> Result<Self> {
        Ok(PostgresDb{})
    }
}

impl CardtestRunnerHelper for PostgresDb {
    async fn eval_true_card(&self, _sql: &str) -> Result<usize> {
        Ok(0)
    }

    async fn eval_est_card(&self, _sql: &str) -> Result<usize> {
        Ok(0)
    }
}