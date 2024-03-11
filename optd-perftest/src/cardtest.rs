use anyhow::{self};
use async_trait::async_trait;

use crate::benchmark::Benchmark;

/// This struct performs cardinality testing across one or more databases.
/// Another design would be for the CardtestRunnerDBHelper trait to expose a function
///   to evaluate the Q-error. However, I chose not to do this design for reasons
///   described in the comments of the CardtestRunnerDBHelper trait. This is why
///   you would use CardtestRunner even for computing the Q-error of a single database.
pub struct CardtestRunner {
    pub databases: Vec<Box<dyn CardtestRunnerDBHelper>>,
}

impl CardtestRunner {
    pub async fn new(databases: Vec<Box<dyn CardtestRunnerDBHelper>>) -> anyhow::Result<Self> {
        Ok(CardtestRunner { databases })
    }

    pub async fn load_databases(&self, benchmark: Benchmark) -> anyhow::Result<()> {
        for database in &self.databases {
            database.load_database(&benchmark).await?;
        }
        Ok(())
    }

    /// Get the Q-error of a query using the cost models of all databases being tested
    /// Q-error is defined in [Leis 2015](https://15721.courses.cs.cmu.edu/spring2024/papers/16-costmodels/p204-leis.pdf)
    /// One detail not specified in the paper is that Q-error is based on the ratio of true and estimated cardinality
    ///   of the entire query, not of a subtree of the query. This detail is specified in Section 7.1 of
    ///   [Yang 2020](https://arxiv.org/pdf/2006.08109.pdf)
    pub async fn eval_qerrors(&self, sql: &str) -> anyhow::Result<Vec<f64>> {
        let mut qerrors = vec![];
        let mut first_true_card = None;

        for database in &self.databases {
            let true_card = database.eval_true_card(sql).await?;
            match first_true_card {
                None => first_true_card = Some(true_card),
                Some(first_true_card) => {
                    if true_card != first_true_card {
                        // you could return an error here but that involves creating
                        // a custom error type which seems overkill for now
                        // this is a testing tool anyways and not production software
                        panic!("The true cardinality of {} ({}), is != the true cardinality of {} ({})", database.as_ref().get_name(), true_card, self.databases.first().unwrap().as_ref().get_name(), first_true_card)
                    }
                }
            };

            let est_card = database.eval_est_card(sql).await?;
            let qerror = Self::calc_qerror(true_card, est_card);
            qerrors.push(qerror);
        }

        Ok(qerrors)
    }

    fn calc_qerror(true_card: usize, est_card: usize) -> f64 {
        f64::max(
            true_card as f64 / est_card as f64,
            est_card as f64 / true_card as f64,
        )
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
#[async_trait]
pub trait CardtestRunnerDBHelper {
    // get_name() has &self so that we're able to do Box<dyn CardtestRunnerDBHelper>
    fn get_name(&self) -> &str;
    async fn load_database(&self, benchmark: &Benchmark) -> anyhow::Result<()>;
    async fn eval_true_card(&self, sql: &str) -> anyhow::Result<usize>;
    async fn eval_est_card(&self, sql: &str) -> anyhow::Result<usize>;
}
