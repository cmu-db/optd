use std::collections::HashMap;
use std::path::Path;

use crate::postgres_db::PostgresDb;
use crate::{benchmark::Benchmark, datafusion_db::DatafusionDb, tpch::TpchConfig};

use anyhow::{self};
use async_trait::async_trait;

/// This struct performs cardinality testing across one or more dbmss.
/// Another design would be for the CardtestRunnerDBMSHelper trait to expose a function
///   to evaluate the Q-error. However, I chose not to do this design for reasons
///   described in the comments of the CardtestRunnerDBMSHelper trait. This is why
///   you would use CardtestRunner even for computing the Q-error of a single dbms.
pub struct CardtestRunner {
    pub dbmss: Vec<Box<dyn CardtestRunnerDBMSHelper>>,
}

impl CardtestRunner {
    pub async fn new(dbmss: Vec<Box<dyn CardtestRunnerDBMSHelper>>) -> anyhow::Result<Self> {
        Ok(CardtestRunner { dbmss })
    }

    /// Get the Q-error of a query using the cost models of all dbmss being tested
    /// Q-error is defined in [Leis 2015](https://15721.courses.cs.cmu.edu/spring2024/papers/16-costmodels/p204-leis.pdf)
    /// One detail not specified in the paper is that Q-error is based on the ratio of true and estimated cardinality
    ///   of the entire query, not of a subtree of the query. This detail is specified in Section 7.1 of
    ///   [Yang 2020](https://arxiv.org/pdf/2006.08109.pdf)
    pub async fn eval_benchmark_qerrors_alldbs(
        &mut self,
        benchmark: &Benchmark,
    ) -> anyhow::Result<HashMap<String, Vec<f64>>> {
        let mut qerrors_alldbs = HashMap::new();

        for dbms in &mut self.dbmss {
            let estcards = dbms.eval_benchmark_estcards(benchmark).await?;
            let truecards = dbms.eval_benchmark_truecards(benchmark).await?;
            assert!(truecards.len() == estcards.len());
            let qerrors = estcards
                .into_iter()
                .zip(truecards.into_iter())
                .map(|(estcard, truecard)| CardtestRunner::calc_qerror(estcard, truecard))
                .collect();
            qerrors_alldbs.insert(String::from(dbms.get_name()), qerrors);
        }

        Ok(qerrors_alldbs)
    }

    fn calc_qerror(estcard: usize, truecard: usize) -> f64 {
        f64::max(
            estcard as f64 / truecard as f64,
            truecard as f64 / estcard as f64,
        )
    }
}

/// This trait defines helper functions to enable cardinality testing on a dbms
/// The reason a "get qerror" function is not exposed is to allow for greater
///   flexibility. If we exposed "get qerror" for each dbms, we would need to
///   get the true and estimated cardinalities for _each_ dbms. However, we
///   can now choose to only get the true cardinalities of _one_ dbms to
///   improve performance or even cache the true cardinalities. Additionally, if
///   we do want to get the true cardinalities of all dbmss, we can compare
///   them against each other to ensure they're all equal. All these options are
///   possible when exposing "get true card" and "get est card" instead of a
///   single "get qerror". If you want to compute the Q-error of a single
///   dbms, just create a CardtestRunner with a single dbms as input.
/// When exposing a "get true card" and "get est card" interface, you could
///   ostensibly do it on the granularity of a single SQL string or on the
///   granularity of an entire benchmark. I chose the latter for a simple reason:
///   different dbmss might have different SQL strings for the same conceptual
///   query (see how qgen in tpch-kit takes in dbms as an input).
/// When more performance tests are implemented, you would probably want to extract
///   get_name() into a generic "DBMS" trait.
#[async_trait]
pub trait CardtestRunnerDBMSHelper {
    // get_name() has &self so that we're able to do Box<dyn CardtestRunnerDBMSHelper>
    fn get_name(&self) -> &str;

    // The order of queries has to be the same between these two functions.
    async fn eval_benchmark_estcards(
        &mut self,
        benchmark: &Benchmark,
    ) -> anyhow::Result<Vec<usize>>;
    async fn eval_benchmark_truecards(
        &mut self,
        benchmark: &Benchmark,
    ) -> anyhow::Result<Vec<usize>>;
}

pub async fn cardtest<P: AsRef<Path> + Clone>(
    workspace_dpath: P,
    pguser: &str,
    pgpassword: &str,
    tpch_config: TpchConfig,
) -> anyhow::Result<HashMap<String, Vec<f64>>> {
    let pg_db = PostgresDb::new(workspace_dpath.clone(), pguser, pgpassword);
    let df_db = DatafusionDb::new(workspace_dpath).await?;
    let dbmss: Vec<Box<dyn CardtestRunnerDBMSHelper>> = vec![Box::new(pg_db), Box::new(df_db)];

    let tpch_benchmark = Benchmark::Tpch(tpch_config.clone());
    let mut cardtest_runner = CardtestRunner::new(dbmss).await?;
    let qerrors_alldbs = cardtest_runner
        .eval_benchmark_qerrors_alldbs(&tpch_benchmark)
        .await?;
    Ok(qerrors_alldbs)
}
