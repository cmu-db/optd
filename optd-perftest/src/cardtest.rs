use std::collections::HashMap;
use std::path::Path;

use crate::postgres_dbms::PostgresDBMS;
use crate::truecard::TruecardGetter;
use crate::{benchmark::Benchmark, datafusion_dbms::DatafusionDBMS, tpch::TpchConfig};

use anyhow::{self};
use async_trait::async_trait;

/// This struct performs cardinality testing across one or more DBMSs.
/// Another design would be for the CardtestRunnerDBMSHelper trait to expose a function
///   to evaluate the Q-error. However, I chose not to do this design for reasons
///   described in the comments of the CardtestRunnerDBMSHelper trait. This is why
///   you would use CardtestRunner even for computing the Q-error of a single DBMS.
pub struct CardtestRunner {
    pub dbmss: Vec<Box<dyn CardtestRunnerDBMSHelper>>,
    truecard_getter: Box<dyn TruecardGetter>,
}

pub struct Cardinfo {
    pub qerror: f64,
    pub estcard: usize,
    pub truecard: usize,
}

impl CardtestRunner {
    pub async fn new(
        dbmss: Vec<Box<dyn CardtestRunnerDBMSHelper>>,
        truecard_getter: Box<dyn TruecardGetter>,
    ) -> anyhow::Result<Self> {
        Ok(CardtestRunner {
            dbmss,
            truecard_getter,
        })
    }

    /// Get the Q-error of a query using the cost models of all DBMSs being tested
    /// Q-error is defined in [Leis 2015](https://15721.courses.cs.cmu.edu/spring2024/papers/16-costmodels/p204-leis.pdf)
    /// One detail not specified in the paper is that Q-error is based on the ratio of true and estimated cardinality
    ///   of the entire query, not of a subtree of the query. This detail is specified in Section 7.1 of
    ///   [Yang 2020](https://arxiv.org/pdf/2006.08109.pdf)
    pub async fn eval_benchmark_cardinfos_alldbs(
        &mut self,
        benchmark: &Benchmark,
    ) -> anyhow::Result<HashMap<String, Vec<Cardinfo>>> {
        let mut cardinfos_alldbs = HashMap::new();
        let truecards = self
            .truecard_getter
            .get_benchmark_truecards(benchmark)
            .await?;

        for dbms in &mut self.dbmss {
            let estcards = dbms.eval_benchmark_estcards(benchmark).await?;
            let cardinfos = estcards
                .into_iter()
                .zip(truecards.iter())
                .map(|(estcard, &truecard)| Cardinfo {
                    qerror: CardtestRunner::calc_qerror(estcard, truecard),
                    estcard,
                    truecard,
                })
                .collect();
            cardinfos_alldbs.insert(String::from(dbms.get_name()), cardinfos);
        }

        Ok(cardinfos_alldbs)
    }

    fn calc_qerror(estcard: usize, truecard: usize) -> f64 {
        f64::max(
            estcard as f64 / truecard as f64,
            truecard as f64 / estcard as f64,
        )
    }
}

/// This trait defines helper functions to enable cardinality testing on a DBMS
/// The reason "get true card" is not a function here is because we don't need to call
///   "get true card" for all DBMSs we are testing, since they'll all return the same
///   answer. We also cache true cardinalities instead of executing queries every time
///   since executing OLAP queries could take minutes to hours. Due to both of these
///   factors, we conceptually view getting the true cardinality as a completely separate
///   problem from getting the estimated cardinalities of each DBMS.
/// When exposing a "get est card" interface, you could do it on the granularity of
///   a single SQL string or on the granularity of an entire benchmark. I chose the
///   latter for a simple reason: different DBMSs might have different SQL strings
///   for the same conceptual query (see how qgen in tpch-kit takes in DBMS as an input).
/// When more performance tests are implemented, you would probably want to extract
///   get_name() into a generic "DBMS" trait.
#[async_trait]
pub trait CardtestRunnerDBMSHelper {
    // get_name() has &self so that we're able to do Box<dyn CardtestRunnerDBMSHelper>
    fn get_name(&self) -> &str;

    // The order of queries in the returned vector has to be the same between all databases,
    //   and it has to be the same as the order returned by TruecardGetter.
    async fn eval_benchmark_estcards(
        &mut self,
        benchmark: &Benchmark,
    ) -> anyhow::Result<Vec<usize>>;
}

pub async fn cardtest<P: AsRef<Path>>(
    workspace_dpath: P,
    use_cached_optd_stats: bool,
    pguser: &str,
    pgpassword: &str,
    tpch_config: TpchConfig,
) -> anyhow::Result<HashMap<String, Vec<Cardinfo>>> {
    let pg_dbms = Box::new(PostgresDBMS::build(&workspace_dpath, pguser, pgpassword)?);
    let truecard_getter = pg_dbms.clone();
    let df_dbms = Box::new(DatafusionDBMS::new(&workspace_dpath, use_cached_optd_stats).await?);
    let dbmss: Vec<Box<dyn CardtestRunnerDBMSHelper>> = vec![pg_dbms, df_dbms];

    let tpch_benchmark = Benchmark::Tpch(tpch_config.clone());
    let mut cardtest_runner = CardtestRunner::new(dbmss, truecard_getter).await?;
    let cardinfos_alldbs = cardtest_runner
        .eval_benchmark_cardinfos_alldbs(&tpch_benchmark)
        .await?;
    Ok(cardinfos_alldbs)
}
