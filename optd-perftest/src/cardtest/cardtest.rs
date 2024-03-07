use optd_sqlplannertest::DatafusionDb;
use anyhow::Result;

trait CardtestRunner {
    /// # Summary
    /// Get the Q-error of a query when using some database's cost model
    /// 
    /// # Contract
    /// ## Preconditions
    /// load_data() has already been called
    /// 
    /// # Details
    /// Q-error is defined in [Leis 2015](https://15721.courses.cs.cmu.edu/spring2024/papers/16-costmodels/p204-leis.pdf)
    /// One detail not specified in the paper is that Q-error is based on the ratio of true and estimated cardinality
    ///   of the entire query, not of a subtree of the query. This detail is specified in Section 7.1 of
    ///   [Yang 2020](https://arxiv.org/pdf/2006.08109.pdf)
    async fn get_qerror(&self, sql: &str) -> Result<f64> {}
}

impl CardtestRunner for DatafusionDb {
    async fn get_qerror(&self, sql: &str) -> Result<f64> {
        // think about await vs block_on
        let datafusion_db = DatafusionDb::new().await?;
        let rows = datafusion_db.execute(sql, true).await?;
        let num_rows = rows.len();
        Ok(num_rows as f64)
    }
}

struct PostgresDb {}

impl CardtestRunner for PostgresDb {}