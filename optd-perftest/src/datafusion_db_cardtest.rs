use crate::cardtest::{Benchmark, CardtestRunnerDBHelper};
use async_trait::async_trait;
use optd_sqlplannertest::DatafusionDb;

#[async_trait]
impl CardtestRunnerDBHelper for DatafusionDb {
    fn get_name(&self) -> &str {
        "DataFusion"
    }

    async fn load_database(&self, benchmark: &Benchmark) -> anyhow::Result<()> {
        match benchmark {
            Benchmark::Test => {
                self.execute("CREATE TABLE t1 (c1 INT);", true).await?;
                self.execute("INSERT INTO t1 VALUES (0);", true).await?;
            }
        };
        Ok(())
    }

    async fn eval_true_card(&self, sql: &str) -> anyhow::Result<usize> {
        let rows = self.execute(sql, true).await?;
        let num_rows = rows.len();
        Ok(num_rows)
    }

    async fn eval_est_card(&self, _sql: &str) -> anyhow::Result<usize> {
        let rows = self.execute("EXPLAIN SELECT * FROM t1;", true).await?;
        println!("eval_est_card(): rows={:?}", rows);
        Ok(12)
    }
}
