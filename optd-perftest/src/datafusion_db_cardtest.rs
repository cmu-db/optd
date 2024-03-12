use crate::{benchmark::Benchmark, cardtest::CardtestRunnerDBHelper};
use async_trait::async_trait;
use optd_sqlplannertest::DatafusionDb;

#[async_trait]
impl CardtestRunnerDBHelper for DatafusionDb {
    fn get_name(&self) -> &str {
        "DataFusion"
    }

    async fn eval_benchmark_truecards(&self, _benchmark: &Benchmark) -> anyhow::Result<Vec<usize>> {
        Ok(vec![])
    }

    async fn eval_benchmark_estcards(&self, _benchmark: &Benchmark) -> anyhow::Result<Vec<usize>> {
        Ok(vec![])
    }
}

// helper functions for ```impl CardtestRunnerDBHelper for DatafusionDb```
// they can't be put in an impl because DatafusionDb is a foreign struct
async fn _eval_query_truecard(db: &DatafusionDb, sql: &str) -> anyhow::Result<usize> {
    let rows = db.execute(sql, true).await?;
    let num_rows = rows.len();
    Ok(num_rows)
}

async fn _eval_query_estcard(db: &DatafusionDb, _sql: &str) -> anyhow::Result<usize> {
    let rows = db.execute("EXPLAIN SELECT * FROM t1;", true).await?;
    println!("eval_est_card(): rows={:?}", rows);
    Ok(12)
}
