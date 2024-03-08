use crate::cardtest::CardtestRunnerDBHelper;
use async_trait::async_trait;
use optd_sqlplannertest::DatafusionDb;

#[async_trait]
impl CardtestRunnerDBHelper for DatafusionDb {
    fn get_name(&self) -> &str {
        "DataFusion"
    }

    async fn eval_true_card(&self, _sql: &str) -> anyhow::Result<usize> {
        Ok(10)
    }

    async fn eval_est_card(&self, _sql: &str) -> anyhow::Result<usize> {
        Ok(12)
    }
}
