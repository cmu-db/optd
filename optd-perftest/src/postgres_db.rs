use crate::cardtest::CardtestRunnerDBHelper;
use anyhow::Result;
use async_trait::async_trait;

pub struct PostgresDb {}

impl PostgresDb {
    pub async fn new() -> Result<Self> {
        Ok(PostgresDb {})
    }
}

#[async_trait]
impl CardtestRunnerDBHelper for PostgresDb {
    fn get_name(&self) -> &str {
        "Postgres"
    }

    async fn eval_true_card(&self, _sql: &str) -> anyhow::Result<usize> {
        Ok(10)
    }

    async fn eval_est_card(&self, _sql: &str) -> anyhow::Result<usize> {
        Ok(5)
    }
}
