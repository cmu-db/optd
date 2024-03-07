use anyhow::Result;

pub struct PostgresDb {}

impl PostgresDb {
    pub async fn new() -> Result<Self> {
        Ok(PostgresDb{})
    }
}