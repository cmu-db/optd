use diesel::prelude::*;
use diesel_migrations::{embed_migrations, EmbeddedMigrations};

pub mod models;
pub mod schema;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

/// The storage manager implements CRUD operation for persisting the optimizer state.
pub struct StorageManager {
    /// Connection to the SQLite database.
    pub conn: SqliteConnection,
}

impl StorageManager {
    /// Execute the diesel migrations that are built into this binary
    pub fn migration_run(&mut self) -> anyhow::Result<()> {
        use diesel_migrations::{HarnessWithOutput, MigrationHarness};

        HarnessWithOutput::write_to_stdout(&mut self.conn)
            .run_pending_migrations(MIGRATIONS)
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }
    /// Create a new `StorageManager` instance.
    pub fn new(database_url: &str) -> anyhow::Result<Self> {
        let connection = SqliteConnection::establish(&database_url)?;
        Ok(Self { conn: connection })
    }

    pub fn new_in_memory() -> anyhow::Result<Self> {
        let connection = SqliteConnection::establish(":memory:")?;
        Ok(Self { conn: connection })
    }
}
